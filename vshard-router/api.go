package vshard_router

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

// --------------------------------------------------------------------------------
// -- API
// --------------------------------------------------------------------------------

type CallMode string

const (
	ReadMode  CallMode = "read"
	WriteMode CallMode = "write"
)

func (c CallMode) String() string {
	return string(c)
}

type StorageCallAssertError struct {
	Code     int         `msgpack:"code"`
	BaseType string      `msgpack:"base_type"`
	Type     string      `msgpack:"type"`
	Message  string      `msgpack:"message"`
	Trace    interface{} `msgpack:"trace"`
}

func (s StorageCallAssertError) Error() string {
	return fmt.Sprintf("vshard.storage.call assert error code: %d, type:%s, message: %s", s.Code, s.Type, s.Message)
}

type StorageCallVShardError struct {
	BucketID       uint64  `msgpack:"bucket_id" mapstructure:"bucket_id"`
	Reason         string  `msgpack:"reason"`
	Code           int     `msgpack:"code"`
	Type           string  `msgpack:"type"`
	Message        string  `msgpack:"message"`
	Name           string  `msgpack:"name"`
	MasterUUID     *string `msgpack:"master_uuid" mapstructure:"master_uuid"`         // mapstructure cant decode to source uuid type
	ReplicasetUUID *string `msgpack:"replicaset_uuid" mapstructure:"replicaset_uuid"` // mapstructure cant decode to source uuid type
}

func (s StorageCallVShardError) Error() string {
	return fmt.Sprintf("vshard.storage.call bucket error bucket_id: %d, reason: %s, name: %s", s.BucketID, s.Reason, s.Name)
}

type StorageResultTypedFunc = func(result interface{}) error

type CallOpts struct {
	Balance       bool // todo:  now is useless
	Mode          CallMode
	PreferReplica bool // todo: now is useless
	Timeout       time.Duration
}

const CallTimeoutMin = time.Second / 2

// RouterCallImpl Perform shard operation function will restart operation
// after wrong bucket response until timeout is reached
// todo: now we ignore mode/preferReplica/balance and use only master node for rs
func (r *Router) RouterCallImpl(ctx context.Context,
	bucketID uint64,
	opts CallOpts,
	fnc string,
	args interface{}) (interface{}, StorageResultTypedFunc, error) {
	if bucketID > r.cfg.TotalBucketCount {
		return nil, nil, fmt.Errorf("bucket is unreachable: bucket id is out of range")
	}

	if opts.Timeout == 0 {
		opts.Timeout = CallTimeoutMin
	}

	timeout := opts.Timeout
	timeStart := time.Now()

	req := tarantool.NewCallRequest("vshard.storage.call")
	req = req.Context(ctx)
	req = req.Args([]interface{}{
		bucketID,
		opts.Mode.String(),
		fnc,
		args,
	})

	var err error

	for {

		if since := time.Since(timeStart); since > timeout {
			r.Metrics().RequestDuration(since, false)

			return nil, nil, err
		}

		rs, err := r.BucketResolve(ctx, bucketID)
		if err != nil {
			r.Metrics().RetryOnCall("bucket_resolve_error")
			continue
		}

		future := rs.conn.Do(req, pool.RW) // todo: make this param changeable

		resp, err := future.Get()
		if err != nil {
			r.Metrics().RetryOnCall("future_get_error")

			continue
		}

		if len(resp.Data) != 2 {
			r.Metrics().RetryOnCall("resp_data_error")

			err = fmt.Errorf("invalid length of response data: must be = 2, current: %d", len(resp.Data))
			continue
		}

		if resp.Data[0] == nil {
			vshardErr := &StorageCallVShardError{}

			err = mapstructure.Decode(resp.Data[1], vshardErr)
			if err != nil {
				r.Metrics().RetryOnCall("internal_error")

				r.Log().Error(ctx, fmt.Sprintf("cant decode vhsard err by trarantool with err: %s", err))
				continue
			}

			err = vshardErr

			if vshardErr.Name == "WRONG_BUCKET" ||
				vshardErr.Name == "BUCKET_IS_LOCKED" ||
				vshardErr.Name == "TRANSFER_IS_IN_PROGRESS" {
				r.BucketReset(bucketID)
				r.Metrics().RetryOnCall("bucket_migrate")

				continue
			}

			if r.cfg.IsMasterAuto { // if master auto we need to use master in error response
				sourceErr, errExists := nameToError[vshardErr.Name]
				if !errExists { // some code error - need fix
					r.Log().Error(ctx, fmt.Sprintf("cant get error by name %s, need library fix", vshardErr.Name))

					continue
				}

				if sourceErr.Name == Errors[2].Name { // if NON-MASTER
					var updateMasterErr error

					r.Log().Info(ctx, "got error invalid master -> something wrong in pool")

					_, updateMasterErr = uuid.Parse(*vshardErr.MasterUUID)
					if updateMasterErr != nil {
						r.Log().Error(ctx, fmt.Sprintf("cant parse new master uuid with err: %s", updateMasterErr))
					}

					continue
				}
			}

			continue
		}

		isVShardRespOk := false
		err = future.GetTyped(&[]interface{}{&isVShardRespOk})
		if err != nil {
			continue
		}

		if !isVShardRespOk { // error
			errorResp := &StorageCallAssertError{}

			err = future.GetTyped(&[]interface{}{&isVShardRespOk, errorResp})
			if err != nil {
				err = fmt.Errorf("cant get typed vshard err with err: %s", err)
			}

			err = errorResp
		}

		r.Metrics().RequestDuration(time.Since(timeStart), true)

		return resp.Data[1], func(result interface{}) error {
			var stub interface{}

			return future.GetTyped(&[]interface{}{&stub, result})
		}, nil
	}
}

// Wrappers for router_call with preset mode.

func (r *Router) RouterCallRO(ctx context.Context, bucketID uint64, fnc string, args interface{}) (interface{}, StorageResultTypedFunc, error) {
	return r.RouterCallImpl(ctx, bucketID, CallOpts{Mode: ReadMode}, fnc, args)
}

func (r *Router) RouterCallBRO(ctx context.Context, bucketID uint64, fnc string, args interface{}) (interface{}, StorageResultTypedFunc, error) {
	return r.RouterCallImpl(ctx, bucketID, CallOpts{Mode: ReadMode}, fnc, args)
}

func (r *Router) RouterCallRW(ctx context.Context, bucketID uint64, fnc string, args interface{}) (interface{}, StorageResultTypedFunc, error) {
	return r.RouterCallImpl(ctx, bucketID, CallOpts{Mode: WriteMode}, fnc, args)
}

func (r *Router) RouterCallRE(ctx context.Context, bucketID uint64, fnc string, args interface{}) (interface{}, StorageResultTypedFunc, error) {
	return r.RouterCallImpl(ctx, bucketID, CallOpts{Mode: ReadMode}, fnc, args)
}

func (r *Router) RouterCallBRE(ctx context.Context, bucketID uint64, fnc string, args interface{}) (interface{}, StorageResultTypedFunc, error) {
	return r.RouterCallImpl(ctx, bucketID, CallOpts{Mode: ReadMode}, fnc, args)
}

// RouterCall is analog for router_call
func (r *Router) RouterCall(ctx context.Context, bucketID uint64, opts CallOpts, fnc string, args interface{}) (interface{}, StorageResultTypedFunc, error) {
	return r.RouterCallImpl(ctx, bucketID, opts, fnc, args)
}

// todo: router_map_callrw

// RouterRoute get replicaset object by bucket identifier.
// alias to BucketResolve
func (r *Router) RouterRoute(ctx context.Context, bucketID uint64) (*Replicaset, error) {
	return r.BucketResolve(ctx, bucketID)
}

// RouterRouteAll return map of all replicasets.
func (r *Router) RouterRouteAll() map[uuid.UUID]*Replicaset {
	return r.idToReplicaset
}