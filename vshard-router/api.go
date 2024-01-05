package vshard_router

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"

	"github.com/tarantool/go-tarantool/v2"
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
	BucketID uint64 `msgpack:"bucket_id" mapstructure:"bucket_id"`
	Reason   string `msgpack:"reason"`
	Code     int    `msgpack:"code"`
	Type     string `msgpack:"type"`
	Message  string `msgpack:"message"`
	Name     string `msgpack:"name"`
}

func (s StorageCallVShardError) Error() string {
	return fmt.Sprintf("vshard.storage.call bucket error bucket_id: %d, reason: %s, name: %s", s.BucketID, s.Reason, s.Name)
}

type StorageResultTypedFunc = func(result interface{}) error

// RouterCallImpl Perform shard operation function will restart operation
// after wrong bucket response until timeout is reached
// todo: now we ignore mode/preferReplica/balance and use only master node for rs
func (r *Router) RouterCallImpl(ctx context.Context,
	bucketID uint64,
	mode CallMode,
	fnc string,
	args interface{}) (interface{}, StorageResultTypedFunc, error) {
	if bucketID > r.cfg.TotalBucketCount {
		return nil, nil, fmt.Errorf("bucket is unreachable: bucket id is out of range")
	}

	timeout := time.Second * 10 // todo: add timeout to opts
	timeStart := time.Now()

	req := tarantool.NewCallRequest("vshard.storage.call")
	req = req.Context(ctx)
	req = req.Args([]interface{}{
		bucketID, //todo: remove after resharding test
		mode.String(),
		fnc,
		args,
	})

	var err error

	for {

		if time.Since(timeStart) > timeout {
			return nil, nil, err
		}

		rs, err := r.BucketResolve(ctx, bucketID)
		if err != nil {
			continue
		}

		future := rs.master.conn.Do(req)

		resp, err := future.Get()
		if err != nil {
			continue
		}

		if len(resp.Data) != 2 {
			err = fmt.Errorf("invalid length of response data: must be = 2, current: %d", len(resp.Data))
			continue
		}

		if resp.Data[0] == nil {
			vshardErr := &StorageCallVShardError{}

			err = mapstructure.Decode(resp.Data[1], vshardErr)
			if err != nil {
				r.Log().Error(fmt.Sprintf("cant decode vhsard err by trarantool with err: %s", err))
				continue
			}

			err = vshardErr
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

		return resp.Data[1], func(result interface{}) error {
			var stub interface{}

			return future.GetTyped(&[]interface{}{&stub, result})
		}, nil
	}
}

// Wrappers for router_call with preset mode.

func (r *Router) RouterCallRO(ctx context.Context, bucketID uint64, fnc string, args interface{}) (interface{}, StorageResultTypedFunc, error) {
	return r.RouterCallImpl(ctx, bucketID, "read", fnc, args)
}

func (r *Router) RouterCallBRO(ctx context.Context, bucketID uint64, fnc string, args interface{}) (interface{}, StorageResultTypedFunc, error) {
	return r.RouterCallImpl(ctx, bucketID, "read", fnc, args)
}

func (r *Router) RouterCallRW(ctx context.Context, bucketID uint64, fnc string, args interface{}) (interface{}, StorageResultTypedFunc, error) {
	return r.RouterCallImpl(ctx, bucketID, "write", fnc, args)
}

func (r *Router) RouterCallRE(ctx context.Context, bucketID uint64, fnc string, args interface{}) (interface{}, StorageResultTypedFunc, error) {
	return r.RouterCallImpl(ctx, bucketID, "read", fnc, args)
}

func (r *Router) RouterCallBRE(ctx context.Context, bucketID uint64, fnc string, args interface{}) (interface{}, StorageResultTypedFunc, error) {
	return r.RouterCallImpl(ctx, bucketID, "read", fnc, args)
}

type CallOpts struct {
	Balance       bool // todo:  now is useless
	Mode          CallMode
	PreferReplica bool // todo: now is useless
}

// RouterCall is analog for router_call
func (r *Router) RouterCall(ctx context.Context, bucketID uint64, opts CallOpts, fnc string, args interface{}) (interface{}, StorageResultTypedFunc, error) {
	return r.RouterCallImpl(ctx, bucketID, opts.Mode, fnc, args)
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
