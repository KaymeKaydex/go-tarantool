package vshard

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/tarantool/go-tarantool/v2"
)

// --------------------------------------------------------------------------------
// -- API
// --------------------------------------------------------------------------------

type CallMode string

const (
	ReadMode  = "read"
	WriteMode = "write"
)

func (c CallMode) String() string {
	return string(c)
}

type StorageCallError struct {
	Code     int         `msgpack:"code"`
	BaseType string      `msgpack:"base_type"`
	Type     string      `msgpack:"type"`
	Message  string      `msgpack:"message"`
	Trace    interface{} `msgpack:"trace"`
}

func (s StorageCallError) Error() string {
	return fmt.Sprintf("vshard.storage.call error code: %d, type:%s, message: %s", s.Code, s.Type, s.Message)
}

type StorageResultTypedFunc = func(result interface{}) error

// RouterCallImpl Perform shard operation function will restart operation
// after wrong bucket response until timeout is reached
func (r *Router) RouterCallImpl(ctx context.Context,
	bucketID uint64,
	mode CallMode,
	fnc string,
	args interface{}) (interface{}, StorageResultTypedFunc, error) {

	if bucketID > r.cfg.TotalBucketCount {
		return nil, nil, fmt.Errorf("bucket is unreachable: bucket id is out of range")
	}

	// todo: now we ignore mode/preferReplica/balance and use only master node for rs
	rs, err := r.BucketResolve(ctx, bucketID)
	if err != nil {
		return nil, nil, err
	}

	req := tarantool.NewCallRequest("vshard.storage.call")
	req = req.Context(ctx)
	req = req.Args([]interface{}{
		bucketID,
		mode.String(),
		fnc,
		args,
	})

	future := rs.master.conn.Do(req)

	resp, err := future.Get()
	if err != nil {
		return nil, nil, err
	}

	isVShardRespOk := false

	// todo: check buckets error
	/*
			---
		- null
		- bucket_id: 11
		  reason: Not found
		  code: 1
		  type: ShardingError
		  message: 'Cannot perform action with bucket 11, reason: Not found'
		  name: WRONG_BUCKET

	*/
	err = future.GetTyped(&[]interface{}{&isVShardRespOk})
	if err != nil {
		return nil, nil, err
	}

	if !isVShardRespOk { // error
		errorResp := &StorageCallError{}

		err = future.GetTyped(&[]interface{}{&isVShardRespOk, errorResp})
		if err != nil {
			return nil, nil, fmt.Errorf("cant get typed vshard err with err: %s", err)
		}

		return nil, nil, errorResp
	}

	return resp.Data[1], func(result interface{}) error {
		var stub interface{}

		return future.GetTyped(&[]interface{}{&stub, result})
	}, nil
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
