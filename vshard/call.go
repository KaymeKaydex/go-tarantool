package vshard

import (
	"fmt"

	"github.com/tarantool/go-tarantool/v2"
)

func (r *Router) Call(shardKey fmt.Stringer, fnc string, args interface{}) (bool, error) {
	bucketID := r.RouterBucketIDStrCRC32(shardKey.String())

	return r.CallWithBucketID(bucketID, fnc, args)
}

func (r *Router) CallWithBucketID(bucketID uint64, fnc string, args interface{}) (bool, error) {
	inst := r.BucketResolve(bucketID)
	callStatus := new(bool)

	req := tarantool.NewCallRequest("vshard.storage.call")
	req = req.Args([]interface{}{bucketID, "write", fnc, args}) // add args

	var result interface{}

	future := inst.conn.Do(req)
	// Vshard.storage.call(func) returns two values: true/false and func result.
	err := future.GetTyped(&[]interface{}{callStatus, &result})
	if err != nil {
		return false, err
	}

	return false, err
}
