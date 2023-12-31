package vshard

import (
	"fmt"

	"github.com/tarantool/go-tarantool/v2"
)

func (r *Router) Call(shardKey fmt.Stringer, fnc string, args interface{}) error {
	bucketID := r.RouterBucketIDStrCRC32(shardKey.String())

	return r.CallWithBucketID(bucketID, fnc, args)
}

func (r *Router) CallWithBucketID(bucketID uint64, fnc string, args interface{}) error {
	inst := r.BucketResolve(bucketID)

	req := tarantool.NewCallRequest("vshard.storage.call")
	req = req.Args([]interface{}{bucketID, "write", fnc, args}) // add args

	future := inst.conn.Do(req)
	fmt.Println(future.Get())

	return nil
}
