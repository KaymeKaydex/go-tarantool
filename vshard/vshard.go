package vshard

import "hash/crc32"

type Router struct {
	cfg Config
}

type Config struct {
	Replicasets      []Replicaset
	TotalBucketCount uint32
	RouteMap         map[int]*Replicaset
}

type Replicaset struct {
	Instances []Instance
	master    *Instance
}

type Instance struct {
	Addr   string // например profile.internal:3388
	Master bool
}

func NewRouter(cfg Config) (*Router, error) {
	err := validateCfg(cfg)
	if err != nil {
		return nil, err
	}

	return &Router{cfg: cfg}, err
}

func DiscoveryBuckets() {
	// vshard.storage.buckets_discovery()
}

func validateCfg(cfg Config) error {
	return nil
}

func (r *Router) BucketResolve() {

}

// RouterBucketID  return the bucket identifier from the parameter used for sharding
// Deprecated: RouterBucketID() is deprecated, use RouterBucketIDStrCRC32() RouterBucketIDMPCRC32() instead
func (r Router) RouterBucketID(shardKey string) uint32 {
	return r.RouterBucketIDStrCRC32(shardKey)
}

func (r Router) RouterBucketIDStrCRC32(shardKey string) uint32 {
	return crc32.ChecksumIEEE([]byte(shardKey))%r.cfg.TotalBucketCount + 1
}

// RouterBucketIDMPCRC32 is not supported now
func (r Router) RouterBucketIDMPCRC32() {}
