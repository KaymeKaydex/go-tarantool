package vshard

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/snksoft/crc"

	"github.com/mitchellh/mapstructure"

	tarantool "github.com/tarantool/go-tarantool/v2"
)

var ErrInvalidConfig = fmt.Errorf("config invalid")

const DefaultTimeout = time.Second * 3

type Router struct {
	cfg Config

	idToReplicaset   map[uuid.UUID]*Replicaset
	routeMap         []*Replicaset
	knownBucketCount int
}

func (r *Router) Log() LogProvider {
	return r.cfg.Logger
}

type Config struct {
	Logger      LogProvider
	Replicasets map[ReplicasetInfo][]InstanceInfo

	IsMasterAuto     bool
	TotalBucketCount uint64
	User             string
	Password         string
	Timeout          time.Duration
}

type ReplicasetInfo struct {
	Name string
	UUID uuid.UUID
}

type Replicaset struct {
	Instances []*Instance

	info        ReplicasetInfo
	master      *Instance
	bucketCount int
}

type BucketStatInfo struct {
	BucketID uint64 `mapstructure:"id"`
	Status   string `mapstructure:"status"`
}

func (rs *Replicaset) BucketStat(ctx context.Context, bucketID uint64) (BucketStatInfo, error) {
	bsInfo := &BucketStatInfo{}
	bsError := &BucketStatError{}

	future := rs.master.conn.Do(tarantool.NewCallRequest("vshard.storage.bucket_stat").Args([]interface{}{bucketID}).Context(ctx))
	resp, err := future.Get()
	if err != nil {
		return BucketStatInfo{}, err
	}

	var tmp interface{} // todo: fix non-panic crutch

	if resp.Data[0] == nil {
		err := future.GetTyped(&[]interface{}{tmp, bsError})
		if err != nil {
			return BucketStatInfo{}, err
		}
	} else {
		// fucking key-code 1
		// todo: fix after https://github.com/tarantool/go-tarantool/issues/368
		err := mapstructure.Decode(resp.Data[0], bsInfo)
		if err != nil {
			return BucketStatInfo{}, err
		}
	}

	return *bsInfo, bsError
}

// AddInstance добавляет инстанс в репликасет
func (rs *Replicaset) addInstance(inst *Instance) {
	rs.Instances = append(rs.Instances, inst)
}

type InstanceInfo struct {
	Addr     string
	IsMaster bool
}

type Instance struct {
	Addr string // for example profile.internal:3388
	conn *tarantool.Connection
}

func NewRouter(ctx context.Context, cfg Config) (*Router, error) {
	var err error

	cfg, err = prepareCfg(cfg)
	if err != nil {
		return nil, err
	}

	router := &Router{
		cfg:              cfg,
		idToReplicaset:   make(map[uuid.UUID]*Replicaset),
		routeMap:         make([]*Replicaset, cfg.TotalBucketCount+1),
		knownBucketCount: 0,
	}

	for rsInfo, rsInstances := range cfg.Replicasets {
		rsInfo := rsInfo
		rsInstances := rsInstances

		replicaset := &Replicaset{
			info: ReplicasetInfo{
				Name: rsInfo.Name,
				UUID: rsInfo.UUID,
			},
			bucketCount: 0,
		}

		router.idToReplicaset[rsInfo.UUID] = replicaset

		for _, instance := range rsInstances {
			dialer := tarantool.NetDialer{
				Address:  instance.Addr,
				User:     cfg.User,
				Password: cfg.Password,
			}
			conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{Timeout: cfg.Timeout})
			if err != nil {
				return nil, err
			}

			inst := &Instance{
				Addr: instance.Addr,
				conn: conn,
			}

			replicaset.addInstance(inst)

			if instance.IsMaster {
				replicaset.master = inst
			}
		}
	}

	err = router.DiscoveryBuckets(ctx)
	if err != nil {
		return nil, err
	}

	return router, err
}

func (r *Router) DiscoveryBuckets(ctx context.Context) error {
	routeMap := make([]*Replicaset, r.cfg.TotalBucketCount+1)
	knownBucket := 0

	for _, rs := range r.idToReplicaset {
		rsMaster := rs.master

		bucketsInRs := make([]uint64, 0)

		future := rsMaster.conn.Do(tarantool.NewCallRequest("vshard.storage.buckets_discovery").Context(ctx).Args([]interface{}{}))

		err := future.GetTyped(&[]interface{}{&bucketsInRs})
		if err != nil {
			return err
		}

		for _, bucket := range bucketsInRs {
			routeMap[bucket] = rs
			knownBucket++
		}
	}

	r.routeMap = routeMap
	r.knownBucketCount = knownBucket

	return nil
}

// BucketSet Set a bucket to a replicaset.
func (r *Router) BucketSet(bucketID uint64, rsID uuid.UUID) (*Replicaset, error) {
	rs := r.idToReplicaset[rsID]
	if rs == nil {
		return nil, Errors[9] // NO_ROUTE_TO_BUCKET
	}

	oldReplicaset := r.routeMap[bucketID]

	if oldReplicaset != rs {
		if oldReplicaset != nil {
			oldReplicaset.bucketCount--
		} else {
			r.knownBucketCount++
		}

		rs.bucketCount++
	}

	r.routeMap[bucketID] = rs

	return rs, nil
}

func (r *Router) BucketReset(bucketID uint64) {
	if bucketID > uint64(len(r.routeMap))+1 {
		return
	}

	r.knownBucketCount--
	r.routeMap[bucketID] = nil
}

func (r *Router) RouteMapClean() {

	r.routeMap = nil
	r.knownBucketCount = 0

	for _, rs := range r.idToReplicaset {
		rs.bucketCount = 0
	}

}

func prepareCfg(cfg Config) (Config, error) {
	err := validateCfg(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("%v: %v", ErrInvalidConfig, err)
	}

	if cfg.Logger == nil {
		cfg.Logger = &EmptyLogger{}
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
		cfg.Logger.Warn("empty config timeout, using default timeout: " + DefaultTimeout.String())
	}

	return cfg, nil
}

func validateCfg(cfg Config) error {
	if len(cfg.Replicasets) < 1 {
		return fmt.Errorf("replicasets are empty")
	}

	if cfg.TotalBucketCount == 0 {
		return fmt.Errorf("bucket count must be grather then 0")
	}

	for rs := range cfg.Replicasets {
		// check replicaset name
		if rs.Name == "" {
			return fmt.Errorf("one of replicaset name is empty")
		}

		// check replicaset uuid
		if rs.UUID == uuid.Nil {
			return fmt.Errorf("one of replicaset uuid is empty")
		}

		for _, node := range cfg.Replicasets[rs] {
			_, err := url.Parse(node.Addr)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// --------------------------------------------------------------------------------
// -- Discovery
// --------------------------------------------------------------------------------

// BucketDiscovery search bucket in whole cluster
func (r *Router) BucketDiscovery(ctx context.Context, bucketID uint64) (*Replicaset, error) {
	rs := r.routeMap[bucketID]
	if rs != nil {
		return rs, nil
	}

	r.cfg.Logger.Info(fmt.Sprintf("Discovering bucket %d", bucketID))

	// todo: async and wrap all errors
	for rsID, rs := range r.idToReplicaset {
		_, err := rs.BucketStat(ctx, bucketID)
		if err == nil {
			return r.BucketSet(bucketID, rsID)
		}
	}

	/*
	   -- All replicasets were scanned, but a bucket was not
	   -- found anywhere, so most likely it does not exist. It
	   -- can be wrong, if rebalancing is in progress, and a
	   -- bucket was found to be RECEIVING on one replicaset, and
	   -- was not found on other replicasets (it was sent during
	   -- discovery).
	*/

	return nil, Errors[9] // NO_ROUTE_TO_BUCKET
}

// BucketResolve resolve bucket id to replicaset
func (r *Router) BucketResolve(ctx context.Context, bucketID uint64) (*Replicaset, error) {
	rs := r.routeMap[bucketID]
	if rs != nil {
		return rs, nil
	}

	// Replicaset removed from cluster, perform discovery
	rs, err := r.BucketDiscovery(ctx, bucketID)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// DiscoveryHandleBuckets arrange downloaded buckets to the route map so as they reference a given replicaset.
func (r *Router) DiscoveryHandleBuckets(rs *Replicaset, buckets []uint64) {
	count := rs.bucketCount
	affected := make(map[*Replicaset]int)

	for _, bucketID := range buckets {
		oldRs := r.routeMap[bucketID]

		if oldRs != rs {
			count++

			if oldRs != nil {
				bc := oldRs.bucketCount

				if _, exists := affected[oldRs]; !exists {
					affected[oldRs] = bc
				}

				oldRs.bucketCount = bc - 1
			} else {
				//                 router.known_bucket_count = router.known_bucket_count + 1
				r.knownBucketCount++
			}
			r.routeMap[bucketID] = rs
		}
	}

	if count != rs.bucketCount {
		r.cfg.Logger.Info(fmt.Sprintf("Updated %s buckets: was %d, became %d", rs.info.Name, rs.bucketCount, count))
	}

	rs.bucketCount = count

	for rs, oldBucketCount := range affected {
		r.Log().Info(fmt.Sprintf("Affected buckets of %s: was %d, became %d", rs.info.Name, oldBucketCount, rs.bucketCount))
	}
}

// todo: discovery_service_f
// todo: discovery_f
// todo: discovery_wakeup
// todo: discovery_set

// --------------------------------------------------------------------------------
// -- API
// --------------------------------------------------------------------------------

// vshard_future_tostring - is useless for golang

// RouterCallImpl Perform shard operation function will restart operation
// after wrong bucket response until timeout is reached
func (r *Router) RouterCallImpl(bucketID uint64, mode string, preferReplica, balance bool, fnc string, args, opts interface{}) {
	// todo
}

// Wrappers for router_call with preset mode.

func (r *Router) RouterCallRO(bucketID uint64, mode string, preferReplica, balance bool, fnc string, args, opts interface{}) {
	r.RouterCallImpl(bucketID, "read", false, false, fnc, args, opts)
}

func (r *Router) RouterCallBRO(bucketID uint64, fnc string, args, opts interface{}) {
	r.RouterCallImpl(bucketID, "read", false, true, fnc, args, opts)
}

func (r *Router) RouterCallRW(bucketID uint64, fnc string, args, opts interface{}) {
	r.RouterCallImpl(bucketID, "write", false, false, fnc, args, opts)
}

func (r *Router) RouterCallRE(bucketID uint64, fnc string, args, opts interface{}) {
	r.RouterCallImpl(bucketID, "read", true, false, fnc, args, opts)
}

func (r *Router) RouterCallBRE(bucketID uint64, fnc string, args, opts interface{}) {
	r.RouterCallImpl(bucketID, "read", true, true, fnc, args, opts)
}

// todo: router_call
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

// --------------------------------------------------------------------------------
// -- Failover
// --------------------------------------------------------------------------------

// todo: failover_ping_round
// todo: failover_need_down_priority
// todo: failover_need_up_priority
// todo: failover_collect_to_update
// todo: failover_step
// todo: failover_service_f
// todo: failover_f

// --------------------------------------------------------------------------------
// -- Failover
// --------------------------------------------------------------------------------

// todo: master_search_step
// todo: master_search_service_f
// todo: master_search_f
// todo: master_search_set
// todo: master_search_wakeup

// --------------------------------------------------------------------------------
// -- Configuration
// --------------------------------------------------------------------------------

// todo: router_cfg
// todo: router_cfg_fiber_safe

// --------------------------------------------------------------------------------
// -- Bootstrap
// --------------------------------------------------------------------------------
// todo: cluster_bootstrap

// --------------------------------------------------------------------------------
// -- Monitoring
// --------------------------------------------------------------------------------

// todo: replicaset_instance_info
// todo: router_info
// todo: router_buckets_info

// --------------------------------------------------------------------------------
// -- Other
// --------------------------------------------------------------------------------

// RouterBucketID  return the bucket identifier from the parameter used for sharding
// Deprecated: RouterBucketID() is deprecated, use RouterBucketIDStrCRC32() RouterBucketIDMPCRC32() instead
func (r *Router) RouterBucketID(shardKey string) uint64 {
	return BucketIDStrCRC32(shardKey, r.cfg.TotalBucketCount)
}

func BucketIDStrCRC32(shardKey string, totalBucketCount uint64) uint64 {
	return crc.CalculateCRC(&crc.Parameters{
		Width:      32,
		Polynomial: 0x1EDC6F41,
		FinalXor:   0x0,
		ReflectIn:  true,
		ReflectOut: true,
		Init:       0xFFFFFFFF,
	}, []byte(shardKey))%totalBucketCount + 1
}

func (r *Router) RouterBucketIDStrCRC32(shardKey string) uint64 {
	return BucketIDStrCRC32(shardKey, r.cfg.TotalBucketCount)
}

// RouterBucketIDMPCRC32 is not supported now
func RouterBucketIDMPCRC32() {}

func (r *Router) RouterBucketCount() uint64 {
	return r.cfg.TotalBucketCount
}

// todo: router_sync

// --------------------------------------------------------------------------------
// -- Other
// --------------------------------------------------------------------------------

// todo: router_api_call_safe
// todo: router_api_call_unsafe
// todo: router_make_api
// todo: router_enable
// todo: router_disable
