package vshard_router

import (
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/snksoft/crc"

	tarantool "github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

var ErrInvalidConfig = fmt.Errorf("config invalid")

const DefaultTimeout = time.Second * 3

type Router struct {
	cfg Config

	idToReplicaset   map[uuid.UUID]*Replicaset
	routeMap         []*Replicaset
	knownBucketCount atomic.Int32

	cancelDiscovery func()
}

func (r *Router) Metrics() MetricsProvider {
	return r.cfg.Metrics
}

func (r *Router) Log() LogProvider {
	return r.cfg.Logger
}

type DiscoveryMode int

const (
	// DiscoveryModeOn is cron discovery with cron timeout
	DiscoveryModeOn DiscoveryMode = iota
	DiscoveryModeOnce
)

type Config struct {
	// providers
	Logger  LogProvider
	Metrics MetricsProvider

	Replicasets map[ReplicasetInfo][]InstanceInfo

	DiscoveryTimeout time.Duration
	DiscoveryMode    DiscoveryMode

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
	conn *pool.ConnectionPool
	info ReplicasetInfo

	bucketCount atomic.Int32
}

type BucketStatInfo struct {
	BucketID uint64 `mapstructure:"id"`
	Status   string `mapstructure:"status"`
}

func (rs *Replicaset) BucketStat(ctx context.Context, bucketID uint64) (BucketStatInfo, error) {
	bsInfo := &BucketStatInfo{}
	bsError := &BucketStatError{}

	req := tarantool.NewCallRequest("vshard.storage.bucket_stat")
	req = req.Args([]interface{}{bucketID})
	req = req.Context(ctx)

	future := rs.conn.Do(req, pool.RO)
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

type InstanceInfo struct {
	Addr string
	UUID uuid.UUID
}

// --------------------------------------------------------------------------------
// -- Configuration
// --------------------------------------------------------------------------------

func NewRouter(ctx context.Context, cfg Config) (*Router, error) {
	var err error

	cfg, err = prepareCfg(ctx, cfg)
	if err != nil {
		return nil, err
	}

	router := &Router{
		cfg:              cfg,
		idToReplicaset:   make(map[uuid.UUID]*Replicaset),
		routeMap:         make([]*Replicaset, cfg.TotalBucketCount+1),
		knownBucketCount: atomic.Int32{},
	}

	router.knownBucketCount.Store(0)

	for rsInfo, rsInstances := range cfg.Replicasets {
		rsInfo := rsInfo
		rsInstances := rsInstances

		replicaset := &Replicaset{
			info: ReplicasetInfo{
				Name: rsInfo.Name,
				UUID: rsInfo.UUID,
			},
			bucketCount: atomic.Int32{},
		}

		replicaset.bucketCount.Store(0)

		router.idToReplicaset[rsInfo.UUID] = replicaset

		rsDialers := make(map[string]tarantool.Dialer, len(rsInstances))

		for _, instance := range rsInstances {
			dialer := tarantool.NetDialer{
				Address:  instance.Addr,
				User:     cfg.User,
				Password: cfg.Password,
			}

			rsDialers[instance.UUID.String()] = dialer
		}

		conn, err := pool.Connect(ctx, rsDialers, tarantool.Opts{
			Timeout: router.cfg.Timeout,
		})
		if err != nil {
			return nil, err
		}

		replicaset.conn = conn
	}

	err = router.DiscoveryAllBuckets(ctx)
	if err != nil {
		return nil, err
	}

	if cfg.DiscoveryMode == DiscoveryModeOn {
		discoveryCronCtx, cancelFunc := context.WithCancel(context.Background())

		go func() {
			discoveryErr := router.startCronDiscovery(discoveryCronCtx)
			if discoveryErr != nil {
				router.Log().Error(ctx, fmt.Sprintf("error when run cron discovery: %s", discoveryErr))
			}
		}()

		router.cancelDiscovery = cancelFunc
	}

	return router, err
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
			oldReplicaset.bucketCount.Add(-1)
		} else {
			r.knownBucketCount.Add(1)
		}

		rs.bucketCount.Add(1)
	}

	r.routeMap[bucketID] = rs

	return rs, nil
}

func (r *Router) BucketReset(bucketID uint64) {
	if bucketID > uint64(len(r.routeMap))+1 {
		return
	}

	r.knownBucketCount.Add(-1)
	r.routeMap[bucketID] = nil
}

func (r *Router) RouteMapClean() {

	r.routeMap = make([]*Replicaset, r.cfg.TotalBucketCount+1)
	r.knownBucketCount.Store(0)

	for _, rs := range r.idToReplicaset {
		rs.bucketCount.Store(0)
	}

}

func prepareCfg(ctx context.Context, cfg Config) (Config, error) {
	err := validateCfg(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("%v: %v", ErrInvalidConfig, err)
	}

	if cfg.Logger == nil {
		cfg.Logger = &EmptyLogger{}
	}

	if cfg.Metrics == nil {
		cfg.Metrics = &EmptyMetrics{}
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
		cfg.Logger.Warn(ctx, "empty config timeout, using default timeout: "+DefaultTimeout.String())
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
// -- Master search
// --------------------------------------------------------------------------------

// todo

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
// -- Public API protection
// --------------------------------------------------------------------------------

// todo: router_api_call_safe
// todo: router_api_call_unsafe
// todo: router_make_api
// todo: router_enable
// todo: router_disable