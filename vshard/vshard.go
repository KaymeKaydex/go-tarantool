package vshard

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/snksoft/crc"

	tarantool "github.com/tarantool/go-tarantool/v2"
)

var ErrInvalidConfig = fmt.Errorf("config invalid")

const DefaultTimeout = time.Second * 3

type Router struct {
	cfg Config

	mu               sync.Mutex
	nameToReplicaset map[string]*Replicaset
	routeMap         []*Instance
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
	mu        sync.Mutex
	Instances []*Instance

	info   ReplicasetInfo
	master *Instance
}

// AddInstance добавляет инстанс в репликасет
func (r *Replicaset) addInstance(inst *Instance) {
	r.mu.Lock()
	r.Instances = append(r.Instances, inst)
	r.mu.Unlock()
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
		mu:               sync.Mutex{},
		nameToReplicaset: make(map[string]*Replicaset),
		routeMap:         make([]*Instance, cfg.TotalBucketCount+1),
	}

	for rsInfo, rsInstances := range cfg.Replicasets {
		rsInfo := rsInfo
		rsInstances := rsInstances

		replicaset := &Replicaset{
			mu: sync.Mutex{},
			info: ReplicasetInfo{
				Name: rsInfo.Name,
				UUID: rsInfo.UUID,
			},
		}

		router.mu.Lock()
		router.nameToReplicaset[rsInfo.Name] = replicaset
		router.mu.Unlock()

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
	for _, rs := range r.nameToReplicaset {
		rsMaster := rs.master

		bucketsInRs := make([]uint64, 0)

		future := rsMaster.conn.Do(tarantool.NewCallRequest("vshard.storage.buckets_discovery").Args([]interface{}{}))

		err := future.GetTyped(&[]interface{}{&bucketsInRs})
		if err != nil {
			return err
		}

		r.mu.Lock()
		for _, bucket := range bucketsInRs {
			r.routeMap[bucket] = rsMaster
		}
		r.mu.Unlock()
	}

	return nil
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

func (r *Router) BucketResolve(bucketID uint64) *Instance {
	return r.routeMap[bucketID]
}

// RouterBucketID  return the bucket identifier from the parameter used for sharding
// Deprecated: RouterBucketID() is deprecated, use RouterBucketIDStrCRC32() RouterBucketIDMPCRC32() instead
func (r *Router) RouterBucketID(shardKey string) uint64 {
	return RouterBucketIDStrCRC32(shardKey, r.cfg.TotalBucketCount)
}

func RouterBucketIDStrCRC32(shardKey string, totalBucketCount uint64) uint64 {
	return crc.CalculateCRC(&crc.Parameters{
		Width:      32,
		Polynomial: 0x1EDC6F41,
		FinalXor:   0x0,
		ReflectIn:  true,
		ReflectOut: true,
		Init:       0xFFFFFFFF,
	}, []byte(shardKey))%totalBucketCount + 1
}

// RouterBucketIDMPCRC32 is not supported now
func RouterBucketIDMPCRC32() {}
