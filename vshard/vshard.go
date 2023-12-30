package vshard

import (
	"context"
	"hash/crc32"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	tarantool "github.com/tarantool/go-tarantool/v2"
)

type Router struct {
	cfg Config

	mu               sync.Mutex
	nameToReplicaset map[string]*Replicaset
	routeMap         map[uint32]*Instance
}

type Config struct {
	logger      LogProvider
	Replicasets map[struct {
		Name string
		UUID uuid.UUID
	}][]struct {
		Addr     string
		IsMaster bool
	}

	IsMasterAuto     bool
	TotalBucketCount uint32
	User             string
	Password         string
	Timeout          time.Duration
}

type Replicaset struct {
	mu        sync.Mutex
	Instances []*Instance

	name   string
	uuid   uuid.UUID
	master *Instance
}

// AddInstance добавляет инстанс в репликасет
func (r *Replicaset) AddInstance(inst *Instance) {}

type Instance struct {
	Addr string // for example profile.internal:3388
	conn *tarantool.Connection
}

func NewRouter(ctx context.Context, cfg Config) (*Router, error) {
	err := validateCfg(cfg)
	if err != nil {
		return nil, err
	}

	router := &Router{cfg: cfg}

	errGr, ctx := errgroup.WithContext(ctx)

	for rsInfo, rsInstances := range cfg.Replicasets {
		rsInfo := rsInfo
		rsInstances := rsInstances

		errGr.Go(func() error {
			replicaset := &Replicaset{
				mu:   sync.Mutex{},
				name: rsInfo.Name,
				uuid: rsInfo.UUID,
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
					return err
				}

				inst := &Instance{
					Addr: instance.Addr,
					conn: conn,
				}

				replicaset.Instances = append(replicaset.Instances, inst)

				if instance.IsMaster {
					replicaset.master = inst
				}
			}
			return nil
		})
	}

	err = errGr.Wait()
	if err != nil {
		return nil, err
	}

	err = router.DiscoveryBuckets()
	if err != nil {
		return nil, err
	}

	return router, err
}

func (r *Router) DiscoveryBuckets() error {
	for _, rs := range r.nameToReplicaset {
		bucketsInRs := make([]uint32, 0)

		err := rs.master.conn.Do(tarantool.NewCallRequest("vshard.storage.buckets_discovery")).
			GetTyped(&[]interface{}{bucketsInRs})
		if err != nil {
			return err
		}

		for _, bucket := range bucketsInRs {
			r.routeMap[bucket] = rs.master
		}
	}

	return nil
}

func validateCfg(cfg Config) error {
	return nil
}

func (r *Router) BucketResolve() {

}

// RouterBucketID  return the bucket identifier from the parameter used for sharding
// Deprecated: RouterBucketID() is deprecated, use RouterBucketIDStrCRC32() RouterBucketIDMPCRC32() instead
func (r *Router) RouterBucketID(shardKey string) uint32 {
	return RouterBucketIDStrCRC32(shardKey, r.cfg.TotalBucketCount)
}

func RouterBucketIDStrCRC32(shardKey string, totalBucketCount uint32) uint32 {
	return crc32.ChecksumIEEE([]byte(shardKey))%totalBucketCount + 1
}

// RouterBucketIDMPCRC32 is not supported now
func RouterBucketIDMPCRC32() {}
