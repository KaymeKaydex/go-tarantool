package vshard_router

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/tarantool/go-tarantool/v2"
)

// --------------------------------------------------------------------------------
// -- Discovery
// --------------------------------------------------------------------------------

// BucketDiscovery search bucket in whole cluster
func (r *Router) BucketDiscovery(ctx context.Context, bucketID uint64) (*Replicaset, error) {
	rs := r.routeMap[bucketID]
	if rs != nil {
		return rs, nil
	}
	// todo: локать в случае если бакет уже в поиске
	r.cfg.Logger.Info(ctx, fmt.Sprintf("Discovering bucket %d", bucketID))

	// todo: async and wrap all errors, вычислить при каком количестве нужна парралельность или сразу парралельно
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
func (r *Router) DiscoveryHandleBuckets(ctx context.Context, rs *Replicaset, buckets []uint64) {
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
				r.knownBucketCount.Add(1)
			}
			r.routeMap[bucketID] = rs
		}
	}

	if count != rs.bucketCount {
		r.cfg.Logger.Info(ctx, fmt.Sprintf("Updated %s buckets: was %d, became %d", rs.info.Name, rs.bucketCount, count))
	}

	rs.bucketCount = count

	for rs, oldBucketCount := range affected {
		r.Log().Info(ctx, fmt.Sprintf("Affected buckets of %s: was %d, became %d", rs.info.Name, oldBucketCount, rs.bucketCount))
	}
}

func (r *Router) DiscoveryAllBuckets(ctx context.Context) error {
	knownBucket := 0

	errGr, ctx := errgroup.WithContext(ctx)

	for _, rs := range r.idToReplicaset {
		rs := rs
		rsMaster := rs.master // todo: тянуть с реплики

		errGr.Go(func() error {
			bucketsInRs := make([]uint64, 0)

			future := rsMaster.conn.Do(tarantool.NewCallRequest("vshard.storage.buckets_discovery").Context(ctx).Args([]interface{}{}))
			// todo: добавить пагинацию там указывается from
			err := future.GetTyped(&[]interface{}{&bucketsInRs})
			if err != nil {
				return err
			}

			for _, bucket := range bucketsInRs {
				r.routeMap[bucket] = rs
				knownBucket++
			}
			return nil
		})

	}

	err := errGr.Wait()
	if err != nil {
		return nil
	}

	r.knownBucketCount.Store(int32(knownBucket))

	return nil
}

// startCronDiscovery is discovery_service_f analog with goroutines instead fibers
func (r *Router) startCronDiscovery(ctx context.Context) error {
	select {
	case <-ctx.Done():
		r.Metrics().CronDiscoveryEvent(false, 0, "ctx-cancel")

		return ctx.Err()
	case <-time.After(r.cfg.DiscoveryTimeout):
		tStartDiscovery := time.Now()

		err := r.DiscoveryAllBuckets(ctx)
		if err != nil {
			r.Metrics().CronDiscoveryEvent(false, time.Since(tStartDiscovery), "discovery-error")

			r.Log().Error(ctx, fmt.Sprintf("cant do cron discovery with error: %s", err))
		}

		r.Metrics().CronDiscoveryEvent(true, time.Since(tStartDiscovery), "ok")
	}

	return nil
}
