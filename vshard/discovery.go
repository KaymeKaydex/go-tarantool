package vshard

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

func (r *Router) DiscoveryAllBuckets(ctx context.Context) error {
	routeMap := make([]*Replicaset, r.cfg.TotalBucketCount+1)
	knownBucket := 0

	errGr, ctx := errgroup.WithContext(ctx)

	for _, rs := range r.idToReplicaset {
		rs := rs
		rsMaster := rs.master

		errGr.Go(func() error {
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
			return nil
		})

	}

	err := errGr.Wait()
	if err != nil {
		return nil
	}

	r.routeMap = routeMap
	r.knownBucketCount = knownBucket

	return nil
}

// StartCronDiscovery is discovery_service_f analog with fibers
func (r *Router) StartCronDiscovery(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(r.cfg.DiscoveryTimeout):
		err := r.DiscoveryAllBuckets(ctx)
		if err != nil {
			r.Log().Error(fmt.Sprintf("cant do cron discovery with error: %s", err))
		}
	}

	return nil
}
