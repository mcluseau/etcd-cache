package etcdcache

import (
	"context"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/mirror"
)

// Backend is a cache backend.
type Backend interface {
	Set(key string, value []byte)
	Delete(key string, value []byte)

	LoadRev() int64
	SaveRev(rev int64)
}

// Sync synchronizes a cache backend with and etcd.
// `prefix` is the key prefix to used (can be empty).
// `backend` is the backend to synchronize.
// `logf` is the log function, called like `fmt.Printf`.
// `client` is the etcd client to use.
// `wg` is an optional wait group. If provided, `wg.Done()` is called at the end the initial copy.
func Sync(prefix string, backend Backend, logf LogFunc, client *clientv3.Client, wg *sync.WaitGroup) {
	ctx := context.Background()

	rev := backend.LoadRev()

	if rev == 0 {
		logf("initializing")
		sync := mirror.NewSyncer(client, prefix, 0)
		gets, errors := sync.SyncBase(ctx)

		go func() {
			err, ok := <-errors
			if ok {
				logf("error while reading: %v", err)
			}
		}()

		for get := range gets {
			for _, kv := range get.Kvs {
				backend.Set(string(kv.Key), kv.Value)
			}
			rev = get.Header.Revision
		}
	}

	if wg != nil {
		wg.Done()
	}

	logf("following changes")
	for {
		sync := mirror.NewSyncer(client, prefix, rev)
		for update := range sync.SyncUpdates(ctx) {
			if err := update.Err(); err != nil {
				logf("got error: %v", err)
				continue
			}

			for _, event := range update.Events {
				if event.Kv == nil {
					backend.Delete(string(event.PrevKv.Key), event.PrevKv.Value)
				} else {
					backend.Set(string(event.Kv.Key), event.Kv.Value)
				}
			}

			rev = update.Header.Revision
			backend.SaveRev(rev)
		}

		logf("sync updates ended, restarting")
	}
}
