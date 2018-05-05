package boltcache

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/coreos/etcd/clientv3"
	etcdcache "github.com/mcluseau/etcd-cache"
)

var (
	bucketName     = []byte("cache")
	metaBucketName = []byte("cache-meta")
)

type Cache struct {
	prefix string
	db     *bolt.DB
}

var _ etcdcache.Backend = &Cache{}

func New(prefix, path string) (*Cache, error) {
	err := os.MkdirAll(filepath.Base(path), 0755)
	if err != nil {
		return nil, err
	}

	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(metaBucketName)
		return err
	})
	if err != nil {
		return nil, err
	}

	return &Cache{
		prefix: prefix,
		db:     db,
	}, nil
}

func (c *Cache) Sync(logf etcdcache.LogFunc, client *clientv3.Client, wg *sync.WaitGroup) {
	etcdcache.Sync(c.prefix, c, logf, client, wg)
}

func (c *Cache) View(fn func(tx *bolt.Tx, bucket *bolt.Bucket) error) error {
	return c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)

		return fn(tx, b)
	})
}

func (c *Cache) Set(key string, value []byte) {
	err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		return b.Put([]byte(key), value)
	})
	if err != nil {
		log.Fatal("boltdb cache: set failed: ", err)
	}
}

func (c *Cache) Delete(key string, value []byte) {
	err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		return b.Delete([]byte(key))
	})
	if err != nil {
		log.Fatal("boltdb cache: delete failed: ", err)
	}
}

func (c *Cache) LoadRev() (rev int64) {
	err := c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucketName)

		v := b.Get([]byte("rev"))

		if v == nil {
			rev = 0
			return nil
		}

		if err := json.Unmarshal(v, &rev); err != nil {
			panic(fmt.Errorf("boltdb cache: failed to decode rev: %v", err))
		}

		return nil
	})

	if err != nil {
		panic(fmt.Errorf("boltdb cache: failed to read rev: %v", err))
	}
	return
}

func (c *Cache) SaveRev(rev int64) {
	v, err := json.Marshal(rev)
	if err != nil {
		panic(fmt.Errorf("failed to marshal rev: %v", err))
	}

	err = c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucketName)
		return b.Put([]byte("rev"), v)
	})

	if err != nil {
		panic(fmt.Errorf("failed to update db: %v", err))
	}

	if err := c.db.Sync(); err != nil {
		log.Print("boltcache: db sync failed: ", err)
	}
}

func (c *Cache) Close() {
	c.db.Close()
}
