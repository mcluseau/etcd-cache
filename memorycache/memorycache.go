package memorycache

import (
	"sync"

	"github.com/coreos/etcd/clientv3"
	etcdcache "github.com/mcluseau/etcd-cache"
)

var _ etcdcache.Backend = &Cache{}

// KeyToIdFunc tranforms an etcd key to the object's ID. Note that `etcdKey` already has the prefix removed.
type KeyToIdFunc func(etcdKey string) (key string)

// UnmarshalFunc parses an etcd value to an object.
type UnmarshalFunc func(data []byte) (object interface{})

type Cache struct {
	prefix    string
	keyToId   KeyToIdFunc
	unmarshal UnmarshalFunc
	l         sync.RWMutex
	db        map[string]interface{}
}

func New(prefix string, keyToId KeyToIdFunc, unmarshal UnmarshalFunc) *Cache {
	return &Cache{
		prefix:    prefix,
		keyToId:   keyToId,
		unmarshal: unmarshal,
		l:         sync.RWMutex{},
		db:        map[string]interface{}{},
	}
}

func (c *Cache) Sync(logf etcdcache.LogFunc, client *clientv3.Client, wg *sync.WaitGroup) {
	etcdcache.Sync(c.prefix, c, logf, client, wg)
}

func (c *Cache) Read(read func(map[string]interface{})) {
	c.l.RLock()
	defer c.l.RUnlock()

	read(c.db)
}

func (c *Cache) Set(key string, value []byte) {
	c.l.Lock()
	defer c.l.Unlock()

	c.db[c.keyToId(key[len(c.prefix):])] = c.unmarshal(value)
}
func (c *Cache) Delete(key string, value []byte) {
	c.l.Lock()
	defer c.l.Unlock()

	delete(c.db, c.keyToId(key[len(c.prefix):]))
}
func (c *Cache) LoadRev() int64    { return 0 }
func (c *Cache) SaveRev(rev int64) {}
