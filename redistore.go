package redistore

import (
	"github.com/garyburd/redigo/redis"
	"io"
	"time"
)

type Store interface {
	io.Closer
	Get(key string) ([]byte, error)
	Set(key string, val []byte) error
	SetEx(key string, val []byte, age int) error
	Del(key string) error
}

type RediStore struct {
	Pool          *redis.Pool
	DefaultMaxAge int // default Redis TTL for a MaxAge == 0 session
	maxLength     int
	keyPrefix     string
	serializer    SessionSerializer
}

// NewRediStore returns a new RediStore.
// size: maximum number of idle connections.
func NewRediStore(size int, network, address, password string, keyPairs ...[]byte) (*RediStore, error) {
	return NewRediStoreWithPool(&redis.Pool{
		MaxIdle:     size,
		IdleTimeout: 240 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Dial: func() (redis.Conn, error) {
			return dial(network, address, password)
		},
	}, keyPairs...)
}

// NewRediStoreWithDB - like NewRedisStore but accepts `DB` parameter to select
// redis DB instead of using the default one ("0")
func NewRediStoreWithDB(size int, network, address, password, DB string, keyPairs ...[]byte) (*RediStore, error) {
	return NewRediStoreWithPool(&redis.Pool{
		MaxIdle:     size,
		IdleTimeout: 240 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Dial: func() (redis.Conn, error) {
			return dialWithDB(network, address, password, DB)
		},
	}, keyPairs...)
}

// NewRediStoreWithPool instantiates a RediStore with a *redis.Pool passed in.
func NewRediStoreWithPool(pool *redis.Pool, keyPairs ...[]byte) (*RediStore, error) {
	s := &RediStore{
		// http://godoc.org/github.com/garyburd/redigo/redis#Pool
		Pool:          pool,
		DefaultMaxAge: 60 * 20, // 20 minutes seems like a reasonable default
		maxLength:     4096,
		keyPrefix:     "session_",
		serializer:    GobSerializer{},
	}
	_, err := s.ping()
	return s, err
}

func dial(network, address, password string) (redis.Conn, error) {
	c, err := redis.Dial(network, address)
	if err != nil {
		return nil, err
	}
	if password != "" {
		if _, err := c.Do("AUTH", password); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, err
}

func dialWithDB(network, address, password, DB string) (redis.Conn, error) {
	c, err := dial(network, address, password)
	if err != nil {
		return nil, err
	}
	if _, err := c.Do("SELECT", DB); err != nil {
		c.Close()
		return nil, err
	}
	return c, err
}

// ping does an internal ping against a server to check if it is alive.
func (s *RediStore) ping() (bool, error) {
	conn := s.Pool.Get()
	defer conn.Close()
	data, err := conn.Do("PING")
	if err != nil || data == nil {
		return false, err
	}
	return (data == "PONG"), nil
}

func (s *RediStore) StoreAs(ser string) *RediStore {
	switch ser {
	case "JSON":
		s.serializer = JSONSerializer{}
	case "BINARY":
		s.serializer = GobSerializer{}
	default:
		s.serializer = GobSerializer{}
	}
	return s
}

func (s *RediStore) Close() error {
	return s.Pool.Close()
}

func (s *RediStore) Get(key string) ([]byte, error) {
	conn := s.Pool.Get()
	defer conn.Close()
	b, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		if err == redis.ErrNil {
			return []byte(nil), nil
		}
		return b, err
	}
	return b, nil
}

func (s *RediStore) Set(key string, val []byte) error {
	conn := s.Pool.Get()
	defer conn.Close()
	if _, err := conn.Do("SET", key, val); err != nil {
		return err
	}
	return nil
}

func (s *RediStore) SetEx(key string, val []byte, age int) error {
	conn := s.Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	if age == 0 {
		age = s.DefaultMaxAge
	}
	_, err := conn.Do("SETEX", key, age, val)
	return err
}

func (s *RediStore) Del(key string) error {
	conn := s.Pool.Get()
	defer conn.Close()
	if _, err := conn.Do("DEL", key); err != nil {
		return err
	}
	return nil
}
