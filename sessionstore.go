package redistore

import (
	"crypto/rand"
	"encoding/base32"
	"net/http"
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/sessions"
)

const (
	SESSION_HEADER  = "X-Core-Session"
	SESSION_PREFIX  = "sess_"
	DEFAULT_MAX_AGE = 60 * 20 // 20 mins
)

type SessionStore struct {
	store   *RediStore
	Options *sessions.Options
}

func randomBytes(size int) []byte {
	b := make([]byte, size)
	rand.Read(b)
	return b
}

func NewSessionStore(size int, network, address, password string, keyPairs ...[]byte) (*SessionStore, error) {
	store, err := NewRediStore(size, network, address, password, keyPairs...)
	if err != nil {
		return nil, err
	}
	return &SessionStore{
		store: store,
		Options: &sessions.Options{
			MaxAge: DEFAULT_MAX_AGE,
		}}, nil
}

func (ss *SessionStore) Get(r *http.Request, name string) (*sessions.Session, error) {
	return sessions.GetRegistry(r).Get(ss, name)
}

func (ss *SessionStore) New(r *http.Request, name string) (*sessions.Session, error) {
	session := sessions.NewSession(ss, name)
	session.Options = ss.Options
	session.IsNew = true

	session.ID = r.Header.Get(SESSION_HEADER)
	ok, err := ss.Load(session)
	session.IsNew = !(err == nil && ok) // not new if no error and data available

	return session, err
}

func (ss *SessionStore) Save(r *http.Request, w http.ResponseWriter, session *sessions.Session) error {
	if session.Options.MaxAge < 0 {
		if err := ss.store.Del(session.ID); err != nil {
			return err
		}
	} else {
		// Build an alphanumeric key for the redis store.
		if session.ID == "" {
			// TODO: Make randomBytes(...) more secure
			session.ID = strings.TrimRight(base32.StdEncoding.EncodeToString(randomBytes(12)), "=")
		}

		data, err := ss.store.serializer.Serialize(session)
		if err != nil {
			return err
		}

		if err := ss.store.SetEx(SESSION_PREFIX+session.ID, data, session.Options.MaxAge); err != nil {
			return err
		}

		w.Header().Set(SESSION_HEADER, session.ID)
	}
	return nil
}

func (ss *SessionStore) Load(session *sessions.Session) (bool, error) {
	data, err := ss.store.Get(SESSION_PREFIX + session.ID)
	if err != nil {
		return false, err
	}
	if data == nil {
		return false, nil // no data was associated with this key
	}
	b, err := redis.Bytes(data, err)
	if err != nil {
		return false, err
	}
	return true, ss.store.serializer.Deserialize(b, session)
}

func (ss *SessionStore) MaxAge(age int) {
	ss.store.DefaultMaxAge = age
}
