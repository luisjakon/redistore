package redistore

import (
	"testing"
)

func TestConn(t *testing.T) {
	var err error

	store, err := NewRediStore(10, "tcp", ":6379", "")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		store.Close()
	}()

	// Set
	err = store.Set("key", []byte("val"))
	if err != nil {
		t.Error(err)
	}

	// Get
	val, err := store.Get("key")
	if string(val) != "val" {
		t.Error("key val get failed")
	}

	// Del
	err = store.Del("key")
	if err != nil {
		t.Error(err)
	}

	// Check Del
	val, err = store.Get("key")
	if string(val) != "" {
		t.Error("key val get failed")
	}

	// Set
	err = store.Set("key.test", []byte("val"))
	if err != nil {
		t.Error(err)
	}

	// Get
	val, err = store.Get("key.test")
	if string(val) != "val" {
		t.Error("key val get failed")
	}

	// Del
	err = store.Del("key.test")
	if err != nil {
		t.Error(err)
	}

}
