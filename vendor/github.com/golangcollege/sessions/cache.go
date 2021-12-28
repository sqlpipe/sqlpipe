package sessions

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"net/http"
	"sort"
	"sync"
	"time"
)

type contextKey string

var contextKeyCache = contextKey("cache")

var errMissingCache = errors.New("session: cache not present in request context")

type cache struct {
	Data      map[string]interface{}
	Expiry    time.Time
	modified  bool
	destroyed bool
	mu        sync.Mutex
}

func newCache(lifetime time.Duration) *cache {
	return &cache{
		Data:   make(map[string]interface{}),
		Expiry: time.Now().Add(lifetime).UTC(),
	}
}

func (c *cache) encode(key [32]byte) (string, error) {
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(c)
	if err != nil {
		return "", err
	}

	return encrypt(b.Bytes(), key)
}

func (c *cache) decode(token string, keys [][32]byte) error {
	b, err := decrypt(token, keys)
	if err != nil {
		return err
	}

	r := bytes.NewReader(b)
	return gob.NewDecoder(r).Decode(c)
}

func addCacheToRequestContext(r *http.Request, c *cache) *http.Request {
	ctx := context.WithValue(r.Context(), contextKeyCache, c)
	return r.WithContext(ctx)
}

func getCacheFromRequestContext(r *http.Request) *cache {
	c, ok := r.Context().Value(contextKeyCache).(*cache)
	if !ok {
		panic(errMissingCache)
	}
	return c
}

// Put adds a key and corresponding value to the session data. Any existing
// value for the key will be replaced.
func (s *Session) Put(r *http.Request, key string, val interface{}) {
	c := getCacheFromRequestContext(r)

	c.mu.Lock()
	c.Data[key] = val
	c.modified = true
	c.mu.Unlock()
}

// Get returns the value for a given key from the session data. The return
// value has the type interface{} so will usually need to be type asserted
// before you can use it. For example:
//
//	foo, ok := session.Get(r, "foo").(string)
//	if !ok {
//		return errors.New("type assertion to string failed")
//	}
//
// Note: Alternatives are the GetString(), GetInt(), GetBytes() and other
// helper methods which wrap the type conversion for common types.
func (s *Session) Get(r *http.Request, key string) interface{} {
	c := getCacheFromRequestContext(r)

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.Data[key]
}

// Pop acts like a one-time Get. It returns the value for a given key from the
// session data and deletes the key and value from the session data. The
// return value has the type interface{} so will usually need to be type
// asserted before you can use it.
func (s *Session) Pop(r *http.Request, key string) interface{} {
	c := getCacheFromRequestContext(r)

	c.mu.Lock()
	defer c.mu.Unlock()

	val, exists := c.Data[key]
	if !exists {
		return nil
	}
	delete(c.Data, key)
	c.modified = true

	return val
}

// Remove deletes the given key and corresponding value from the session data.
// If the key is not present this operation is a no-op.
func (s *Session) Remove(r *http.Request, key string) {
	c := getCacheFromRequestContext(r)

	c.mu.Lock()
	defer c.mu.Unlock()

	_, exists := c.Data[key]
	if !exists {
		return
	}

	delete(c.Data, key)
	c.modified = true
}

// Exists returns true if the given key is present in the session data.
func (s *Session) Exists(r *http.Request, key string) bool {
	c := getCacheFromRequestContext(r)

	c.mu.Lock()
	_, exists := c.Data[key]
	c.mu.Unlock()

	return exists
}

// Keys returns a slice of all key names present in the session data, sorted
// alphabetically. If the cache contains no data then an empty slice will be
// returned.
func (s *Session) Keys(r *http.Request) []string {
	c := getCacheFromRequestContext(r)

	c.mu.Lock()
	keys := make([]string, len(c.Data))
	i := 0
	for key := range c.Data {
		keys[i] = key
		i++
	}
	c.mu.Unlock()

	sort.Strings(keys)
	return keys
}

// Destroy deletes the current session. The session data is deleted from memory
// and the client is instructed to delete the session cookie.
//
// Any further operations on the session data *within the same request cycle*
// will result in a panic.
func (s *Session) Destroy(r *http.Request) {
	c := getCacheFromRequestContext(r)

	c.mu.Lock()
	c.Data = nil
	c.Expiry = time.Time{}
	c.modified = true
	c.destroyed = true
	c.mu.Unlock()
}

// GetString returns the string value for a given key from the session data.
// The zero value for a string ("") is returned if the key does not exist or the
// value could not be type asserted to a string.
func (s *Session) GetString(r *http.Request, key string) string {
	val := s.Get(r, key)
	str, ok := val.(string)
	if !ok {
		return ""
	}
	return str
}

// GetBool returns the bool value for a given key from the session data. The
// zero value for a bool (false) is returned if the key does not exist or the
// value could not be type asserted to a bool.
func (s *Session) GetBool(r *http.Request, key string) bool {
	val := s.Get(r, key)
	b, ok := val.(bool)
	if !ok {
		return false
	}
	return b
}

// GetInt returns the int value for a given key from the session data. The
// zero value for an int (0) is returned if the key does not exist or the
// value could not be type asserted to an int.
func (s *Session) GetInt(r *http.Request, key string) int {
	val := s.Get(r, key)
	i, ok := val.(int)
	if !ok {
		return 0
	}
	return i
}

// GetFloat returns the float64 value for a given key from the session data. The
// zero value for an float64 (0) is returned if the key does not exist or the
// value could not be type asserted to a float64.
func (s *Session) GetFloat(r *http.Request, key string) float64 {
	val := s.Get(r, key)
	f, ok := val.(float64)
	if !ok {
		return 0
	}
	return f
}

// GetBytes returns the byte slice ([]byte) value for a given key from the session
// cache. The zero value for a slice (nil) is returned if the key does not exist
// or could not be type asserted to []byte.
func (s *Session) GetBytes(r *http.Request, key string) []byte {
	val := s.Get(r, key)
	b, ok := val.([]byte)
	if !ok {
		return nil
	}
	return b
}

// GetTime returns the time.Time value for a given key from the session data. The
// zero value for a time.Time object is returned if the key does not exist or the
// value could not be type asserted to a time.Time. This can be tested with the
// time.IsZero() method.
func (s *Session) GetTime(r *http.Request, key string) time.Time {
	val := s.Get(r, key)
	t, ok := val.(time.Time)
	if !ok {
		return time.Time{}
	}
	return t
}

// PopString returns the string value for a given key and then deletes it from the
// session data. The zero value for a string ("") is returned if the key does not
// exist or the value could not be type asserted to a string.
func (s *Session) PopString(r *http.Request, key string) string {
	val := s.Pop(r, key)
	str, ok := val.(string)
	if !ok {
		return ""
	}
	return str
}

// PopBool returns the bool value for a given key and then deletes it from the
// session data. The zero value for a bool (false) is returned if the key does not
// exist or the value could not be type asserted to a bool.
func (s *Session) PopBool(r *http.Request, key string) bool {
	val := s.Pop(r, key)
	b, ok := val.(bool)
	if !ok {
		return false
	}
	return b
}

// PopInt returns the int value for a given key and then deletes it from the
// session data. The zero value for an int (0) is returned if the key does not
// exist or the value could not be type asserted to an int.
func (s *Session) PopInt(r *http.Request, key string) int {
	val := s.Pop(r, key)
	i, ok := val.(int)
	if !ok {
		return 0
	}
	return i
}

// PopFloat returns the float64 value for a given key and then deletes it from the
// session data. The zero value for an float64 (0) is returned if the key does not
// exist or the value could not be type asserted to a float64.
func (s *Session) PopFloat(r *http.Request, key string) float64 {
	val := s.Pop(r, key)
	f, ok := val.(float64)
	if !ok {
		return 0
	}
	return f
}

// PopBytes returns the byte slice ([]byte) value for a given key and then deletes
// it from the from the session data. The zero value for a slice (nil) is returned
// if the key does not exist or could not be type asserted to []byte.
func (s *Session) PopBytes(r *http.Request, key string) []byte {
	val := s.Pop(r, key)
	b, ok := val.([]byte)
	if !ok {
		return nil
	}
	return b
}

// PopTime returns the time.Time value for a given key and then deletes it from the
// session data. The zero value for a time.Time object is returned if the key does
// not exist or the value could not be type asserted to a time.Time.
func (s *Session) PopTime(r *http.Request, key string) time.Time {
	val := s.Pop(r, key)
	t, ok := val.(time.Time)
	if !ok {
		return time.Time{}
	}
	return t
}
