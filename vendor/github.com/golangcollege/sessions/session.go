// Package sessions provides a minimalist and lightweight HTTP session cookie
// implementation for Go. Session cookies are encrypted and authenticated using
// nacl/secretbox.
//
// Example usage:
//	package main
//
//	import (
//		"net/http"
//		"time"
//
//		"github.com/golangcollege/sessions"
//	)
//
//	var session *sessions.Session
//	var secret = []byte("u46IpCV9y5Vlur8YvODJEhgOY8m9JVE4")
//
//	func main() {
//		session = sessions.New(secret)
//		session.Lifetime = 3 * time.Hour
//
//		mux := http.NewServeMux()
//		mux.HandleFunc("/put", putHandler)
//		mux.HandleFunc("/get", getHandler)
//		http.ListenAndServe(":4000", session.Enable(mux))
//	}
//
//	func putHandler(w http.ResponseWriter, r *http.Request) {
//		session.Put(r, "msg", "Hello world")
//		w.WriteHeader(200)
//	}
//
//	func getHandler(w http.ResponseWriter, r *http.Request) {
//		msg := session.GetString(r, "msg")
//		w.Write([]byte(msg))
//	}
//
package sessions

import (
	"bufio"
	"bytes"
	"errors"
	"log"
	"net"
	"net/http"
	"time"
)

const cookieName = "session"

var ErrCookieTooLong = errors.New("session: cookie length greater than 4096 bytes")

// Session holds the configuration settings that you want to use for your sessions.
type Session struct {
	// Domain sets the 'Domain' attribute on the session cookie. By default
	// it will be set to the domain name that the cookie was issued from.
	Domain string

	// HttpOnly sets the 'HttpOnly' attribute on the session cookie. The
	// default value is true.
	HttpOnly bool

	// Lifetime sets the maximum length of time that a session is valid for
	// before it expires. The lifetime is an 'absolute expiry' which is set when
	// the session is first created and does not change. The default value is 24
	// hours.
	Lifetime time.Duration

	// Path sets the 'Path' attribute on the session cookie. The default value
	// is "/". Passing the empty string "" will result in it being set to the
	// path that the cookie was issued from.
	Path string

	// Persist sets whether the session cookie should be persistent or not
	// (i.e. whether it should be retained after a user closes their browser).
	// The default value is true, which means that the session cookie will not
	// be destroyed when the user closes their browser and the appropriate
	// 'Expires' and 'MaxAge' values will be added to the session cookie.
	Persist bool

	// Secure sets the 'Secure' attribute on the session cookie. The default
	// value is false. It's recommended that you set this to true and serve all
	// requests over HTTPS in production environments.
	Secure bool

	// SameSite controls the value of the 'SameSite' attribute on the session
	// cookie. By default this is set to 'SameSite=Lax'. If you want no SameSite
	// attribute or value in the session cookie then you should set this to 0.
	SameSite http.SameSite

	// ErrorHandler allows you to control behaviour when an error is encountered
	// loading or writing the session cookie. By default the client is sent a
	// generic "500 Internal Server Error" response and the actual error message
	// is logged using the standard logger. If a custom ErrorHandler function is
	// provided then control will be passed to this instead.
	ErrorHandler func(http.ResponseWriter, *http.Request, error)
	keys         [][32]byte
}

// New initializes a new Session object to hold the configuration settings for
// your sessions.
//
// The key parameter is the secret you want to use to authenticate and encrypt
// session cookies. It should be exactly 32 bytes long.
//
// Optionally, the variadic oldKeys parameter can be used to provide an arbitrary
// number of old Keys. This can be used to ensure that valid cookies continue
// to work correctly after key rotation.
func New(key []byte, oldKeys ...[]byte) *Session {
	keys := make([][32]byte, 1)
	copy(keys[0][:], key)

	for _, key := range oldKeys {
		var newKey [32]byte
		copy(newKey[:], key)
		keys = append(keys, newKey)
	}

	return &Session{
		Domain:       "",
		HttpOnly:     true,
		Lifetime:     24 * time.Hour,
		Path:         "/",
		Persist:      true,
		Secure:       false,
		SameSite:     http.SameSiteLaxMode,
		ErrorHandler: defaultErrorHandler,
		keys:         keys,
	}
}

// Enable is middleware which loads and saves session data to and from the
// session cookie. You should use this middleware to wrap ALL handlers which
// need to access to the session data. A common way to do this is to wrap your
// servemux.
//
// Note that session cookies are only sent to the client when the session data
// has been modified.
func (s *Session) Enable(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error

		c, ok := r.Context().Value(contextKeyCache).(*cache)
		if !ok {
			c, err = s.load(r)
			if err != nil {
				s.ErrorHandler(w, r, err)
				return
			}
			r = addCacheToRequestContext(r, c)
		}

		bw := &bufferedResponseWriter{ResponseWriter: w}
		next.ServeHTTP(bw, r)

		err = s.save(w, c)
		if err != nil {
			s.ErrorHandler(w, r, err)
			return
		}

		if bw.code != 0 {
			w.WriteHeader(bw.code)
		}
		w.Write(bw.buf.Bytes())
	})
}

func (s *Session) load(r *http.Request) (*cache, error) {
	cookie, err := r.Cookie(cookieName)
	if err == http.ErrNoCookie {
		return newCache(s.Lifetime), nil
	} else if err != nil {
		return nil, err
	}

	c := &cache{}
	err = c.decode(cookie.Value, s.keys)
	if err == errInvalidToken {
		return newCache(s.Lifetime), nil
	} else if err != nil {
		return nil, err
	}

	if time.Now().After(c.Expiry) {
		return newCache(s.Lifetime), nil
	}

	return c, nil
}

func (s *Session) save(w http.ResponseWriter, c *cache) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.modified {
		return nil
	}

	if c.destroyed {
		http.SetCookie(w, &http.Cookie{
			Name:     cookieName,
			Value:    "",
			Path:     s.Path,
			Domain:   s.Domain,
			Secure:   s.Secure,
			HttpOnly: s.HttpOnly,
			SameSite: s.SameSite,
			Expires:  time.Unix(1, 0),
			MaxAge:   -1,
		})
		return nil
	}

	token, err := c.encode(s.keys[0])
	if err != nil {
		return err
	}

	cookie := &http.Cookie{
		Name:     cookieName,
		Value:    token,
		Path:     s.Path,
		Domain:   s.Domain,
		Secure:   s.Secure,
		HttpOnly: s.HttpOnly,
		SameSite: s.SameSite,
	}
	if s.Persist {
		cookie.Expires = time.Unix(c.Expiry.Unix()+1, 0)        // Round up to the nearest second.
		cookie.MaxAge = int(time.Until(c.Expiry).Seconds() + 1) // Round up to the nearest second.
	}

	if len(cookie.String()) > 4096 {
		return ErrCookieTooLong
	}
	w.Header().Add("Vary", "Cookie")
	http.SetCookie(w, cookie)

	return nil
}

type bufferedResponseWriter struct {
	http.ResponseWriter
	buf  bytes.Buffer
	code int
}

func (bw *bufferedResponseWriter) Write(b []byte) (int, error) {
	return bw.buf.Write(b)
}

func (bw *bufferedResponseWriter) WriteHeader(code int) {
	bw.code = code
}

func (bw *bufferedResponseWriter) Push(target string, opts *http.PushOptions) error {
	if pusher, ok := bw.ResponseWriter.(http.Pusher); ok {
		return pusher.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (bw *bufferedResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj := bw.ResponseWriter.(http.Hijacker)
	return hj.Hijack()
}

func (bw *bufferedResponseWriter) Flush() {
	f, ok := bw.ResponseWriter.(http.Flusher)
	if ok == true {
		bw.ResponseWriter.Write(bw.buf.Bytes())
		f.Flush()
		bw.buf.Reset()
	}
}

func defaultErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	log.Output(2, err.Error())
	http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
}
