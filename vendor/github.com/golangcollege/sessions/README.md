# Sessions [![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/golangcollege/sessions) [![Go Report Card](https://goreportcard.com/badge/github.com/golangcollege/sessions?style=flat-square)](https://goreportcard.com/report/github.com/golangcollege/sessions) [![Build Status](http://img.shields.io/travis/golangcollege/sessions.svg?style=flat-square)](https://travis-ci.org/golangcollege/sessions) [![License](http://img.shields.io/badge/license-mit-blue.svg?style=flat-square)](https://raw.githubusercontent.com/golangcollege/sessions/master/LICENSE)

A minimalist and lightweight HTTP session cookie implementation for Go 1.11+. Session cookies are encrypted and authenticated using [nacl/secretbox](https://godoc.org/golang.org/x/crypto/nacl/secretbox).

## Example Usage

```go
package main

import (
	"net/http"
	"time"

	"github.com/golangcollege/sessions"
)

var session *sessions.Session
var secret = []byte("u46IpCV9y5Vlur8YvODJEhgOY8m9JVE4")

func main() {
	// Initialize and configure new session instance, passing in a 32 byte
	// long secret key (which is used to encrypt and authenticate the
	// session data).
	session = sessions.New(secret)
	session.Lifetime = 3 * time.Hour

	mux := http.NewServeMux()
	mux.HandleFunc("/put", putHandler)
	mux.HandleFunc("/get", getHandler)

	// Wrap your handlers with the session middleware.
	http.ListenAndServe(":4000", session.Enable(mux))
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	// Use the Put() method to store a new key and associated value in the
	// session data.
	session.Put(r, "msg", "Hello world")
	w.WriteHeader(200)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
    // Use the GetString() method helper to retrieve the value associated with
    // a key and convert it to a string. The empty string is returned if the
    // key does not exist in the session data.
	msg := session.GetString(r, "msg")
	w.Write([]byte(msg))
}
```

## Configuring sessions

When setting up a session instance you can specify a mixture of options, or none at all if you're happy with the defaults.

```go
session = sessions.New([]byte("u46IpCV9y5Vlur8YvODJEhgOY8m9JVE4"))

// Domain sets the 'Domain' attribute on the session cookie. By default
// it will be set to the domain name that the cookie was issued from.
session.Domain = "example.org"

// HttpOnly sets the 'HttpOnly' attribute on the session cookie. The
// default value is true.
session.HttpOnly = false

// Lifetime sets the maximum length of time that a session is valid for
// before it expires. The lifetime is an 'absolute expiry' which is set when
// the session is first created and does not change. The default value is 24
// hours.
session.Lifetime = 10*time.Minute

// Path sets the 'Path' attribute on the session cookie. The default value
// is "/". Passing the empty string "" will result in it being set to the
// path that the cookie was issued from.
session.Path = "/account"

// Persist sets whether the session cookie should be persistent or not
// (i.e. whether it should be retained after a user closes their browser).
// The default value is true, which means that the session cookie will not
// be destroyed when the user closes their browser and the appropriate
// 'Expires' and 'MaxAge' values will be added to the session cookie.
session.Persist = false

// Secure sets the 'Secure' attribute on the session cookie. The default
// value is false. It's recommended that you set this to true and serve all
// requests over HTTPS in production environments.
session.Secure = true

// SameSite controls the value of the 'SameSite' attribute on the session
// cookie. By default this is set to 'SameSite=Lax'. If you want no SameSite
// attribute or value in the session cookie then you should set this to 0.
session.SameSite = http.SameSiteStrictMode

// ErrorHandler allows you to control behaviour when an error is encountered
// loading or writing the session cookie. By default the client is sent a
// generic "500 Internal Server Error" response and the actual error message
// is logged using the standard logger. If a custom ErrorHandler function is
// provided then control will be passed to this instead.
session.ErrorHandler  = func(http.ResponseWriter, *http.Request, error) {
	log.Println(err.Error())
    http.Error(w, "Sorry, the application encountered an error", 500)
}
```

### Key rotation

Secret key rotation is supported. An arbitrary number of old secret keys can be provided when initializing a new session instance, like so:

```go
secretKey := []byte("Nrqe6etTZ68GymwxsgpjqwecHqyKLQrr")
oldSecretKey := []byte("TSV2GUduLGYwMkVcssFrHwCHXLhfBH5e")
veryOldSecretKey := []byte("mtuKkskgHwfJzzP56apvNWzrbqfKHvTB")

session = sessions.New(secretKey, oldSecretKey, veryOldSecretKey)
session.Lifetime = 3 * time.Hour
```

When a session cookie is received from a client, all secret keys are looped through to try to decode the session data. When sending the session cookie to a client the first secret key is used to encrypt the session data.

## Managing session data

### Adding data

* [`Put()`]() &mdash; Add a key and corresponding value to the session data.

**Important:** Because session data is encrypted, signed and stored in a cookie, and cookies are limited to 4096 characters in length, storing large amounts of data may result in a [`ErrCookieTooLong`](https://godoc.org/github.com/golangcollege/sessions#pkg-variables) error.

### Fetching data

* [`Get()`]() &mdash; Fetch the value for a given key from the session data. The returned type is `interface{}` so will usually need to be type asserted before use.
* [`GetBool()`]() &mdash; Fetch a `bool` value for a given key from the session data.
* [`GetBytes()`]() &mdash; Fetch a byte slice (`[]byte`) value for a given key from the session data.
* [`GetFloat()`]() &mdash; Fetch a `float64` value for a given key from the session data.
* [`GetInt()`]() &mdash; Fetch a `int` value for a given key from the session data.
* [`GetString()`]() &mdash; Fetch a `string` value for a given key from the session data.
* [`GetTime()`]() &mdash; Fetch a `time.Time` value for a given key from the session data.

* [`Pop()`]() &mdash; Fetch the value for a given key and then delete it from the session data. The returned type is `interface{}` so will usually need to be type asserted before use.
* [`PopBool()`]() &mdash; Fetch a `bool` value for a given key and then delete it from the session data.
* [`PopBytes()`]() &mdash;  Fetch a byte slice (`[]byte`) value for a given key and then delete it from the session data.
* [`PopFloat()`]() &mdash;  Fetch a `float64` value for a given key and then delete it from the session data.
* [`PopInt()`]() &mdash; Fetch a `int` value for a given key and then delete it from the session data.
* [`PopString()`]() &mdash;  Fetch a `string` value for a given key and then delete it from the session data.
* [`PopTime()`]() &mdash;  Fetch a `time.Time` value for a given key and then delete it from the session data.

### Deleting data

* [`Remove()`]() &mdash; Deletes a specific key and value from the session data.
* [`Destroy()`]() &mdash; Destroy the current session. The session data is deleted from memory and the client is instructed to delete the session cookie.

### Other

* [`Exists()`]() &mdash; Returns `true` if a given key exists in the session data.
* [`Keys()`]() &mdash; Returns a slice of all keys in the session data.

### Custom data types

Behind the scenes SCS uses gob encoding to store custom data types. For this to work properly:

* Your custom type must first be registered with the encoding/gob package.
* The fields of your custom types must be exported.

For example:

```go
package main

import (
    "encoding/gob"
    "errors"
    "fmt"
    "log"
    "net/http"
    "time"

    "github.com/golangcollege/sessions"
)

var session *sessions.Session
var secret = []byte("u46IpCV9y5Vlur8YvODJEhgOY8m9JVE4")

// Note that the fields on the custom type are all exported.
type User struct {
    Name  string
    Email string
}

func main() {
    // Register the type with the encoding/gob package.
    gob.Register(User{})

    session = sessions.New(secret)
    session.Lifetime = 3 * time.Hour

    mux := http.NewServeMux()
    mux.HandleFunc("/put", putHandler)
    mux.HandleFunc("/get", getHandler)
    http.ListenAndServe(":4000", session.Enable(mux))
}

func putHandler(w http.ResponseWriter, r *http.Request) {
    user := User{"Alice", "alice@example.com"}
    session.Put(r, "user", user)
    w.WriteHeader(200)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
    user, ok := session.Get(r, "user").(User)
    if !ok {
        log.Println(errors.New("type assertion to User failed"))
        http.Error(w, http.StatusText(500), 500)
        return
    }

    fmt.Fprintf(w, "Name: %s, Email: %s", user.Name, user.Email)
}
```
