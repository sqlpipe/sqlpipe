package sessions

import (
	"net/http"
	"time"
)

func MockRequest(r *http.Request) *http.Request {
	c := newCache(time.Hour)
	return addCacheToRequestContext(r, c)
}
