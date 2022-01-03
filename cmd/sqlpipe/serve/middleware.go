package serve

import (
	"errors"
	"expvar"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/validator"
	"github.com/felixge/httpsnoop"
	"github.com/tomasen/realip"
	"golang.org/x/time/rate"
)

func (app *application) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				w.Header().Set("Connection", "close")
				app.serverErrorResponse(w, r, fmt.Errorf("%s", err))
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func (app *application) logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		app.logger.PrintInfo(
			"Request received",
			map[string]string{
				"Remote address":    r.RemoteAddr,
				"Protocol":          r.Proto,
				"Method":            r.Method,
				"Requested address": r.URL.RequestURI(),
			},
		)

		next.ServeHTTP(w, r)
	})
}

func (app *application) metrics(next http.Handler) http.Handler {
	totalRequestsReceived := expvar.NewInt("total_requests_received")
	totalResponsesSent := expvar.NewInt("total_responses_sent")
	totalProcessingTimeMicroseconds := expvar.NewInt("total_processing_time_μs")

	totalResponsesSentByStatus := expvar.NewMap("total_responses_sent_by_status")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		totalRequestsReceived.Add(1)

		metrics := httpsnoop.CaptureMetrics(next, w, r)

		totalResponsesSent.Add(1)

		totalProcessingTimeMicroseconds.Add(metrics.Duration.Microseconds())

		totalResponsesSentByStatus.Add(strconv.Itoa(metrics.Code), 1)
	})
}

func (app *application) rateLimit(next http.Handler) http.Handler {
	type client struct {
		limiter  *rate.Limiter
		lastSeen time.Time
	}

	var (
		mu      sync.Mutex
		clients = make(map[string]*client)
	)

	go func() {
		for {
			time.Sleep(time.Minute)

			mu.Lock()

			for ip, client := range clients {
				if time.Since(client.lastSeen) > 3*time.Minute {
					delete(clients, ip)
				}
			}

			mu.Unlock()
		}
	}()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if app.config.limiter.enabled {
			ip := realip.FromRequest(r)

			mu.Lock()

			if _, found := clients[ip]; !found {
				clients[ip] = &client{
					limiter: rate.NewLimiter(rate.Limit(app.config.limiter.rps), app.config.limiter.burst),
				}
			}

			clients[ip].lastSeen = time.Now()

			if !clients[ip].limiter.Allow() {
				mu.Unlock()
				app.rateLimitExceededResponse(w, r)
				return
			}

			mu.Unlock()
		}

		next.ServeHTTP(w, r)
	})
}

func (app *application) basicAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
			app.UnauthorizedResponse(w, r)
			return
		}

		v := validator.New()
		data.ValidateUsername(v, username)
		data.ValidatePasswordPlaintext(v, password)

		if !v.Valid() {
			app.failedValidationResponse(w, r, v.Errors)
			return
		}

		user, err := app.models.Users.GetByUsername(username)
		if err != nil {
			switch {
			case errors.Is(err, data.ErrRecordNotFound):
				w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
				app.invalidCredentialsResponse(w, r)
			default:
				app.serverErrorResponse(w, r, err)
			}
			return
		}

		match, err := user.Password.Matches(password)
		if err != nil {
			app.serverErrorResponse(w, r, err)
			return
		}

		if !match {
			w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
			app.invalidCredentialsResponse(w, r)
			return
		}

		r = app.contextSetAdmin(r, user.Admin)

		next.ServeHTTP(w, r)
	})
}

func (app *application) requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		admin := app.contextGetAdmin(r)

		if !admin {
			w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
			app.RequireAdminResponse(w, r)
			return
		}

		next.ServeHTTP(w, r)
	})
}
