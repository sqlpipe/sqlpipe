package serve

import (
	"context"
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
	"github.com/justinas/nosurf"
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

func (app *application) authenticateApi(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok {
			ctx := context.WithValue(r.Context(), userContextKey, data.AnonymousUser)
			next.ServeHTTP(w, r.WithContext(ctx))
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
			app.invalidCredentialsResponse(w, r)
			return
		}

		r = app.contextSetUser(r, user)

		next.ServeHTTP(w, r)
	})
}

func (app *application) requireAuthApi(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := app.contextGetUser(r)

		if user.IsAnonymous() {
			app.authenticationRequiredResponse(w, r)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (app *application) requireAdminApi(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := app.contextGetUser(r)

		if !user.Admin {
			w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
			app.RequireAdminResponse(w, r)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func secureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("X-Frame-Options", "deny")

		next.ServeHTTP(w, r)
	})
}

func noSurf(next http.Handler) http.Handler {
	csrfHandler := nosurf.New(next)
	csrfHandler.SetBaseCookie(http.Cookie{
		HttpOnly: true,
		Path:     "/",
		Secure:   true,
	})

	return csrfHandler
}

func (app *application) authenticateUi(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userId := app.session.GetInt(r, "authenticatedUserID")
		if userId == 0 {
			ctx := context.WithValue(r.Context(), userContextKey, data.AnonymousUser)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		user, err := app.models.Users.GetById(int64(userId))
		if err == nil {
			ctx := context.WithValue(r.Context(), userContextKey, user)
			next.ServeHTTP(w, r.WithContext(ctx))
		} else if errors.Is(err, data.ErrRecordNotFound) {
			ctx := context.WithValue(r.Context(), userContextKey, data.AnonymousUser)
			next.ServeHTTP(w, r.WithContext(ctx))
		} else {
			app.serverErrorResponse(w, r, err)
		}
	})
}

func (app *application) isAuthenticated(r *http.Request) bool {
	user := app.contextGetUser(r)
	return !user.IsAnonymous()
}

func (app *application) isAdmin(r *http.Request) bool {
	user := app.contextGetUser(r)
	return user.Admin
}

func (app *application) requireAuthUi(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !app.isAuthenticated(r) {
			http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
			return
		}

		w.Header().Add("Cache-Control", "no-store")

		next.ServeHTTP(w, r)
	})
}

func (app *application) requireAdminUi(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := app.contextGetUser(r)

		if !user.Admin {
			fmt.Fprint(w, "You must be logged in as an admin to access this resource")
			return
		}

		next.ServeHTTP(w, r)
	})
}
