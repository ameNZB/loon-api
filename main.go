// loon-api is a standalone, read-only API host for a loon indexer. It boots loon
// in the "api" process and mounts ONLY the Newznab/Torznab search API + NZB
// download, sharing the Postgres the web/worker processes use. No sessions,
// templates, admin, or view system — a thin, horizontally-scalable read tier
// (run several behind a load balancer; point them at a read replica later).
//
// This is the "separate project" shape of the api worker: the host wiring is
// tiny now that every feature lives in loon + the plugins. The web demo boots
// the same plugins in Process "all"; this boots them in "api", so usenet only
// publishes its read capabilities (no crawl jobs, no admin views).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	goredis "github.com/redis/go-redis/v9"

	"github.com/ameNZB/loon/core"
	"github.com/ameNZB/loon/schedule"

	"github.com/ameNZB/loon-baseline/apikey"
	"github.com/ameNZB/loon-baseline/cache"
	cachememory "github.com/ameNZB/loon-baseline/cache/memory"
	cacheredis "github.com/ameNZB/loon-baseline/cache/redis"
	"github.com/ameNZB/loon-baseline/jobsettings"
	"github.com/ameNZB/loon-baseline/ratelimit"
	rlmemory "github.com/ameNZB/loon-baseline/ratelimit/memory"
	rlredis "github.com/ameNZB/loon-baseline/ratelimit/redis"

	"github.com/ameNZB/loon-plugins/pluginapi"
	_ "github.com/ameNZB/loon-plugins/usenet"
)

// apiServiceName is the schedule service the loon-api read tier registers for
// its admin-editable settings (cache TTLs). The WEB admin registers a service
// with this SAME name as a MarkRemote stub so an operator edits the values
// there; loon-api reads them from the shared job_settings table. The names must
// match — that's the join key.
const apiServiceName = "Search API"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	db, err := connect(getenv("LOON_API_DSN", "postgres://demo:demo@localhost:5544/loon_demo?sslmode=disable"))
	if err != nil {
		logger.Error("db connect", "err", err)
		os.Exit(1)
	}

	// Admin-editable settings for this read tier. The values live in the shared
	// job_settings table (keyed by service name) and are edited from the WEB
	// admin's config page; loon-api reads them here. Migrate is idempotent — the
	// web process creates the same table.
	settings := jobsettings.NewPGStore(db.DB)
	if err := settings.Migrate(context.Background()); err != nil {
		logger.Error("job_settings migrate", "err", err)
		os.Exit(1)
	}
	// API-key auth: resolve the ?apikey= a Newznab client sends to a user. Pure
	// read (a SELECT), so this stays replica-safe. Keys are minted/rotated on the
	// web tier; here we only validate. Migrate is idempotent.
	apiKeys := apikey.NewPGStore(db.DB)
	if err := apiKeys.Migrate(context.Background()); err != nil {
		logger.Error("api_keys migrate", "err", err)
		os.Exit(1)
	}

	apiSvc := schedule.RegisterService(apiServiceName, "Newznab/Torznab read tier (loon-api)")
	apiSvc.DeclareConfig(settings,
		schedule.JobConfigVar{Key: "cache_ttl_secs", Label: "Search cache TTL (seconds)", Type: schedule.JobConfigInt, Default: "90",
			Description: "How long search/tvsearch/movie/rss responses stay cached."},
		schedule.JobConfigVar{Key: "caps_ttl_secs", Label: "Caps cache TTL (seconds)", Type: schedule.JobConfigInt, Default: "3600",
			Description: "How long the caps (category tree) response stays cached — nearly static."},
		schedule.JobConfigVar{Key: "rate_per_min", Label: "Requests per minute", Type: schedule.JobConfigInt, Default: "60",
			Description: "Per-API-key (or IP) request cap per minute — burst protection. 0 disables."},
		schedule.JobConfigVar{Key: "rate_per_day", Label: "Requests per day", Type: schedule.JobConfigInt, Default: "10000",
			Description: "Per-API-key (or IP) request cap per day — the daily quota. 0 disables."},
	)

	engine := gin.New()
	engine.Use(gin.Recovery())

	// core.New requires every dep non-nil, but the api process only exercises
	// Storage + Config (usenet). The rest are minimal stubs — no auth, points,
	// notifications, or scheduler work happens here.
	c, err := core.New(core.Deps{
		Process:       "api",
		Users:         core.NewUsers(core.UsersAdapter{}),
		Auth:          core.NewAuth(core.AuthAdapter{}),
		RBAC:          core.NewRBAC(),
		Storage:       core.NewStorage(db),
		Scheduler:     schedule.CoreScheduler(schedule.Default),
		Router:        core.NewRouter(core.RouterAdapter{Engine: engine}),
		Logger:        logger,
		Config:        core.NewConfig(map[string]any{}),
		Notifications: core.NewNotifications(core.NotificationsAdapter{}),
		Points:        core.NewPoints(core.PointsAdapter{}),
		HTTPClient:    core.NewHTTPClient(),
		Errors:        core.NewErrorReporter(core.ErrorAdapter{}),
	})
	if err != nil {
		logger.Error("core.New", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The schedule config cache is loaded once per process and only refreshed on
	// an in-process SetConfig — which this read-only tier never calls. Re-read
	// the shared table periodically so an admin's TTL edit in the web process
	// takes effect here within the interval (no restart, no message bus).
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				apiSvc.RefreshConfig()
			}
		}
	}()

	rt, err := core.Boot(ctx, c)
	if err != nil {
		logger.Error("core.Boot", "err", err)
		os.Exit(1)
	}
	logger.Info("api process booted", "plugins", len(rt.Plugins()))

	var idx pluginapi.UsenetIndex
	var api pluginapi.UsenetNewznab
	if v, ok := c.Lookup(pluginapi.UsenetIndexName); ok {
		idx, _ = v.(pluginapi.UsenetIndex)
	}
	if v, ok := c.Lookup(pluginapi.UsenetNewznabName); ok {
		api, _ = v.(pluginapi.UsenetNewznab)
	}

	// Shared Redis (deployed shape: many api workers, one Redis) backs both the
	// response cache and the rate-limit counters when REDIS_ADDR is set;
	// otherwise both fall back to per-process in-memory impls (dev). One client
	// for both.
	var rdb *goredis.Client
	if addr := getenv("REDIS_ADDR", ""); addr != "" {
		rdb = goredis.NewClient(&goredis.Options{Addr: addr})
	}

	// Read-through cache in front of the Newznab responses — the whole point of
	// a read tier. Best-effort: a Redis outage degrades to serving from the plugin.
	var responses cache.Cache
	var counter ratelimit.Counter
	if rdb != nil {
		responses = cacheredis.New(rdb)
		counter = rlredis.New(rdb)
		logger.Info("response cache + rate limiter", "backend", "redis")
	} else {
		responses = cachememory.New()
		counter = rlmemory.New()
		logger.Info("response cache + rate limiter", "backend", "memory")
	}

	// Per-caller request limiting (burst + daily quota), keyed by the
	// authenticated user (so the quota follows a caller across a key rotation)
	// and falling back to client IP for keyless requests (caps discovery).
	// Limits read live from the admin settings (0 = off). A Newznab client sees
	// the spec's "Request limit reached" error.
	limiter := ratelimit.Middleware(ratelimit.Config{
		Counter: counter,
		Key:     rateKey,
		Rules: []ratelimit.Rule{
			{Name: "min", Window: time.Minute, Limit: func() int { return apiSvc.GetConfigInt("rate_per_min") }},
			{Name: "day", Window: 24 * time.Hour, Limit: func() int { return apiSvc.GetConfigInt("rate_per_day") }},
		},
		OnLimit: newznabLimitError,
	})

	// Newznab auth: a valid ?apikey= is required for everything except caps
	// (capability discovery is keyless, matching common indexer behavior + how
	// Prowlarr probes). Auth runs before the limiter so it can key by user id.
	authAPI := requireAPIKey(apiKeys.Resolve, logger, func(g *gin.Context) bool { return g.Query("t") == "caps" })
	authFeed := requireAPIKey(apiKeys.Resolve, logger, nil)

	engine.GET("/healthz", func(g *gin.Context) { g.String(http.StatusOK, "ok") })
	engine.GET("/api", authAPI, limiter, newznab(api, responses, apiSvc)) // t=caps|search|tvsearch|movie|rss|get
	engine.GET("/rss", authFeed, limiter, newznab(api, responses, apiSvc))
	engine.GET("/nzb/:id", nzb(idx))

	srv := &http.Server{Addr: getenv("LOON_API_ADDR", ":8091"), Handler: engine}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http", "err", err)
			stop()
		}
	}()
	logger.Info("listening", "addr", srv.Addr)

	<-ctx.Done()
	sc, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(sc)
}

// cachedResp is the serialized Newznab response stored in the cache.
type cachedResp struct {
	Body        []byte `json:"b"`
	ContentType string `json:"c"`
	Filename    string `json:"f"`
}

func newznab(api pluginapi.UsenetNewznab, ca cache.Cache, svc *schedule.JobInfo) gin.HandlerFunc {
	return func(g *gin.Context) {
		if api == nil {
			g.String(http.StatusServiceUnavailable, "indexer not configured")
			return
		}
		limit, _ := strconv.Atoi(g.Query("limit"))
		offset, _ := strconv.Atoi(g.Query("offset"))
		req := pluginapi.NewznabRequest{
			Function:   g.Query("t"),
			Query:      g.Query("q"),
			Categories: parseCats(g.Query("cat")),
			Limit:      limit,
			Offset:     offset,
			ID:         g.Query("id"),
			BaseURL:    baseURL(g),
			Title:      "loon api",
			APIKey:     g.Query("apikey"),
		}

		// Cache read functions only. t=get streams a (potentially large) NZB —
		// don't hold those in Redis.
		cacheable := ca != nil && req.Function != "get"
		var key string
		if cacheable {
			key = newznabKey(req)
			var cr cachedResp
			if ok, _ := cache.GetJSON(g.Request.Context(), ca, key, &cr); ok {
				writeResp(g, cr, "hit")
				return
			}
		}

		res, err := api.Newznab(g.Request.Context(), req)
		if err != nil {
			g.String(http.StatusInternalServerError, "api error")
			return
		}
		cr := cachedResp{Body: res.Body, ContentType: res.ContentType, Filename: res.Filename}
		if cacheable {
			_ = cache.SetJSON(g.Request.Context(), ca, key, cr, ttlFor(svc, req.Function))
		}
		writeResp(g, cr, "miss")
	}
}

func writeResp(g *gin.Context, cr cachedResp, status string) {
	if cr.Filename != "" {
		g.Header("Content-Disposition", `attachment; filename="`+cr.Filename+`"`)
	}
	g.Header("X-Cache", status)
	g.Data(http.StatusOK, cr.ContentType, cr.Body)
}

// newznabKey hashes the request fields that determine the response. BaseURL is
// excluded (constant per deployment / public host); APIKey is INCLUDED because
// the plugin embeds it in the download links, so two keys must not share an
// entry.
func newznabKey(r pluginapi.NewznabRequest) string {
	payload := struct {
		T, Q  string
		C     []int
		L, O  int
		ID, K string
	}{r.Function, r.Query, r.Categories, r.Limit, r.Offset, r.ID, r.APIKey}
	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return "newznab:v1:" + hex.EncodeToString(sum[:16])
}

// ttlFor picks a per-function TTL from the read tier's admin-editable settings
// (refreshed periodically from the shared job_settings table). Caps are ~static
// (the category tree) so they cache far longer than search/feed results. A
// non-positive override (admin typo, or a var not yet declared) falls back to a
// sane built-in so the cache never gets a 0 / negative TTL.
func ttlFor(svc *schedule.JobInfo, fn string) time.Duration {
	key, fallback := "cache_ttl_secs", 90
	if fn == "caps" {
		key, fallback = "caps_ttl_secs", 3600
	}
	secs := svc.GetConfigInt(key)
	if secs <= 0 {
		secs = fallback
	}
	return time.Duration(secs) * time.Second
}

// ctxUserID is the gin context key holding the authenticated user id (int64)
// once requireAPIKey resolves a valid key.
const ctxUserID = "uid"

// requireAPIKey authenticates a request by its ?apikey=. On success it stashes
// the user id for the limiter/handlers and continues. allowKeyless (may be nil)
// lets specific requests through without a key — used for caps discovery. A
// resolve error (DB blip) fails closed: we don't serve what we can't
// authenticate. Everything else gets the Newznab "incorrect credentials" error.
func requireAPIKey(resolve apikey.Resolver, logger *slog.Logger, allowKeyless func(*gin.Context) bool) gin.HandlerFunc {
	return func(g *gin.Context) {
		uid, ok, err := resolve(g.Request.Context(), g.Query("apikey"))
		if err != nil {
			logger.Warn("apikey resolve", "err", err)
		}
		if ok {
			g.Set(ctxUserID, uid)
			g.Next()
			return
		}
		if allowKeyless != nil && allowKeyless(g) {
			g.Next()
			return
		}
		newznabAuthError(g)
		g.Abort()
	}
}

// rateKey attributes a request to a caller for rate limiting: the authenticated
// user id when present (so a quota survives an API-key rotation), else the
// client IP for keyless (caps) requests.
func rateKey(g *gin.Context) string {
	if uid, ok := g.Get(ctxUserID); ok {
		return "u:" + strconv.FormatInt(uid.(int64), 10)
	}
	return "ip:" + g.ClientIP()
}

// newznabAuthError renders a missing/invalid-key rejection as a Newznab error
// document (code 100 = "Incorrect user credentials" in the spec) + HTTP 401.
func newznabAuthError(g *gin.Context) {
	g.Data(http.StatusUnauthorized, "application/xml; charset=utf-8",
		[]byte(`<?xml version="1.0" encoding="UTF-8"?>`+"\n"+`<error code="100" description="Incorrect user credentials"/>`))
}

// newznabLimitError renders an over-limit rejection as a Newznab error document
// (code 500 = "Request limit reached" in the Newznab spec) so Prowlarr/Sonarr
// surface it correctly, alongside the standard 429 + Retry-After the middleware
// already set.
func newznabLimitError(g *gin.Context, _ time.Duration) {
	g.Data(http.StatusTooManyRequests, "application/xml; charset=utf-8",
		[]byte(`<?xml version="1.0" encoding="UTF-8"?>`+"\n"+`<error code="500" description="Request limit reached"/>`))
}

func nzb(idx pluginapi.UsenetIndex) gin.HandlerFunc {
	return func(g *gin.Context) {
		if idx == nil {
			g.String(http.StatusServiceUnavailable, "indexer not configured")
			return
		}
		id, _ := strconv.ParseInt(g.Param("id"), 10, 64)
		data, filename, err := idx.NZB(g.Request.Context(), id)
		if err != nil {
			g.String(http.StatusNotFound, "not found")
			return
		}
		g.Header("Content-Disposition", `attachment; filename="`+filename+`"`)
		g.Data(http.StatusOK, "application/x-nzb", data)
	}
}

func parseCats(s string) []int {
	var out []int
	for _, p := range strings.Split(s, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
			out = append(out, n)
		}
	}
	return out
}

func baseURL(g *gin.Context) string {
	scheme := "http"
	if g.Request.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + g.Request.Host
}

func connect(dsn string) (*sqlx.DB, error) {
	var db *sqlx.DB
	var err error
	for i := 0; i < 10; i++ {
		if db, err = sqlx.Connect("postgres", dsn); err == nil {
			return db, nil
		}
		time.Sleep(2 * time.Second)
	}
	return nil, err
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
