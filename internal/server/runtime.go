package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/auditlog"
	"github.com/komari-monitor/komari/database/metricstore"
	d_notification "github.com/komari-monitor/komari/database/notification"
	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/internal/config"
	"github.com/komari-monitor/komari/internal/scheduler"
	"github.com/komari-monitor/komari/utils"
	"github.com/komari-monitor/komari/utils/geoip"
	logger "github.com/komari-monitor/komari/utils/log"
	"github.com/komari-monitor/komari/utils/messageSender"
	"github.com/komari-monitor/komari/utils/notifier"
	"github.com/komari-monitor/komari/utils/visitorsecurity"
	"github.com/komari-monitor/komari/web/api"
	"github.com/komari-monitor/komari/web/oauth"
	recoveryweb "github.com/komari-monitor/komari/web/recovery"
	"github.com/komari-monitor/komari/web/router"
	"github.com/komari-monitor/komari/web/security"
)

// StartBackground starts scheduled work after all stores are ready.
func (a *App) StartBackground() error {
	registerScheduledWork()
	a.addCleanup("scheduler", func(context.Context) error {
		scheduler.StopAll()
		return nil
	})
	return nil
}

func (a *App) registerReloadHandlers(cors *security.CorsController) {
	a.reload.Register("oauth-provider", func(event config.ConfigEvent) {
		if ok, providerName := config.IsChangedT[string](event, config.OAuthProviderKey); ok {
			if providerName == "" || providerName == "none" {
				providerName = "github"
			}
			oidcProvider, err := database.GetOidcConfigByName(providerName)
			if err != nil {
				logger.Errorf("server", "Failed to get OIDC provider config: %v", err)
				return
			}
			logger.Infof("server", "Using %s as OIDC provider", oidcProvider.Name)
			if err := oauth.LoadProvider(oidcProvider.Name, oidcProvider.Addition); err != nil {
				auditlog.EventLog("error", fmt.Sprintf("Failed to load OIDC provider: %v", err))
			}
		}
	})
	a.reload.Register("geoip-provider", func(event config.ConfigEvent) {
		if event.IsChanged(config.GeoIpProviderKey) {
			go geoip.InitGeoIp()
		}
	})
	a.reload.Register("message-sender", func(event config.ConfigEvent) {
		if event.IsChanged(config.NotificationMethodKey) {
			go messageSender.Initialize()
		}
	})
	a.reload.Register("visitor-security", func(event config.ConfigEvent) {
		if event.IsChanged(config.VisitorNotificationEnabledKey) ||
			event.IsChanged(config.VisitorNotificationCooldownMinutesKey) ||
			event.IsChanged(config.VisitorNotificationWhitelistKey) ||
			event.IsChanged(config.VisitorIPBlocklistKey) {
			if err := visitorsecurity.Reload(); err != nil {
				logger.Errorf("server", "Failed to reload visitor security settings: %v", err)
			}
		}
	})
	a.reload.Register("cors", func(event config.ConfigEvent) { cors.Update(event) })
}

// BuildRouter constructs the normal application router and starts reloads.
func (a *App) BuildRouter() error {
	r := gin.New()
	if err := r.SetTrustedProxies(trustedReverseProxies()); err != nil {
		return fmt.Errorf("configure trusted reverse proxies: %w", err)
	}
	if err := visitorsecurity.Reload(); err != nil {
		return fmt.Errorf("load visitor security settings: %w", err)
	}
	r.Use(logger.GinLogger(), logger.GinRecovery())
	cors := security.NewCorsController(a.settings.CorsOriginCheckEnabled, a.settings.CorsAllowedOrigins)
	r.Use(cors.Middleware(), api.IdentityMiddleware(), api.VisitorIPBlockMiddleware(), api.PrivateSiteMiddleware(), noStoreAPIResponses())

	// The recovery UI belongs only to its temporary restricted listener.
	r.GET(recoveryweb.PagePath, func(c *gin.Context) {
		c.Redirect(http.StatusTemporaryRedirect, "/")
	})
	router.Register(r)
	a.registerReloadHandlers(cors)
	a.reload.Start()
	a.engine = r
	return nil
}

func trustedReverseProxies() []string {
	configured := strings.TrimSpace(os.Getenv("KOMARI_TRUSTED_PROXIES"))
	if configured == "" {
		return []string{"127.0.0.1", "::1"}
	}
	proxies := strings.FieldsFunc(configured, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\r' || r == '\t' || r == ' '
	})
	if len(proxies) == 0 {
		return []string{"127.0.0.1", "::1"}
	}
	return proxies
}

// Run starts the normal HTTP server and blocks until shutdown or fatal error.
func (a *App) Run() error {
	a.server = &http.Server{Addr: a.listenAddr, Handler: a.engine}
	serverErr := make(chan error, 1)
	logger.Infof("server", "Starting server on %s ...", a.listenAddr)
	go func() {
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(quit)
	select {
	case err := <-serverErr:
		a.onFatal(err)
		return fmt.Errorf("listen: %w", err)
	case <-quit:
		return a.Shutdown()
	}
}

// Shutdown stops HTTP first, then releases registered resources in LIFO order.
func (a *App) Shutdown() error {
	if a.dbReady {
		auditlog.Log("", "", "server is shutting down", "info")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if a.server != nil {
		if err := a.server.Shutdown(ctx); err != nil {
			logger.Infof("server", "HTTP server forced to shutdown: %v", err)
		}
	}
	a.runCleanups(ctx)
	return nil
}

func (a *App) onFatal(err error) {
	if a.dbReady {
		auditlog.Log("", "", "server encountered a fatal error: "+err.Error(), "error")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a.runCleanups(ctx)
}

func (a *App) runCleanups(ctx context.Context) {
	for i := len(a.cleanups) - 1; i >= 0; i-- {
		cleanup := a.cleanups[i]
		if err := cleanup.fn(ctx); err != nil {
			logger.Errorf("server", "cleanup %q failed: %v", cleanup.name, err)
		}
	}
}

func registerScheduledWork() {
	if err := tasks.ReloadPingSchedule(); err != nil {
		logger.ErrorArgs("server", "Failed to reload ping schedule:", err)
	}
	if err := tasks.ReloadTCPQualitySchedule(); err != nil {
		logger.ErrorArgs("server", "Failed to reload TCP quality schedule:", err)
	}
	if err := tasks.ReloadUnlockQualitySchedule(); err != nil {
		logger.ErrorArgs("server", "Failed to reload unlock quality schedule:", err)
	}
	if err := d_notification.ReloadLoadNotificationSchedule(); err != nil {
		logger.ErrorArgs("server", "Failed to reload load notification schedule:", err)
	}
	if err := scheduler.AddFunc("records:cleanup", "@every 30m", cleanupScheduledData); err != nil {
		logger.ErrorArgs("server", "Failed to add cleanup scheduled task:", err)
	}
	if err := scheduler.AddContextFunc("metrics:compact", "@every 5m", true, compactMetricStore); err != nil {
		logger.ErrorArgs("server", "Failed to add metric compact scheduled task:", err)
	}
	if err := scheduler.AddContextFunc("tcp-quality:catalog", "@every 10m", true, refreshTCPQualityCatalog); err != nil {
		logger.ErrorArgs("server", "Failed to add TCP quality catalog refresh:", err)
	}
	if err := scheduler.AddContextFunc("tcp-quality:snapshots", "@every 5m", true, refreshTCPQualitySnapshots); err != nil {
		logger.ErrorArgs("server", "Failed to add TCP quality snapshot refresh:", err)
	}
	if err := scheduler.AddContextFunc("unlock-quality:snapshots", "@every 1m", true, refreshUnlockQualitySnapshots); err != nil {
		logger.ErrorArgs("server", "Failed to add unlock quality snapshot refresh:", err)
	}
	if err := scheduler.AddFunc("notifier:traffic", "@every 1m", notifier.CheckTraffic); err != nil {
		logger.ErrorArgs("server", "Failed to add traffic notification task:", err)
	}
	if err := scheduler.AddFunc("notifier:expire", "0 0 9 * * *", notifier.CheckExpireScheduledWork); err != nil {
		logger.ErrorArgs("server", "Failed to add expire notification task:", err)
	}
	notifier.InitTrafficReportSchedule()
}

const taskResultRetentionDays = 30

func cleanupScheduledData() {
	before := time.Now().UTC().Add(-24 * time.Hour * taskResultRetentionDays)
	if err := tasks.ClearTaskResultsByTimeBefore(before); err != nil {
		logger.Errorf("server", "Failed to clean expired task results: %v", err)
	}
	if err := tasks.ClearTCPQualityRunsBefore(before); err != nil {
		logger.Errorf("server", "Failed to clean expired TCP quality runs: %v", err)
	}
	unlockBefore := time.Now().UTC().Add(-7 * 24 * time.Hour)
	if err := tasks.ClearUnlockQualityRunsBefore(unlockBefore); err != nil {
		logger.Errorf("server", "Failed to clean expired unlock quality runs: %v", err)
	}
	auditlog.RemoveOldLogs()
	accounts.RemoveExpiredSessions()
}

func refreshTCPQualityCatalog(ctx context.Context) {
	refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := utils.GetTCPQualityCatalog(refreshCtx, true); err != nil {
		logger.Errorf("server", "Failed to refresh TCP quality catalog: %v", err)
	}
}

func refreshTCPQualitySnapshots(ctx context.Context) {
	refreshCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	if err := tasks.RefreshTCPQualitySnapshots(refreshCtx); err != nil {
		logger.Errorf("server", "Failed to refresh TCP quality snapshots: %v", err)
	}
}

func refreshUnlockQualitySnapshots(ctx context.Context) {
	refreshCtx, cancel := context.WithTimeout(ctx, 50*time.Second)
	defer cancel()
	if err := tasks.RefreshUnlockQualitySnapshots(refreshCtx); err != nil {
		logger.Errorf("server", "Failed to refresh unlock quality snapshots: %v", err)
	}
}

func compactMetricStore(ctx context.Context) {
	compactCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	written, err := metricstore.Compact(compactCtx, time.Now().UTC())
	if errors.Is(err, metricstore.ErrCompactInProgress) {
		return
	}
	if err != nil {
		logger.Errorf("server", "Failed to compact metric store after writing %d rollup buckets: %v", written, err)
		return
	}
	if written > 0 {
		logger.Infof("server", "Metric store compacted %d rollup buckets", written)
	}
}
