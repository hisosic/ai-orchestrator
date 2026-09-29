package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"ai-container-go/internal/secrets"
	"ai-container-go/internal/server"
	"ai-container-go/internal/usersecrets"
)

func main() {
	// Read dashboard HTML
	dashboardPath := "static/dashboard.html"
	if data, err := os.ReadFile(dashboardPath); err == nil {
		server.DashboardHTML = string(data)
	} else {
		// Try alternative paths
		altPaths := []string{
			"/app/static/dashboard.html",
			"src/orchestrator/static/dashboard.html",
		}
		for _, p := range altPaths {
			if data, err := os.ReadFile(p); err == nil {
				server.DashboardHTML = string(data)
				break
			}
		}
	}

	// Read the self-service user portal page.
	for _, p := range []string{"static/portal.html", "/app/static/portal.html", "src/orchestrator/static/portal.html"} {
		if data, err := os.ReadFile(p); err == nil {
			server.PortalHTML = string(data)
			break
		}
	}

	// Initialize the secret-encryption master key (loads or generates one
	// under ORCHESTRATOR_STATE_DIR). Required before any service handler
	// can encrypt or decrypt env-var values.
	stateDir := strings.TrimSpace(os.Getenv("ORCHESTRATOR_STATE_DIR"))
	if stateDir == "" {
		stateDir = "/data"
	}
	if err := secrets.Init(stateDir); err != nil {
		log.Printf("WARN: secrets.Init failed: %v (secret env-vars disabled)", err)
	}
	if err := usersecrets.Init(stateDir); err != nil {
		log.Printf("WARN: usersecrets.Init failed: %v", err)
	}

	// Initialize cluster components
	server.InitCluster()

	// Start background tasks
	server.StartBackgroundTasks()

	// Create router
	router := server.NewRouter()

	// Primary port
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	// Additional ports (comma-separated, e.g. "80,8080").
	// Default: also listen on port 80 so the dashboard is reachable as a front page.
	extraPorts := os.Getenv("EXTRA_PORTS")
	if extraPorts == "" {
		extraPorts = "80"
	}

	// Wrap the router with a global body-size cap. Mitigation for Moby/Docker
	// oversized request body AuthZ bypass (GO-2026-4887) and general
	// denial-of-service / memory exhaustion protection. The source-upload
	// endpoint uses its own 800MB MaxBytesReader so this cap is set higher
	// to accommodate multipart overhead.
	const globalMaxBodyBytes = 1024 << 20 // 1GB
	bodyCapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, globalMaxBodyBytes)
		}
		router.ServeHTTP(w, r)
	})

	// Collect all servers so we can shut them down gracefully.
	// Slow-loris mitigation lives on ReadHeaderTimeout; body-phase and
	// response-phase limits are generous to allow large source uploads
	// (up to 800MB on slow home links) and long-running SSE streams
	// (AI-assisted build pipelines can run for 15+ minutes).
	mkServer := func(addr string) *http.Server {
		return &http.Server{
			Addr:              addr,
			Handler:           bodyCapped,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Minute,
			WriteTimeout:      30 * time.Minute,
			IdleTimeout:       5 * time.Minute,
			MaxHeaderBytes:    1 << 20, // 1MB
		}
	}
	var servers []*http.Server
	servers = append(servers, mkServer("0.0.0.0:"+port))

	for _, p := range strings.Split(extraPorts, ",") {
		p = strings.TrimSpace(p)
		if p == "" || p == port {
			continue
		}
		servers = append(servers, mkServer("0.0.0.0:"+p))
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		for _, s := range servers {
			s.Close()
		}
	}()

	// Start all listeners. Extra ports run in goroutines; we block on the primary.
	for i := 1; i < len(servers); i++ {
		s := servers[i]
		go func() {
			log.Printf("AI Container Orchestrator also listening on %s", s.Addr)
			if err := s.ListenAndServe(); err != http.ErrServerClosed {
				// Port conflict (e.g. 80 already taken) is non-fatal — log and continue.
				log.Printf("listener %s failed: %v (continuing with primary port only)", s.Addr, err)
			}
		}()
	}

	log.Printf("AI Container Orchestrator starting on :%s (role=%s)", port, server.OrchestratorRole)
	if err := servers[0].ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
