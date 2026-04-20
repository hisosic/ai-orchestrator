package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"ai-container-go/internal/server"
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

	// Collect all servers so we can shut them down gracefully.
	var servers []*http.Server
	servers = append(servers, &http.Server{Addr: "0.0.0.0:" + port, Handler: router})

	for _, p := range strings.Split(extraPorts, ",") {
		p = strings.TrimSpace(p)
		if p == "" || p == port {
			continue
		}
		servers = append(servers, &http.Server{Addr: "0.0.0.0:" + p, Handler: router})
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
