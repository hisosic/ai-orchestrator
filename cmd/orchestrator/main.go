package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
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

	// Get port
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	// Start server
	srv := &http.Server{
		Addr:    "0.0.0.0:" + port,
		Handler: router,
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		srv.Close()
	}()

	log.Printf("AI Container Orchestrator starting on :%s (role=%s)", port, server.OrchestratorRole)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
