package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pgtest-transient/internal/config"
	"pgtest-transient/internal/proxy"
	"pgtest-transient/pkg/logger"
)

func main() {
	// Aceita o caminho do arquivo de configuração como argumento
	// Se não fornecido, usa string vazia (busca automática)
	configPath := ""
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	configResult, err := config.LoadConfigWithPath(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	config.Init()
	config.SetOnce(configResult.Config, configResult.ConfigPath)

	// Inicializa o logger a partir da configuração
	if err := logger.InitFromConfig(config.GetCfg()); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	// Imprime o caminho do arquivo de configuração usado, se houver
	if configResult.ConfigPath != "" {
		logger.Info("Using config file: %s", configResult.ConfigPath)
	}

	cfg := config.GetCfg()
	server := proxy.NewServer(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
		cfg.Postgres.SessionTimeout.Duration,
		cfg.Proxy.KeepaliveInterval.Duration,
		cfg.Proxy.ListenHost,
		cfg.Proxy.ListenPort,
		true, // GUI on same port at /gui
	)
	if err := server.StartError(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	log.Printf("PGTest server started on port %d. Press Ctrl+C to stop.", cfg.Proxy.ListenPort)
	log.Printf("GUI: http://%s:%d/", cfg.Proxy.ListenHost, cfg.Proxy.ListenPort)

	<-sigChan
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Stop(); err != nil {
		log.Printf("Error stopping server: %v", err)
	}

	_ = ctx
	log.Println("Server stopped")
}
