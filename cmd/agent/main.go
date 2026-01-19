package main

import (
	"log/slog"
	"context"
	"starless/kadath/internal/agent"
	"starless/kadath/configs"
)

func main() {
	cfg, err := configs.LoadConfig()
	if err != nil {
		panic(err)
	}
	logger := slog.Default()
	ctx := context.Background()

	logger.Info("Running Agent", "connector_id", cfg.ConnectorId)
	client, err := agent.NewAgent(ctx, "localhost:9001", cfg.ConnectorId)
	if err != nil {
		panic(err)
	}

	if err := client.SendHeartbeat(ctx); err != nil {
		logger.Error("Failed to send heartbeat", "error", err)
	} else {
		logger.Info("Heartbeat sent successfully")
	}
}
