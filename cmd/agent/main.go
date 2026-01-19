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
	client, err := agent.NewAgent("localhost:9001", cfg.ConnectorId)
	if err != nil {
		panic(err)
	}

	client.SendHeartbeat(ctx)
}
