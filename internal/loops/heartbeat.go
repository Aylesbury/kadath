package loops

import (
	"log/slog"
	"context"
	"time"

	"starless/kadath/internal/agent"
)

func NewHeartBeatLoop(ctx context.Context, client *agent.Agent) error {
	logger := slog.Default()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Send initial heartbeat
	if err := client.SendHeartbeat(ctx); err != nil {
		logger.Error("Heartbeat failed", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := client.SendHeartbeat(ctx); err != nil {
				logger.Error("Heartbeat failed", "error", err)
			}
		}
	}

	return nil
}
