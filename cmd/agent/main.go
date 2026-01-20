package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"starless/kadath/configs"
	"starless/kadath/internal/agent"
	"starless/kadath/internal/loops"

	pb "starless/kadath/gen/proto"
)



func handleJob(ctx context.Context, client *agent.Agent, job *agent.JobResponse) agent.JobResult {
	logger := slog.Default()
	logger.Info("Handling job", "job_id", job.Id, "kind", job.Kind, "payload", job.Payload)

	// TODO: implement actual job processing
	return agent.JobResult{
		Success:      true,
		ResultJSON:   "{}",
		ErrorMessage: "",
	}
}

func main() {
	cfg, err := configs.LoadConfig()
	if err != nil {
		panic(err)
	}
	logger := slog.Default()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger.Info("Starting agent", "connector_id", cfg.ConnectorId)

	supportedKinds := []pb.JobKind{
		pb.JobKind_JOB_KIND_PING,
		pb.JobKind_JOB_KIND_FETCH_COLUMNS,
		pb.JobKind_JOB_KIND_DSL_QUERY,
		pb.JobKind_JOB_KIND_SCHEMA_REFRESH,
	}

	a, err := agent.NewAgent(ctx, "localhost:9001", cfg.ConnectorId, cfg.AuthToken, supportedKinds, logger)
	if err != nil {
		logger.Error("Failed to create agent", "error", err)
		os.Exit(1)
	}

	logger.Info("Agent connected, starting loops")

	go loops.NewHeartBeatLoop(ctx, a)
	loops.NewJobProcessLoop(ctx, a, handleJob)

	logger.Info("Agent stopped")
}

