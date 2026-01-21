package main

import (
	"context"
	"log/slog"
	"os"
	"fmt"
	"os/signal"
	"syscall"

	"starless/kadath/configs"
	"starless/kadath/internal/agent"
	"starless/kadath/internal/loops"
	"starless/kadath/internal/engine"

	pb "starless/kadath/gen/proto"
)


func handlePing(ctx context.Context, eng engine.Engine) agent.JobResult {
		eng.Ping(ctx)
		return agent.JobResult{
			Success:      true,
			ResultJSON:   "{}",
			ErrorMessage: "",
		}
}



func handleJob(ctx context.Context, client *agent.Agent, job *agent.JobResponse) agent.JobResult {
	logger := slog.Default()
	logger.Info("Handling job", "job_id", job.Id, "kind", job.Kind, "payload", job.Payload)

	cfg, err := configs.LoadConfig()
	if err != nil {
		panic(err)
	}

	eng, err := engine.NewEngine(cfg)

	if err != nil {
		return agent.JobResult{
			Success:      false,
			ResultJSON:   "{}",
			ErrorMessage: "Unable to initialize Engine",
		}
	}

	switch pb.JobKind(job.Kind) {
	case pb.JobKind_JOB_KIND_PING: 
		return handlePing(ctx, eng)
	default: 
		return agent.JobResult{
			Success: false,
			ResultJSON: "{}",
			ErrorMessage: fmt.Sprintf("Unhandled Job Kind: %d", job.Kind),
		}
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

