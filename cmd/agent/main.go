package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"starless/kadath/configs"
	"starless/kadath/internal/agent"
	"starless/kadath/internal/engine"
	"starless/kadath/internal/loops"
	"starless/kadath/internal/types"

	pb "starless/kadath/gen/proto"
)


func handlePing(ctx context.Context, eng types.Engine) agent.JobResult {
	err := eng.Ping(ctx)
	if err != nil {
		return agent.JobResult{
			Success:      false,
			ResultJSON:   "{}",
			ErrorMessage: fmt.Sprintf("Ping failed: %v", err),
		}
	}
	return agent.JobResult{
		Success:      true,
		ResultJSON:   "{}",
		ErrorMessage: "",
	}
}

func handleDslQuery(ctx context.Context, eng types.Engine, payload map[string]interface{}) agent.JobResult {
	logger := slog.Default()

	// Convert payload map to JSON string for parsing
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		logger.Error("Failed to marshal payload", "error", err)
		return agent.JobResult{
			Success:      false,
			ResultJSON:   "{}",
			ErrorMessage: fmt.Sprintf("Invalid payload format: %v", err),
		}
	}

	// Parse query parameters
	queryParams, err := types.ParseQueryParams(string(payloadJSON))
	if err != nil {
		logger.Error("Failed to parse query params", "error", err)
		return agent.JobResult{
			Success:      false,
			ResultJSON:   "{}",
			ErrorMessage: fmt.Sprintf("Invalid query parameters: %v", err),
		}
	}

	// Execute query
	result, err := eng.ExecuteQuery(ctx, queryParams)
	if err != nil {
		logger.Error("Failed to execute query", "error", err)
		return agent.JobResult{
			Success:      false,
			ResultJSON:   "{}",
			ErrorMessage: fmt.Sprintf("Query execution failed: %v", err),
		}
	}

	// Serialize result to JSON
	resultJSON, err := json.Marshal(result)
	if err != nil {
		logger.Error("Failed to marshal result", "error", err)
		return agent.JobResult{
			Success:      false,
			ResultJSON:   "{}",
			ErrorMessage: fmt.Sprintf("Failed to serialize result: %v", err),
		}
	}

	logger.Info("Query executed successfully", "row_count", result.RowCount)
	return agent.JobResult{
		Success:      true,
		ResultJSON:   string(resultJSON),
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
	case pb.JobKind_JOB_KIND_DSL_QUERY:
		return handleDslQuery(ctx, eng, job.Payload)
	default:
		return agent.JobResult{
			Success:      false,
			ResultJSON:   "{}",
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

