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

	// The DSL lives under the "query" key of the job payload. Fall back to the
	// whole payload for backwards compatibility with older brokers.
	queryPayload, ok := payload["query"].(map[string]interface{})
	if !ok {
		queryPayload = payload
	}

	// Convert query payload to JSON string for parsing
	payloadJSON, err := json.Marshal(queryPayload)
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



// resolveConnection picks the database connection for a job. Platform runners
// receive a per-job "connection" block in the payload (the broker holds the
// credentials); client runners have none and use their locally-injected config.
func resolveConnection(cfg *configs.Config, payload map[string]interface{}) *configs.Config {
	conn, ok := payload["connection"].(map[string]interface{})
	if !ok {
		return cfg
	}

	resolved := *cfg
	if dsn, ok := conn["dsn"].(string); ok && dsn != "" {
		resolved.DSN = dsn
	}
	if sslmode, ok := conn["sslmode"].(string); ok && sslmode != "" {
		resolved.SSLMode = sslmode
	}
	if engineType, ok := conn["type"].(string); ok && engineType != "" {
		resolved.EngineType = engineType
	}
	return &resolved
}

func makeJobHandler(cfg *configs.Config) loops.JobHandler {
	return func(ctx context.Context, client *agent.Agent, job *agent.JobResponse) agent.JobResult {
		logger := slog.Default()
		logger.Info("Handling job", "job_id", job.Id, "kind", job.Kind)

		conn := resolveConnection(cfg, job.Payload)
		if cfg.IsAdmin() && conn.DSN == "" {
			return agent.JobResult{
				Success:      false,
				ResultJSON:   "{}",
				ErrorMessage: "Admin mode requires a per-job connection block, none received",
			}
		}

		eng, err := engine.NewEngine(conn)

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
}

func main() {
	cfg, err := configs.LoadConfig()
	if err != nil {
		panic(err)
	}
	logger := slog.Default()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if cfg.IsAdmin() {
		logger.Info("Starting agent in admin (platform) mode", "server", cfg.ServerAddr)
	} else {
		logger.Info("Starting agent in connector mode", "server", cfg.ServerAddr, "connector_id", cfg.ConnectorId)
	}

	// Only advertise kinds handleJob actually implements: anything else would
	// be claimed by the broker and immediately failed. In admin mode that
	// would swallow e.g. schema_refresh jobs for every tenant.
	supportedKinds := []pb.JobKind{
		pb.JobKind_JOB_KIND_PING,
		pb.JobKind_JOB_KIND_DSL_QUERY,
	}

	a, err := agent.NewAgent(ctx, cfg.ServerAddr, cfg.ConnectorId, cfg.AuthToken, supportedKinds, cfg.ServerTLS, logger)
	if err != nil {
		logger.Error("Failed to create agent", "error", err)
		os.Exit(1)
	}

	logger.Info("Agent connected, starting loops")

	go loops.NewHeartBeatLoop(ctx, a)
	loops.NewJobProcessLoop(ctx, a, makeJobHandler(cfg))

	logger.Info("Agent stopped")
}

