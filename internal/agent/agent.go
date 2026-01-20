package agent

import (
	"context"
	"fmt"
	"log/slog"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "starless/kadath/gen/proto"
	"starless/kadath/internal/utils"
)

type NoJobs struct {}

func (e NoJobs) Error() string {
	return "No jobs available"
}

type JobResult struct {
	Success      bool
	ResultJSON   string
	ErrorMessage string
}

type JobResponse struct {
	Id			string
	Kind		int32
	Payload map[string]interface{}
}

type Agent struct {
	client         pb.SqlRunnerClient
	connectorID    string
	agentID        string
	authToken      string
	logger         *slog.Logger
	supportedKinds []pb.JobKind
}

func NewAgent(ctx context.Context, serverAddr, connectorID, authToken string, supportedKinds []pb.JobKind, logger *slog.Logger) (*Agent, error) {
	conn, err := grpc.DialContext(ctx, serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", serverAddr, err)
	}

	return &Agent{
		client:         pb.NewSqlRunnerClient(conn),
		connectorID:    connectorID,
		agentID:        fmt.Sprintf("%s.%s", utils.GetHostname(), utils.RandomUUID()),
		authToken:      authToken,
		logger:         logger,
		supportedKinds: supportedKinds,
	}, nil
}

func (a *Agent) authCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+a.authToken)
}

func (a *Agent) SendHeartbeat(ctx context.Context) error {
	_, err := a.client.Heartbeat(a.authCtx(ctx), &pb.HeartbeatRequest{
		AgentId: a.agentID,
	})
	if err == nil {
		a.logger.Debug("Heartbeat sent")
	}
	return err
}

func (a *Agent) GetJob(ctx context.Context) (*JobResponse, error) {
	resp, err := a.client.GetJob(a.authCtx(ctx), &pb.GetJobRequest{
		AgentId:        a.agentID,
		SupportedKinds: a.supportedKinds,
	})

	if err != nil {
		return nil, err
	}

	if !resp.HasJob {
		return nil, &NoJobs{}
	}

	var payload map[string]interface{}

	job := resp.Job


	json.Unmarshal([]byte(job.PayloadJson), &payload)

	return &JobResponse{Id: job.Id, Kind: int32(job.Kind), Payload: payload}, nil
}

func (a *Agent) UpdateJob(ctx context.Context, jobId string, result JobResult) error {
	_, err := a.client.UpdateJob(a.authCtx(ctx), &pb.UpdateJobRequest{
		JobId:        jobId,
		AgentId:      a.agentID,
		Success:      result.Success,
		ResultJson:   result.ResultJSON,
		ErrorMessage: result.ErrorMessage,
	})

	return err
}
