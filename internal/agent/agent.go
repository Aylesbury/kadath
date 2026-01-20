package agent

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "starless/kadath/gen/proto"
	"starless/kadath/internal/utils"
)

type Agent struct {
	client      pb.SqlRunnerClient
	connectorID string
	agentID     string
	authToken   string
}

func NewAgent(ctx context.Context, serverAddr, connectorID, authToken string) (*Agent, error) {
	conn, err := grpc.DialContext(ctx, serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", serverAddr, err)
	}

	return &Agent{
		client:      pb.NewSqlRunnerClient(conn),
		connectorID: connectorID,
		agentID:     fmt.Sprintf("%s.%s", utils.GetHostname(), utils.RandomUUID()),
		authToken:   authToken,
	}, nil
}

func (a *Agent) authCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+a.authToken)
}

func (a *Agent) SendHeartbeat(ctx context.Context) error {
	_, err := a.client.Heartbeat(a.authCtx(ctx), &pb.HeartbeatRequest{
		AgentId: a.agentID,
	})
	return err
}
