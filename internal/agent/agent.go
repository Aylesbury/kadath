package agent

import (
	"fmt"
	"context"
  "google.golang.org/grpc"
	pb "starless/kadath/gen/proto"
	"starless/kadath/internal/utils"
)

type Agent struct {
	  client      pb.SqlRunnerClient
    connectorID string
    agentID     string
}

func NewAgent(ctx context.Context, serverAddr string, connectorID string) (*Agent, error) {
    conn, err := grpc.DialContext(ctx, serverAddr, grpc.WithInsecure(), grpc.WithBlock())
    if err != nil {
        return nil, fmt.Errorf("failed to connect to %s: %w", serverAddr, err)
    }

    return &Agent{
        client:      pb.NewSqlRunnerClient(conn),
        connectorID: connectorID,
        agentID:     fmt.Sprintf("%s.%s", utils.GetHostname(), utils.RandomUUID()),
    }, nil
}

func (a *Agent) SendHeartbeat(ctx context.Context) error {
    _, err := a.client.Heartbeat(ctx, &pb.HeartbeatRequest{
        ConnectorId: a.connectorID,
        AgentId:     a.agentID,
    })
    return err
}
