package agent

import (
    "context"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    pb "starless/kadath/gen/proto"
)

type Agent struct {
	  client      pb.SqlRunnerClient
    connectorID string
    agentID     string
}

func NewAgent(serverAddr, connectorID, agentID string) (*Agent, error) {
    conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, err
    }

    return &Agent{
        client:      pb.NewSqlRunnerClient(conn),
        connectorID: connectorID,
        agentID:     agentID,
    }, nil
}

func (a *Agent) SendHeartbeat(ctx context.Context) error {
    _, err := a.client.Heartbeat(ctx, &pb.HeartbeatRequest{
        ConnectorId: a.connectorID,
        AgentId:     a.agentID,
    })
    return err
}
