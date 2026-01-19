package agent

import (
    "google.golang.org/grpc"
    pb "starless/kadath/proto/sql_runner"
)

type Agent struct {
	  client      pb.SqlRunnerClient
    connectorID string
    agentID     string
}

func NewAgent(serverAddr, connectorID string) (*Agent, error) {
    conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
    if err != nil {
        return nil, err
    }

    return &Agent{
        client:      pb.NewSqlRunnerClient(conn),
        connectorID: connectorID,
        agentID:     "agent-" + generateUUID(),
    }, nil
}

func (a *Agent) SendHeartbeat(ctx context.Context) error {
    _, err := a.client.Heartbeat(ctx, &pb.HeartbeatRequest{
        ConnectorId: a.connectorID,
        AgentId:     a.agentID,
    })
    return err
}
