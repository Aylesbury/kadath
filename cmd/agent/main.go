package main

import (
	"context"
	"starless/kadath/internal/agent"
)

func main() {
	ctx := context.Background()
	a, err := agent.NewAgent("localhost:9001", "test-connector", "test-agent")
	if err != nil {
		panic(err)
	}

	a.SendHeartbeat(ctx)
}
