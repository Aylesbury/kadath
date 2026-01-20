package loops

import (
	"context"
	"time"
	"log/slog"

	"starless/kadath/internal/agent"
)



type JobHandler func(ctx context.Context, client *agent.Agent, job *agent.JobResponse) agent.JobResult

func pollAndProcessJob(ctx context.Context, client *agent.Agent, handler JobHandler) error {
	logger := slog.Default()
	resp, err := client.GetJob(ctx)
	if err != nil {
		if _, ok := err.(*agent.NoJobs); ok {
			// Ignore it. This is normal case
			return nil
		}
		logger.Error("GetJob failed", "error", err)
		return err
	}

	logger.Info("Processing job", "job_id", resp.Id, "kind", resp.Kind)

	result := handler(ctx, client, resp)

	err = client.UpdateJob(ctx, resp.Id, result)

	if err != nil {
		logger.Error("UpdateJob failed", "job_id", resp.Id, "error", err)
		return err
	}

	logger.Info("Job completed", "job_id", resp.Id, "success", result.Success)

	return nil
}

func NewJobProcessLoop(ctx context.Context, client *agent.Agent, handler JobHandler) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pollAndProcessJob(ctx, client, handler)
		}
	}

	return nil
}
