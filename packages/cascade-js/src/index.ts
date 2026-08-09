/**
 * Cascade JS client (stub).
 */

export interface WorkflowHandle {
  workflowId: string;
  runId: string;
}

export interface StartOptions {
  taskQueue?: string;
  workflowId?: string;
}

export class CascadeClient {
  constructor(private endpoint: string = "http://localhost:7233") {}

  async startWorkflow(
    workflowType: string,
    _input: unknown,
    _opts: StartOptions = {}
  ): Promise<WorkflowHandle> {
    return {
      workflowId: crypto.randomUUID(),
      runId: crypto.randomUUID(),
    };
  }

  async signal(workflowId: string, signalName: string, _input?: unknown): Promise<void> {
    console.debug(`[cascade] signal ${signalName} → ${workflowId}`);
  }

  async getStatus(runId: string): Promise<string> {
    return "Running";
  }
}

export default CascadeClient;
