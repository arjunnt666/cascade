// Client for `cascade serve`. Uses global fetch (node 18+, browsers).

export class CascadeError extends Error {
  constructor(public status: number, message: string) {
    super(`${status}: ${message}`);
  }
}

export type RunStatus = "Running" | "Completed" | "Failed" | "Cancelled" | "TimedOut" | "ContinuedAsNew";

export interface RunInfo {
  run_id: string;
  workflow_type?: string;
  status: RunStatus;
  events: number;
  result?: unknown;
  error?: string;
}

export class Run {
  constructor(private client: CascadeClient, readonly runId: string) {}

  describe(): Promise<RunInfo> {
    return this.client.call("GET", `/runs/${this.runId}`);
  }

  history(): Promise<unknown[]> {
    return this.client.call("GET", `/runs/${this.runId}/history`);
  }

  async signal(name: string, value: unknown = null): Promise<void> {
    await this.client.call("POST", `/runs/${this.runId}/signals/${encodeURIComponent(name)}`, value);
  }

  async cancel(reason?: string): Promise<void> {
    await this.client.call("POST", `/runs/${this.runId}/cancel`, reason ? { reason } : null);
  }

  /** Polls until the run ends. Rejects if it failed or was cancelled. */
  async result<T = unknown>(timeoutMs = 60_000, pollMs = 250): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const info = await this.describe();
      if (info.status === "Completed") return info.result as T;
      if (info.status !== "Running") {
        throw new CascadeError(409, `run ${info.status.toLowerCase()}: ${info.error ?? ""}`);
      }
      if (Date.now() > deadline) throw new Error(`run ${this.runId} still running after ${timeoutMs}ms`);
      await new Promise((r) => setTimeout(r, pollMs));
    }
  }
}

export class CascadeClient {
  private endpoint: string;

  constructor(endpoint = "http://127.0.0.1:7233") {
    this.endpoint = endpoint.replace(/\/+$/, "");
  }

  async start(workflowType: string, input: unknown = null): Promise<Run> {
    const body = await this.call<{ run_id: string }>("POST", `/workflows/${encodeURIComponent(workflowType)}`, input);
    return new Run(this, body.run_id);
  }

  run(runId: string): Run {
    return new Run(this, runId);
  }

  workflows(): Promise<string[]> {
    return this.call("GET", "/workflows");
  }

  async call<T = any>(method: string, path: string, body?: unknown): Promise<T> {
    const res = await fetch(this.endpoint + path, {
      method,
      headers: { "content-type": "application/json" },
      body: method === "GET" ? undefined : JSON.stringify(body ?? null),
    });
    const text = await res.text();
    if (!res.ok) {
      let msg = text;
      try {
        msg = JSON.parse(text).error ?? text;
      } catch {}
      throw new CascadeError(res.status, msg);
    }
    return (text ? JSON.parse(text) : undefined) as T;
  }
}

export default CascadeClient;
