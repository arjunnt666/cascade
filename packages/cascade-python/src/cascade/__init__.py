"""Cascade Python client (stub)."""

from typing import Any, Optional
import uuid

class WorkflowHandle:
    def __init__(self, workflow_id: str, run_id: str):
        self.workflow_id = workflow_id
        self.run_id = run_id

class CascadeClient:
    def __init__(self, endpoint: str = "http://localhost:7233"):
        self.endpoint = endpoint

    async def start_workflow(
        self,
        workflow_type: str,
        input: Any = None,
        task_queue: str = "default",
        workflow_id: Optional[str] = None,
    ) -> WorkflowHandle:
        return WorkflowHandle(
            workflow_id or str(uuid.uuid4()),
            str(uuid.uuid4()),
        )

    async def signal(self, workflow_id: str, signal_name: str, input: Any = None) -> None:
        pass

    async def get_status(self, run_id: str) -> str:
        return "Running"

__all__ = ["CascadeClient", "WorkflowHandle"]
