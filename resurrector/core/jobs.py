"""In-process async job system (v0.7 — Feature E).

Long operations — scanning a directory of huge bags, exporting a fleet,
indexing frames — currently run on the request thread and block until done.
On a TB-scale fleet that means a hung dashboard and HTTP timeouts. This
module moves that work onto a background thread pool with a pollable status
registry, so the UI submits a job, gets an id, and polls for progress.

Scope is deliberately a **single-user, single-process** design — matching the
project's "localhost dev tool" stance. No Redis, no Celery, no cross-process
broker. A ``ThreadPoolExecutor`` plus a lock-guarded dict of job records is
the right amount of machinery here; anything more is for a different product.

A job's worker receives a ``progress`` callback — ``progress(frac, message)``
— so long operations report incremental status the UI can render as a bar.
Workers should also check ``job.cancelled`` cooperatively at chunk
boundaries; the manager can't force-kill a thread.
"""

from __future__ import annotations

import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable

JobStatus = str  # "queued" | "running" | "succeeded" | "failed" | "cancelled"


@dataclass
class Job:
    """A single background job's state. All fields are read under the manager lock."""
    id: str
    kind: str
    status: JobStatus = "queued"
    progress: float = 0.0
    message: str = ""
    result: Any = None
    error: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    cancelled: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "status": self.status,
            "progress": round(self.progress, 4),
            "message": self.message,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "cancelled": self.cancelled,
        }


# Type of a job worker: receives a progress reporter, returns a JSON-able result.
ProgressFn = Callable[[float, str], None]
WorkerFn = Callable[[ProgressFn], Any]


class JobManager:
    """Thread-pool-backed registry of background jobs.

    Thread-safe. ``submit`` returns immediately with a job id; the work runs
    on the pool. Poll with ``get``. Completed jobs are retained (up to
    ``max_retained``, oldest evicted) so the UI can read a result after the
    fact.
    """

    def __init__(self, max_workers: int = 2, max_retained: int = 200):
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()
        self._max_retained = max_retained

    def _now(self) -> float:
        return time.time()

    def _evict_if_needed(self) -> None:
        """Drop oldest finished jobs once we exceed the retention cap."""
        if len(self._jobs) <= self._max_retained:
            return
        finished = sorted(
            (j for j in self._jobs.values()
             if j.status in ("succeeded", "failed", "cancelled")),
            key=lambda j: j.updated_at,
        )
        overflow = len(self._jobs) - self._max_retained
        for j in finished[:overflow]:
            self._jobs.pop(j.id, None)

    def submit(self, kind: str, worker: WorkerFn) -> str:
        """Schedule ``worker`` on the pool. Returns the new job id immediately."""
        job_id = uuid.uuid4().hex[:12]
        now = self._now()
        job = Job(id=job_id, kind=kind, created_at=now, updated_at=now)
        with self._lock:
            self._jobs[job_id] = job
            self._evict_if_needed()
        self._executor.submit(self._run, job_id, worker)
        return job_id

    def _run(self, job_id: str, worker: WorkerFn) -> None:
        def progress(frac: float, message: str = "") -> None:
            with self._lock:
                j = self._jobs.get(job_id)
                if j is None:
                    return
                j.progress = max(0.0, min(1.0, frac))
                if message:
                    j.message = message
                j.updated_at = self._now()

        with self._lock:
            j = self._jobs.get(job_id)
            if j is None:
                return
            if j.cancelled:
                j.status = "cancelled"
                j.updated_at = self._now()
                return
            j.status = "running"
            j.updated_at = self._now()

        try:
            result = worker(progress)
        except Exception as e:  # noqa: BLE001 — surface any worker failure as job state
            with self._lock:
                j = self._jobs.get(job_id)
                if j is not None:
                    j.status = "failed"
                    j.error = f"{type(e).__name__}: {e}"
                    j.message = "failed"
                    j.updated_at = self._now()
                    # Stash a short traceback tail for debugging via the API.
                    tb = traceback.format_exc().strip().splitlines()
                    j.result = {"traceback_tail": tb[-3:]}
            return

        with self._lock:
            j = self._jobs.get(job_id)
            if j is not None:
                if j.cancelled:
                    j.status = "cancelled"
                else:
                    j.status = "succeeded"
                    j.result = result
                    j.progress = 1.0
                    j.message = j.message or "done"
                j.updated_at = self._now()

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(self, kind: str | None = None) -> list[Job]:
        with self._lock:
            jobs = list(self._jobs.values())
        if kind is not None:
            jobs = [j for j in jobs if j.kind == kind]
        return sorted(jobs, key=lambda j: -j.created_at)

    def cancel(self, job_id: str) -> bool:
        """Request cooperative cancellation. Returns True if the job exists and
        wasn't already finished. The worker must check ``job.cancelled``."""
        with self._lock:
            j = self._jobs.get(job_id)
            if j is None or j.status in ("succeeded", "failed", "cancelled"):
                return False
            j.cancelled = True
            j.updated_at = self._now()
            if j.status == "queued":
                # Hasn't started — mark cancelled now; _run will short-circuit.
                j.status = "cancelled"
            return True

    def shutdown(self, wait: bool = False) -> None:
        self._executor.shutdown(wait=wait)


# Process-wide singleton used by the dashboard. Tests construct their own
# JobManager instances for isolation.
_MANAGER: JobManager | None = None


def get_job_manager() -> JobManager:
    global _MANAGER
    if _MANAGER is None:
        _MANAGER = JobManager()
    return _MANAGER
