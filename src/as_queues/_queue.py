"""
Shared serialized job queue for ic-* FastAPI microservices.

Usage pattern:
    from as_queues import JobQueue, JobRecord

    job_queue = JobQueue("my_service")

    async def _run_job(job: JobRecord) -> Any:
        if job.job_type == "do_work":
            return await asyncio.to_thread(my_sync_fn, **job.request)
        raise ValueError(f"Unknown job type: {job.job_type}")

    @asynccontextmanager
    async def lifespan(app):
        await job_queue.start(_run_job)
        yield
        await job_queue.stop()

    app = FastAPI(lifespan=lifespan)
    app.include_router(job_queue.make_router())
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine, Dict, List, Optional, Protocol, runtime_checkable
from uuid import uuid4

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

_logger = logging.getLogger(__name__)

MAX_COMPLETED_JOBS = 100

RunJobFn = Callable[["JobRecord"], Coroutine[Any, Any, Any]]


@runtime_checkable
class JobPersistence(Protocol):
    """Optional backend for persisting job state across restarts.

    Implement this and pass to JobQueue(..., persistence=...) to get
    crash recovery. on_running/on_done fire inside the async worker so
    there are no sync/event-loop constraints on the caller side.
    """
    async def on_running(self, job: "JobRecord") -> None:
        """Called when a job transitions to running. INSERT into the store."""
        ...

    async def on_done(self, job: "JobRecord") -> None:
        """Called when a job completes, fails, or is recovered. UPDATE the store."""
        ...

    async def load_stale(self) -> "List[JobRecord]":
        """Return jobs that were running when the service last died."""
        ...


class JobRecord(BaseModel):
    """Represents a single queued or completed job."""
    job_id: str
    job_type: str
    status: str  # queued | running | completed | failed
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    progress_total: Optional[int] = None
    progress_done: int = 0
    progress_message: Optional[str] = None
    request: Dict[str, Any] = {}
    result: Optional[Any] = None
    error: Optional[str] = None



class JobEnqueueResponse(BaseModel):
    """Returned immediately when a job is accepted into the queue."""
    job_id: str
    status: str
    queue_size: int
    position: int


class JobQueue:
    """
    Serialized FIFO job queue for a single FastAPI service.

    - One job runs at a time (serial execution, no GPU contention).
    - Retains up to MAX_COMPLETED_JOBS completed/failed records in memory.
    - Provides a FastAPI router with GET /jobs and GET /jobs/{job_id}.
    """

    def __init__(
        self,
        service_name: str,
        logger=None,
        max_concurrency: int = 1,
        persistence: Optional[JobPersistence] = None,
    ):
        self.service_name = service_name
        self._log = logger or _logger
        self._store: Dict[str, JobRecord] = {}
        self._order: List[str] = []
        self._queue: asyncio.Queue = asyncio.Queue()
        self._current_job_id: Optional[str] = None
        self._worker_task: Optional[asyncio.Task] = None
        self._run_job_fn: Optional[RunJobFn] = None
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self.max_concurrency = max_concurrency
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._persistence: Optional[JobPersistence] = persistence

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    def _dedup_key(job_type: str, request_data: Dict[str, Any]) -> str:
        """Canonical key for deduplication — job_type + sorted request fields."""
        try:
            canonical = json.dumps(request_data, sort_keys=True, default=str)
        except Exception:
            canonical = str(request_data)
        return f"{job_type}::{canonical}"

    def enqueue(
        self,
        job_type: str,
        request_data: Dict[str, Any],
        progress_total: Optional[int] = None,
    ) -> JobRecord:
        """Add a job to the queue and return the record immediately.

        If an identical job (same job_type + request_data) is already queued or
        running, the existing record is returned instead of creating a duplicate.
        """
        key = self._dedup_key(job_type, request_data)
        for job_id in self._order:
            job = self._store.get(job_id)
            if job and job.status in ("queued", "running") and self._dedup_key(job.job_type, job.request) == key:
                self._log.info(f"[{self.service_name}] Dedup: returning existing job {job_id} ({job_type})")
                return job

        job_id = str(uuid4())
        record = JobRecord(
            job_id=job_id,
            job_type=job_type,
            status="queued",
            created_at=datetime.now(timezone.utc).isoformat(),
            progress_total=progress_total,
            request=request_data,
        )
        self._store[job_id] = record
        self._order.append(job_id)
        self._queue.put_nowait(job_id)
        self._log.info(f"[{self.service_name}] Queued job {job_id} ({job_type})")
        return record

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a queued or running job. Returns True if found and cancelled."""
        job = self._store.get(job_id)
        if not job:
            return False

        if job.status == "queued":
            job.status = "failed"
            job.error = "Cancelled by user"
            job.finished_at = datetime.now(timezone.utc).isoformat()
            self._log.info(f"[{self.service_name}] Cancelled queued job {job_id}")
            return True

        if job.status == "running":
            task = self._running_tasks.get(job_id)
            if task:
                task.cancel()
                self._log.info(f"[{self.service_name}] Requested cancellation for running job {job_id}")
                return True

        return False

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        return self._store.get(job_id)

    def get_queue_snapshot(self) -> List[Dict[str, Any]]:
        """Return lightweight dicts for all currently queued (not yet running) jobs."""
        result = []
        for job_id in self._order:
            job = self._store.get(job_id)
            if job and job.status == "queued":
                result.append({
                    "job_id": job.job_id,
                    "job_type": job.job_type,
                    "created_at": job.created_at,
                    "progress_total": job.progress_total,
                    "progress_done": job.progress_done,
                    "progress_message": job.progress_message,
                })
        return result

    def get_job_position(self, job_id: str) -> int:
        """1-based position in the queue, or 0 if not queued."""
        queued = [
            jid for jid in self._order
            if self._store.get(jid) and self._store[jid].status == "queued"
        ]
        try:
            return queued.index(job_id) + 1
        except ValueError:
            return 0

    @property
    def current_job(self) -> Optional[JobRecord]:
        return self._store.get(self._current_job_id) if self._current_job_id else None

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    def job_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {"queued": 0, "running": 0, "completed": 0, "failed": 0}
        for job in self._store.values():
            if job.status in counts:
                counts[job.status] += 1
        return counts

    async def start(self, run_job_fn: RunJobFn) -> None:
        """Start the background worker. Call from lifespan startup."""
        self._run_job_fn = run_job_fn
        await self._recover_stale()
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._worker())
            self._log.info(f"[{self.service_name}] Queue worker started")

    async def stop(self) -> None:
        """Stop the background worker and wait for in-flight jobs. Call from lifespan shutdown."""
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        self._worker_task = None

        if self._running_tasks:
            for task in list(self._running_tasks.values()):
                task.cancel()
            await asyncio.gather(*list(self._running_tasks.values()), return_exceptions=True)

        self._log.info(f"[{self.service_name}] Queue worker stopped")

    def make_router(self) -> APIRouter:
        """Return a FastAPI router that exposes GET /jobs, GET /jobs/{job_id}, and DELETE /jobs/{job_id}."""
        router = APIRouter(tags=["jobs"])
        queue = self

        @router.get("/jobs")
        async def list_jobs(status: Optional[str] = None) -> List[JobRecord]:
            """List all retained jobs, optionally filtered by status."""
            jobs = list(queue._store.values())
            if status:
                jobs = [j for j in jobs if j.status == status]
            return sorted(jobs, key=lambda j: j.created_at, reverse=True)

        @router.get("/jobs/{job_id}")
        async def get_job(job_id: str) -> JobRecord:
            """Get the status and result of a specific job."""
            job = queue.get_job(job_id)
            if not job:
                raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
            return job

        @router.delete("/jobs/{job_id}")
        async def delete_job(job_id: str):
            """Cancel or remove a job."""
            job = queue.get_job(job_id)
            if not job:
                raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
            if job.status in ("completed", "failed"):
                raise HTTPException(status_code=409, detail=f"Job '{job_id}' is already {job.status} and cannot be cancelled")
            queue.cancel_job(job_id)
            return {"status": "cancelled", "job_id": job_id}

        return router

    # ------------------------------------------------------------------
    # Internal worker
    # ------------------------------------------------------------------

    async def _worker(self) -> None:
        self._log.info(f'[{self.service_name}] Queue worker running (concurrency: {self.max_concurrency})')
        while True:
            job_id = await self._queue.get()
            # Launch a concurrent handler for this job
            asyncio.create_task(self._process_job(job_id))

    async def _process_job(self, job_id: str) -> None:
        async with self._semaphore:
            job = self._store.get(job_id)
            if not job or (job.status == 'failed' and job.error == 'Cancelled by user'):
                self._queue.task_done()
                return

            self._current_job_id = job_id
            job.status = 'running'
            job.started_at = datetime.now(timezone.utc).isoformat()
            self._log.info(f'[{self.service_name}] Worker picked up job {job_id} ({job.job_type})')

            if self._persistence:
                try:
                    await self._persistence.on_running(job)
                except Exception as exc:
                    self._log.error(f"[{self.service_name}] Persistence on_running failed for job {job_id}: {exc}", exc_info=True)

            task = asyncio.create_task(self._run_job_fn(job))
            self._running_tasks[job_id] = task
            try:
                job.result = await task
                job.status = 'completed'
                self._log.info(f'[{self.service_name}] Job {job_id} completed')
            except asyncio.CancelledError:
                job.status = 'failed'
                job.error = 'Cancelled by user'
                self._log.info(f'[{self.service_name}] Job {job_id} was cancelled')
            except Exception as exc:
                job.error = str(exc)
                job.status = 'failed'
                self._log.error(f'[{self.service_name}] Job {job_id} failed: {exc}', exc_info=True)
            finally:
                self._running_tasks.pop(job_id, None)
                job.finished_at = datetime.now(timezone.utc).isoformat()
                if self._current_job_id == job_id:
                    self._current_job_id = None
                if self._persistence:
                    try:
                        await self._persistence.on_done(job)
                    except Exception as exc:
                        self._log.error(f"[{self.service_name}] Persistence on_done failed for job {job_id}: {exc}", exc_info=True)
                self._queue.task_done()
                self._trim_completed_jobs()
                if callback_url := job.request.get("callback_url"):
                    asyncio.create_task(self._fire_callback(callback_url, job))

    async def _fire_callback(self, url: str, job: "JobRecord") -> None:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "job_id": job.job_id,
                    "status": job.status,
                    "result": job.result,
                    "error": job.error,
                })
        except Exception as e:
            self._log.warning(f"[{self.service_name}] Callback to {url} failed: {e}")

    async def _recover_stale(self) -> None:
        """On startup: mark any jobs that were running when we died as failed."""
        if not self._persistence:
            return
        try:
            stale = await self._persistence.load_stale()
        except Exception as exc:
            self._log.error(f"[{self.service_name}] Persistence load_stale failed: {exc}", exc_info=True)
            return

        for job in stale:
            job.status = "failed"
            job.error = "Service restarted — job did not complete"
            job.finished_at = datetime.now(timezone.utc).isoformat()
            self._store[job.job_id] = job
            if job.job_id not in self._order:
                self._order.append(job.job_id)
            try:
                await self._persistence.on_done(job)
            except Exception as exc:
                self._log.error(f"[{self.service_name}] Persistence on_done failed for stale job {job.job_id}: {exc}", exc_info=True)
        if stale:
            self._log.info(f"[{self.service_name}] Recovered {len(stale)} stale job(s) as failed")

    def _trim_completed_jobs(self) -> None:
        """Remove oldest completed/failed jobs beyond MAX_COMPLETED_JOBS."""
        done = [
            jid for jid in self._order
            if self._store.get(jid) and self._store[jid].status in ("completed", "failed")
        ]
        while len(done) > MAX_COMPLETED_JOBS:
            oldest = done.pop(0)
            self._store.pop(oldest, None)
            try:
                self._order.remove(oldest)
            except ValueError:
                pass
