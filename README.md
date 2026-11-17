# as-queues

A serialized async job queue for `as_*` FastAPI microservices. It lets a service run background jobs one-at-a-time without a separate task broker — no Celery, no Redis. Consumed as a dependency by other `as_*` repos that need simple in-process background processing.

## Features

- **Serialized worker** — jobs run one at a time by default (`max_concurrency=1`), safe for GPU/CPU-bound work.
- **Dedup** — identical queued/running jobs are coalesced instead of duplicated.
- **Optional persistence** — plug in a `JobPersistence` implementation to survive restarts and recover crashed jobs.
- **Callbacks** — POST final status/result to a `callback_url` when a job finishes.
- **FastAPI router** — drop-in `/jobs` endpoints.

## Install

```bash
uv sync
```

## Usage

```python
from typing import Any
from contextlib import asynccontextmanager
from fastapi import FastAPI
from as_queues import JobQueue, JobRecord

job_queue = JobQueue("my_service")

async def run_job(job: JobRecord) -> Any:
    if job.job_type == "do_work":
        return await asyncio.to_thread(my_sync_fn, **job.request)
    raise ValueError(f"Unknown job type: {job.job_type}")

@asynccontextmanager
async def lifespan(app):
    await job_queue.start(run_job)
    yield
    await job_queue.stop()

app = FastAPI(lifespan=lifespan)
app.include_router(job_queue.make_router())   # exposes GET /jobs, GET /jobs/{id}, DELETE /jobs/{id}
```

Enqueue a job (e.g. from a route). If an identical job is already queued/running, the existing record is returned:

```python
record = job_queue.enqueue(job_type="do_work", request_data={"path": "/tmp/f"})
```

## Public API

`JobQueue`, `JobRecord`, `JobPersistence`, `JobEnqueueResponse` — all re-exported from the package root.
