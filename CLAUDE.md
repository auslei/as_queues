# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`ic_queues` is a shared library (not a service) providing a serialized async job queue for `ic-*` FastAPI microservices. It's consumed as a dependency by other `ic_*` repos (see `/Users/auslei/dev/innovationchild/CLAUDE.md` for the repo map) that need to run background jobs one-at-a-time without a separate task broker (no Celery/Redis).

All code lives in a single module: [src/ic_queues/_queue.py](src/ic_queues/_queue.py), re-exported via [src/ic_queues/__init__.py](src/ic_queues/__init__.py).

## Commands

```bash
uv sync              # install deps (incl. dev group)
uv run pytest        # run all tests
uv run pytest tests/test_queue.py::test_dedup   # run a single test
```

Build backend is hatchling; package sources under `src/ic_queues`.

## Architecture

`JobQueue` (in `_queue.py`) is the whole surface area:

- **Enqueue → worker → completion** pipeline built on `asyncio.Queue`. `_worker()` pulls job IDs off the queue and spawns a `_process_job` task per job; concurrency is capped by `self._semaphore` (`max_concurrency`, default 1 — jobs run serially unless raised).
- **Dedup**: `enqueue()` computes a canonical key (`job_type` + JSON-sorted `request_data`) via `_dedup_key`. If a queued/running job already matches, the existing `JobRecord` is returned instead of creating a duplicate — callers should not assume `enqueue()` always creates a new job.
- **State**: jobs live in `self._store` (dict) + `self._order` (list, for ordering/position lookups). There's no external storage by default — everything is in-process memory, so job history is lost on restart unless persistence is wired up.
- **Persistence hook (optional)**: `JobPersistence` is a `runtime_checkable` `Protocol` with `on_running`, `on_done`, `load_stale`. Pass an implementation to `JobQueue(persistence=...)` to survive restarts — `on_running`/`on_done` fire inside the worker (no sync/event-loop constraints), and `load_stale()` is called once at `start()` to mark previously-running jobs as failed (crash recovery). Exceptions from persistence hooks are caught and logged, never allowed to crash job processing.
- **Callbacks**: if a job's `request` dict contains `callback_url`, `_fire_callback` POSTs the final job status/result there after completion (fire-and-forget `asyncio.create_task`, errors only logged).
- **Trimming**: `_trim_completed_jobs()` keeps at most `MAX_COMPLETED_JOBS` (100) completed/failed records, evicting oldest first — queued/running jobs are never evicted.
- **FastAPI integration**: `make_router()` returns an `APIRouter` exposing `GET /jobs`, `GET /jobs/{job_id}`, `DELETE /jobs/{job_id}` (cancel — 404 if missing, 409 if already completed/failed). Host services call `job_queue.start(run_job_fn)` on lifespan startup and `job_queue.stop()` on shutdown; `run_job_fn` is the single callback dispatching on `job.job_type`.

## Testing conventions

Tests in [tests/test_queue.py](tests/test_queue.py) are async smoketests using `pytest-asyncio`, driving `JobQueue` directly (no live FastAPI app/TestClient). The common pattern for asserting on worker-processed state is a short poll loop (`for _ in range(50): await asyncio.sleep(0.02); if <condition>: break`) rather than fixed sleeps.
