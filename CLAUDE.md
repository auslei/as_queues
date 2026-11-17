# CLAUDE.md

`as_queues` — a shared library (not a service): a serialized async job queue consumed as a dependency by `as_*` FastAPI microservices that run background jobs one-at-a-time without a broker (no Celery/Redis).

Usage examples live in `README.md`; this file is the working map. All code is one module: [src/as_queues/_queue.py](src/as_queues/_queue.py), re-exported via [src/as_queues/__init__.py](src/as_queues/__init__.py).

## Commands

```bash
uv sync                                        # install deps (incl. dev group)
uv run pytest                                  # run all tests
uv run pytest tests/test_queue.py::test_dedup  # run a single test
```

Build backend is hatchling; package sources under `src/as_queues`.

## Architecture

`JobQueue` (in `_queue.py`) is the whole surface area. Public exports: `JobQueue`, `JobRecord`, `JobPersistence`, `JobEnqueueResponse`.

- **Enqueue → worker → completion** pipeline on `asyncio.Queue`. `_worker()` pulls job IDs and spawns a `_process_job` task per job; concurrency capped by `self._semaphore` (`max_concurrency`, default 1 — jobs run serially unless raised).
- **Dedup**: `enqueue()` computes a canonical key (`job_type` + JSON-sorted `request_data`) via `_dedup_key`. If a queued/running job matches, the existing `JobRecord` is returned — callers must not assume `enqueue()` always creates a new job.
- **State**: jobs live in `self._store` (dict) + `self._order` (list, for ordering/position). No external storage by default — in-process memory, so history is lost on restart unless persistence is wired up.
- **Persistence hook (optional)**: `JobPersistence` is a `runtime_checkable` `Protocol` with `on_running`, `on_done`, `load_stale`. Pass to `JobQueue(persistence=...)`. `on_running`/`on_done` fire inside the worker; `load_stale()` runs once at `start()` to mark previously-running jobs failed (crash recovery). Exceptions from persistence hooks are caught and logged, never allowed to crash processing.
- **Callbacks**: if a job's `request` dict has `callback_url`, `_fire_callback` POSTs the final status/result after completion (fire-and-forget `asyncio.create_task`, errors only logged).
- **Trimming**: `_trim_completed_jobs()` keeps at most `MAX_COMPLETED_JOBS` (100) completed/failed records, oldest evicted first — queued/running are never evicted.
- **FastAPI integration**: `make_router()` returns an `APIRouter` exposing `GET /jobs`, `GET /jobs/{job_id}`, `DELETE /jobs/{job_id}` (cancel — 404 if missing, 409 if already done). Host services call `start(run_job_fn)` on lifespan startup, `stop()` on shutdown; `run_job_fn` is the single callback dispatching on `job.job_type`.

## Conventions (this repo's stack rules)

- `uv` for all deps — never pip/poetry.
- Logging uses stdlib `logging` (`_logger = logging.getLogger(__name__)`) — this library stays dependency-light and does NOT use `as_common`'s `get_logger()`.
- Type hints on every signature; Pydantic models for `JobRecord`/responses.

## Testing conventions

Tests in [tests/test_queue.py](tests/test_queue.py) are async smoketests using `pytest-asyncio`, driving `JobQueue` directly (no live FastAPI app/TestClient). To assert on worker-processed state, use a short poll loop (`for _ in range(50): await asyncio.sleep(0.02); if <condition>: break`) rather than fixed sleeps.
