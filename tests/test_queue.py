"""Basic smoketests for as_queues — exercises the public API without a live FastAPI app."""

import asyncio
import pytest
from fastapi import HTTPException
from as_queues import JobQueue, JobRecord, JobPersistence


@pytest.mark.asyncio
async def test_enqueue_and_get():
    q = JobQueue("test")
    record = q.enqueue("do_work", {"key": "val"})
    assert record.job_id
    assert record.status == "queued"
    assert q.get_job(record.job_id) is record
    assert q.queue_size == 1


@pytest.mark.asyncio
async def test_dedup():
    q = JobQueue("test")
    a = q.enqueue("do_work", {"x": 1})
    b = q.enqueue("do_work", {"x": 1})
    assert a.job_id == b.job_id
    assert q.queue_size == 1


@pytest.mark.asyncio
async def test_dedup_nested_dict():
    """Nested dicts with different key insertion orders yield identical dedup keys."""
    q = JobQueue("test")
    payload1 = {"params": {"b": 2, "a": 1}}
    payload2 = {"params": {"a": 1, "b": 2}}
    a = q.enqueue("do_work", payload1)
    b = q.enqueue("do_work", payload2)
    assert a.job_id == b.job_id


@pytest.mark.asyncio
async def test_cancel_queued():
    q = JobQueue("test")
    r = q.enqueue("do_work", {})
    assert q.cancel_job(r.job_id)
    assert q.get_job(r.job_id).status == "failed"


@pytest.mark.asyncio
async def test_run_job():
    q = JobQueue("test")

    async def _run(job: JobRecord):
        return f"done: {job.request['n']}"

    await q.start(_run)
    r = q.enqueue("do_work", {"n": 42})
    # Wait for processing
    for _ in range(50):
        await asyncio.sleep(0.02)
        if q.get_job(r.job_id).status in ("completed", "failed"):
            break
    await q.stop()

    assert q.get_job(r.job_id).status == "completed"
    assert q.get_job(r.job_id).result == "done: 42"


@pytest.mark.asyncio
async def test_job_counts():
    q = JobQueue("test")
    assert q.job_counts() == {"queued": 0, "running": 0, "completed": 0, "failed": 0}
    assert q.get_job_position("nonexistent") == 0


@pytest.mark.asyncio
async def test_queue_snapshot():
    q = JobQueue("test")
    r = q.enqueue("t1", {})
    assert len(q.get_queue_snapshot()) == 1
    assert q.get_queue_snapshot()[0]["job_id"] == r.job_id


@pytest.mark.asyncio
async def test_callback_fires():
    """_fire_callback is called with callback_url in request."""
    q = JobQueue("test")
    called_with = {}

    # monkey-patch _fire_callback to capture args
    async def _fake_callback(url, job):
        called_with["url"] = url
        called_with["status"] = job.status

    q._fire_callback = _fake_callback

    async def _run(job: JobRecord):
        return "ok"

    await q.start(_run)
    q.enqueue("do_work", {"callback_url": "http://example.com/cb"})
    for _ in range(50):
        await asyncio.sleep(0.02)
        if called_with:
            break
    await q.stop()

    assert called_with["url"] == "http://example.com/cb"
    assert called_with["status"] == "completed"


@pytest.mark.asyncio
async def test_persistence_protocol():
    """JobPersistence is a runtime-checkable Protocol."""
    class FakePersistence:
        async def on_running(self, job): pass
        async def on_done(self, job): pass
        async def load_stale(self): return []

    assert isinstance(FakePersistence(), JobPersistence)


@pytest.mark.asyncio
async def test_persistence_error_resilience():
    """Exceptions in persistence hooks do not crash job processing."""
    class BuggyPersistence:
        async def on_running(self, job):
            raise RuntimeError("Database connection lost in on_running")
        async def on_done(self, job):
            raise RuntimeError("Database connection lost in on_done")
        async def load_stale(self):
            raise RuntimeError("Database connection lost in load_stale")

    q = JobQueue("test", persistence=BuggyPersistence())

    async def _run(job: JobRecord):
        return "success"

    await q.start(_run)
    r = q.enqueue("do_work", {})
    for _ in range(50):
        await asyncio.sleep(0.02)
        if q.get_job(r.job_id).status in ("completed", "failed"):
            break
    await q.stop()

    assert q.get_job(r.job_id).status == "completed"
    assert q.get_job(r.job_id).result == "success"


@pytest.mark.asyncio
async def test_router_delete_semantics():
    """DELETE /jobs/{id} returns 404 for missing jobs, 409 for completed jobs, 200 for queued."""
    q = JobQueue("test")
    router = q.make_router()
    delete_route = [r for r in router.routes if r.path == "/jobs/{job_id}" and "DELETE" in r.methods][0].endpoint

    # 404 for non-existent job
    with pytest.raises(HTTPException) as exc_404:
        await delete_route("non-existent-id")
    assert exc_404.value.status_code == 404

    # 200 for queued job
    r = q.enqueue("t1", {})
    res = await delete_route(r.job_id)
    assert res == {"status": "cancelled", "job_id": r.job_id}

    # 409 for finished job
    with pytest.raises(HTTPException) as exc_409:
        await delete_route(r.job_id)
    assert exc_409.value.status_code == 409
