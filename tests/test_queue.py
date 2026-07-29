"""Basic smoketests for ic_queues — exercises the public API without a live FastAPI app."""

import asyncio
import pytest
from ic_queues import JobQueue, JobRecord, JobEnqueueResponse, JobPersistence


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
