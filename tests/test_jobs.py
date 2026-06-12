"""Tests for the async job system — v0.7 Feature E.

Covers the core JobManager (submit/poll/progress/failure/cancel/eviction)
and the dashboard scan-job endpoints. Uses short polling with a timeout so
the tests stay fast and don't hang if a job never completes.
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import pytest

from resurrector.core.jobs import Job, JobManager


def _wait_for(manager: JobManager, job_id: str, status: str, timeout: float = 5.0) -> Job:
    """Poll until the job reaches `status` (or any terminal state), or time out."""
    deadline = time.time() + timeout
    terminal = {"succeeded", "failed", "cancelled"}
    while time.time() < deadline:
        job = manager.get(job_id)
        assert job is not None
        if job.status == status or (status in terminal and job.status in terminal):
            return job
        time.sleep(0.02)
    raise AssertionError(f"job {job_id} never reached {status}; last={manager.get(job_id).status}")


@pytest.fixture
def manager():
    m = JobManager(max_workers=2)
    yield m
    m.shutdown(wait=False)


class TestJobManager:
    def test_submit_returns_id_and_runs(self, manager):
        def work(progress):
            progress(0.5, "halfway")
            return {"answer": 42}

        job_id = manager.submit("test", work)
        assert isinstance(job_id, str) and len(job_id) > 0
        job = _wait_for(manager, job_id, "succeeded")
        assert job.status == "succeeded"
        assert job.result == {"answer": 42}
        assert job.progress == 1.0

    def test_progress_is_recorded(self, manager):
        gate = {"go": False}

        def work(progress):
            progress(0.3, "step 1")
            # Spin until the test has observed the 0.3 progress.
            for _ in range(200):
                if gate["go"]:
                    break
                time.sleep(0.01)
            return "ok"

        job_id = manager.submit("test", work)
        # Observe intermediate progress before releasing the worker.
        deadline = time.time() + 2.0
        saw = False
        while time.time() < deadline:
            j = manager.get(job_id)
            if j.status == "running" and j.progress >= 0.3:
                saw = True
                break
            time.sleep(0.01)
        gate["go"] = True
        assert saw
        _wait_for(manager, job_id, "succeeded")

    def test_failure_is_captured(self, manager):
        def work(progress):
            raise ValueError("boom")

        job_id = manager.submit("test", work)
        job = _wait_for(manager, job_id, "failed")
        assert job.status == "failed"
        assert "boom" in (job.error or "")
        assert "ValueError" in (job.error or "")

    def test_cancel_before_start_marks_cancelled(self, manager):
        # Saturate the pool so this job stays queued, then cancel it.
        block = {"release": False}

        def blocker(progress):
            while not block["release"]:
                time.sleep(0.01)
            return "done"

        # Fill both workers.
        b1 = manager.submit("block", blocker)
        b2 = manager.submit("block", blocker)
        queued_id = manager.submit("test", lambda p: "should not run")
        ok = manager.cancel(queued_id)
        assert ok
        job = manager.get(queued_id)
        assert job.cancelled is True
        block["release"] = True
        _wait_for(manager, b1, "succeeded")
        _wait_for(manager, b2, "succeeded")

    def test_cancel_finished_returns_false(self, manager):
        job_id = manager.submit("test", lambda p: "x")
        _wait_for(manager, job_id, "succeeded")
        assert manager.cancel(job_id) is False

    def test_list_jobs_filters_by_kind(self, manager):
        manager.submit("alpha", lambda p: 1)
        manager.submit("beta", lambda p: 2)
        time.sleep(0.1)
        alphas = manager.list_jobs(kind="alpha")
        assert all(j.kind == "alpha" for j in alphas)
        assert len(alphas) == 1

    def test_get_unknown_returns_none(self, manager):
        assert manager.get("nope") is None

    def test_eviction_caps_retained_jobs(self):
        # Eviction is lazy — it runs on submit and only drops *finished*
        # jobs. So the guarantee is "converges to the cap as new jobs arrive
        # after old ones finish", which we exercise by submitting one at a
        # time and waiting for each to complete before the next.
        m = JobManager(max_workers=2, max_retained=5)
        try:
            for _ in range(20):
                jid = m.submit("t", lambda p: 1)
                _wait_for(m, jid, "succeeded")
            # Each post-completion submit evicts the oldest finished job, so
            # the registry stays at the cap.
            assert len(m.list_jobs()) <= 5
        finally:
            m.shutdown(wait=False)


class TestScanJobAPI:
    def test_submit_scan_job_and_poll(self, monkeypatch):
        from resurrector.demo.sample_bag import generate_bag, BagConfig
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            generate_bag(root / "a.mcap", BagConfig(duration_sec=0.5))
            generate_bag(root / "b.mcap", BagConfig(duration_sec=0.5))
            db = root / "index.db"
            monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db))
            monkeypatch.setenv("RESURRECTOR_ALLOWED_ROOTS", str(root))

            import importlib
            from resurrector.dashboard import api as api_mod
            importlib.reload(api_mod)
            # Reset the job-manager singleton so it's fresh for this test.
            import resurrector.core.jobs as jobs_mod
            jobs_mod._MANAGER = None

            from fastapi.testclient import TestClient
            client = TestClient(api_mod.app)

            r = client.post("/api/jobs/scan", json={"path": str(root)})
            assert r.status_code == 200, r.text
            job_id = r.json()["job_id"]

            # Poll until terminal.
            deadline = time.time() + 10
            status = None
            while time.time() < deadline:
                jr = client.get(f"/api/jobs/{job_id}")
                assert jr.status_code == 200
                status = jr.json()["status"]
                if status in ("succeeded", "failed", "cancelled"):
                    break
                time.sleep(0.05)
            assert status == "succeeded", jr.json()
            result = jr.json()["result"]
            assert result["scanned"] == 2
            assert result["indexed"] == 2

    def test_scan_job_missing_path_400(self, monkeypatch):
        import importlib
        from resurrector.dashboard import api as api_mod
        importlib.reload(api_mod)
        from fastapi.testclient import TestClient
        client = TestClient(api_mod.app)
        r = client.post("/api/jobs/scan", json={})
        assert r.status_code == 400

    def test_get_unknown_job_404(self, monkeypatch):
        import importlib
        from resurrector.dashboard import api as api_mod
        importlib.reload(api_mod)
        from fastapi.testclient import TestClient
        client = TestClient(api_mod.app)
        r = client.get("/api/jobs/deadbeef")
        assert r.status_code == 404
