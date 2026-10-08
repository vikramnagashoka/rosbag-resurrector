"""Guards on .github/workflows/ci.yml that a green run can't reveal.

A ``run:`` step without an explicit ``shell:`` runs under ``bash -e``,
which has no ``pipefail``: in ``pytest ... | tee log`` the step's status is
tee's, so failing tests pass the step. The extras job's "tests actually
ran" guards pipe pytest into tee to grep the log for SKIPPED, and before
this was fixed a failing LeRobot or RLDS test there read as green.
"""

from __future__ import annotations

import re
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"
_TEE = re.compile(r"\|\s*tee\b")


def _steps(text: str) -> list[str]:
    # Every step in ci.yml opens with "- name:" or "- uses:".
    return re.split(r"\n\s*- (?=name:|uses:)", text)


def test_tee_pipelines_propagate_failures():
    steps = [s for s in _steps(WORKFLOW.read_text()) if _TEE.search(s)]
    names = [s.splitlines()[0] for s in steps]
    assert any("LeRobot" in n for n in names) and any("RLDS" in n for n in names), names
    for step, name in zip(steps, names):
        before_tee = step[:_TEE.search(step).start()]
        # `shell: bash` runs `bash --noprofile --norc -eo pipefail {0}`.
        assert (
            re.search(r"^\s*shell:\s*bash\s*$", step, re.M)
            or re.search(r"^\s*set -[a-z]*o pipefail\b", before_tee, re.M)
        ), f"{name}: `| tee` without pipefail hides a failing command"
