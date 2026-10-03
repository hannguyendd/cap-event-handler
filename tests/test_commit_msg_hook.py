"""The commit-msg hook enforces docs/commit-convention.md."""

import subprocess
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parent.parent / ".githooks" / "commit-msg"


def run_hook(tmp_path: Path, message: str) -> subprocess.CompletedProcess[str]:
    msg_file = tmp_path / "COMMIT_EDITMSG"
    msg_file.write_text(message)
    return subprocess.run(
        ["bash", str(HOOK), str(msg_file)], capture_output=True, text=True, check=False
    )


@pytest.mark.parametrize(
    "message",
    [
        "feat: add the worker CLI\n",
        "feat(consumer): add per-handler concurrency limit\n",
        "fix(publisher): stage outbox ids only after a successful insert\n\n"
        "A failed insert left its id in session.info.\n\nCloses: #42\n",
        "feat(storage)!: move tables to the cap schema\n\n"
        "BREAKING CHANGE: the tables now live in the cap schema.\n",
        "refactor!: drop the legacy sender\n\nBREAKING CHANGE: it is gone.\n",
        "chore: init\n",
        "build: bump sqlalchemy to 2.0.36\n",
        "ops: add a CI workflow\n",
        "docs: document the retention settings\n"
        "# Please enter the commit message for your changes.\n"
        "# Lines starting with '#' will be ignored.\n",
        "test(storage): cover dedup under concurrent inserts\n\n"
        "# ------------------------ >8 ------------------------\n"
        "diff --git a/x b/x\nThis Line Is Diff Noise.\n",
        "Merge feat/v1-implementation: transactional outbox/inbox v1\n",
        "Merge branch 'feat/x'\n",
        'Revert "feat: add the worker CLI"\n\nThis reverts commit 2c43640.\n',
        "fixup! feat: add the worker CLI\n",
        "squash! feat: add the worker CLI\n",
        "feat: add sweeper\n\nSee https://example.com/a/very/long/url/that/goes/well/past/"
        "the/seventy/two/character/limit/for/body/lines\n",
        "docs: add usage\n\nCo-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>\n",
    ],
)
def test_accepts(tmp_path: Path, message: str) -> None:
    result = run_hook(tmp_path, message)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("message", "reason"),
    [
        ("Fixed bug.\n", "header"),
        ("update stuff\n", "header"),
        ("feature: add CLI\n", "header"),
        ("feat: Add CLI\n", "lower-case"),
        ("feat: add CLI.\n", "full stop"),
        ("feat:add CLI\n", "header"),
        ("feat(PROJ-123): handle timeout\n", "scope"),
        ("feat(database): add index\n", "scope"),
        ("feat(): add CLI\n", "header"),
        ("feat: " + "a" * 70 + "\n", "72"),
        ("feat: add CLI\nno blank line\n", "blank"),
        ("feat!: drop the sender\n", "BREAKING CHANGE"),
        ("feat: drop the sender\n\nBREAKING CHANGE: it is gone.\n", "!"),
        ("feat: add CLI\n\n" + "word " * 20 + "\n", "72"),
        ("\n# only comments\n", "empty"),
    ],
)
def test_rejects(tmp_path: Path, message: str, reason: str) -> None:
    result = run_hook(tmp_path, message)
    assert result.returncode == 1
    assert reason in result.stderr
    assert "docs/commit-convention.md" in result.stderr
