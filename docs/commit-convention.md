# Commit Convention

Every commit on this repository follows this convention. It is based on
[Conventional Commits](https://www.conventionalcommits.org/) as summarised in
[qoomon's cheatsheet](https://gist.github.com/qoomon/5dfcdf8eec66a051ecd85625518cfd13), with
the scopes and project rules below added on top.

## Enforcement

`.githooks/commit-msg` rejects messages that break the mechanical rules: the type and scope (read
from the tables below, so adding a scope here allows it), header length and shape, the blank
second line, body wrapping (long URLs and `Key: value` trailers excepted), and the pairing of `!`
with a `BREAKING CHANGE:` footer. Git's merge and revert messages and `fixup!`/`squash!` commits
pass untouched. The imperative mood and "one logical change" remain on you.

Enable it once per clone:

```bash
git config core.hooksPath .githooks
```

`tests/test_commit_msg_hook.py` pins its behaviour. Bypass it only deliberately, with
`git commit --no-verify`.

## 1. Format

```
<type>(<optional scope>)<optional !>: <description>

<optional body>

<optional footer>
```

- The header (first line) is **mandatory** and at most **72 characters**.
- A blank line separates the header, the body and the footer.

## 2. Types

| Type       | Use it when the commit…                                                              | Version bump |
|------------|--------------------------------------------------------------------------------------|--------------|
| `feat`     | adds, adjusts or removes a feature of the public API or the CLI                      | minor        |
| `fix`      | fixes a bug in behaviour introduced by an earlier `feat`                             | patch        |
| `refactor` | restructures code without changing behaviour                                         | patch        |
| `perf`     | is a `refactor` whose purpose is better performance                                  | patch        |
| `style`    | changes formatting, whitespace or lint-only details, with no behaviour change        | patch        |
| `test`     | adds missing tests or corrects existing ones                                         | patch        |
| `docs`     | changes documentation only (`docs/`, `README`, `CLAUDE.md`, docstrings)              | patch        |
| `build`    | changes the build, packaging, dependencies (`pyproject.toml`, `uv.lock`) or version  | patch        |
| `ops`      | changes infrastructure, CI/CD, deployment, Docker or monitoring                       | patch        |
| `chore`    | is anything else: `.gitignore`, tooling scripts such as `check.sh`, housekeeping     | patch        |

Choosing between types:

- Behaviour visible to a library user changed → `feat` or `fix`, even if most of the diff is tests.
- Behaviour is unchanged → never `feat` or `fix`.
- A `fix` repairs something; it is not a way to label "a change I made today".

## 3. Scope

The scope is optional. When used, it names the part of the library the commit touches, and must be
one of:

| Scope        | Covers                                                    |
|--------------|-----------------------------------------------------------|
| `app`        | `app.py` — `CapApp` wiring and lifecycle                  |
| `publisher`  | `publisher.py` — publish pipeline, outbox sender          |
| `consumer`   | `consumer.py` — consume pipeline, deduplication           |
| `processors` | `processors.py` — outbox sweeper, retention collector     |
| `storage`    | `storage/` — schema and `SqlStorage`                      |
| `transport`  | `transport/` — transport seam, SNS/SQS, in-memory broker  |
| `envelope`   | `envelope.py` — wire envelope and header constants        |
| `config`     | `config.py`, `validation.py` — `Settings` and validation  |
| `registry`   | `registry.py`, `context.py` — handlers and context        |
| `errors`     | `errors.py` — exception hierarchy                         |
| `cli`        | `cli.py` — the worker CLI                                 |

Rules:

- Leave the scope out when a commit spans several areas, rather than inventing a broad one.
- Never use an issue or work-item identifier as a scope — put it in the footer.
- Add a new scope to this table in the same commit that first needs it.

## 4. Breaking changes

A breaking change is anything that forces a library user to change their code, their database
schema or their deployment: a removed or renamed public API, a changed default, a new required
migration, a changed wire format or header name.

- Mark it with `!` immediately before the colon: `feat(envelope)!: rename the message id header`.
- Explain it in the footer with `BREAKING CHANGE: <what breaks and how to migrate>`.
- Both are required. Any type may be breaking; a breaking commit bumps the **major** version
  (while the version is `0.x`, it bumps the minor).

## 5. Description

- Imperative, present tense: "add", not "added" or "adds".
- Lower-case first letter; no full stop at the end.
- Says *what* the commit does to the code, read as completing "If applied, this commit will …".

## 6. Body

- Optional for small, self-explanatory commits; expected otherwise.
- Imperative, present tense, wrapped at 72 characters.
- Explains *why* the change is made and how behaviour differs from before. The diff already shows
  *what* changed.
- Where a commit departs from the design spec, say so and why.

## 7. Footer

- `BREAKING CHANGE: <description>` for breaking changes (section 4).
- Issue references, e.g. `Refs: #123` or `Closes: #123`.
- Trailers such as `Co-Authored-By: Name <email>`.

## 8. Special commits

| Commit  | Format                                                         |
|---------|----------------------------------------------------------------|
| Initial | `chore: init`                                                  |
| Merge   | git's default: `Merge branch '<name>'`                         |
| Revert  | git's default: `Revert "<reverted header>"` plus `This reverts commit <sha>.` |

## 9. Commit discipline

- One logical change per commit. If the description needs "and", consider two commits.
- Every commit passes `./check.sh` (`GATE: PASS`), so any commit can be checked out and bisected.
- Tests are written first, but land in the same commit as the code they test, typed by the
  behaviour they cover (`feat`/`fix`). Use `test` only for commits that touch tests alone.

## 10. Examples

```
feat(consumer): add per-handler concurrency limit
```

```
fix(publisher): stage outbox ids only after a successful insert

A failed insert left its id in session.info, so the after_commit
listener handed a non-existent row to the sender.

Closes: #42
```

```
feat(storage)!: move outbox and inbox tables to the cap schema

BREAKING CHANGE: the tables now live in the `cap` schema instead of
`public`. Run the 0.2.0 migration before upgrading.
```

```
refactor(transport): extract SQS receive loop into its own method
docs: document the retention settings
build: bump sqlalchemy to 2.0.36
test(storage): cover ON CONFLICT dedup under concurrent inserts
chore: ignore the .ruff_cache directory
```

Non-examples:

```
Fixed bug.                       # no type, past tense, full stop
feat: Add CLI                    # capitalised description
fix(PROJ-123): handle timeout    # issue id as scope
update stuff                     # no type, says nothing
```
