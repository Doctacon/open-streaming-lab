Status: recorded
Created: 2026-06-15
Updated: 2026-06-15
Relates-To: .loom/tickets/done/2026-06-15-baseline-closure-and-repo-hygiene.md, .loom/tickets/done/20260613-python-kafka-mockingbird-tutorial.md

# Baseline Closure and Repository Hygiene Evidence

## What was observed

- The baseline Kafka tutorial scaffold already had prior runtime validation in `.loom/evidence/20260613-python-kafka-tutorial-validation.md`.
- `make test` passed on 2026-06-15 from `/Users/crlough/Code/personal/streaming-tutorial`.
- A local git repository was initialized with `git init` and the default branch was set to `main`.
- README roadmap text was updated from a generic future-chapters list to the concrete next-level roadmap tracked in Loom.
- Docker runtime was not started during this closure pass; prior full Kafka smoke evidence remains the runtime proof for the baseline.

## Procedure

Commands run:

```bash
make test
git init
git branch -M main
git status --short --branch
```

Observed `make test` behavior:

```text
uv run pytest
collected 4 items
src/streaming_tutorial/test_producer.py ....                             [100%]
4 passed in 0.02s
node --check tools/mockingbird-jsonl.mjs
docker-compose config >/dev/null
```

Observed git initialization behavior:

```text
Initialized empty Git repository in /Users/crlough/Code/personal/streaming-tutorial/.git/
```

Initial tracked/untracked status after init showed project files as untracked, including `.gitignore`, `.loom/`, `README.md`, `compose.yaml`, Python source, Node helper, and lockfiles.

## What this supports or challenges

Supports closing the baseline scaffold ticket and the baseline hygiene ticket:

- Static tests and Compose config still pass.
- Source control has been initialized locally.
- The roadmap is now durable in Loom and surfaced in README.
- The missing independent audit is explicitly accepted as residual risk for this local tutorial scaffold because previous validation included a successful full runtime smoke test.

## Limits

- This evidence does not include a fresh runtime Kafka smoke test on 2026-06-15.
- Docker/Colima runtime availability was not re-established in this pass.
- No initial git commit was created yet; repository files remain untracked until the operator approves the first commit content/message.
- The repository branch is `main`.
- The local git repository name is effectively the folder name unless the folder or remote repository is renamed.
