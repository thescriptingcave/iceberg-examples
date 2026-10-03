# Dev tools

These scripts are **not part of any lab.** Learners never need them, and nothing
in the tutorials tells them to run anything here. They exist so the lab content
can be re-verified after an edit, by extracting the fenced code blocks out of the
lab READMEs and replaying them against a running stack.

## Prerequisites

- The stack is up (`./lab0-setup/startup.sh`).
- `lab0-setup/spark-init.sql` has been copied into `notebooks/`, which is
  bind-mounted to `/home/jovyan/notebooks` in the jupyter container:
  ```
  cp lab0-setup/spark-init.sql notebooks/
  ```

## Running

```
./tools/run_all.sh
```

**This is destructive.** It starts by dropping every table in `iceberg.tutorial`
(`customers`, `orders`, and all the scratch tables the labs create) so the labs
run against a clean slate in handout order. Don't run it against a stack whose
data you care about.

Output goes to the terminal and to `tools/full-run.log`.

| File | Runs |
|------|------|
| `run_all.sh` | Every lab, in order, from an empty namespace |
| `runner.py` | `python` blocks; copied into the jupyter container and driven against one shared `SparkSession` |
| `trino_runner.py` | `sql` blocks through the trino CLI |
| `polaris_runner.py` | `bash`/`curl` blocks against the live Polaris API |
| `lab0_bash.sh` | The read-only health-check blocks in Lab 0 |
| `isolation_test.py` | Lab 1's snapshot-isolation demo — see below |

## Reading the output

**Treat this as a convenience, not a gate.** The scripts are heuristic and a
clean summary line is not proof the labs are correct:

- `trino_runner.py` whitelists certain errors as "expected" so that the failures
  the lab deliberately demonstrates don't read as breakage. That list is broad
  enough (`does not exist`, `already exists`, ...) that a genuine regression
  producing one of those messages would be reported as expected. Check what it
  classified, not just the totals.
- Blocks are skipped when they can't be replayed: hand-written `<placeholder>`
  values, statements inside an interactive `START TRANSACTION`, and `curl`
  blocks that drop or rename things. Skipped blocks still need a human pass.
- `polaris_runner.py` skips mutating and destructive calls, so the Polaris lab's
  commit-conflict and drop examples are covered by reading the README, not by
  running it.

The `python` results are the most trustworthy signal: those blocks need no
session or environment reconstruction, so a failure there is a real failure.

## Why `isolation_test.py` is separate

Lab 1's snapshot-isolation demo needs two concurrent terminals. The `trino` CLI
**buffers piped stdin and executes at EOF**, so a long-lived interactive
transaction cannot be driven through a pipe — the statements all run at once,
after the other terminal has already committed, and the demo silently shows the
wrong thing. `isolation_test.py` allocates a real pty for `docker exec -it`,
which is what a learner actually gets.

```
python3 ./tools/isolation_test.py some-new-email@example.com
```

It prints `RESULT: PASS` when the in-transaction read stays pinned to the
pre-update value and only picks up the new value after `COMMIT`.