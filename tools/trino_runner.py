#!/usr/bin/env python3
"""Run the sql blocks of a lab README through Trino, in order, the way a
learner typing into the interactive `trino` prompt would.

Fidelity notes:
  * Lab 1 Trino says `USE iceberg.tutorial;` once; the interactive prompt keeps
    it for the session. `trino --execute` is a fresh session per statement, so
    USE is re-issued ahead of each one.
  * Newlines inside a statement are preserved. Collapsing them to spaces would
    turn a trailing `-- comment` into one that swallows the rest.
  * Statements the lab shows inside an interactive transaction (START
    TRANSACTION) and text with <placeholder> are SKIPped: they cannot be
    replayed here and are meant to be typed by hand.
  * Errors the lab itself presents as the outcome are "expected"; anything
    else is reported as breakage.
"""
import re
import subprocess
import sys

README = sys.argv[1]

EXPECT_FAIL = re.compile(
    r"NOT NULL column|mismatched column types|Cannot write incompatible"
    r"|already exists|No transaction in progress|autocommit|does not exist"
)

def strip_comments(text):
    """Drop `--` line comments that are not inside a single-quoted literal."""
    out, in_str, i = [], False, 0
    while i < len(text):
        c = text[i]
        if c == "'":
            if in_str and text[i + 1:i + 2] == "'":   # escaped quote '' inside literal
                out.append("''")
                i += 2
                continue
            in_str = not in_str
            out.append(c)
        elif not in_str and text[i:i + 2] == "--":
            while i < len(text) and text[i] != "\n":
                i += 1
            continue
        else:
            out.append(c)
        i += 1
    return "".join(out)


def split_statements(text):
    """Split on `;` that are not inside a single-quoted literal."""
    stmts, buf, in_str, i = [], [], False, 0
    while i < len(text):
        c = text[i]
        if c == "'":
            if in_str and text[i + 1:i + 2] == "'":
                buf.append("''")
                i += 2
                continue
            in_str = not in_str
        elif not in_str and c == ";":
            s = "".join(buf).strip()
            if s:
                stmts.append(s)
            buf = []
            i += 1
            continue
        buf.append(c)
        i += 1
    s = "".join(buf).strip()
    if s:
        stmts.append(s)
    return stmts


src = open(README).read()
blocks = re.findall(r"```(\w*)\n(.*?)```", src, re.S)

statements = []
idx = -1
for lang, body in blocks:
    idx += 1
    if lang != "sql":
        continue
    if re.search(r"^\s*>", body, re.M):
        continue
    if re.search(r"\(\.\.\.\)|^\s*\.\.\.\s*$", body, re.M):
        continue
    # Comments come out first: a `;` inside a wrapped -- comment must not be
    # mistaken for a statement terminator.
    for s in split_statements(strip_comments(body)):
        if s:
            statements.append((idx, s))

print(f"### {README}")
print(f"### {len(statements)} statements\n")

unexpected = []
expected = 0
skipped = 0
ok = 0

for bidx, sql in statements:
    if re.search(r"<[^>]+>", sql):
        print(f"--- block {bidx}: SKIP (hand-written placeholder)")
        skipped += 1
        continue
    if re.match(r"(START TRANSACTION|COMMIT|ROLLBACK)", sql, re.I):
        print(f"--- block {bidx}: SKIP (interactive transaction)")
        skipped += 1
        continue

    proc = subprocess.run(
        ["docker", "exec", "-i", "iceberg-trino", "trino"],
        input=f"USE iceberg.tutorial;\n{sql};\n",
        capture_output=True, text=True,
    )
    out = proc.stdout + proc.stderr
    out = "\n".join(
        l for l in out.splitlines()
        if l.strip()
        and "org.jline" not in l
        and "Unable to create a system terminal" not in l
        and l.strip() != "USE"
    )

    if re.search(r"Error|failed:", out):
        if EXPECT_FAIL.search(out):
            reason = re.search(r"failed: (.*)", out)
            print(f"--- block {bidx}: expected error -- {reason.group(1)[:90] if reason else 'as the lab shows'}")
            expected += 1
        else:
            print(f"--- block {bidx}: *** UNEXPECTED ERROR ***")
            print("\n".join("    " + l for l in out.splitlines()[:4]))
            unexpected.append((bidx, sql[:120], out[:300]))
    else:
        ok += 1
        shown = [l for l in out.splitlines() if not l.startswith("Query ")]
        for l in shown[:10]:
            print("    " + l)

print(f"\n### summary: {ok} ok, {expected} expected-error, {skipped} skipped, "
      f"{len(unexpected)} UNEXPECTED")
for bidx, sql, out in unexpected:
    print(f"\n### block {bidx}: {sql}")
    print("    " + out.replace("\n", "\n    ")[:400])

sys.exit(1 if unexpected else 0)
