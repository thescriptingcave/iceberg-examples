#!/usr/bin/env python3
"""Reproduce the Lab 1 Trino snapshot-isolation demo over a real TTY.

The `trino` CLI buffers piped stdin and only executes at EOF, so a long-lived
interactive transaction cannot be driven through a pipe. `docker exec -it` with
a real pty is what a learner actually gets.

Expected: Terminal 1's transaction keeps seeing the pre-update email even after
Terminal 2 commits, and only picks up the new value after COMMIT.
"""
import os
import pty
import re
import select
import subprocess
import sys
import time

NEW_EMAIL = sys.argv[1] if len(sys.argv) > 1 else "alice.iso2@example.com"


def strip(txt):
    out = []
    for line in txt.splitlines():
        line = re.sub(r"^\s*trino>\s?", "", line.rstrip())
        if not line.strip():
            continue
        if any(s in line for s in ("org.jline", "Unable to create a system terminal")):
            continue
        out.append(line)
    return out


def drain(fd, seconds):
    """Read whatever the child prints for `seconds`."""
    buf, end = [], time.time() + seconds
    while time.time() < end:
        r, _, _ = select.select([fd], [], [], 0.3)
        if r:
            try:
                chunk = os.read(fd, 65536).decode(errors="replace")
            except OSError:
                break
            if not chunk:
                break
            buf.append(chunk)
    return "".join(buf)


def send(fd, sql, wait=4.0):
    os.write(fd, (sql + "\n").encode())
    return strip(drain(fd, wait))


master, slave = pty.openpty()
proc = subprocess.Popen(
    ["docker", "exec", "-it", "iceberg-trino", "trino"],
    stdin=slave, stdout=slave, stderr=slave, close_fds=True,
    env={**os.environ, "TERM": "xterm"},
)
os.close(slave)
drain(master, 6)  # let the CLI draw its prompt

send(master, "USE iceberg.tutorial;", 3)
print("--- Terminal 1: open a read transaction ---")
send(master, "START TRANSACTION;", 3)
r1 = send(master, "SELECT email FROM tutorial.customers WHERE customer_id = 1;", 6)
print(f"  read #1 -> {r1}")

print("--- Terminal 2 (autocommit) commits an update while Terminal 1 is open ---")
subprocess.run(
    ["docker", "exec", "iceberg-trino", "trino", "--execute",
     f"UPDATE iceberg.tutorial.customers SET email = '{NEW_EMAIL}' "
     "WHERE customer_id = 1"],
    capture_output=True, text=True,
)

r2 = send(master, "SELECT email FROM tutorial.customers WHERE customer_id = 1;", 6)
print(f"  read #2 -> {r2}   (isolation holds if this is still the OLD email)")

send(master, "COMMIT;", 3)
r3 = send(master, "SELECT email FROM tutorial.customers WHERE customer_id = 1;", 6)
print(f"  post-COMMIT -> {r3}   (should now be the NEW email)")

os.write(master, b"EXIT\n")
time.sleep(1)
proc.kill()

old_seen = NEW_EMAIL not in "\n".join(r1) if r1 else None
pinned = NEW_EMAIL not in "\n".join(r2) if r2 else None
fresh = NEW_EMAIL in "\n".join(r3) if r3 else None
print()
print(f"read #1 saw pre-update value : {old_seen}")
print(f"read #2 still pinned (isolation): {pinned}")
print(f"post-COMMIT sees new value   : {fresh}")
print("RESULT:", "PASS" if (old_seen and pinned and fresh) else "CHECK ABOVE")