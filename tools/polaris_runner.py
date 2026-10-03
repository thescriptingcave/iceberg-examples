#!/usr/bin/env python3
"""Run the bash/curl blocks of the Polaris lab README against the live stack.

Read-only catalog reads run as written. Blocks that mutate (register, commit,
set-properties, rename, drop) and blocks needing a hand-supplied id are SKIPped
and listed, so the manual pass knows what to cover.

Each block runs in its own bash process with TOKEN already exported, the way a
learner would have it after pasting the token-fetching block once.
"""
import os
import re
import subprocess
import sys

README = sys.argv[1]

token = subprocess.run(
    ["curl", "-s", "-X", "POST",
     "http://localhost:8181/api/catalog/v1/oauth/tokens",
     "-d", "grant_type=client_credentials",
     "-d", "client_id=root",
     "-d", "client_secret=root",
     "-d", "scope=PRINCIPAL_ROLE:ALL"],
    capture_output=True, text=True,
).stdout
TOKEN = re.search(r'"access_token"\s*:\s*"([^"]+)"', token).group(1)

src = open(README).read()
blocks = re.findall(r"```(\w*)\n(.*?)```", src, re.S)

# The lab sets CAT / MGMT / TOKEN in setup blocks and every later block
# interpolates them. An interactive shell keeps those assignments, so the
# runner has to supply them or each $CAT/... URL comes out malformed (curl rc=3).
assigns = dict(re.findall(r"^([A-Z][A-Z0-9_]*)=(\S+)\s*$", src, re.M))
env = dict(os.environ, **assigns, TOKEN=TOKEN)

print(f"### {README}")
print(f"### token acquired ({len(TOKEN)} chars), vars={assigns}\n")

ok = skipped = bad = 0
idx = -1
for lang, body in blocks:
    idx += 1
    if lang not in ("bash", "sh"):
        continue
    if re.search(r"^\s*>", body, re.M):
        continue
    if re.search(r"\(\.\.\.\)|^\s*\.\.\.\s*$", body, re.M):
        continue

    reason = None
    if re.search(r"<[^>]+>", body):
        reason = "needs a hand-supplied id"
    elif re.search(r"\bDROP\b|unregister|rename|-X DELETE", body):
        reason = "destructive"
    elif "access_token" in body:
        reason = "token fetch (already done)"

    if reason:
        print(f"--- block {idx}: SKIP ({reason})")
        skipped += 1
        continue

    proc = subprocess.run(["bash", "-c", body], capture_output=True,
                          text=True, env=env, timeout=120)
    out = (proc.stdout + proc.stderr).strip()
    if proc.returncode != 0 or "Traceback" in out or "curl: (" in out:
        print(f"--- block {idx}: *** FAILED (rc={proc.returncode})")
        print("\n".join("    " + l for l in out.splitlines()[:6]))
        bad += 1
    else:
        for l in out.splitlines()[:8]:
            print("    " + l)
        ok += 1

print(f"\n### {README}: {ok} ok, {skipped} skipped, {bad} failed")
sys.exit(1 if bad else 0)
