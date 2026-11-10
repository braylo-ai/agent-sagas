import json
import subprocess
import sys
from pathlib import Path


def test_fault_campaign_zero_duplicates_for_saga_engine():
    root = Path(__file__).resolve().parents[1]
    subprocess.run([sys.executable, str(root / "eval" / "run_faults.py")], check=True, capture_output=True)
    r = json.loads((root / "results" / "fault_campaign.json").read_text())
    s, n = r["saga_engine"], r["naive_baseline"]
    for k in ("duplicate_charges", "duplicate_confirmation_emails", "silent_wrong_net_charge", "orphan_reservations"):
        assert s[k] == 0, k
    assert n["duplicate_charges"] > 0  # the baseline really does fail
