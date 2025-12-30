import json
import os
from pathlib import Path

here = Path(__file__).resolve().parent
src = here / 'metadata_migration.json'
if not src.exists():
    raise SystemExit(f"Missing migration file: {src}")

# Prefer host-mounted config path used by the container.
candidate_paths = [
    Path('/data/config/onthespot/.cache/onthespot/metadata_migration.json'),
    Path('/config/.cache/onthespot/metadata_migration.json'),
]

target = None
for path in candidate_paths:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        target = path
        break
    except Exception:
        continue

if target is None:
    raise SystemExit("Unable to create migration cache directory")

with src.open('r', encoding='utf-8') as f:
    data = json.load(f)

with target.open('w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False)

print(f"Wrote metadata migration cache to {target}")
