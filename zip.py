"""
archive_sheets.py
─────────────────
Create a separate ZIP archive for each sheet-named sub-folder produced by
download.py.

Folder layout expected beforehand
.
├─ download.py
├─ downloads/
│  ├─ List 1/
│  ├─ List 2/
│  └─ Archive/
└─ (this script)

After running you’ll have
.
├─ archives/
│  ├─ List 1.zip
│  ├─ List 2.zip
│  └─ Archive.zip
└─ ...
"""

from pathlib import Path
from shutil   import make_archive
from datetime import datetime

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DOWNLOAD_ROOT = Path("downloads")             # where sheet folders live
ARCHIVE_ROOT  = Path("archives")              # where .zip files will go
TIMESTAMP_TAG = False                         # True → add YYYYMMDD-hhmm to names
# ───────────────────────────────────────────────────────────────────────────────

def main() -> None:
    if not DOWNLOAD_ROOT.exists():
        raise SystemExit(f"Folder {DOWNLOAD_ROOT} not found — nothing to archive")

    ARCHIVE_ROOT.mkdir(exist_ok=True)

    created = 0
    for sheet_dir in filter(Path.is_dir, DOWNLOAD_ROOT.iterdir()):
        sheet_name = sheet_dir.name

        # Skip empty folders
        if not any(sheet_dir.iterdir()):
            print(f"⏩  Skipping empty folder “{sheet_name}”")
            continue

        tag  = datetime.now().strftime("_%Y%m%d-%H%M") if TIMESTAMP_TAG else ""
        zip_name = ARCHIVE_ROOT / f"{sheet_name}{tag}"

        # make_archive adds .zip automatically
        zip_path = make_archive(base_name=zip_name, format="zip",
                                root_dir=sheet_dir, base_dir=".")
        print(f"✓  Created  {zip_path}")
        created += 1

    if created == 0:
        print("No folders archived (none contained files).")
    else:
        print(f"\nDone — {created} archive(s) written to {ARCHIVE_ROOT.resolve()}")

if __name__ == "__main__":
    main()
