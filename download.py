import asyncio, pathlib, pandas as pd, aiohttp, aiofiles, tqdm, re, traceback

# ── CONFIG ────────────────────────────────────────────────────────────────────
#EXCEL_PATH  = r"SampleData\Sample.xlsx"             # path to the Excel file"
DEST_ROOT   = pathlib.Path("downloads")             # each sheet gets a sub-folder
CONCURRENT  = 20                                    # simultaneous downloads
TIMEOUT     = aiohttp.ClientTimeout(total=60)       # seconds per request
HEADERS     = {
    "User-Agent": "FileDownloaderDemo/0.5 (Ron Olds – ron@example.com)"
}
# ───────────────────────────────────────────────────────────────────────────────


def safe_sheet_name(name: str) -> str:
    """Windows-safe folder name (replaces reserved chars with '_')."""
    return re.sub(r'[<>:"/\\|?*]', "_", name)


def print_queue_stats(sheet_rows: dict[str, list[int]]):
    total = sum(len(r) for r in sheet_rows.values())
    print("\n=== Download plan ===")
    for s, rows in sheet_rows.items():
        print(f"  {s:<25} {len(rows):>5} files queued")
    print(f"  {'TOTAL':<25} {total:>5} files\n")


async def fetch_one(session, sem, url, sheet_name, row_idx):
    """Return (sheet_name, row_idx, status, reason)."""
    folder   = DEST_ROOT / safe_sheet_name(sheet_name)
    folder.mkdir(parents=True, exist_ok=True)

    filename = url.split("/")[-1] or "file"
    dest     = folder / filename

    if dest.exists():
        return sheet_name, row_idx, "exists", ""

    async with sem:
        try:
            async with session.get(url, headers=HEADERS) as resp:
                resp.raise_for_status()

                async with aiofiles.open(dest, "wb") as f:
                    async for chunk in resp.content.iter_chunked(8192):
                        await f.write(chunk)

            return sheet_name, row_idx, "ok", ""

        except Exception as exc:
            reason = f"{type(exc).__name__}: {exc}"
            return sheet_name, row_idx, "fail", reason


async def run_async(dataframes: dict[str, pd.DataFrame],
                    tasks_to_do: list[tuple[str, str, int]]):
    """tasks_to_do: (url, sheet_name, row_idx)"""
    sem  = asyncio.Semaphore(CONCURRENT)
    conn = aiohttp.TCPConnector(limit=CONCURRENT)

    async with aiohttp.ClientSession(timeout=TIMEOUT, connector=conn) as sess:
        coros = [fetch_one(sess, sem, url, sh, idx) for url, sh, idx in tasks_to_do]

        for coro in tqdm.tqdm(asyncio.as_completed(coros), total=len(coros)):
            sheet, row, status, reason = await coro
            df = dataframes[sheet]
            df.at[row, "Status"] = status
            df.at[row, "Reason"] = reason
            print(f"{status:7} [{sheet}] {df.at[row, 'URL']}")


def main(path):
    DEST_ROOT.mkdir(exist_ok=True)

    # 1️⃣  Load all sheets
    sheets = pd.read_excel(path, sheet_name=None)

    # 2️⃣  Ensure Status/Reason columns & gather rows to download
    rows_needed: dict[str, list[int]] = {}
    tasks: list[tuple[str, str, int]] = []

    for sheet_name, df in sheets.items():
        for col in ("Status", "Reason"):
            if col not in df.columns:
                df[col] = ""

        need_rows = [
            idx for idx, status in df["Status"].items()
            if (not isinstance(status, str)) or status == "" or status.startswith("fail")
        ]
        rows_needed[sheet_name] = need_rows

        for idx in need_rows:
            tasks.append((df.at[idx, "URL"], sheet_name, idx))

    print_queue_stats(rows_needed)

    if not tasks:
        print("Nothing to do – all sheets already completed.")
        return

    # 3️⃣  Download
    asyncio.run(run_async(sheets, tasks))

    # 4️⃣  Save workbook back in place
    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as xl:
        for name, df in sheets.items():
            df.to_excel(xl, sheet_name=name, index=False)

    print(f"\nWorkbook updated → {path}")


#main()