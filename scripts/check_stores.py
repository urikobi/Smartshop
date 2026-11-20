#!/usr/bin/env python3
"""Smart Shopping - store files feasibility check.

Downloads ONLY the published store (branch) files of five chains, finds the
branches in Ma'ale Adumim and Mishor Adumim, and writes a report:
  * GitHub Job Summary (markdown)
  * stores_report/matches.csv, stores_report/chains_status.csv, report.md

No price files are downloaded. Nothing is stored outside the runner.

Downloading uses il-supermarket-scraper by Sefi Erlich
(https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers),
used unmodified, for non-commercial purposes. See README.md.
"""
import argparse
import codecs
import csv
import datetime
import gzip
import importlib.metadata
import inspect
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

# (scraper name in il-supermarket-scraper, Hebrew display name)
CHAINS = [
    ("RAMI_LEVY", "רמי לוי"),
    ("YOHANANOF", "יוחננוף"),
    ("ZOL_VEBEGADOL", "זול ובגדול"),
    ("MAAYAN_2000", "מעיין 2000"),
    ("SHUFERSAL", "שופרסל"),
]

# Area label -> normalized search phrases (see norm()).
AREAS = {
    "מעלה אדומים": ["מעלה אדומים", "maale adumim", "maaleh adumim", "maale adomim"],
    "מישור אדומים": ["מישור אדומים", "mishor adumim", "mishor adomim"],
}

FILES_LIMIT = 3          # newest store files per chain
CHAIN_TIMEOUT_SEC = 240  # hard stop per chain
OUT_DIR = Path("stores_report")
DUMPS_DIR = Path("dumps")


# --------------------------------------------------------------------------
# Download (runs in a separate process so one chain cannot block the others)
# --------------------------------------------------------------------------
def download_worker(chain: str, out_dir: str) -> None:
    from il_supermarket_scarper import ScarpingTask, ScraperFactory

    available = ScraperFactory.all_scrapers_name()
    if chain not in available:
        raise SystemExit(f"scraper '{chain}' is not available in this version")

    task = ScarpingTask(
        enabled_scrapers=[chain],
        files_types=["STORE_FILE"],
        multiprocessing=1,
        output_configuration={"output_mode": "disk", "base_storage_path": out_dir},
        status_configuration={
            "database_type": "json",
            "base_path": os.path.join(out_dir, "_status"),
        },
    )
    kwargs = {"limit": FILES_LIMIT}
    if "single_pass" in inspect.signature(task.start).parameters:
        kwargs["single_pass"] = True
    task.start(**kwargs)
    task.join()


def tail(text, lines=12):
    if not text:
        return ""
    if isinstance(text, bytes):
        text = text.decode("utf-8", "replace")
    return "\n".join(text.strip().splitlines()[-lines:])


def run_download(chain: str, out_dir: Path):
    """Returns (ok, message). Never raises."""
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, os.path.abspath(__file__), "--download", chain, str(out_dir)]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=CHAIN_TIMEOUT_SEC
        )
    except subprocess.TimeoutExpired as exc:
        return False, (
            f"timeout after {CHAIN_TIMEOUT_SEC}s. Last output:\n"
            f"{tail(exc.stderr) or tail(exc.stdout)}"
        )
    except Exception as exc:  # noqa: BLE001
        return False, f"could not start download: {type(exc).__name__}: {exc}"
    if proc.returncode != 0:
        return False, (
            f"exit code {proc.returncode}. Last output:\n"
            f"{tail(proc.stderr) or tail(proc.stdout)}"
        )
    return True, ""


def collect_files(out_dir: Path):
    files = []
    for p in out_dir.rglob("*"):
        if not p.is_file():
            continue
        if "_status" in p.parts or p.suffix.lower() == ".json":
            continue
        files.append(p)
    return sorted(files)


# --------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------
def read_text(path: Path) -> str:
    data = path.read_bytes()
    for _ in range(3):  # some files are gzipped more than once
        if data[:2] == b"\x1f\x8b":
            data = gzip.decompress(data)
        else:
            break
    if data.startswith(codecs.BOM_UTF16_LE) or data.startswith(codecs.BOM_UTF16_BE):
        return data.decode("utf-16")
    if data[:1] == b"<" and data[1:2] == b"\x00":
        return data.decode("utf-16-le")
    if data[:1] == b"\x00" and data[1:2] == b"<":
        return data.decode("utf-16-be")
    for enc in ("utf-8-sig", "cp1255"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1")


def local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def parse_stores(path: Path):
    """Return a list of dicts (lowercase field name -> text)."""
    text = read_text(path).lstrip("\ufeff")
    text = re.sub(r"^\s*<\?xml[^>]*\?>", "", text)
    try:
        root = ET.fromstring(text.encode("utf-8"))
    except ET.ParseError as exc:
        head = text[:150].replace("\n", " ")
        raise ValueError(f"not valid XML ({exc}). File starts with: {head!r}") from exc
    stores = []
    for el in root.iter():
        if local(el.tag).lower() != "store":
            continue
        fields = {}
        for child in el:
            if len(child) == 0 and child.text is not None:
                fields[local(child.tag).lower()] = child.text.strip()
        if fields:
            stores.append(fields)
    return stores


def pick(d, *names):
    for n in names:
        v = d.get(n)
        if v:
            return v
    return ""


def to_row(fields: dict, source_file: str) -> dict:
    return {
        "store_id": pick(fields, "storeid", "store_id", "storecode"),
        "store_name": pick(fields, "storename", "storenm", "name"),
        "address": pick(fields, "address", "storeaddress", "street"),
        "city": pick(fields, "city", "cityname"),
        "zip": pick(fields, "zipcode", "zip"),
        "source_file": source_file,
    }


# --------------------------------------------------------------------------
# Matching
# --------------------------------------------------------------------------
def norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[\"'`’‘׳״\u200e\u200f]", "", s)
    s = re.sub(r"[-–—_.,/\\()]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def match_areas(row: dict):
    """Return list of (area, [fields it matched in])."""
    fields = {
        "שם סניף": norm(row["store_name"]),
        "כתובת": norm(row["address"]),
        "עיר": norm(row["city"]),
    }
    found = []
    for area, phrases in AREAS.items():
        hit_in = [fname for fname, val in fields.items()
                  if any(ph in val for ph in phrases)]
        if hit_in:
            found.append((area, hit_in))
    return found


def city_unknown(row: dict) -> bool:
    c = norm(row["city"])
    return c == "" or c.isdigit()


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------
def md(s) -> str:
    return str(s).replace("|", "/").replace("\n", " ").strip()


def israel_time_note():
    try:
        from zoneinfo import ZoneInfo
        now = datetime.datetime.now(ZoneInfo("Asia/Jerusalem"))
        if now.hour < 8:
            return (f"שים לב: ההרצה בוצעה ב-{now:%H:%M} שעון ישראל. "
                    "מתחזק הסורק ציין שחלק מהרשתות מושבתות לפני 08:00, "
                    "ולכן כדאי לנסות שוב אחרי 08:00.")
    except Exception:  # noqa: BLE001
        pass
    return ""


def check_chain(chain: str, display: str):
    """Returns (status_row, match_rows). Never raises."""
    status = {
        "chain": display, "scraper": chain, "status": "ERROR",
        "files": 0, "total_stores": 0, "matches": 0,
        "unknown_city_unmatched": 0, "message": "",
    }
    matches = []
    try:
        out_dir = DUMPS_DIR / chain
        ok, msg = run_download(chain, out_dir)
        files = collect_files(out_dir)
        status["files"] = len(files)
        if not files:
            status["status"] = "NO_DATA"
            status["message"] = msg or "the scraper finished but returned no store files"
            return status, matches

        notes = []
        if not ok:
            notes.append(f"הורדה הסתיימה עם שגיאה אך נמצאו קבצים: {msg}")

        by_id = {}
        parse_errors = []
        for f in files:
            try:
                for fields in parse_stores(f):
                    row = to_row(fields, f.name)
                    key = row["store_id"].lstrip("0") or row["store_id"] or f"?{len(by_id)}"
                    by_id[key] = row
            except Exception as exc:  # noqa: BLE001
                parse_errors.append(f"{f.name}: {exc}")

        status["total_stores"] = len(by_id)
        if parse_errors:
            notes.append("שגיאות פענוח: " + " | ".join(parse_errors))
        if not by_id:
            status["status"] = "PARSE_ERROR"
            status["message"] = " ; ".join(notes) or "no stores found in files"
            return status, matches

        unknown_unmatched = 0
        for row in by_id.values():
            found = match_areas(row)
            if found:
                for area, hit_in in found:
                    matches.append({**row, "chain": display, "area": area,
                                    "matched_in": ", ".join(hit_in)})
            elif city_unknown(row):
                unknown_unmatched += 1
        status["matches"] = len({m["store_id"] for m in matches})
        status["unknown_city_unmatched"] = unknown_unmatched
        status["status"] = "OK"
        status["message"] = " ; ".join(notes)
        return status, matches
    except Exception as exc:  # noqa: BLE001
        status["message"] = f"{type(exc).__name__}: {exc}"
        return status, matches


def build_markdown(statuses, matches):
    lines = ["# בדיקת סניפים: מעלה אדומים ומישור אדומים", ""]
    lines.append(f"הורצה: {datetime.datetime.utcnow():%Y-%m-%d %H:%M} UTC. "
                 "הורדו קובצי סניפים בלבד, ללא מחירים.")
    note = israel_time_note()
    if note:
        lines += ["", f"> {note}"]
    lines += ["", "## סטטוס לפי רשת", "",
              "| רשת | סטטוס | קבצים | סה\"כ סניפים | באזור | הערות |",
              "|---|---|---|---|---|---|"]
    for s in statuses:
        extra = ""
        if s["status"] == "OK" and s["unknown_city_unmatched"]:
            extra = (f"{s['unknown_city_unmatched']} סניפים ללא שם עיר בקובץ ולא "
                     "זוהו לפי שם/כתובת (לא ניתן לשלול). ")
        lines.append(f"| {md(s['chain'])} | {s['status']} | {s['files']} | "
                     f"{s['total_stores']} | {s['matches']} | "
                     f"{md(extra + s['message'])[:300]} |")
    lines += ["", "## סניפים שנמצאו", ""]
    if matches:
        lines += ["| רשת | אזור | מזהה סניף | שם סניף | כתובת | עיר | נמצא ב |",
                  "|---|---|---|---|---|---|---|"]
        for m in sorted(matches, key=lambda x: (x["area"], x["chain"], x["store_id"])):
            lines.append(f"| {md(m['chain'])} | {md(m['area'])} | {md(m['store_id'])} | "
                         f"{md(m['store_name'])} | {md(m['address'])} | "
                         f"{md(m['city'])} | {md(m['matched_in'])} |")
    else:
        lines.append("לא נמצאו סניפים באזורים אלה בקבצים שהורדו. "
                     "זה נכון רק לקבצים שנבדקו בפועל, ראה סטטוס לכל רשת.")
    errs = [s for s in statuses if s["status"] != "OK"]
    if errs:
        lines += ["", "## שגיאות", ""]
        for s in errs:
            lines += [f"**{s['chain']}** ({s['scraper']}): {s['status']}", "",
                      "```", s["message"][:1500], "```", ""]
    return "\n".join(lines) + "\n"


def write_outputs(statuses, matches):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUT_DIR / "matches.csv", "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["chain", "area", "store_id", "store_name", "address",
                    "city", "zip", "matched_in", "source_file"])
        for m in matches:
            w.writerow([m["chain"], m["area"], m["store_id"], m["store_name"],
                        m["address"], m["city"], m["zip"], m["matched_in"],
                        m["source_file"]])
    with open(OUT_DIR / "chains_status.csv", "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["chain", "scraper", "status", "files", "total_stores",
                    "matches", "unknown_city_unmatched", "message"])
        for s in statuses:
            w.writerow([s["chain"], s["scraper"], s["status"], s["files"],
                        s["total_stores"], s["matches"],
                        s["unknown_city_unmatched"], s["message"]])
    report = build_markdown(statuses, matches)
    (OUT_DIR / "report.md").write_text(report, encoding="utf-8")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(report)
    print(report)


def main() -> int:
    try:
        ver = importlib.metadata.version("il-supermarket-scraper")
    except Exception:  # noqa: BLE001
        ver = "unknown"
    print(f"python {sys.version.split()[0]}, il-supermarket-scraper {ver}", flush=True)
    statuses, all_matches = [], []
    for chain, display in CHAINS:
        print(f"--- {display} ({chain}) ---", flush=True)
        s, m = check_chain(chain, display)
        print(f"{s['status']}: files={s['files']} stores={s['total_stores']} "
              f"matches={s['matches']}", flush=True)
        statuses.append(s)
        all_matches.extend(m)
    write_outputs(statuses, all_matches)
    return 0 if any(s["status"] == "OK" for s in statuses) else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--download", nargs=2, metavar=("CHAIN", "OUT_DIR"))
    args = ap.parse_args()
    if args.download:
        download_worker(args.download[0], args.download[1])
        sys.exit(0)
    sys.exit(main())
