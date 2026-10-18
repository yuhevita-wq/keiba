#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""JRA multi-year data ingestion for Benter-style RESEARCH (no betting model).
All predictive features are computed from earlier DATES only.
Public source: keibamar/keiba_ai_ver2.0 (pin to immutable commit).
"""
import collections
import csv
import datetime as dt
import hashlib
import io
import itertools
import json
import pathlib
import re
import sqlite3
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "output"
CACHE = OUT / "cache"
COMMIT = "6f526df677202d5ffa674b0473f0fa9dd99b8c91"
BASE = "https://raw.githubusercontent.com/keibamar/keiba_ai_ver2.0/" + COMMIT + "/data/RaceResults/"
PLACES = ["01_sapporo", "02_hakodate", "03_fukushima", "04_nigata", "05_tokyo",
          "06_nakayama", "07_chukyo", "08_kyoto", "09_hanshin", "10_kokura"]
YEARS = range(2019, 2026)
# Kyoto did not hold JRA races in 2021 and 2022 while facilities were renovated.
SOURCES = [(p, y) for p in PLACES for y in YEARS
           if not (p == "08_kyoto" and y in (2021, 2022))]
EXPECTED_SOURCES = len(SOURCES)  # 68
CUTOFF = "2026-10-18"  # target race must NOT enter training or feature history

DDL = """
CREATE TABLE source_files (
  source_path TEXT PRIMARY KEY, url TEXT NOT NULL, sha256 TEXT NOT NULL,
  source_rows INTEGER NOT NULL, accepted_rows INTEGER NOT NULL);
CREATE TABLE races (
  race_id TEXT PRIMARY KEY, race_date TEXT NOT NULL, place TEXT NOT NULL,
  surface TEXT, distance_m INTEGER, going TEXT, weather TEXT, race_class TEXT);
CREATE TABLE entries (
  race_id TEXT NOT NULL, horse_no INTEGER NOT NULL, horse_id TEXT NOT NULL,
  horse_name TEXT NOT NULL, sexage TEXT, impost_kg REAL, jockey TEXT,
  jockey_id TEXT, trainer TEXT, finish_rank INTEGER, result_status TEXT,
  finish_seconds REAL, margin_text TEXT, final_win_odds REAL,
  final_popularity INTEGER, last3f REAL, body_weight INTEGER,
  passing_order TEXT, draw INTEGER, PRIMARY KEY (race_id, horse_no));
CREATE TABLE race_audit (
  race_id TEXT PRIMARY KEY, n_runners INTEGER, n_finishers INTEGER,
  n_winners INTEGER, basic_valid INTEGER, issues TEXT);
CREATE TABLE features (
  race_id TEXT NOT NULL, horse_no INTEGER NOT NULL, race_date TEXT NOT NULL,
  split TEXT NOT NULL, horse_id TEXT NOT NULL, label_won INTEGER NOT NULL,
  place TEXT, surface TEXT, distance_m INTEGER, going TEXT, draw INTEGER,
  impost_kg REAL, sexage TEXT, horse_prior_starts INTEGER,
  horse_prior_wins INTEGER, horse_prior_top3 INTEGER, last_finish INTEGER,
  days_since_last INTEGER, last3_avg_finish REAL,
  prior_same_surface INTEGER, prior_near_distance INTEGER,
  prior_same_place INTEGER, jockey_prior_starts INTEGER,
  jockey_prior_wins INTEGER, trainer_prior_starts INTEGER,
  trainer_prior_wins INTEGER, PRIMARY KEY (race_id, horse_no));
CREATE INDEX idx_entries_horse ON entries(horse_id, race_id);
CREATE INDEX idx_races_date ON races(race_date, race_id);
"""

def as_int(v):
    try:
        x = float(v)
        return int(x) if x.is_integer() else None
    except (ValueError, TypeError):
        return None

def as_float(v):
    try:
        x = float(v)
        return x if x == x else None
    except (ValueError, TypeError):
        return None

def clean(v):
    s = str(v or "").strip()
    return None if s.lower() in ("", "nan", "null", "none", "-") else s

def date_iso(v):
    m = re.search(r"(20\d{2})\D+(\d{1,2})\D+(\d{1,2})", str(v))
    if not m:
        return None
    try:
        return dt.date(*(int(i) for i in m.groups())).isoformat()
    except ValueError:
        return None

def seconds(v):
    s = clean(v)
    if not s:
        return None
    try:
        seg = [float(x) for x in s.split(":")]
        if len(seg) == 3:
            return 3600 * seg[0] + 60 * seg[1] + seg[2]
        if len(seg) == 2:
            return 60 * seg[0] + seg[1]
    except ValueError:
        pass
    return None

def download(url, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and target.stat().st_size > 200:
        return target.read_bytes()
    req = urllib.request.Request(url, headers={"User-Agent": "JRA-academic-data-research/1.0"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=75) as response:
                content = response.read()
            if len(content) < 200 or b"horse_id" not in content[:1500]:
                raise ValueError("Response is not the expected racing CSV")
            target.write_bytes(content)
            time.sleep(0.12)
            return content
        except (urllib.error.URLError, ValueError, TimeoutError) as e:
            if attempt == 2:
                raise RuntimeError(str(e)) from e
            time.sleep(2 * (attempt + 1))

def import_source(con, place, year, log):
    relative = f"{place}/{year}_race_results.csv"
    url = BASE + relative
    raw = download(url, CACHE / relative)
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp932")
    rdr = csv.DictReader(io.StringIO(text))
    if not {"horse_id", "date", "着順", "馬番"}.issubset(rdr.fieldnames or []):
        raise ValueError(f"Unexpected CSV columns: {rdr.fieldnames}")
    n = accepted = 0
    for r in rdr:
        n += 1
        rid = clean(r.get("") or r.get("Unnamed: 0") or r.get("race_id"))
        if not rid or not re.fullmatch(r"20\d{10}", rid):
            continue
        date = date_iso(r.get("date"))
        if not date or date >= CUTOFF:
            continue
        horse_no = as_int(r.get("馬番"))
        horse_id, horse_name = clean(r.get("horse_id")), clean(r.get("馬名"))
        if not horse_no or not horse_id or not horse_name:
            continue
        rank = as_int(r.get("着順"))
        con.execute("INSERT OR IGNORE INTO races VALUES (?,?,?,?,?,?,?,?)",
                    (rid, date, place, clean(r.get("race_type")), as_int(r.get("course_len")),
                     clean(r.get("ground_state")), clean(r.get("weather")), clean(r.get("class"))))
        values = (rid, horse_no, horse_id, horse_name, clean(r.get("性齢")),
                  as_float(r.get("斤量")), clean(r.get("騎手")), clean(r.get("jockey_id")),
                  clean(r.get("調教師")), rank, "finished" if rank else (clean(r.get("着順")) or "unknown"),
                  seconds(r.get("タイム")), clean(r.get("着差")), as_float(r.get("単勝")),
                  as_int(r.get("人気")), as_float(r.get("上り")), as_int(r.get("馬体重")),
                  clean(r.get("通過")), as_int(r.get("枠番")))
        existing = con.execute("SELECT horse_id,finish_rank FROM entries WHERE race_id=? AND horse_no=?",
                               (rid, horse_no)).fetchone()
        if existing and existing != (horse_id, rank):
            raise ValueError(f"Conflicting entry {rid} no.{horse_no}: {existing}")
        con.execute("INSERT OR IGNORE INTO entries VALUES (" + ",".join(["?"] * 19) + ")", values)
        accepted += 1
    con.execute("INSERT OR REPLACE INTO source_files VALUES (?,?,?,?,?)",
                (relative, url, hashlib.sha256(raw).hexdigest(), n, accepted))
    con.commit()
    log.append({"file": relative, "status": "ok", "source_rows": n, "accepted_rows": accepted})
    print(f"OK {relative}: {accepted} accepted / {n} source", flush=True)

def audit(con):
    src = con.execute("SELECT race_id FROM races ORDER BY race_id").fetchall()
    with con:
        for (rid,) in src:
            rows = con.execute("SELECT horse_no,horse_id,finish_rank FROM entries WHERE race_id=?", (rid,)).fetchall()
            finished = [row for row in rows if row[2] is not None and row[2] > 0]
            wins = sum(row[2] == 1 for row in finished)
            issues = []
            if len(finished) < 5:
                issues.append("LT5_FINISHERS")
            if wins != 1:
                issues.append("WINNER_COUNT_NOT_ONE")
            if len({row[1] for row in rows}) < len(rows):
                issues.append("DUPLICATE_HORSE_ID")
            if len({row[0] for row in rows}) < len(rows):
                issues.append("DUPLICATE_POST_NUMBER")
            con.execute("INSERT INTO race_audit VALUES (?,?,?,?,?,?)",
                        (rid, len(rows), len(finished), wins, int(not issues), "|".join(issues)))

def build_features(con):
    fields = ("e.race_id,e.horse_no,e.horse_id,e.jockey_id,e.jockey,e.trainer,"
              "e.finish_rank,r.race_date,r.place,r.surface,r.distance_m,r.going,"
              "e.draw,e.impost_kg,e.sexage")
    rows = con.execute(f"SELECT {fields} FROM entries e JOIN races r ON e.race_id=r.race_id "
                       "JOIN race_audit a ON e.race_id=a.race_id "
                       "WHERE a.basic_valid=1 AND e.finish_rank>0 "
                       "ORDER BY r.race_date,e.race_id,e.horse_no").fetchall()
    horse_hist = collections.defaultdict(list)
    jockey_hist = collections.defaultdict(lambda: [0, 0])
    trainer_hist = collections.defaultdict(lambda: [0, 0])
    batches = []
    for date, group in itertools.groupby(rows, key=lambda x: x[7]):
        pending = list(group)
        d = dt.date.fromisoformat(date)
        for row in pending:
            rid, number, hid, jid, jockey, trainer, rank, _, place, surface, dist, going, draw, kg, sexage = row
            h = horse_hist[hid]
            j = jockey_hist[jid or jockey or "?"]
            t = trainer_hist[trainer or "?"]
            recent = h[-3:]
            split = "train" if date < "2024-01-01" else ("validation" if date < "2025-01-01" else "test")
            values = (rid, number, date, split, hid, int(rank == 1), place, surface, dist,
                      going, draw, kg, sexage, len(h), sum(x[1] == 1 for x in h),
                      sum(x[1] <= 3 for x in h),
                      h[-1][1] if h else None,
                      (d - dt.date.fromisoformat(h[-1][0])).days if h else None,
                      sum(x[1] for x in recent) / len(recent) if recent else None,
                      sum(x[2] == surface for x in h),
                      sum(abs(x[3] - dist) <= 200 for x in h if x[3] and dist),
                      sum(x[4] == place for x in h), j[0], j[1], t[0], t[1])
            batches.append(values)
        # Update history only AFTER all runners for this calendar date are featured.
        for row in pending:
            rid, number, hid, jid, jockey, trainer, rank, _, place, surface, dist, going, draw, kg, sexage = row
            horse_hist[hid].append((date, rank, surface, dist, place))
            j = jockey_hist[jid or jockey or "?"]
            j[0] += 1
            j[1] += int(rank == 1)
            t = trainer_hist[trainer or "?"]
            t[0] += 1
            t[1] += int(rank == 1)
    with con:
        con.executemany("INSERT INTO features VALUES (" + ",".join(["?"] * 26) + ")", batches)
    print(f"Generated {len(batches)} leakage-aware feature rows", flush=True)

def csv_export(con, table, name, where="", params=()):
    sql = f"SELECT * FROM {table} " + where
    cursor = con.execute(sql, params)
    columns = [i[0] for i in cursor.description]
    with (OUT / name).open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        while True:
            chunk = cursor.fetchmany(5000)
            if not chunk:
                break
            writer.writerows(chunk)

def run():
    OUT.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(OUT / "jra_history.sqlite")
    con.executescript(DDL)
    con.commit()
    con.execute("PRAGMA journal_mode=WAL")
    log = []
    errors = []
    for place, year in SOURCES:
        try:
            import_source(con, place, year, log)
        except Exception as exc:
            errors.append({"file": f"{place}/{year}_race_results.csv", "error": str(exc)})
            print(f"FAILED {place}/{year}: {exc}", flush=True)
    audit(con)
    build_features(con)
    summary = {"source_commit": COMMIT, "expected_csvs": EXPECTED_SOURCES,
               "downloaded_csvs": len(log), "failed_csvs": errors,
               "races": con.execute("SELECT COUNT(*) FROM races").fetchone()[0],
               "entries": con.execute("SELECT COUNT(*) FROM entries").fetchone()[0],
               "basic_valid_races": con.execute("SELECT COUNT(*) FROM race_audit WHERE basic_valid=1").fetchone()[0]}
    for split in ("train", "validation", "test"):
        summary[split + "_races"] = con.execute("SELECT COUNT(DISTINCT race_id) FROM features WHERE split=?", (split,)).fetchone()[0]
        summary[split + "_entries"] = con.execute("SELECT COUNT(*) FROM features WHERE split=?", (split,)).fetchone()[0]
        csv_export(con, "features", split + ".csv", "WHERE split=? ORDER BY race_date,race_id,horse_no", (split,))
    csv_export(con, "race_audit", "race_audit.csv")
    csv_export(con, "source_files", "source_files.csv")
    summary["limitations"] = [
        "Basic audit does not independently establish that every starter was collected.",
        "Final win odds are post-race market prices, NOT odds recorded 15 minutes before start.",
        "No independent ability model, Benter market combination, or expected values trained yet.",
        "2026 Akikasho 17-horse / 110-run seed from the prior downloadable ZIP is not imported here.",
        "Source licenses and third-party-data redistribution terms require separate review."
    ]
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    if errors or len(log) != EXPECTED_SOURCES:
        raise SystemExit("Data source coverage incomplete; inspect summary.json")
    if summary["train_races"] < 2000 or summary["validation_races"] < 100 or summary["test_races"] < 100:
        raise SystemExit("Insufficient training, validation or test race coverage")
    print("PASS: data acquisition and basic quality thresholds met")

if __name__ == "__main__":
    run()
