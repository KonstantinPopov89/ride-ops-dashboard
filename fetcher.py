#!/usr/bin/env python3
"""Ride Ops Dashboard - fetcher for Lime API (Tyumen park, installationId=52)"""

import os, sys, json, time, argparse
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request, urllib.error

# ── Config ───────────────────────────────────────────────────────────
API_BASE = os.environ.get("LIME_API_BASE", "https://rep.lime-it.ru")
API_TOKEN = os.environ.get("LIME_TOKEN", "")
REPORT_ID = "ServiceSellDetailedReport"
INSTALLATION_ID = 52          # Тюмень
TZ_OFFSET = 5                 # UTC+5
SCH_START = 10 * 60           # 10:00
SCH_END = 22 * 60             # 22:00
SLOT = 15                     # минут
GREEN_TH = 15                 # порог green
YELLOW_MUL = 2                # множитель yellow
MIN_EVENTS_DAY = 50           # мин. событий = рабочий день
EXCLUDE = {"admin", "kenshi2", "kenshNEW", "891256"}
MAX_CHUNK_H = 4               # часов на один запрос
MAX_DAYS = 90                 # хранить событий
SLOTS_DAYS = 30               # дней слотов в drill-down

OUT_JSON = Path("docs/dashboard_data.json")
EVENTS_FILE = Path("data/events.json")
STATE_FILE = Path("state.json")


def now_local():
    return datetime.utcnow() + timedelta(hours=TZ_OFFSET)

def log(msg):
    print(f"[{now_local():%H:%M:%S}] {msg}", flush=True)


# ── API ──────────────────────────────────────────────────────────────

def api_fetch(dt_from, dt_to):
    url = f"{API_BASE}/api/Reports/Report/{REPORT_ID}"
    body = json.dumps({
        "userId": 0, "from": dt_from.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_to.strftime("%Y-%m-%dT%H:%M:%S"),
        "payKinds": [], "showReturns": False,
        "showAdministrative": False, "organizationId": None, "hideBonuses": True,
    }).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Authorization", f"LimeToken {API_TOKEN}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        log(f"  HTTP {e.code}: {e.read().decode()[:120]}")
        return None
    except Exception as e:
        log(f"  Error: {e}")
        return None
    if isinstance(data, dict) and data.get("isErrorDescription"):
        log(f"  API error: {data['errors'][0]['message'][:100]}")
        return None
    if isinstance(data, list) and data and isinstance(data[0], list):
        return data[0]
    return data if isinstance(data, list) else []


def fetch_range(dt_from, dt_to):
    result = []
    cur = dt_from
    while cur < dt_to:
        end = min(cur + timedelta(hours=MAX_CHUNK_H), dt_to)
        log(f"  API {cur:%Y-%m-%d %H:%M} -> {end:%Y-%m-%d %H:%M}")
        chunk = api_fetch(cur, end)
        if chunk is None:
            log("  Skip chunk")
        else:
            # Filter by installation right here
            filtered = [r for r in chunk if r.get("installationId") == INSTALLATION_ID]
            result.extend(filtered)
            log(f"  +{len(filtered)} Tyumen records (of {len(chunk)} total)")
        cur = end
        if cur < dt_to:
            time.sleep(1)
    return result


# ── Events ───────────────────────────────────────────────────────────

def parse_events(records):
    events, names = [], {}
    seen = set()
    for r in records:
        sp = r.get("servicePointName", "")
        t = r.get("installationTime", "")
        if not sp or not t or sp in EXCLUDE:
            continue
        card = r.get("cardCode", "")
        key = f"{t}|{sp}|{card}"
        if key in seen:
            continue
        seen.add(key)
        names[sp] = r.get("serviceName", sp)
        try:
            dt = datetime.fromisoformat(t.split("+")[0].replace("Z", ""))
        except ValueError:
            continue
        events.append({"dt": dt, "rk": sp, "c": card})
    return events, names


def load_events():
    if EVENTS_FILE.exists():
        with open(EVENTS_FILE) as f:
            return [{"dt": datetime.fromisoformat(e["dt"]), "rk": e["rk"], "c": e["c"]} for e in json.load(f)]
    return []


def save_events(events):
    EVENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    cutoff = now_local() - timedelta(days=MAX_DAYS)
    keep = [e for e in events if e["dt"] >= cutoff]
    with open(EVENTS_FILE, "w") as f:
        json.dump([{"dt": e["dt"].isoformat(), "rk": e["rk"], "c": e["c"]} for e in keep], f, separators=(",", ":"))
    log(f"Saved {len(keep)} events")


# ── Dashboard builder ────────────────────────────────────────────────

RIDE_NAMES = {
    "af777015_(ЭКСПРЕСС)": "STAR EXPRESS", "af000003_(БАШНЯ)": "БАШНЯ",
    "af777007_(АКУЛА)": "ГОРКА АКУЛА", "af777008_(РАКУШКИ)": "РАКУШКИ",
    "AF777010": "РОСКОШНАЯ КАРУСЕЛЬ", "af777013_(МАЯТНИК)": "МАЯТНИК",
    "af777018_(МЕДУЗА)": "МЕДУЗА", "af777011_(ЛЕС)": "ВОЛШЕБНЫЙ ЛЕС",
    "AF777016": "АВТОДРОМ", "af777012_(ВЕРТОЛЁТЫ)": "ВЕРТОЛЕТЫ",
    "af777017_(ОСТРОВ)": "ПАРОВОЗИК", "af777006_(АРГО)": "АРГО",
    "AF777023": "ПИРАТ", "af000004_(НЕБО)": "СЕДЬМОЕ НЕБО",
    "af777020_(МИНИКАР)": "МИНИКАР", "af777009_(ТОРНАДО)": "ТОРНАДО",
}


def build_dashboard(events, api_names, loaded_at):
    ride_keys = sorted(set(e["rk"] for e in events))
    day_cnt = {}
    for e in events:
        d = e["dt"].date()
        day_cnt[d] = day_cnt.get(d, 0) + 1
    open_days = sorted(d for d, c in day_cnt.items() if c >= MIN_EVENTS_DAY)
    if not open_days:
        return None

    recent = set(str(d) for d in open_days[-SLOTS_DAYS:])
    asof_end = datetime(loaded_at.year, loaded_at.month, loaded_at.day,
                        loaded_at.hour, (loaded_at.minute // SLOT) * SLOT)
    asof_start = asof_end - timedelta(minutes=SLOT)

    rides = []
    for rk in ride_keys:
        rk_ev = sorted([e for e in events if e["rk"] == rk], key=lambda e: e["dt"])
        days_d, slots_d = [], []
        for day in open_days:
            d0 = datetime.combine(day, datetime.min.time())
            ls = d0 + timedelta(minutes=SCH_START)
            g = y = r = p = 0
            ds = str(day)
            for m in range(SCH_START, SCH_END, SLOT):
                s0 = d0 + timedelta(minutes=m)
                s1 = d0 + timedelta(minutes=m + SLOT)
                se = [e for e in rk_ev if s0 <= e["dt"] < s1]
                if se:
                    ls = se[-1]["dt"]
                mins = round((s1 - ls).total_seconds() / 60)
                st = "g" if mins <= GREEN_TH else ("y" if mins <= GREEN_TH * YELLOW_MUL else "r")
                p += 1
                if st == "g": g += 1
                elif st == "y": y += 1
                else: r += 1
                if ds in recent:
                    slots_d.append([s0.strftime("%Y-%m-%d %H:%M"), len(se), st, mins])
            days_d.append({"date": ds, "uptime": round(g/p*100, 1) if p else 0,
                           "green": g, "yellow": y, "red": r, "planned": p})

        cst, cmins = "red", 999
        ad = asof_start.date()
        if ad in open_days:
            d0 = datetime.combine(ad, datetime.min.time())
            ls2 = d0 + timedelta(minutes=SCH_START)
            for m in range(SCH_START, SCH_END, SLOT):
                s0 = d0 + timedelta(minutes=m)
                s1 = d0 + timedelta(minutes=m + SLOT)
                se = [e for e in rk_ev if s0 <= e["dt"] < s1]
                if se:
                    ls2 = se[-1]["dt"]
                if s0 == asof_start:
                    cmins = round((s1 - ls2).total_seconds() / 60)
                    cst = "green" if cmins <= GREEN_TH else ("yellow" if cmins <= GREEN_TH * YELLOW_MUL else "red")
                    break

        tg = sum(d["green"] for d in days_d)
        tp = sum(d["planned"] for d in days_d)
        name = RIDE_NAMES.get(rk, api_names.get(rk, rk)).upper().strip()
        rides.append({
            "ride_id": rk[:12], "lime_key": rk, "name": name,
            "current_status": cst, "last_signal": "", "minutes_since": cmins,
            "overall_uptime": round(tg/tp*100, 1) if tp else 0,
            "days": days_d, "slots": slots_d,
        })

    rides.sort(key=lambda r: r["overall_uptime"])
    return {
        "config": {"slot_minutes": SLOT, "green_threshold": GREEN_TH,
                    "yellow_multiplier": YELLOW_MUL,
                    "schedule_start": f"{SCH_START//60}:00",
                    "schedule_end": f"{SCH_END//60}:00",
                    "loaded_at": loaded_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "asof_slot": asof_start.strftime("%Y-%m-%d %H:%M:%S")},
        "rides": rides,
        "dates": [str(d) for d in open_days],
    }


# ── Main ─────────────────────────────────────────────────────────────

def run(backfill=None):
    if not API_TOKEN:
        log("LIME_TOKEN not set")
        sys.exit(1)

    state = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}
    now = now_local()

    if backfill:
        dt_from = now - timedelta(days=backfill)
    elif state.get("last_to"):
        dt_from = datetime.fromisoformat(state["last_to"])
    else:
        dt_from = now - timedelta(days=7)

    log(f"Fetch: {dt_from:%Y-%m-%d %H:%M} -> {now:%Y-%m-%d %H:%M}")
    records = fetch_range(dt_from, now)
    log(f"API total: {len(records)} records")

    new_ev, names = parse_events(records)
    log(f"New events: {len(new_ev)}, rides: {len(names)}")

    stored = load_events()
    all_ev = stored + new_ev
    seen = set()
    deduped = []
    for e in all_ev:
        k = f"{e['dt'].isoformat()}|{e['rk']}|{e['c']}"
        if k not in seen:
            seen.add(k)
            deduped.append(e)
    all_ev = deduped
    log(f"Total events: {len(all_ev)}")

    for e in stored:
        if e["rk"] not in names:
            names[e["rk"]] = e["rk"]

    dash = build_dashboard(all_ev, names, now)
    if dash:
        OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        with open(OUT_JSON, "w") as f:
            json.dump(dash, f, ensure_ascii=False, separators=(",", ":"))
        log(f"Dashboard: {len(dash['rides'])} rides, {len(dash['dates'])} days")
    else:
        log("Not enough data for dashboard")

    save_events(all_ev)
    state.update({"last_to": now.isoformat(), "last_run": now.isoformat(),
                  "events": len(all_ev), "rides": len(names)})
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)
    log("Done")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--backfill", type=int)
    a = p.parse_args()
    run(a.backfill)
