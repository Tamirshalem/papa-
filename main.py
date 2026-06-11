"""
PapaGoal v3 — Market Intelligence Engine
─────────────────────────────────────────
- 10 rules engine (R1-R10) with Slow Climb Pattern filter
- match_minute + score persistence in DB
- CSV export endpoints for observations, signals, goals
- Football API integration for live minutes
"""

import os
import time
import json
import csv
import io
import logging
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse
from flask import Flask, jsonify, render_template_string, request, Response
import pg8000.native
import requests

# ─── Config ──────────────────────────────────────────────
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")
FOOTBALL_API_KEY = os.environ.get("FOOTBALL_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
PORT = int(os.environ.get("PORT", 8080))
POLL_INTERVAL = 30  # seconds

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("papagoal")

app = Flask(__name__)

# ─── Database ────────────────────────────────────────────
def parse_db_url(url):
    p = urlparse(url)
    return {
        "host": p.hostname,
        "port": p.port or 5432,
        "database": p.path.lstrip("/"),
        "user": p.username,
        "password": p.password,
        "ssl_context": True
    }

def get_db():
    return pg8000.native.Connection(**parse_db_url(DATABASE_URL))

def init_db():
    conn = get_db()
    try:
        # odds_snapshots with match_minute + score support
        conn.run("""
            CREATE TABLE IF NOT EXISTS odds_snapshots (
                id SERIAL PRIMARY KEY,
                captured_at TIMESTAMPTZ DEFAULT NOW(),
                match_id TEXT,
                home_team TEXT,
                away_team TEXT,
                sport TEXT,
                commence_time TIMESTAMPTZ,
                match_minute INT,
                match_period TEXT,
                score_home INT,
                score_away INT,
                bookmaker TEXT,
                market TEXT,
                outcome TEXT,
                price FLOAT,
                prev_price FLOAT,
                opening_price FLOAT,
                price_held_seconds INT DEFAULT 0
            )
        """)
        conn.run("CREATE INDEX IF NOT EXISTS idx_match_id ON odds_snapshots(match_id)")
        conn.run("CREATE INDEX IF NOT EXISTS idx_captured_at ON odds_snapshots(captured_at)")
        conn.run("CREATE INDEX IF NOT EXISTS idx_match_minute ON odds_snapshots(match_minute)")

        # Add new columns if they don't exist (migration for existing DBs)
        for col_def in [
            "match_minute INT",
            "match_period TEXT",
            "score_home INT",
            "score_away INT",
            "commence_time TIMESTAMPTZ",
            "opening_price FLOAT",
        ]:
            col_name = col_def.split()[0]
            try:
                conn.run(f"ALTER TABLE odds_snapshots ADD COLUMN IF NOT EXISTS {col_def}")
            except Exception as e:
                log.warning(f"Migration warning for {col_name}: {e}")

        conn.run("""
            CREATE TABLE IF NOT EXISTS goals (
                id SERIAL PRIMARY KEY,
                recorded_at TIMESTAMPTZ DEFAULT NOW(),
                match_id TEXT,
                home_team TEXT,
                away_team TEXT,
                match_minute INT,
                score TEXT,
                over_price_30s FLOAT,
                over_price_60s FLOAT,
                notes TEXT
            )
        """)

        conn.run("""
            CREATE TABLE IF NOT EXISTS signals (
                id SERIAL PRIMARY KEY,
                detected_at TIMESTAMPTZ DEFAULT NOW(),
                match_id TEXT,
                home_team TEXT,
                away_team TEXT,
                match_minute INT,
                score_home INT,
                score_away INT,
                rule_id TEXT,
                rule_name TEXT,
                status TEXT,
                market TEXT,
                prediction TEXT,
                over_price FLOAT,
                draw_price FLOAT,
                opening_price FLOAT,
                gap FLOAT,
                slow_climb_present BOOLEAN,
                confidence INT,
                details TEXT
            )
        """)
        # Migration for existing signals table
        for col_def in [
            "match_minute INT",
            "score_home INT",
            "score_away INT",
            "rule_id TEXT",
            "status TEXT",
            "market TEXT",
            "prediction TEXT",
            "opening_price FLOAT",
            "gap FLOAT",
            "slow_climb_present BOOLEAN",
        ]:
            try:
                conn.run(f"ALTER TABLE signals ADD COLUMN IF NOT EXISTS {col_def}")
            except Exception as e:
                log.warning(f"Signals migration warning: {e}")

        log.info("✅ Database initialized")
    except Exception as e:
        log.error(f"DB init error: {e}")
    finally:
        conn.close()


# ─── Live Match Minutes (Football API) ───────────────────
live_match_minutes = {}

def fetch_live_minutes():
    """Fetch live match minutes + scores from API-Football"""
    if not FOOTBALL_API_KEY:
        return
    try:
        headers = {"x-apisports-key": FOOTBALL_API_KEY}
        resp = requests.get(
            "https://v3.football.api-sports.io/fixtures?live=all",
            headers=headers, timeout=10
        )
        if resp.status_code != 200:
            log.warning(f"Football API error: {resp.status_code}")
            return
        data = resp.json()
        for fixture in data.get("response", []):
            teams = fixture.get("teams", {})
            status = fixture.get("fixture", {}).get("status", {})
            goals = fixture.get("goals", {})

            home = teams.get("home", {}).get("name", "")
            away = teams.get("away", {}).get("name", "")
            minute = status.get("elapsed", 0) or 0
            period = status.get("short", "")
            sh = goals.get("home", 0) or 0
            sa = goals.get("away", 0) or 0

            key = (home + "_" + away).lower()
            live_match_minutes[key] = {
                "minute": minute,
                "period": period,
                "sh": sh,
                "sa": sa,
                "score": f"{sh}-{sa}"
            }
        log.info(f"⏱ Got {len(data.get('response', []))} live fixtures")
    except Exception as e:
        log.error(f"Football API error: {e}")

def get_match_state(home, away):
    """Find minute + score for a match, with fuzzy fallback"""
    key = (home + "_" + away).lower()
    if key in live_match_minutes:
        return live_match_minutes[key]

    h_first = home.split()[0].lower() if home else ""
    a_first = away.split()[0].lower() if away else ""
    for k, v in live_match_minutes.items():
        if h_first and h_first in k and a_first and a_first in k:
            return v
    return None


# ─── Opening Odds Cache ──────────────────────────────────
opening_odds = {}

def get_opening_price(match_id, market, outcome, current_price):
    """Get or set opening price for a match+market+outcome"""
    key = f"{match_id}_{market}_{outcome}"
    if key not in opening_odds:
        opening_odds[key] = current_price
    return opening_odds[key]


# ─── Slow Climb Pattern Detector ─────────────────────────
def check_slow_climb(conn, match_id, market, outcome,
                    observations=4, step_min=0.02, step_max=0.05,
                    direction="UP"):
    """
    Detect Slow Climb Pattern in recent observations.

    Returns True if the last N observations show:
    - Consistent direction (all UP or all DOWN)
    - Each step between step_min and step_max
    """
    try:
        rows = conn.run("""
            SELECT price, captured_at
            FROM odds_snapshots
            WHERE match_id = :mid AND market = :mkt AND outcome = :out
            ORDER BY captured_at DESC
            LIMIT :lim
        """, mid=match_id, mkt=market, out=outcome, lim=observations)

        if len(rows) < observations:
            return False

        prices = [float(r[0]) for r in reversed(rows)]

        for i in range(1, len(prices)):
            diff = prices[i] - prices[i-1]
            if direction == "UP":
                if not (step_min <= diff <= step_max):
                    return False
            else:
                if not (step_min <= -diff <= step_max):
                    return False
        return True
    except Exception as e:
        log.error(f"Slow climb check error: {e}")
        return False


# ─── PapaGoal Rules Engine v3 ────────────────────────────
def run_engine(conn, match_id, home, away,
               over_ft, over_ht, draw, home_win, away_win,
               minute, score_home, score_away, opening_over_ft, opening_over_ht):
    """
    Run all 10 rules. Returns list of triggered signals.

    Slow Climb Pattern is applied as:
    - REQUIRED for OVER rules (must be present)
    - ABSENT for UNDER rules (must NOT be present)
    """
    signals = []
    o_ft = over_ft or 0
    o_ht = over_ht or 0
    d = draw or 0
    hw = home_win or 0
    m = minute or 0
    total_goals = (score_home or 0) + (score_away or 0)

    def has_slow_climb(market_key, outcome_name="Over"):
        return check_slow_climb(conn, match_id, market_key, outcome_name)

    def add_signal(rule_id, name, status, market, prediction,
                   confidence, slow_climb_present, gap=None, details=""):
        signals.append({
            "rule_id": rule_id,
            "name": name,
            "status": status,
            "market": market,
            "prediction": prediction,
            "confidence": confidence,
            "slow_climb_present": slow_climb_present,
            "gap": gap,
            "details": details
        })

    # ═══ R1 · Market Shut (UNDER) ═══
    # Slow Climb must be ABSENT
    if m >= 82 and o_ft >= 2.70:
        sc = has_slow_climb("totals")
        if not sc:
            add_signal("R1", "Market Shut", "VALIDATED", "FT", "UNDER",
                       88, False,
                       details=f"Minute {m}, FT Over {o_ft:.2f}, no slow climb detected")

    # ═══ R2 · Early Drop Signal (OVER) ═══
    if 16 <= m <= 20 and 1.40 <= o_ht <= 1.66:
        sc = has_slow_climb("totals_h1")
        if sc:
            add_signal("R2", "Early Drop Signal", "PROMISING", "HT", "OVER",
                       86, True,
                       details=f"Minute {m}, HT Over {o_ht:.2f}, slow climb confirmed")

    # ═══ R3 · H1 Mid Pressure (OVER) ═══
    if 30 <= m <= 35 and 1.80 <= o_ht <= 2.10 and total_goals <= 1:
        sc = has_slow_climb("totals_h1")
        if sc:
            add_signal("R3", "H1 Mid Pressure", "TESTING", "HT", "OVER",
                       78, True,
                       details=f"Minute {m}, HT Over {o_ht:.2f}, goals {total_goals}, slow climb confirmed")

    # ═══ R4 · H1 Mid Shut (UNDER) ═══
    if 30 <= m <= 35 and o_ht >= 2.60:
        sc = has_slow_climb("totals_h1")
        if not sc:
            add_signal("R4", "H1 Mid Shut", "TESTING", "HT", "UNDER",
                       75, False,
                       details=f"Minute {m}, HT Over {o_ht:.2f}, no slow climb detected")

    # ═══ R5 · Late FT Goal Hold (OVER) ═══
    if 83 <= m <= 95 and 2.10 <= o_ft <= 3.00:
        sc = has_slow_climb("totals")
        if sc:
            add_signal("R5", "Late FT Goal Hold", "TESTING", "FT", "OVER",
                       80, True,
                       details=f"Minute {m}, FT Over {o_ft:.2f}, slow climb confirmed")

    # ═══ R6 · H1 Opening Gap Signal (OVER) ═══
    if 25 <= m <= 40 and 1.70 <= o_ht <= 3.50 and opening_over_ht:
        gap = o_ht - opening_over_ht
        if gap >= 0.50:
            sc = has_slow_climb("totals_h1")
            if sc:
                add_signal("R6", "H1 Opening Gap Signal", "TESTING", "HT", "OVER",
                           82, True, gap=gap,
                           details=f"Minute {m}, HT Over {o_ht:.2f}, gap +{gap:.2f}, slow climb confirmed")

    # ═══ R7 · Next Goal Imminent (OVER) ═══
    if m >= 77 and 1.65 <= o_ft <= 1.79:
        sc = has_slow_climb("totals")
        if sc:
            add_signal("R7", "Next Goal Imminent", "TESTING", "NEXT_GOAL", "OVER",
                       82, True,
                       details=f"Minute {m}, Next-goal Over {o_ft:.2f}, slow climb confirmed (score {score_home}-{score_away})")

    # ═══ R8 · Slow Climb Pressure (OVER) ═══
    if 65 <= m <= 80 and 1.45 <= o_ft <= 1.55:
        sc = has_slow_climb("totals")
        if sc:
            add_signal("R8", "Slow Climb Pressure", "TESTING", "NEXT_GOAL", "OVER",
                       85, True,
                       details=f"Minute {m}, Over {o_ft:.2f} climbing slowly — goal imminent")

    # ═══ R9 · H1 Goal Rush Window (OVER) ═══
    if 25 <= m <= 35 and 1.55 <= o_ht <= 1.75:
        sc = has_slow_climb("totals_h1")
        if sc:
            add_signal("R9", "H1 Goal Rush Window", "AI", "HT", "OVER",
                       72, True,
                       details=f"Minute {m}, HT Over {o_ht:.2f}, slow climb confirmed")

    # ═══ R10 · FT Late Comeback Signal (OVER) ═══
    if 60 <= m <= 75 and 1.65 <= o_ft <= 1.95 and total_goals >= 1:
        sc = has_slow_climb("totals")
        if sc:
            add_signal("R10", "FT Late Comeback Signal", "AI", "FT", "OVER",
                       70, True,
                       details=f"Minute {m}, FT Over {o_ft:.2f}, goals {total_goals}, slow climb confirmed")

    return signals


# ─── Odds Collector ──────────────────────────────────────
last_prices = {}

def collect_odds():
    try:
        url = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {
            "apiKey": ODDS_API_KEY,
            "regions": "eu",
            "markets": "h2h,totals",
            "oddsFormat": "decimal",
            "dateFormat": "iso"
        }
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            log.warning(f"Odds API error: {resp.status_code} {resp.text[:200]}")
            return

        games = resp.json()
        log.info(f"📡 Fetched {len(games)} games")

        conn = get_db()
        try:
            for game in games:
                match_id = game["id"]
                home = game["home_team"]
                away = game["away_team"]
                sport = game["sport_key"]
                commence = game.get("commence_time")

                state = get_match_state(home, away)
                minute = state["minute"] if state else None
                period = state["period"] if state else None
                sh = state["sh"] if state else None
                sa = state["sa"] if state else None

                over_ft = None
                over_ht = None
                draw_price = None
                home_win = None
                away_win = None
                opening_over_ft = None
                opening_over_ht = None

                for bookmaker in game.get("bookmakers", [])[:1]:
                    bname = bookmaker["key"]
                    for market in bookmaker.get("markets", []):
                        mkey = market["key"]
                        for outcome in market.get("outcomes", []):
                            oname = outcome["name"]
                            price = float(outcome["price"])
                            key = f"{match_id}_{mkey}_{oname}"
                            now = time.time()

                            prev_price = None
                            held_seconds = 0
                            if key in last_prices:
                                lp = last_prices[key]
                                prev_price = lp["price"]
                                if abs(price - lp["price"]) < 0.01:
                                    held_seconds = int(now - lp["since"])
                                else:
                                    last_prices[key] = {"price": price, "since": now}
                            else:
                                last_prices[key] = {"price": price, "since": now}

                            if key in last_prices:
                                held_seconds = int(now - last_prices[key]["since"])

                            opening = get_opening_price(match_id, mkey, oname, price)

                            if mkey == "totals" and oname == "Over":
                                over_ft = price
                                opening_over_ft = opening
                            elif mkey == "totals_h1" and oname == "Over":
                                over_ht = price
                                opening_over_ht = opening
                            if mkey == "h2h":
                                if oname == "Draw":
                                    draw_price = price
                                elif oname == home:
                                    home_win = price
                                else:
                                    away_win = price

                            conn.run("""
                                INSERT INTO odds_snapshots
                                (match_id, home_team, away_team, sport, commence_time,
                                 match_minute, match_period, score_home, score_away,
                                 bookmaker, market, outcome, price, prev_price,
                                 opening_price, price_held_seconds)
                                VALUES (:mid, :h, :a, :s, :ct, :min, :per, :sh, :sa,
                                        :bm, :mk, :on, :p, :pp, :op, :hs)
                            """,
                            mid=match_id, h=home, a=away, s=sport, ct=commence,
                            min=minute, per=period, sh=sh, sa=sa,
                            bm=bname, mk=mkey, on=oname,
                            p=price, pp=prev_price, op=opening, hs=held_seconds)

                if minute is not None and (over_ft or over_ht):
                    sigs = run_engine(conn, match_id, home, away,
                                      over_ft, over_ht, draw_price, home_win, away_win,
                                      minute, sh, sa, opening_over_ft, opening_over_ht)
                    for s in sigs:
                        conn.run("""
                            INSERT INTO signals
                            (match_id, home_team, away_team, match_minute,
                             score_home, score_away, rule_id, rule_name, status,
                             market, prediction, over_price, draw_price,
                             opening_price, gap, slow_climb_present,
                             confidence, details)
                            VALUES (:mid, :h, :a, :min, :sh, :sa, :rid, :rn, :st,
                                    :mk, :pr, :op, :dp, :opn, :gp, :scp, :cf, :dt)
                        """,
                        mid=match_id, h=home, a=away, min=minute, sh=sh, sa=sa,
                        rid=s["rule_id"], rn=s["name"], st=s["status"],
                        mk=s["market"], pr=s["prediction"],
                        op=over_ft, dp=draw_price,
                        opn=opening_over_ft if s["market"] == "FT" else opening_over_ht,
                        gp=s["gap"], scp=s["slow_climb_present"],
                        cf=s["confidence"], dt=s["details"])
                        log.info(f"🎯 {s['rule_id']} {s['name']} → {home} vs {away} @ {minute}'")

            log.info("✅ Data saved")
        finally:
            conn.close()

    except Exception as e:
        log.error(f"Collect error: {e}", exc_info=True)


def collector_loop():
    time.sleep(5)
    fetch_live_minutes()
    while True:
        try:
            collect_odds()
            fetch_live_minutes()
        except Exception as e:
            log.error(f"Loop error: {e}")
        time.sleep(POLL_INTERVAL)


# ─── Dashboard HTML ──────────────────────────────────────
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PapaGoal v3 ⚽</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #04342C;
    --bg-soft: #052D26;
    --card: #0A4338;
    --border: #1D5C4F;
    --green: #1D9E75;
    --green-soft: #5DCAA5;
    --red: #E04545;
    --yellow: #EF9F27;
    --text: #E0F2EC;
    --muted: #7A9990;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Inter', sans-serif; min-height: 100vh; }
  header {
    background: linear-gradient(90deg, #02261F, #043930);
    border-bottom: 1px solid var(--border);
    padding: 16px 24px; display: flex; align-items: center; gap: 16px;
    position: sticky; top: 0; z-index: 100; flex-wrap: wrap;
  }
  .logo { font-size: 22px; font-family: 'JetBrains Mono', monospace; font-weight: 700; color: #fff; letter-spacing: 2px; }
  .logo span { color: var(--green-soft); }
  .status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green-soft); animation: blink 1.5s infinite; }
  .live-badge { display: flex; align-items: center; gap: 8px; font-size: 11px; color: var(--green-soft); letter-spacing: 1.5px; font-weight: 600; }
  .header-spacer { flex: 1; }
  .header-actions { display: flex; gap: 8px; flex-wrap: wrap; }
  .btn-download {
    background: rgba(29, 158, 117, 0.15); color: var(--green-soft);
    border: 1px solid rgba(29, 158, 117, 0.4);
    padding: 8px 14px; border-radius: 8px; text-decoration: none;
    font-size: 12px; font-weight: 600; cursor: pointer; transition: all 0.2s; font-family: inherit;
  }
  .btn-download:hover { background: rgba(29, 158, 117, 0.3); transform: translateY(-1px); }
  .last-update { font-size: 11px; color: var(--muted); font-family: 'JetBrains Mono', monospace; }
  .container { max-width: 1200px; margin: 0 auto; padding: 24px 16px; }
  .stats-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 32px; }
  .stat-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 18px; text-align: center; }
  .stat-num { font-size: 28px; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
  .stat-label { font-size: 10px; color: var(--muted); letter-spacing: 1.5px; text-transform: uppercase; margin-top: 6px; }
  .section-title { font-size: 11px; letter-spacing: 2px; color: var(--muted); text-transform: uppercase; margin-bottom: 14px; padding-bottom: 8px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; }
  .signals-grid { display: grid; gap: 12px; margin-bottom: 32px; }
  .signal-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px; display: grid; grid-template-columns: auto 1fr auto; gap: 16px; align-items: center; }
  .signal-card.over { border-left: 3px solid var(--green); }
  .signal-card.under { border-left: 3px solid var(--red); }
  .rule-badge { width: 50px; height: 50px; border-radius: 10px; display: flex; flex-direction: column; align-items: center; justify-content: center; font-family: 'JetBrains Mono', monospace; font-weight: 700; flex-shrink: 0; }
  .rule-badge.over { background: rgba(29, 158, 117, 0.15); color: var(--green-soft); }
  .rule-badge.under { background: rgba(224, 69, 69, 0.15); color: var(--red); }
  .rule-id { font-size: 14px; }
  .rule-conf { font-size: 9px; opacity: 0.7; margin-top: 2px; }
  .signal-info .match { font-size: 15px; font-weight: 600; }
  .signal-info .rule-name { font-size: 12px; color: var(--muted); margin-top: 2px; }
  .signal-info .meta { display: flex; gap: 10px; margin-top: 8px; font-family: 'JetBrains Mono', monospace; font-size: 11px; flex-wrap: wrap; }
  .tag { background: rgba(255,255,255,0.05); border-radius: 4px; padding: 3px 8px; color: var(--text); }
  .tag.green { color: var(--green-soft); }
  .tag.yellow { color: var(--yellow); }
  .tag.red { color: var(--red); }
  .verdict { padding: 8px 14px; border-radius: 8px; font-size: 11px; font-weight: 700; letter-spacing: 1px; white-space: nowrap; }
  .verdict.over { background: rgba(29, 158, 117, 0.2); color: var(--green-soft); }
  .verdict.under { background: rgba(224, 69, 69, 0.2); color: var(--red); }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .table-wrap { background: var(--card); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; margin-bottom: 32px; }
  th { background: var(--bg-soft); padding: 12px; text-align: left; font-size: 10px; letter-spacing: 1px; color: var(--muted); text-transform: uppercase; font-weight: 600; }
  td { padding: 10px 12px; border-top: 1px solid rgba(29, 92, 79, 0.5); font-family: 'JetBrains Mono', monospace; }
  tr:hover td { background: rgba(255,255,255,0.02); }
  .empty { text-align: center; padding: 48px; color: var(--muted); font-size: 13px; }
  @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
  @media (max-width: 700px) {
    .stats-row { grid-template-columns: repeat(2, 1fr); }
    .signal-card { grid-template-columns: auto 1fr; }
    .verdict { display: none; }
    .header-actions { width: 100%; justify-content: flex-start; }
  }
</style>
</head>
<body>

<header>
  <div class="logo">PAPA<span>GOAL</span><span style="color:var(--muted);font-size:13px;">v3</span></div>
  <div class="live-badge"><div class="status-dot"></div>LIVE</div>
  <div class="last-update" id="lastUpdate">Loading...</div>
  <div class="header-spacer"></div>
  <div class="header-actions">
    <a href="/api/export/observations" class="btn-download" download>📥 Observations</a>
    <a href="/api/export/signals" class="btn-download" download>📥 Signals</a>
    <a href="/api/export/goals" class="btn-download" download>📥 Goals</a>
  </div>
</header>

<div class="container">

  <div class="stats-row">
    <div class="stat-card"><div class="stat-num" style="color:var(--green-soft)" id="statGames">—</div><div class="stat-label">Live Matches</div></div>
    <div class="stat-card"><div class="stat-num" style="color:var(--yellow)" id="statSignals">—</div><div class="stat-label">Signals Today</div></div>
    <div class="stat-card"><div class="stat-num" style="color:var(--text)" id="statSnapshots">—</div><div class="stat-label">Observations</div></div>
    <div class="stat-card"><div class="stat-num" style="color:var(--red)" id="statGoals">—</div><div class="stat-label">Goals Tracked</div></div>
  </div>

  <div class="section-title"><span>🎯 Recent Signals</span><span style="font-size:10px">last 30 min</span></div>
  <div class="signals-grid" id="signalsGrid">
    <div class="empty">📡 Collecting data... check back in 30 seconds</div>
  </div>

  <div class="section-title"><span>📊 Latest Observations</span><span style="font-size:10px">last 2 min</span></div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr><th>Match</th><th>Min</th><th>Score</th><th>Market</th><th>Outcome</th><th>Price</th><th>Held</th></tr>
      </thead>
      <tbody id="oddsBody"><tr><td colspan="7" class="empty">Loading...</td></tr></tbody>
    </table>
  </div>

</div>

<script>
async function load() {
  try {
    const [stats, signals, odds] = await Promise.all([
      fetch('/api/stats').then(r=>r.json()),
      fetch('/api/signals').then(r=>r.json()),
      fetch('/api/odds').then(r=>r.json())
    ]);

    document.getElementById('statGames').textContent = stats.games || 0;
    document.getElementById('statSignals').textContent = stats.signals_today || 0;
    document.getElementById('statSnapshots').textContent = (stats.snapshots || 0).toLocaleString();
    document.getElementById('statGoals').textContent = stats.goals || 0;
    document.getElementById('lastUpdate').textContent = 'Updated ' + new Date().toLocaleTimeString();

    const sg = document.getElementById('signalsGrid');
    if (!signals.length) {
      sg.innerHTML = '<div class="empty">✅ No active signals right now</div>';
    } else {
      sg.innerHTML = signals.map(s => {
        const cls = (s.prediction || 'OVER').toLowerCase();
        const slowClimb = s.slow_climb_present ? 'SC ✓' : 'no SC';
        const gap = s.gap !== null && s.gap !== undefined ? `gap ${s.gap >= 0 ? '+' : ''}${parseFloat(s.gap).toFixed(2)}` : '';
        return `<div class="signal-card ${cls}">
          <div class="rule-badge ${cls}"><div class="rule-id">${s.rule_id || 'R?'}</div><div class="rule-conf">${s.confidence}%</div></div>
          <div class="signal-info">
            <div class="match">${s.home_team} vs ${s.away_team}</div>
            <div class="rule-name">${s.rule_name}</div>
            <div class="meta">
              <span class="tag">${s.match_minute || '?'}'</span>
              <span class="tag">${s.score_home ?? 0}-${s.score_away ?? 0}</span>
              <span class="tag">${s.market}</span>
              ${s.over_price ? `<span class="tag green">Over ${parseFloat(s.over_price).toFixed(2)}</span>` : ''}
              <span class="tag ${s.slow_climb_present ? 'green' : 'yellow'}">${slowClimb}</span>
              ${gap ? `<span class="tag">${gap}</span>` : ''}
            </div>
          </div>
          <div class="verdict ${cls}">${s.prediction || 'OVER'}</div>
        </div>`;
      }).join('');
    }

    const ob = document.getElementById('oddsBody');
    if (!odds.length) {
      ob.innerHTML = '<tr><td colspan="7" class="empty">No recent observations</td></tr>';
    } else {
      ob.innerHTML = odds.map(o => {
        const held = o.price_held_seconds > 0 ? Math.floor(o.price_held_seconds/60) + 'm ' + (o.price_held_seconds % 60) + 's' : '—';
        return `<tr>
          <td>${o.home_team} vs ${o.away_team}</td>
          <td>${o.match_minute !== null && o.match_minute !== undefined ? o.match_minute + "'" : '—'}</td>
          <td>${(o.score_home ?? '?') + '-' + (o.score_away ?? '?')}</td>
          <td>${o.market}</td>
          <td>${o.outcome}</td>
          <td><b>${parseFloat(o.price).toFixed(2)}</b></td>
          <td>${held}</td>
        </tr>`;
      }).join('');
    }
  } catch(e) { console.error(e); }
}
load();
setInterval(load, 15000);
</script>
</body>
</html>
"""


# ─── API Routes ──────────────────────────────────────────
@app.route("/")
def dashboard():
    return render_template_string(DASHBOARD_HTML)

@app.route("/api/stats")
def api_stats():
    try:
        conn = get_db()
        try:
            r1 = conn.run("SELECT COUNT(DISTINCT match_id) FROM odds_snapshots WHERE captured_at > NOW() - INTERVAL '1 hour'")
            r2 = conn.run("SELECT COUNT(*) FROM signals WHERE detected_at > NOW() - INTERVAL '24 hours'")
            r3 = conn.run("SELECT COUNT(*) FROM odds_snapshots")
            r4 = conn.run("SELECT COUNT(*) FROM goals")
            return jsonify({
                "games": r1[0][0],
                "signals_today": r2[0][0],
                "snapshots": r3[0][0],
                "goals": r4[0][0]
            })
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/signals")
def api_signals():
    try:
        conn = get_db()
        try:
            rows = conn.run("""
                SELECT id, detected_at, match_id, home_team, away_team,
                       match_minute, score_home, score_away,
                       rule_id, rule_name, status, market, prediction,
                       over_price, draw_price, opening_price, gap,
                       slow_climb_present, confidence, details
                FROM signals
                WHERE detected_at > NOW() - INTERVAL '30 minutes'
                ORDER BY detected_at DESC LIMIT 30
            """)
            cols = ["id","detected_at","match_id","home_team","away_team",
                    "match_minute","score_home","score_away",
                    "rule_id","rule_name","status","market","prediction",
                    "over_price","draw_price","opening_price","gap",
                    "slow_climb_present","confidence","details"]
            result = [dict(zip(cols, r)) for r in rows]
            for r in result:
                r["detected_at"] = str(r["detected_at"])
            return jsonify(result)
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/odds")
def api_odds():
    try:
        conn = get_db()
        try:
            rows = conn.run("""
                SELECT DISTINCT ON (match_id, market, outcome)
                    match_id, home_team, away_team, match_minute,
                    score_home, score_away, market, outcome,
                    price, prev_price, price_held_seconds, captured_at
                FROM odds_snapshots
                WHERE captured_at > NOW() - INTERVAL '2 minutes'
                ORDER BY match_id, market, outcome, captured_at DESC
                LIMIT 100
            """)
            cols = ["match_id","home_team","away_team","match_minute",
                    "score_home","score_away","market","outcome",
                    "price","prev_price","price_held_seconds","captured_at"]
            result = [dict(zip(cols, r)) for r in rows]
            for r in result:
                r["captured_at"] = str(r["captured_at"])
            return jsonify(result)
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ═══════ CSV EXPORT ENDPOINTS ════════════════════════════
def csv_response(rows, headers, filename_prefix):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)
    csv_data = output.getvalue()
    filename = f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(csv_data, mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={filename}"})

@app.route("/api/export/observations")
def export_observations():
    try:
        days = request.args.get("days", default=30, type=int)
        conn = get_db()
        try:
            rows = conn.run(f"""
                SELECT id, captured_at, match_id, home_team, away_team,
                       sport, commence_time, match_minute, match_period,
                       score_home, score_away, bookmaker, market, outcome,
                       price, prev_price, opening_price, price_held_seconds
                FROM odds_snapshots
                WHERE captured_at > NOW() - INTERVAL '{int(days)} days'
                ORDER BY captured_at DESC
            """)
            headers = ["id","captured_at","match_id","home_team","away_team",
                       "sport","commence_time","match_minute","match_period",
                       "score_home","score_away","bookmaker","market","outcome",
                       "price","prev_price","opening_price","price_held_seconds"]
            return csv_response(rows, headers, "papagoal_observations")
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Export observations error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/export/signals")
def export_signals():
    try:
        days = request.args.get("days", default=30, type=int)
        conn = get_db()
        try:
            rows = conn.run(f"""
                SELECT id, detected_at, match_id, home_team, away_team,
                       match_minute, score_home, score_away,
                       rule_id, rule_name, status, market, prediction,
                       over_price, draw_price, opening_price, gap,
                       slow_climb_present, confidence, details
                FROM signals
                WHERE detected_at > NOW() - INTERVAL '{int(days)} days'
                ORDER BY detected_at DESC
            """)
            headers = ["id","detected_at","match_id","home_team","away_team",
                       "match_minute","score_home","score_away",
                       "rule_id","rule_name","status","market","prediction",
                       "over_price","draw_price","opening_price","gap",
                       "slow_climb_present","confidence","details"]
            return csv_response(rows, headers, "papagoal_signals")
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Export signals error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/export/goals")
def export_goals():
    try:
        conn = get_db()
        try:
            rows = conn.run("""
                SELECT id, recorded_at, match_id, home_team, away_team,
                       match_minute, score, over_price_30s, over_price_60s, notes
                FROM goals
                ORDER BY recorded_at DESC
            """)
            headers = ["id","recorded_at","match_id","home_team","away_team",
                       "match_minute","score","over_price_30s","over_price_60s","notes"]
            return csv_response(rows, headers, "papagoal_goals")
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Export goals error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "time": datetime.now(timezone.utc).isoformat(),
        "live_matches": len(live_match_minutes)
    })


# ─── Start ──────────────────────────────────────────────
if __name__ == "__main__":
    log.info("🚀 PapaGoal v3 starting...")
    init_db()
    t = threading.Thread(target=collector_loop, daemon=True)
    t.start()
    log.info(f"📡 Collector started — polling every {POLL_INTERVAL}s")
    app.run(host="0.0.0.0", port=PORT, debug=False)
