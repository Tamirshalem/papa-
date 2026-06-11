"""
PapaGoal v4 — Market Intelligence Engine
─────────────────────────────────────────
Merged features:
- All original v10 features (AI analysis, manual goal logging, manual minute override)
- 10 new rules engine (R1-R10) with Slow Climb Pattern filter
- match_minute + score persistence in DB
- CSV export endpoints for observations, signals, goals
- Hebrew RTL dashboard preserved
"""

import os
import time
import csv
import io
import logging
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse
from flask import Flask, jsonify, render_template_string, request, Response
import pg8000.native
import requests

# ─── Config (env var names match Railway exactly) ────────
ODDS_API_KEY = os.environ.get("ODDSAPI_KEY", "")
FOOTBALL_API_KEY = os.environ.get("APIFOOTBALL_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
PORT = int(os.environ.get("PORT", 8080))
POLL_INTERVAL = 30

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
        # odds_snapshots — extended with new columns
        conn.run("""
            CREATE TABLE IF NOT EXISTS odds_snapshots (
                id SERIAL PRIMARY KEY,
                captured_at TIMESTAMPTZ DEFAULT NOW(),
                match_id TEXT, home_team TEXT, away_team TEXT,
                sport TEXT, commence_time TIMESTAMPTZ,
                bookmaker TEXT, market TEXT, outcome TEXT,
                price FLOAT, prev_price FLOAT,
                opening_price FLOAT,
                price_held_seconds INT DEFAULT 0,
                match_minute INT DEFAULT 0,
                match_period TEXT,
                match_score TEXT DEFAULT '0-0',
                score_home INT, score_away INT
            )
        """)
        conn.run("CREATE INDEX IF NOT EXISTS idx_match_id ON odds_snapshots(match_id)")
        conn.run("CREATE INDEX IF NOT EXISTS idx_captured_at ON odds_snapshots(captured_at)")
        conn.run("CREATE INDEX IF NOT EXISTS idx_match_minute ON odds_snapshots(match_minute)")

        # Migration for existing DB
        for col_def in [
            "match_minute INT DEFAULT 0",
            "match_period TEXT",
            "match_score TEXT DEFAULT '0-0'",
            "score_home INT",
            "score_away INT",
            "commence_time TIMESTAMPTZ",
            "opening_price FLOAT",
        ]:
            try:
                conn.run(f"ALTER TABLE odds_snapshots ADD COLUMN IF NOT EXISTS {col_def}")
            except Exception as e:
                log.warning(f"Migration warning: {e}")

        conn.run("""
            CREATE TABLE IF NOT EXISTS goals (
                id SERIAL PRIMARY KEY,
                recorded_at TIMESTAMPTZ DEFAULT NOW(),
                match_id TEXT,
                home_team TEXT,
                away_team TEXT,
                match_minute INT,
                match_score TEXT,
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
                rule_name TEXT,
                rule_id TEXT,
                rule_number INT,
                status TEXT,
                market TEXT,
                prediction TEXT,
                confidence INT,
                verdict TEXT,
                over_price FLOAT,
                draw_price FLOAT,
                opening_price FLOAT,
                gap FLOAT,
                slow_climb_present BOOLEAN,
                match_minute INT DEFAULT 0,
                score_home INT,
                score_away INT,
                details TEXT
            )
        """)
        for col_def in [
            "rule_id TEXT",
            "status TEXT",
            "market TEXT",
            "prediction TEXT",
            "opening_price FLOAT",
            "gap FLOAT",
            "slow_climb_present BOOLEAN",
            "score_home INT",
            "score_away INT",
            "details TEXT",
        ]:
            try:
                conn.run(f"ALTER TABLE signals ADD COLUMN IF NOT EXISTS {col_def}")
            except Exception as e:
                log.warning(f"Signals migration: {e}")

        conn.run("""
            CREATE TABLE IF NOT EXISTS ai_analyses (
                id SERIAL PRIMARY KEY,
                analyzed_at TIMESTAMPTZ DEFAULT NOW(),
                match_id TEXT,
                home_team TEXT,
                away_team TEXT,
                over_price FLOAT,
                draw_price FLOAT,
                match_minute INT,
                analysis TEXT
            )
        """)

        log.info("✅ Database initialized")
    except Exception as e:
        log.error(f"DB init error: {e}")
    finally:
        conn.close()


# ─── State (in-memory) ───────────────────────────────────
last_prices = {}           # key = match_id+market+outcome → {price, since}
match_minutes = {}         # manual minute overrides
live_match_data = {}       # from Football API (live minutes + scores)
opening_odds = {}          # key = match_id+market+outcome → opening price


# ─── Football API — Live Minutes + Scores ────────────────
def fetch_live_minutes():
    """Fetch live minutes + scores from API-Football"""
    if not FOOTBALL_API_KEY:
        log.warning("APIFOOTBALL_KEY not set — skipping live minutes")
        return
    try:
        headers = {"x-apisports-key": FOOTBALL_API_KEY}
        resp = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers=headers,
            params={"live": "all"},
            timeout=10
        )
        if resp.status_code != 200:
            log.warning(f"Football API: {resp.status_code}")
            return
        fixtures = resp.json().get("response", [])
        log.info(f"⏱ Football API: {len(fixtures)} live fixtures")
        for f in fixtures:
            try:
                home = f["teams"]["home"]["name"]
                away = f["teams"]["away"]["name"]
                minute = f["fixture"]["status"]["elapsed"] or 0
                period = f["fixture"]["status"]["short"] or ""
                hg = f["goals"]["home"] or 0
                ag = f["goals"]["away"] or 0
                score = f"{hg}-{ag}"

                payload = {"minute": minute, "score": score, "period": period, "sh": hg, "sa": ag}
                live_match_data[f"{home}_{away}"] = payload
                # Fuzzy keys by first word (for matching with Odds API names)
                if home:
                    live_match_data[home.split()[0].lower()] = payload
                if away:
                    live_match_data[away.split()[0].lower()] = payload
            except Exception as e:
                continue
    except Exception as e:
        log.error(f"Football API error: {e}")


def get_live_data(home, away, match_id=None):
    """Return (minute, score, period, sh, sa)"""
    # Manual override first
    if match_id and match_id in match_minutes:
        return match_minutes[match_id], "0-0", "", 0, 0

    # Try exact match
    key = f"{home}_{away}"
    if key in live_match_data:
        d = live_match_data[key]
        return d["minute"], d["score"], d.get("period", ""), d.get("sh", 0), d.get("sa", 0)

    # Fuzzy by home first word
    h1 = home.split()[0].lower() if home else ""
    if h1 and h1 in live_match_data:
        d = live_match_data[h1]
        return d["minute"], d["score"], d.get("period", ""), d.get("sh", 0), d.get("sa", 0)

    # Fuzzy by away first word
    a1 = away.split()[0].lower() if away else ""
    if a1 and a1 in live_match_data:
        d = live_match_data[a1]
        return d["minute"], d["score"], d.get("period", ""), d.get("sh", 0), d.get("sa", 0)

    return 0, "0-0", "", 0, 0


# ─── Opening Odds Tracker ────────────────────────────────
def get_opening_price(match_id, market, outcome, current_price):
    key = f"{match_id}_{market}_{outcome}"
    if key not in opening_odds:
        opening_odds[key] = current_price
    return opening_odds[key]


# ─── Slow Climb Pattern Detector ─────────────────────────
def check_slow_climb(conn, match_id, market, outcome,
                    observations=4, step_min=0.02, step_max=0.05,
                    direction="UP"):
    """Detect Slow Climb Pattern in recent observations"""
    try:
        rows = conn.run("""
            SELECT price FROM odds_snapshots
            WHERE match_id = :mid AND market = :mkt AND outcome = :out
            ORDER BY captured_at DESC LIMIT :lim
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
        log.error(f"Slow climb error: {e}")
        return False


# ─── PapaGoal Rules Engine v3 (R1-R10) ───────────────────
def run_engine(conn, match_id, home, away,
               over_ft, over_ht, draw, home_win, away_win,
               minute, score_home, score_away,
               opening_over_ft, opening_over_ht):
    """Run all 10 rules. Returns list of triggered signal dicts."""
    signals = []
    o_ft = over_ft or 0
    o_ht = over_ht or 0
    m = minute or 0
    total_goals = (score_home or 0) + (score_away or 0)

    def has_slow_climb(market_key):
        return check_slow_climb(conn, match_id, market_key, "Over")

    def add(rule_id, rule_num, name, status, market, prediction,
            verdict, confidence, slow_climb_present, gap=None, details=""):
        signals.append({
            "rule_id": rule_id,
            "rule_number": rule_num,
            "name": name,
            "status": status,
            "market": market,
            "prediction": prediction,
            "verdict": verdict,
            "confidence": confidence,
            "slow_climb_present": slow_climb_present,
            "gap": gap,
            "details": details
        })

    # ═══ R1 · Market Shut (UNDER) — Slow Climb ABSENT ═══
    if m >= 82 and o_ft >= 2.70:
        if not has_slow_climb("totals"):
            add("R1", 1, "Market Shut", "VALIDATED", "FT", "UNDER",
                "NO GOAL", 88, False,
                details=f"Min {m}, FT Over {o_ft:.2f}, no slow climb")

    # ═══ R2 · Early Drop Signal (OVER) — Slow Climb REQUIRED ═══
    if 16 <= m <= 20 and 1.40 <= o_ht <= 1.66:
        if has_slow_climb("totals_h1"):
            add("R2", 2, "Early Drop Signal", "PROMISING", "HT", "OVER",
                "GOAL ENTRY", 86, True,
                details=f"Min {m}, HT Over {o_ht:.2f}, slow climb confirmed")

    # ═══ R3 · H1 Mid Pressure (OVER) ═══
    if 30 <= m <= 35 and 1.80 <= o_ht <= 2.10 and total_goals <= 1:
        if has_slow_climb("totals_h1"):
            add("R3", 3, "H1 Mid Pressure", "TESTING", "HT", "OVER",
                "GOAL ENTRY", 78, True,
                details=f"Min {m}, HT Over {o_ht:.2f}, goals {total_goals}")

    # ═══ R4 · H1 Mid Shut (UNDER) ═══
    if 30 <= m <= 35 and o_ht >= 2.60:
        if not has_slow_climb("totals_h1"):
            add("R4", 4, "H1 Mid Shut", "TESTING", "HT", "UNDER",
                "NO H1 GOAL", 75, False,
                details=f"Min {m}, HT Over {o_ht:.2f}, no slow climb")

    # ═══ R5 · Late FT Goal Hold (OVER) ═══
    if 83 <= m <= 95 and 2.10 <= o_ft <= 3.00:
        if has_slow_climb("totals"):
            add("R5", 5, "Late FT Goal Hold", "TESTING", "FT", "OVER",
                "GOAL ENTRY", 80, True,
                details=f"Min {m}, FT Over {o_ft:.2f}, slow climb")

    # ═══ R6 · H1 Opening Gap Signal (OVER) ═══
    if 25 <= m <= 40 and 1.70 <= o_ht <= 3.50 and opening_over_ht:
        gap = o_ht - opening_over_ht
        if gap >= 0.50 and has_slow_climb("totals_h1"):
            add("R6", 6, "H1 Opening Gap Signal", "TESTING", "HT", "OVER",
                "GOAL ENTRY", 82, True, gap=gap,
                details=f"Min {m}, HT Over {o_ht:.2f}, gap +{gap:.2f}")

    # ═══ R7 · Next Goal Imminent (OVER) ═══
    if m >= 77 and 1.65 <= o_ft <= 1.79:
        if has_slow_climb("totals"):
            add("R7", 7, "Next Goal Imminent", "TESTING", "NEXT_GOAL", "OVER",
                "GOAL ENTRY", 82, True,
                details=f"Min {m}, Next-goal Over {o_ft:.2f}, score {score_home}-{score_away}")

    # ═══ R8 · Slow Climb Pressure (OVER) ═══
    if 65 <= m <= 80 and 1.45 <= o_ft <= 1.55:
        if has_slow_climb("totals"):
            add("R8", 8, "Slow Climb Pressure", "TESTING", "NEXT_GOAL", "OVER",
                "GOAL ENTRY", 85, True,
                details=f"Min {m}, Over {o_ft:.2f} climbing slowly")

    # ═══ R9 · H1 Goal Rush Window (OVER) ═══
    if 25 <= m <= 35 and 1.55 <= o_ht <= 1.75:
        if has_slow_climb("totals_h1"):
            add("R9", 9, "H1 Goal Rush Window", "AI", "HT", "OVER",
                "GOAL ENTRY", 72, True,
                details=f"Min {m}, HT Over {o_ht:.2f}")

    # ═══ R10 · FT Late Comeback Signal (OVER) ═══
    if 60 <= m <= 75 and 1.65 <= o_ft <= 1.95 and total_goals >= 1:
        if has_slow_climb("totals"):
            add("R10", 10, "FT Late Comeback Signal", "AI", "FT", "OVER",
                "GOAL ENTRY", 70, True,
                details=f"Min {m}, FT Over {o_ft:.2f}, goals {total_goals}")

    return signals


# ─── AI Analysis (Claude) ────────────────────────────────
def get_ai_analysis(home, away, over, draw, home_win, away_win, minute, signals):
    """Get Claude AI analysis in Hebrew for triggered signals"""
    if not ANTHROPIC_API_KEY:
        return None
    try:
        sig_text = ", ".join([f"{s['rule_id']} {s['name']}" for s in signals]) if signals else "אין אותות"
        prompt = f"""אתה PapaGoal AI – מומחה לניתוח שוק הימורים בכדורגל.

משחק: {home} vs {away}
דקה: {minute}
Over: {over} | Draw: {draw} | {home}: {home_win} | {away}: {away_win}
אותות שזוהו: {sig_text}

הפילוסופיה שלנו: אתה לא מנתח משחק – אתה קורא את השוק.
יחסים זזים = כסף חכם נכנס.
Slow Climb (עליה הדרגתית 0.02-0.05 כל ~50s) = השוק מצפה לגול.
Duration Rule: יחס שמחזיק 2+ דקות = שוק מאמין. יחס שקופץ ב-30 שניות = דחייה.

תן המלצה קצרה ב-3 משפטים בעברית:
1. מה השוק אומר?
2. האם כדאי להיכנס?
3. מה הסיכון?"""

        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 300,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=15
        )
        if resp.status_code == 200:
            return resp.json()["content"][0]["text"]
    except Exception as e:
        log.error(f"AI error: {e}")
    return None


# ─── Odds Collector ──────────────────────────────────────
def collect_odds():
    if not ODDS_API_KEY:
        log.warning("ODDSAPI_KEY not set — skipping collection")
        return
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

        # Also fetch scores
        scores_resp = requests.get(
            f"https://api.the-odds-api.com/v4/sports/soccer/scores/?apiKey={ODDS_API_KEY}&daysFrom=1",
            timeout=10
        )
        live_scores = {}
        if scores_resp.status_code == 200:
            for s in scores_resp.json():
                if not s.get("completed") and s.get("scores"):
                    h = next((x["score"] for x in s["scores"] if x["name"] == s["home_team"]), "0")
                    a = next((x["score"] for x in s["scores"] if x["name"] == s["away_team"]), "0")
                    live_scores[s["id"]] = f"{h}-{a}"

        log.info(f"📡 Fetched {len(games)} games")
        conn = get_db()
        try:
            for game in games:
                match_id = game["id"]
                home = game["home_team"]
                away = game["away_team"]
                sport = game["sport_key"]
                commence = game.get("commence_time")

                # Get live minute + score (Football API > Odds API > 0/0-0)
                minute, score, period, sh, sa = get_live_data(home, away, match_id)
                if score == "0-0" and match_id in live_scores:
                    score = live_scores[match_id]
                    try:
                        parts = score.split("-")
                        sh = int(parts[0])
                        sa = int(parts[1])
                    except:
                        pass

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
                                 bookmaker, market, outcome, price, prev_price,
                                 opening_price, price_held_seconds,
                                 match_minute, match_period, match_score,
                                 score_home, score_away)
                                VALUES (:mid, :h, :a, :s, :ct, :bm, :mk, :on, :p, :pp,
                                        :op, :hs, :min, :per, :sc, :sh, :sa)
                            """,
                            mid=match_id, h=home, a=away, s=sport, ct=commence,
                            bm=bname, mk=mkey, on=oname,
                            p=price, pp=prev_price, op=opening, hs=held_seconds,
                            min=minute, per=period, sc=score, sh=sh, sa=sa)

                # Run rules engine
                if minute and (over_ft or over_ht):
                    sigs = run_engine(conn, match_id, home, away,
                                      over_ft, over_ht, draw_price, home_win, away_win,
                                      minute, sh, sa, opening_over_ft, opening_over_ht)
                    for s in sigs:
                        conn.run("""
                            INSERT INTO signals
                            (match_id, home_team, away_team,
                             rule_name, rule_id, rule_number, status,
                             market, prediction, confidence, verdict,
                             over_price, draw_price, opening_price, gap,
                             slow_climb_present, match_minute,
                             score_home, score_away, details)
                            VALUES (:mid, :h, :a, :rn, :rid, :rnum, :st, :mk, :pr,
                                    :cf, :v, :op, :dp, :opn, :gp, :scp, :min,
                                    :sh, :sa, :dt)
                        """,
                        mid=match_id, h=home, a=away,
                        rn=s["name"], rid=s["rule_id"], rnum=s["rule_number"], st=s["status"],
                        mk=s["market"], pr=s["prediction"],
                        cf=s["confidence"], v=s["verdict"],
                        op=over_ft, dp=draw_price,
                        opn=opening_over_ft if s["market"] == "FT" else opening_over_ht,
                        gp=s["gap"], scp=s["slow_climb_present"], min=minute,
                        sh=sh, sa=sa, dt=s["details"])
                        log.info(f"🎯 {s['rule_id']} {s['name']} → {home} vs {away} @ {minute}'")

                    # AI analysis on significant signals
                    if sigs and ANTHROPIC_API_KEY:
                        analysis = get_ai_analysis(home, away, over_ft, draw_price,
                                                    home_win, away_win, minute, sigs)
                        if analysis:
                            conn.run("""
                                INSERT INTO ai_analyses
                                (match_id, home_team, away_team, over_price,
                                 draw_price, match_minute, analysis)
                                VALUES (:mid, :h, :a, :op, :dp, :m, :an)
                            """, mid=match_id, h=home, a=away,
                            op=over_ft, dp=draw_price, m=minute, an=analysis)
                            log.info(f"🤖 AI analysis: {home} vs {away}")

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


# ─── Dashboard HTML (Hebrew RTL preserved) ───────────────
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="he" dir="rtl">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>PapaGoal v4 ⚽</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;700&family=Heebo:wght@400;700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#04040f;--card:#0a0a1e;--border:#1a1a3a;--green:#00ff88;--red:#ff3355;--yellow:#ffcc00;--orange:#ff6b35;--blue:#00cfff;--text:#e0e0ff;--muted:#555577}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Heebo',sans-serif}
header{background:linear-gradient(90deg,#000010,#0a0a2e);border-bottom:1px solid var(--border);padding:16px 24px;display:flex;align-items:center;gap:16px;position:sticky;top:0;z-index:100;flex-wrap:wrap}
.logo{font-size:24px;font-family:'IBM Plex Mono',monospace;font-weight:700;color:#fff;letter-spacing:3px}
.logo span{color:var(--green)}
.logo small{font-size:13px;color:var(--muted)}
.dot{width:10px;height:10px;border-radius:50%;background:var(--green);animation:blink 1s infinite}
.live{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--green);letter-spacing:2px}
.spacer{flex:1}
.dl-btns{display:flex;gap:6px;flex-wrap:wrap}
.dl-btn{background:var(--blue)22;color:var(--blue);border:1px solid var(--blue)44;padding:6px 12px;border-radius:6px;text-decoration:none;font-size:12px;font-weight:600;font-family:inherit;cursor:pointer}
.dl-btn:hover{background:var(--blue)44}
.upd{font-size:11px;color:var(--muted);font-family:'IBM Plex Mono',monospace}
.wrap{max-width:1200px;margin:0 auto;padding:24px 16px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
.sc{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;text-align:center}
.sn{font-size:32px;font-weight:900;font-family:'IBM Plex Mono',monospace}
.sl{font-size:11px;color:var(--muted);margin-top:4px}
.st{font-size:12px;letter-spacing:3px;color:var(--muted);text-transform:uppercase;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border)}
.sg{display:grid;gap:12px;margin-bottom:32px}
.scard{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px}
.scard.green{border-color:var(--green)44}
.scard.red{border-color:var(--red)44}
.scard.yellow{border-color:var(--yellow)44}
.scard.orange{border-color:var(--orange)44}
.scard-top{display:flex;align-items:center;gap:12px}
.rb{width:44px;height:44px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-family:'IBM Plex Mono',monospace;font-weight:700;font-size:13px;flex-shrink:0}
.rb.green{background:var(--green)22;color:var(--green)}
.rb.red{background:var(--red)22;color:var(--red)}
.rb.yellow{background:var(--yellow)22;color:var(--yellow)}
.rb.orange{background:var(--orange)22;color:var(--orange)}
.sm{font-size:15px;font-weight:700}
.srn{font-size:12px;color:var(--muted);margin-top:2px}
.or{display:flex;gap:8px;margin-top:6px;font-family:'IBM Plex Mono',monospace;font-size:12px;flex-wrap:wrap}
.ot{background:#ffffff0a;border-radius:4px;padding:2px 8px}
.ot.sc-yes{background:var(--green)22;color:var(--green)}
.ot.sc-no{background:#ffffff0a;color:var(--muted)}
.vb{padding:6px 14px;border-radius:8px;font-size:12px;font-weight:700;letter-spacing:1px;white-space:nowrap;margin-right:auto}
.vb.green{background:var(--green)22;color:var(--green);border:1px solid var(--green)44}
.vb.red{background:var(--red)22;color:var(--red);border:1px solid var(--red)44}
.vb.yellow{background:var(--yellow)22;color:var(--yellow);border:1px solid var(--yellow)44}
.vb.orange{background:var(--orange)22;color:var(--orange);border:1px solid var(--orange)44}
.ai-box{margin-top:12px;padding:12px;background:#ffffff05;border-radius:8px;border:1px solid #ffffff11;font-size:13px;line-height:1.6;color:#aaa}
.ai-label{font-size:10px;letter-spacing:2px;color:var(--blue);margin-bottom:6px}
.minute-form{display:flex;gap:8px;margin-top:8px;align-items:center}
.minute-input{background:#ffffff0a;border:1px solid var(--border);border-radius:6px;color:var(--text);padding:4px 8px;width:70px;font-size:13px;text-align:center}
.minute-btn{background:var(--blue)22;border:1px solid var(--blue)44;color:var(--blue);border-radius:6px;padding:4px 12px;cursor:pointer;font-size:12px}
.tw{background:var(--card);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:32px}
table{width:100%;border-collapse:collapse;font-size:13px}
th{background:#0f0f2a;padding:10px 12px;text-align:right;font-size:11px;color:var(--muted);font-weight:400}
td{padding:10px 12px;border-top:1px solid var(--border)88}
.empty{text-align:center;padding:40px;color:var(--muted)}
.pu{color:var(--red)}
.pd{color:var(--green)}
.goal-section{background:var(--card);border:1px solid #ff335544;border-radius:12px;padding:20px;margin-bottom:32px}
.goal-form{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}
.finput{background:#ffffff0a;border:1px solid var(--border);border-radius:8px;color:var(--text);padding:8px 12px;font-size:14px;width:100%}
.goal-btn{grid-column:1/-1;background:var(--red)22;border:1px solid var(--red)44;color:var(--red);border-radius:8px;padding:12px;cursor:pointer;font-size:15px;font-weight:700;letter-spacing:1px}
.goal-btn:hover{background:var(--red)44}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.3}}
@media(max-width:600px){.stats{grid-template-columns:repeat(2,1fr)}.goal-form{grid-template-columns:1fr}.dl-btns{width:100%;justify-content:flex-start}}
</style></head>
<body>
<header>
  <div class="logo">PAPA<span>GOAL</span> <small>v4</small></div>
  <div class="live"><div class="dot"></div>LIVE</div>
  <div class="upd" id="upd">מתעדכן...</div>
  <div class="spacer"></div>
  <div class="dl-btns">
    <a href="/api/export/observations" class="dl-btn" download>📥 תצפיות</a>
    <a href="/api/export/signals" class="dl-btn" download>📥 אותות</a>
    <a href="/api/export/goals" class="dl-btn" download>📥 גולים</a>
  </div>
</header>

<div class="wrap">

<div class="stats">
<div class="sc"><div class="sn" style="color:var(--blue)" id="g">—</div><div class="sl">משחקים פעילים</div></div>
<div class="sc"><div class="sn" style="color:var(--green)" id="s">—</div><div class="sl">אותות היום</div></div>
<div class="sc"><div class="sn" style="color:var(--yellow)" id="d">—</div><div class="sl">דגימות נשמרו</div></div>
<div class="sc"><div class="sn" style="color:var(--orange)" id="gl">—</div><div class="sl">גולים מוקלטים</div></div>
</div>

<div class="st">⚽ רישום גול ידני</div>
<div class="goal-section">
  <div style="font-size:13px;color:var(--muted)">כשנכנס גול – רשום אותו כאן לניתוח עתידי</div>
  <div class="goal-form">
    <input class="finput" id="gMatch" placeholder="משחק (לדוגמה: Al-Shabab vs Al-Fateh)">
    <input class="finput" id="gMinute" type="number" placeholder="דקה">
    <input class="finput" id="gScore" placeholder="תוצאה (לדוגמה: 1-0)">
    <input class="finput" id="gNotes" placeholder="הערות (אופציונלי)">
    <button class="goal-btn" onclick="recordGoal()">⚽ רשום גול!</button>
  </div>
</div>

<div class="st">🔥 אותות פעילים – PapaGoal Engine v3</div>
<div class="sg" id="sg"><div class="empty">📡 אוסף נתונים...</div></div>

<div class="st">📊 יחסים אחרונים</div>
<div class="tw"><table>
<thead><tr><th>משחק</th><th>שוק</th><th>תוצאה</th><th>יחס</th><th>שינוי</th><th>החזיק</th><th>דקה</th><th>תוצאה</th></tr></thead>
<tbody id="ob"><tr><td colspan="8" class="empty">טוען...</td></tr></tbody>
</table></div>

</div>

<script>
const cm={
  1:'red', 2:'green', 3:'green', 4:'red', 5:'green', 6:'green',
  7:'green', 8:'green', 9:'green', 10:'green'
};

async function load(){
try{
const[st,si,od,ai]=await Promise.all([
  fetch('/api/stats').then(r=>r.json()),
  fetch('/api/signals').then(r=>r.json()),
  fetch('/api/odds').then(r=>r.json()),
  fetch('/api/ai').then(r=>r.json())
]);
document.getElementById('g').textContent=st.games||0;
document.getElementById('s').textContent=st.signals_today||0;
document.getElementById('d').textContent=(st.snapshots||0).toLocaleString();
document.getElementById('gl').textContent=st.goals||0;
document.getElementById('upd').textContent='עדכון: '+new Date().toLocaleTimeString('he-IL');

const aiMap={};
ai.forEach(a=>aiMap[a.match_id]=a.analysis);

const sg=document.getElementById('sg');
if(!si.length){sg.innerHTML='<div class="empty">✅ אין אותות פעילים כרגע</div>';}
else{
  sg.innerHTML=si.map(s=>{
    const c=cm[s.rule_number]||'yellow';
    const aiText=aiMap[s.match_id]?`<div class="ai-box"><div class="ai-label">🤖 CLAUDE AI</div>${aiMap[s.match_id]}</div>`:'';
    const scTag=s.slow_climb_present?'<span class="ot sc-yes">SC ✓</span>':'<span class="ot sc-no">no SC</span>';
    const gapTag=s.gap?`<span class="ot">gap ${s.gap>=0?'+':''}${parseFloat(s.gap).toFixed(2)}</span>`:'';
    const scoreTag=(s.score_home!==null&&s.score_home!==undefined)?`<span class="ot">${s.score_home}-${s.score_away||0}</span>`:'';
    return`<div class="scard ${c}">
      <div class="scard-top">
        <div class="rb ${c}">${s.rule_id||'R?'}</div>
        <div style="flex:1">
          <div class="sm">${s.home_team} vs ${s.away_team}</div>
          <div class="srn">${s.rule_name} · ${s.market||''}</div>
          <div class="or">
            <span class="ot">Over: ${s.over_price||'—'}</span>
            ${s.draw_price?'<span class="ot">Draw: '+s.draw_price+'</span>':''}
            ${s.match_minute>0?'<span class="ot">⏱ '+s.match_minute+"'</span>":''}
            ${scoreTag}
            ${scTag}
            ${gapTag}
          </div>
        </div>
        <div class="vb ${c}">${s.verdict||s.prediction||''}</div>
      </div>
      <div class="minute-form">
        <span style="font-size:12px;color:var(--muted)">עדכן דקה:</span>
        <input class="minute-input" type="number" id="min_${s.match_id}" placeholder="דקה" value="${s.match_minute||''}">
        <button class="minute-btn" onclick="setMinute('${s.match_id}', document.getElementById('min_${s.match_id}').value)">✓</button>
      </div>
      ${aiText}
    </div>`;
  }).join('');}

const ob=document.getElementById('ob');
if(!od.length){ob.innerHTML='<tr><td colspan="8" class="empty">אין נתונים עדיין</td></tr>';}
else{ob.innerHTML=od.map(o=>{
  const diff=o.prev_price?(o.price-o.prev_price).toFixed(2):null;
  const dc=!diff?'':(parseFloat(diff)>0?'pu':'pd');
  const dt=!diff?'—':(parseFloat(diff)>0?'▲ '+diff:'▼ '+Math.abs(diff));
  const h=o.price_held_seconds>0?Math.floor(o.price_held_seconds/60)+'m '+o.price_held_seconds%60+'s':'—';
  return`<tr><td>${o.home_team} vs ${o.away_team}</td><td>${o.market}</td><td>${o.outcome}</td><td><b>${o.price}</b></td><td class="${dc}">${dt}</td><td>${h}</td><td>${o.match_minute>0?o.match_minute+"'":'—'}</td><td>${o.match_score||'—'}</td></tr>`;
}).join('');}
}catch(e){console.error(e);}
}

async function setMinute(matchId, minute) {
  await fetch('/api/set_minute', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({match_id: matchId, minute: parseInt(minute)||0})});
  load();
}

async function recordGoal() {
  const match = document.getElementById('gMatch').value;
  const minute = document.getElementById('gMinute').value;
  const score = document.getElementById('gScore').value;
  const notes = document.getElementById('gNotes').value;
  if (!match || !minute) { alert('נא למלא משחק ודקה'); return; }
  await fetch('/api/goal', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({match, minute: parseInt(minute), score, notes})});
  document.getElementById('gMatch').value='';
  document.getElementById('gMinute').value='';
  document.getElementById('gScore').value='';
  document.getElementById('gNotes').value='';
  alert('✅ גול נרשם!');
  load();
}

load();
setInterval(load, 15000);
</script>
</body></html>"""


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
        return jsonify({"games": 0, "signals_today": 0, "snapshots": 0, "goals": 0})

@app.route("/api/signals")
def api_signals():
    try:
        conn = get_db()
        try:
            rows = conn.run("""
                SELECT id, detected_at, match_id, home_team, away_team,
                       rule_name, rule_id, rule_number, status, market, prediction,
                       confidence, verdict, over_price, draw_price,
                       opening_price, gap, slow_climb_present,
                       match_minute, score_home, score_away, details
                FROM signals
                WHERE detected_at > NOW() - INTERVAL '30 minutes'
                ORDER BY detected_at DESC LIMIT 30
            """)
            cols = ["id","detected_at","match_id","home_team","away_team",
                    "rule_name","rule_id","rule_number","status","market","prediction",
                    "confidence","verdict","over_price","draw_price",
                    "opening_price","gap","slow_climb_present",
                    "match_minute","score_home","score_away","details"]
            result = [dict(zip(cols, r)) for r in rows]
            for r in result:
                r["detected_at"] = str(r["detected_at"])
            return jsonify(result)
        finally:
            conn.close()
    except Exception as e:
        log.error(f"signals API: {e}")
        return jsonify([])

@app.route("/api/odds")
def api_odds():
    try:
        conn = get_db()
        try:
            rows = conn.run("""
                SELECT DISTINCT ON (match_id, market, outcome)
                    match_id, home_team, away_team, market, outcome,
                    price, prev_price, price_held_seconds, captured_at,
                    match_minute, match_score
                FROM odds_snapshots
                WHERE captured_at > NOW() - INTERVAL '2 minutes'
                ORDER BY match_id, market, outcome, captured_at DESC
                LIMIT 100
            """)
            cols = ["match_id","home_team","away_team","market","outcome",
                    "price","prev_price","price_held_seconds","captured_at",
                    "match_minute","match_score"]
            result = [dict(zip(cols, r)) for r in rows]
            for r in result:
                r["captured_at"] = str(r["captured_at"])
            return jsonify(result)
        finally:
            conn.close()
    except Exception as e:
        log.error(f"odds API: {e}")
        return jsonify([])

@app.route("/api/ai")
def api_ai():
    try:
        conn = get_db()
        try:
            rows = conn.run("""
                SELECT match_id, home_team, away_team, over_price,
                       draw_price, match_minute, analysis
                FROM ai_analyses
                WHERE analyzed_at > NOW() - INTERVAL '30 minutes'
                ORDER BY analyzed_at DESC LIMIT 20
            """)
            cols = ["match_id","home_team","away_team","over_price",
                    "draw_price","match_minute","analysis"]
            return jsonify([dict(zip(cols, r)) for r in rows])
        finally:
            conn.close()
    except Exception as e:
        return jsonify([])

@app.route("/api/set_minute", methods=["POST"])
def api_set_minute():
    data = request.json or {}
    match_id = data.get("match_id")
    minute = int(data.get("minute", 0))
    if match_id:
        match_minutes[match_id] = minute
        log.info(f"⏱ Manual minute set: {match_id} = {minute}'")
    return jsonify({"status": "ok"})

@app.route("/api/goal", methods=["POST"])
def api_goal():
    data = request.json or {}
    try:
        conn = get_db()
        try:
            match_text = data.get("match", "")
            parts = match_text.split(" vs ")
            home = parts[0].strip() if parts else match_text
            away = parts[1].strip() if len(parts) > 1 else ""
            r30 = conn.run("""
                SELECT price FROM odds_snapshots
                WHERE home_team=:a AND market='totals' AND outcome='Over'
                  AND captured_at < NOW() - INTERVAL '30 seconds'
                ORDER BY captured_at DESC LIMIT 1
            """, a=home)
            r60 = conn.run("""
                SELECT price FROM odds_snapshots
                WHERE home_team=:a AND market='totals' AND outcome='Over'
                  AND captured_at < NOW() - INTERVAL '60 seconds'
                ORDER BY captured_at DESC LIMIT 1
            """, a=home)
            conn.run("""
                INSERT INTO goals
                (home_team, away_team, match_minute, match_score,
                 over_price_30s, over_price_60s, notes)
                VALUES (:a, :b, :c, :d, :e, :f, :g)
            """,
            a=home, b=away,
            c=data.get("minute", 0),
            d=data.get("score", ""),
            e=r30[0][0] if r30 else None,
            f=r60[0][0] if r60 else None,
            g=data.get("notes", ""))
            log.info(f"⚽ Goal recorded: {home} vs {away} @ {data.get('minute')}'")
        finally:
            conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        log.error(f"Goal error: {e}")
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
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.route("/api/export/observations")
def export_observations():
    try:
        days = request.args.get("days", default=30, type=int)
        conn = get_db()
        try:
            rows = conn.run(f"""
                SELECT id, captured_at, match_id, home_team, away_team,
                       sport, commence_time, match_minute, match_period,
                       match_score, score_home, score_away,
                       bookmaker, market, outcome,
                       price, prev_price, opening_price, price_held_seconds
                FROM odds_snapshots
                WHERE captured_at > NOW() - INTERVAL '{int(days)} days'
                ORDER BY captured_at DESC
            """)
            headers = ["id","captured_at","match_id","home_team","away_team",
                       "sport","commence_time","match_minute","match_period",
                       "match_score","score_home","score_away",
                       "bookmaker","market","outcome",
                       "price","prev_price","opening_price","price_held_seconds"]
            return csv_response(rows, headers, "papagoal_observations")
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/export/signals")
def export_signals():
    try:
        days = request.args.get("days", default=30, type=int)
        conn = get_db()
        try:
            rows = conn.run(f"""
                SELECT id, detected_at, match_id, home_team, away_team,
                       rule_id, rule_number, rule_name, status, market, prediction,
                       confidence, verdict, over_price, draw_price,
                       opening_price, gap, slow_climb_present,
                       match_minute, score_home, score_away, details
                FROM signals
                WHERE detected_at > NOW() - INTERVAL '{int(days)} days'
                ORDER BY detected_at DESC
            """)
            headers = ["id","detected_at","match_id","home_team","away_team",
                       "rule_id","rule_number","rule_name","status","market","prediction",
                       "confidence","verdict","over_price","draw_price",
                       "opening_price","gap","slow_climb_present",
                       "match_minute","score_home","score_away","details"]
            return csv_response(rows, headers, "papagoal_signals")
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/export/goals")
def export_goals():
    try:
        conn = get_db()
        try:
            rows = conn.run("""
                SELECT id, recorded_at, match_id, home_team, away_team,
                       match_minute, match_score, over_price_30s, over_price_60s, notes
                FROM goals
                ORDER BY recorded_at DESC
            """)
            headers = ["id","recorded_at","match_id","home_team","away_team",
                       "match_minute","match_score","over_price_30s","over_price_60s","notes"]
            return csv_response(rows, headers, "papagoal_goals")
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "time": datetime.now(timezone.utc).isoformat(),
        "live_matches": len(live_match_data)
    })


# ─── Bootstrap (works with both gunicorn and direct run) ─
log.info("🚀 PapaGoal v4 starting...")
init_db()
_collector_thread = threading.Thread(target=collector_loop, daemon=True)
_collector_thread.start()
log.info(f"📡 Collector started — polling every {POLL_INTERVAL}s")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
