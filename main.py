import os, time, json, logging, threading, re
from datetime import datetime, timezone
from urllib.parse import urlparse
from flask import Flask, jsonify, render_template_string, request
import pg8000.native
import requests

ODDSAPI_KEY       = os.environ.get("ODDSAPI_KEY", "")
APIFOOTBALL_KEY   = os.environ.get("APIFOOTBALL_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DATABASE_URL      = os.environ.get("DATABASE_URL", "")
PORT              = int(os.environ.get("PORT", 8080))
POLL_INTERVAL     = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("papagoal")
app = Flask(__name__)

EXPECTED = {
    "H1_0.5": {0:1.25,5:1.28,10:1.32,15:1.38,20:1.45,25:1.55,30:1.68,35:1.85,40:2.10,45:2.50},
    "H1_1.5": {0:2.10,5:2.15,10:2.22,15:2.32,20:2.45,25:2.65,30:2.90,35:3.20,40:3.60,45:4.20},
    "H1_2.5": {0:3.50,5:3.60,10:3.75,15:3.95,20:4.20,25:4.60,30:5.20,35:6.00,40:7.50,45:10.0},
    "FT_0.5": {0:1.10,10:1.12,20:1.15,30:1.20,40:1.28,50:1.38,60:1.55,70:1.85,80:2.50,88:4.00},
    "FT_1.5": {0:1.85,10:1.88,20:1.92,30:2.05,40:2.25,50:2.55,60:3.00,70:3.80,80:5.50,88:9.00},
    "FT_2.5": {0:2.80,10:2.85,20:2.95,30:3.15,40:3.50,50:4.00,60:4.80,70:6.50,80:10.0,88:18.0},
    "FT_3.5": {0:5.50,10:5.60,20:5.80,30:6.20,40:7.00,50:8.50,60:11.0,70:16.0,80:28.0,88:55.0},
}

def get_expected(mtype, line, minute):
    key = f"{mtype}_{line}"
    curve = EXPECTED.get(key)
    if not curve: return None
    keys = sorted(curve.keys())
    m = min(max(minute, 0), keys[-1])
    for i, k in enumerate(keys):
        if m <= k:
            if i == 0: return curve[k]
            prev = keys[i-1]
            r = (m - prev) / (k - prev)
            return round(curve[prev] + r * (curve[k] - curve[prev]), 3)
    return curve[keys[-1]]

def calc_pressure(real, opening, expected):
    if not opening or not real or not expected or opening == 0: return 0
    rise = real / opening
    exp_rise = expected / opening
    if exp_rise <= 0: return 0
    return max(0, min(100, int((1 - rise / exp_rise) * 100)))

def parse_db(url):
    p = urlparse(url)
    return {"host":p.hostname,"port":p.port or 5432,"database":p.path.lstrip("/"),
            "user":p.username,"password":p.password,"ssl_context":True}

# Simple connection pool
import threading
_pool_lock = threading.Lock()
_pool = []
MAX_POOL = 5

def get_db():
    with _pool_lock:
        if _pool:
            return _pool.pop()
    try:
        return pg8000.native.Connection(**parse_db(DATABASE_URL))
    except Exception as e:
        log.error(f"DB connect: {e}")
        raise

def release_db(conn):
    try:
        with _pool_lock:
            if len(_pool) < MAX_POOL:
                _pool.append(conn)
                return
    except: pass
    try: conn.close()
    except: pass

def init_db():
    if not DATABASE_URL:
        log.error("DATABASE_URL not set -- DB skipped")
        return
    try:
        conn = get_db()
    except Exception as e:
        log.error(f"DB connect failed: {e}")
        return
    try:
        conn.run("""CREATE TABLE IF NOT EXISTS matches (
            id SERIAL PRIMARY KEY, mid TEXT UNIQUE NOT NULL, eid TEXT,
            home TEXT DEFAULT '', away TEXT DEFAULT '', league TEXT DEFAULT '',
            minute INT DEFAULT 0, score_home INT DEFAULT 0, score_away INT DEFAULT 0,
            total_goals INT DEFAULT 0, period TEXT DEFAULT 'H1',
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )""")
        for idx in ["CREATE INDEX IF NOT EXISTS idx_m_mid ON matches(mid)",
                    "CREATE INDEX IF NOT EXISTS idx_m_upd ON matches(updated_at)"]:
            try: conn.run(idx)
            except: pass
        conn.run("""CREATE TABLE IF NOT EXISTS goals (
            id SERIAL PRIMARY KEY, mid TEXT NOT NULL, minute INT,
            goal_time TIMESTAMPTZ DEFAULT NOW(),
            score_before TEXT, score_after TEXT, period TEXT,
            home TEXT DEFAULT '', away TEXT DEFAULT '', league TEXT DEFAULT '')""")
        conn.run("""CREATE TABLE IF NOT EXISTS observations (
            id SERIAL PRIMARY KEY, mid TEXT NOT NULL,
            detected_at TIMESTAMPTZ DEFAULT NOW(),
            home TEXT, away TEXT, league TEXT, rule_id INT, rule_name TEXT,
            minute INT DEFAULT 0, score TEXT DEFAULT '0-0',
            mtype TEXT, line FLOAT, over_odd FLOAT, under_odd FLOAT,
            expected_odd FLOAT, gap FLOAT DEFAULT 0, pressure INT DEFAULT 0,
            action_type TEXT, selected_side TEXT, entry_odd FLOAT,
            confidence INT DEFAULT 50, reason TEXT)""")
        conn.run("""CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY, mid TEXT NOT NULL, rule_id INT, rule_name TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(), resolved_at TIMESTAMPTZ,
            home TEXT, away TEXT, league TEXT, mtype TEXT, line FLOAT,
            side TEXT, action_type TEXT, entry_odd FLOAT, expected_odd FLOAT,
            entry_min INT DEFAULT 0, entry_goals INT DEFAULT 0,
            score_entry TEXT DEFAULT '0-0', gap FLOAT DEFAULT 0,
            pressure INT DEFAULT 0, validation_window TEXT DEFAULT '10m',
            result TEXT DEFAULT 'pending', profit FLOAT DEFAULT 0, fail_reason TEXT,
            UNIQUE(mid, rule_id, validation_window))""")
        conn.run("""CREATE TABLE IF NOT EXISTS rules (
            id SERIAL PRIMARY KEY, rule_name TEXT UNIQUE NOT NULL,
            description TEXT, source TEXT DEFAULT 'manual', mtype TEXT,
            line_min FLOAT, line_max FLOAT, min_min INT, min_max INT,
            over_min FLOAT, over_max FLOAT, under_min FLOAT, under_max FLOAT,
            held_min INT DEFAULT 0, action_type TEXT, side TEXT DEFAULT 'over',
            val_window TEXT DEFAULT '10m', status TEXT DEFAULT 'ACTIVE',
            is_active BOOLEAN DEFAULT TRUE, total_signals INT DEFAULT 0,
            wins INT DEFAULT 0, losses INT DEFAULT 0, win_rate FLOAT DEFAULT 0,
            profit FLOAT DEFAULT 0, created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW())""")
        conn.run("""CREATE TABLE IF NOT EXISTS insights (
            id SERIAL PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW(),
            itype TEXT, content TEXT, goals_n INT DEFAULT 0, rules_n INT DEFAULT 0)""")
        conn.run("""CREATE TABLE IF NOT EXISTS match_stats (
            id SERIAL PRIMARY KEY,
            mid TEXT NOT NULL,
            minute INT,
            dangerous_attacks_home INT DEFAULT 0,
            dangerous_attacks_away INT DEFAULT 0,
            shots_home INT DEFAULT 0,
            shots_away INT DEFAULT 0,
            possession_home INT DEFAULT 0,
            saved_at TIMESTAMPTZ DEFAULT NOW())""")
        conn.run("CREATE INDEX IF NOT EXISTS idx_stats_mid ON match_stats(mid,saved_at DESC)")
        conn.run("""CREATE TABLE IF NOT EXISTS key_minutes (
            id SERIAL PRIMARY KEY,
            mid TEXT NOT NULL,
            minute INT NOT NULL,
            score_home INT, score_away INT,
            over_ft NUMERIC, under_ft NUMERIC,
            dangerous_attacks_home INT, dangerous_attacks_away INT,
            saved_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(mid, minute))""")
        conn.run("""CREATE TABLE IF NOT EXISTS odds_snapshots (
            id SERIAL PRIMARY KEY,
            mid TEXT NOT NULL,
            minute INT,
            mtype TEXT,
            line NUMERIC,
            over_odd NUMERIC,
            under_odd NUMERIC,
            saved_at TIMESTAMPTZ DEFAULT NOW())""")
        conn.run("CREATE INDEX IF NOT EXISTS idx_snapshots_mid ON odds_snapshots(mid,saved_at DESC)")
        conn.run("""CREATE TABLE IF NOT EXISTS opening_odds (
            id SERIAL PRIMARY KEY,
            mid TEXT NOT NULL,
            home TEXT, away TEXT, league TEXT,
            mtype TEXT NOT NULL,
            line NUMERIC NOT NULL,
            over_open NUMERIC,
            under_open NUMERIC,
            saved_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(mid, mtype, line))""")
        if conn.run("SELECT COUNT(*) FROM rules")[0][0] == 0:
            _seed_rules(conn)
        conn.run("UPDATE rules SET mtype='H1',action_type='H1_OVER_LINE_BEFORE_HT',val_window='HT',description='Over H1 1.50-1.57 at min 17-20 -- goal before HT' WHERE rule_name='Early Drop Signal' AND mtype='FT'")
        # Fix trades where UNDER won but line was actually crossed (score_entry goals < final goals)
        conn.run("""UPDATE trades SET result='lose',fail_reason='Line crossed -- corrected',
            profit=-100
            WHERE result='win' AND side='under' AND action_type='UNDER_HOLDS_10M'
            AND id IN (
                SELECT t.id FROM trades t
                JOIN matches m ON t.mid=m.mid
                WHERE t.result='win' AND t.side='under' 
                AND m.total_goals > t.line
            )""")
        try:
            conn.run("ALTER TABLE rules ADD COLUMN IF NOT EXISTS min_gap NUMERIC DEFAULT 0")
            conn.run("ALTER TABLE rules ADD COLUMN IF NOT EXISTS max_goals INT DEFAULT NULL")
        except: pass
        # Fix AI rules - they have wrong action_type or val_window
        conn.run("UPDATE rules SET val_window='FT',action_type='H1_OVER_LINE_BEFORE_HT' WHERE source='ai' AND mtype='H1' AND side='over'")
        conn.run("UPDATE rules SET val_window='FT',action_type='OVER_LINE_BEFORE_FT' WHERE source='ai' AND mtype='FT' AND side='over'")
        conn.run("UPDATE rules SET val_window='FT' WHERE val_window IN ('HT','10m','15m','5m')")
        conn.run("UPDATE rules SET over_min=2.70,description='FT Over >=2.70 after min 82 -- hold Under' WHERE rule_name='Market Shut'")
        conn.run("UPDATE rules SET over_min=2.00,over_max=2.65,min_min=85,min_max=95,description='FT Over 2.00-2.65 at min 85+ -- market expects goal' WHERE rule_name='Late FT Goal Hold'")
        conn.run("UPDATE rules SET side='under' WHERE rule_name='Market Shut' AND side='over'")
        # Sync rules stats from existing resolved trades
        conn.run("""UPDATE rules r SET
            wins=(SELECT COUNT(*) FROM trades t WHERE t.rule_id=r.id AND t.result='win'),
            losses=(SELECT COUNT(*) FROM trades t WHERE t.rule_id=r.id AND t.result='lose'),
            total_signals=(SELECT COUNT(*) FROM trades t WHERE t.rule_id=r.id),
            profit=(SELECT COALESCE(SUM(t.profit),0) FROM trades t WHERE t.rule_id=r.id AND t.result!='pending'),
            win_rate=CASE WHEN (SELECT COUNT(*) FROM trades t WHERE t.rule_id=r.id AND t.result!='pending')>0
                THEN ROUND((SELECT COUNT(*) FROM trades t WHERE t.rule_id=r.id AND t.result='win')::numeric/
                    NULLIF((SELECT COUNT(*) FROM trades t WHERE t.rule_id=r.id AND t.result!='pending'),0)*100,1)
                ELSE 0 END""")
        log.info("DB ready")
    except Exception as e:
        log.error(f"DB init: {e}")
    finally:
        conn.close()

def _seed_rules(conn):
    rules = [
        ("Market Shut","FT Over >=2.70 after min 82 -- hold Under","FT",1.5,5.5,82,95,2.70,99.0,None,None,0,"UNDER_HOLDS_10M","under","10m","VALIDATED"),
        ("Early Drop Signal","Over H1 1.50-1.57 at min 17-20 -- goal before HT","H1",0.5,1.5,17,20,1.50,1.57,None,None,0,"H1_OVER_LINE_BEFORE_HT","over","HT","PROMISING"),
        ("H1 Minute 18 Pressure","Over H1 1.40-1.60 at min 15-22","H1",0.5,3.5,15,22,1.40,1.60,None,None,0,"H1_OVER_LINE_BEFORE_HT","over","HT","TESTING"),
        ("H1 Under 1.66","Under H1 1.60-1.72 at min 30-38","H1",0.5,3.5,30,38,None,None,1.60,1.72,0,"UNDER_HOLDS_TO_HT","under","HT","TESTING"),
        ("H1 Opening Gap Signal","H1 Over 0.5 rose 0.50+ from opening by min 25-40 -- gap opportunity","H1",0.5,1.5,25,40,1.70,3.50,None,None,0,"H1_OVER_LINE_BEFORE_HT","over","HT","TESTING"),
        ("FT Opening Gap Signal","FT Over 0.5 rose 0.80+ from opening by min 60-80 -- gap opportunity","FT",0.5,1.5,60,80,2.00,4.00,None,None,0,"OVER_LINE_BEFORE_FT","over","FT","TESTING"),
        ("Late FT Gap Bomb","FT Over 0.5 rose 1.50+ from opening by min 75-88 -- extreme gap","FT",0.5,1.5,75,88,2.80,5.00,None,None,0,"OVER_LINE_WITHIN_10M","over","10m","TESTING"),
        ("Late FT Goal Hold","FT Over 2.00-2.65 at min 85+ -- market expects goal","FT",1.5,4.5,85,95,2.00,2.65,None,None,0,"OVER_LINE_BEFORE_FT","over","5m","TESTING"),
    ]
    for r in rules:
        try:
            conn.run("""INSERT INTO rules (rule_name,description,mtype,line_min,line_max,min_min,min_max,
                 over_min,over_max,under_min,under_max,held_min,action_type,side,val_window,status)
                VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,:j,:k,:l,:m,:n,:o,:p) ON CONFLICT DO NOTHING""",
                a=r[0],b=r[1],c=r[2],d=r[3],e=r[4],f=r[5],g=r[6],h=r[7],i=r[8],
                j=r[9],k=r[10],l=r[11],m=r[12],n=r[13],o=r[14],p=r[15])
        except Exception as e: log.error(f"Seed: {e}")

price_cache = {}
opening_cache = {}
last_goals = {}

def ckey(mid, mtype, line): return f"{mid}_{mtype}_{line}"

def fetch_events():
    if not ODDSAPI_KEY: log.warning("ODDSAPI_KEY not set"); return []
    try:
        r = requests.get("https://api.odds-api.io/v3/events",
            params={"apiKey":ODDSAPI_KEY,"sport":"football","status":"live","limit":50}, timeout=15)
        if r.status_code != 200: log.warning(f"Events {r.status_code}: {r.text[:150]}"); return []
        raw = r.json()
        events = raw if isinstance(raw,list) else (raw.get("data") or raw.get("events") or [])
        log.info(f"OddsAPI: {len(events)} live events")
        return events
    except Exception as e: log.error(f"fetch_events: {e}"); return []

def parse_event(event):
    eid = str(event.get("id") or "")
    home = event.get("home") or ""
    away = event.get("away") or ""
    lg = event.get("league") or ""
    league = lg.get("name","") if isinstance(lg,dict) else str(lg)
    scores = event.get("scores") or {}
    score_h = int(scores.get("home") or 0) if isinstance(scores,dict) else 0
    score_a = int(scores.get("away") or 0) if isinstance(scores,dict) else 0
    periods = scores.get("periods") or {} if isinstance(scores,dict) else {}
    period = "H2" if "p2" in periods else "H1"
    minute = 0
    try:
        start = datetime.fromisoformat(event.get("date","").replace("Z","+00:00"))
        elapsed = (datetime.now(timezone.utc) - start).total_seconds() / 60
        minute = max(0, min(90, int(elapsed)))
        if period == "H2" and minute < 45: minute = 45
    except: pass
    return {"eid":eid,"home":home,"away":away,"league":league,
            "minute":minute,"score_h":score_h,"score_a":score_a,
            "period":period,"total":score_h+score_a}

def fetch_match_stats(fixture_id):
    """Fetch live stats from api-football"""
    if not APIFOOTBALL_KEY: return None
    try:
        r = requests.get("https://v3.football.api-sports.io/fixtures/statistics",
            headers={"x-apisports-key": APIFOOTBALL_KEY},
            params={"fixture": fixture_id},
            timeout=5)
        if r.status_code != 200: return None
        data = r.json()
        teams = data.get("response", [])
        stats = {"dangerous_attacks_home":0,"dangerous_attacks_away":0,
                 "shots_home":0,"shots_away":0,"possession_home":50}
        for i, team in enumerate(teams[:2]):
            prefix = "home" if i==0 else "away"
            for s in team.get("statistics",[]):
                stype = s.get("type","").lower()
                val = s.get("value") or 0
                try: val = int(str(val).replace("%",""))
                except: val = 0
                if "dangerous" in stype: stats[f"dangerous_attacks_{prefix}"] = val
                elif "shots on" in stype: stats[f"shots_{prefix}"] = val
                elif "possession" in stype and prefix=="home": stats["possession_home"] = val
        return stats
    except Exception as e:
        log.debug(f"Stats error: {e}")
        return None

def get_apifootball_id(home, away):
    """Find api-football fixture id by team names"""
    if not APIFOOTBALL_KEY: return None
    try:
        r = requests.get("https://v3.football.api-sports.io/fixtures",
            headers={"x-apisports-key": APIFOOTBALL_KEY},
            params={"live":"all","search":home[:10]},
            timeout=5)
        if r.status_code != 200: return None
        for fix in r.json().get("response",[]):
            h = fix.get("teams",{}).get("home",{}).get("name","").lower()
            a = fix.get("teams",{}).get("away",{}).get("name","").lower()
            if home[:5].lower() in h or away[:5].lower() in a:
                return fix.get("fixture",{}).get("id")
    except: pass
    return None

def get_odds_velocity(conn, mid, mtype, line, current_over):
    """Calculate how fast odds are moving -- key signal"""
    try:
        snaps = conn.run("""SELECT over_odd, saved_at FROM odds_snapshots
            WHERE mid=:a AND mtype=:b AND line=:c
            ORDER BY saved_at DESC LIMIT 4""",
            a=mid, b=mtype, c=line)
        if len(snaps) < 2: return 0, "stable"
        oldest = float(snaps[-1][0]) if snaps[-1][0] else None
        if not oldest or not current_over: return 0, "stable"
        velocity = round(current_over - oldest, 3)
        if velocity > 0.15: direction = "rising_fast"   # market giving up
        elif velocity > 0.05: direction = "rising"
        elif velocity < -0.15: direction = "dropping_fast"  # goal expected
        elif velocity < -0.05: direction = "dropping"
        else: direction = "stable"  # strongest signal!
        return velocity, direction
    except: return 0, "stable"

def save_opening_odds(conn, mid, home, away, league, markets):
    """Save opening odds for a match -- only saves once per mid+mtype+line"""
    saved = 0
    for mkt in markets:
        try:
            conn.run("""INSERT INTO opening_odds (mid,home,away,league,mtype,line,over_open,under_open)
                VALUES (:a,:b,:c,:d,:e,:f,:g,:h)
                ON CONFLICT (mid,mtype,line) DO NOTHING""",
                a=mid,b=home,c=away,d=league,
                e=mkt["mtype"],f=mkt["line"],
                g=mkt.get("over"),h=mkt.get("under"))
            saved += 1
        except Exception as e:
            log.debug(f"Opening odds: {e}")
    if saved > 0:
        log.info(f"💾 Opening odds saved: {home} vs {away} -- {saved} markets")

def get_opening(conn, mid, mtype, line):
    """Get opening odds for a specific market"""
    try:
        r = conn.run("SELECT over_open,under_open FROM opening_odds WHERE mid=:a AND mtype=:b AND line=:c",
            a=mid,b=mtype,c=line)
        return {"over":float(r[0][0]),"under":float(r[0][1])} if r else None
    except: return None

def check_rules(conn, mid, home, away, league, minute, sh, sa, period, markets, held_map):
    try:
        rules = conn.run("""SELECT id,rule_name,mtype,line_min,line_max,
            min_min,min_max,over_min,over_max,under_min,under_max,
            held_min,COALESCE(min_gap,0),action_type,side,val_window,status,
            COALESCE(max_goals,999)
            FROM rules WHERE is_active=TRUE""")
    except Exception as e:
        log.error(f"Rules fetch: {e}"); return

    total_goals = sh + sa

    for rule in rules:
        (rid,rname,mtype,lmin,lmax,mmin,mmax,
         ovmin,ovmax,unmin,unmax,held_min,min_gap,action,side,val_win,status,max_goals_rule) = rule
        if mmin and minute < mmin: continue
        if mmax and minute > mmax: continue
        if max_goals_rule < 999 and total_goals > max_goals_rule: continue

        # Special condition: FT Late Comeback requires 2+ goals already
        if rname == "FT Late Comeback Signal" and total_goals < 2: continue
        # Check max_goals condition
        if min_gap and min_gap > 0:
            pass  # checked per market below

        # Check minimum GAP from opening odds
        if min_gap and min_gap > 0:
            op_db = get_opening(conn, mid, mtype, str(lmin))
            if op_db:
                op_side_open = op_db.get("over") if side=="over" else op_db.get("under")
                if op_side_open:
                    # We need current odds to check gap -- get from markets
                    pass  # gap will be checked per market below

        for mkt in markets:
            if mkt["mtype"] != mtype: continue
            line = mkt["line"]
            if lmin and line < lmin: continue
            if lmax and line > lmax: continue
            over  = mkt.get("over")
            under = mkt.get("under")
            if side == "over":
                if over is None: continue
                if ovmin and over < ovmin: continue
                if ovmax and over > ovmax: continue
                entry_odd = over
            else:
                if under is None: continue
                if unmin and under < unmin: continue
                if unmax and under > unmax: continue
                entry_odd = under

            # Check GAP from opening if required
            if min_gap and min_gap > 0:
                op_db = get_opening(conn, mid, mtype, str(line))
                if op_db:
                    op_s = float(op_db.get("over") or 0) if side=="over" else float(op_db.get("under") or 0)
                    actual_gap = entry_odd - op_s if op_s else 0
                    if actual_gap < min_gap: continue
                else:
                    continue  # No opening odds saved yet -- skip

            hk = ckey(mid, mtype, str(line))
            held = held_map.get(hk, 0)
            if held_min and held < held_min: continue

            exp = get_expected(mtype, str(line), minute)
            # Use real opening odds from DB
            op_db = get_opening(conn, mid, mtype, str(line))
            op = op_db or opening_cache.get(hk)
            op_over = float(op.get("over") or 0) if op else None
            op_under = float(op.get("under") or 0) if op else None
            op_side = op_over if side=="over" else op_under
            # GAP = current odd vs opening odd (positive = moved away = market signal)
            if op_side and entry_odd:
                gap = round(entry_odd - op_side, 3)
            elif exp and entry_odd:
                gap = round(exp - entry_odd, 3)
            else:
                gap = 0
            pres = calc_pressure(entry_odd, op_side, exp) if op_side and exp else 0
            score_str = f"{sh}-{sa}"
            confidence = min(95, 50+pres//3+(20 if status=="VALIDATED" else 10 if status=="PROMISING" else 0))

            try:
                ex = conn.run("SELECT COUNT(*) FROM trades WHERE mid=:a AND rule_id=:b AND line=:c AND result='pending'",
                    a=mid, b=rid, c=line)
                if ex[0][0] > 0: continue

                conn.run("""INSERT INTO observations
                    (mid,home,away,league,rule_id,rule_name,minute,score,
                     mtype,line,over_odd,under_odd,expected_odd,gap,pressure,
                     action_type,selected_side,entry_odd,confidence,reason)
                    VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,:j,:k,:l,:m,:n,:o,:p,:q,:r,:s,:t)""",
                    a=mid,b=home,c=away,d=league,e=rid,f=rname,g=minute,h=score_str,
                    i=mtype,j=line,k=over,l=under,m=exp,n=gap,o=pres,
                    p=action,q=side,r=entry_odd,s=confidence,
                    t=f"{rname}: {side} {line} @ {entry_odd} gap={gap:.2f}")

                conn.run("""INSERT INTO trades
                    (mid,rule_id,rule_name,home,away,league,mtype,line,side,action_type,
                     entry_odd,expected_odd,entry_min,entry_goals,score_entry,
                     gap,pressure,validation_window)
                    VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,:j,:k,:l,:m,:n,:o,:p,:q,:r)""",
                    a=mid,b=rid,c=rname,d=home,e=away,f=league,g=mtype,h=line,
                    i=side,j=action,k=entry_odd,l=exp,m=minute,n=sh+sa,
                    o=score_str,p=gap,q=pres,r=val_win)

                conn.run("UPDATE rules SET total_signals=total_signals+1,updated_at=NOW() WHERE id=:a", a=rid)
                log.info(f"🎯 SIGNAL: {rname} | {home} vs {away} | {mtype}{line} {side}@{entry_odd} min:{minute}")
            except Exception as e:
                log.debug(f"Signal: {e}")

def validate_trades(conn):
    try:
        pending = conn.run("""SELECT id,mid,rule_id,action_type,validation_window,
            entry_odd,side,mtype,line,created_at,entry_goals
            FROM trades WHERE result='pending'""")
    except Exception as e: log.warning(f"Validate: {e}"); return
    for p in pending:
        tid,mid,rid,action,val_win,entry_odd,side,mtype,line,created_at,entry_goals = p
        now = datetime.now(timezone.utc)
        created = created_at if created_at.tzinfo else created_at.replace(tzinfo=timezone.utc)
        elapsed = (now - created).total_seconds() / 60
        try:
            mrow = conn.run("SELECT score_home,score_away,minute,period,total_goals FROM matches WHERE mid=:a",a=mid)
            sh,sa,cur_min,cur_period,cur_goals = mrow[0] if mrow else (0,0,0,"FT",0)
        except: sh,sa,cur_min,cur_period,cur_goals = 0,0,0,"FT",0
        total_now = (sh or 0)+(sa or 0)
        goals_since = max(0,total_now-(entry_goals or 0))
        line_crossed = total_now > float(line or 0)
        result = fail = None
        if action == "OVER_LINE_WITHIN_10M":
            if goals_since>0 and line_crossed: result="win"
            elif elapsed>12: result,fail="lose","No goal in 10min"
        elif action == "OVER_LINE_WITHIN_15M":
            if goals_since>0 and line_crossed: result="win"
            elif elapsed>17: result,fail="lose","No goal in 15min"
        elif action == "OVER_LINE_WITHIN_5M":
            if goals_since>0 and line_crossed: result="win"
            elif elapsed>7: result,fail="lose","No goal in 5min"
        elif action == "UNDER_HOLDS_10M":
            if line_crossed: result,fail="lose","Line crossed"
            elif cur_period=="FT": result="win"
            elif cur_min and entry_min and cur_min >= 95: result="win"  # match clearly over
        elif action in ("H1_OVER_LINE_BEFORE_HT","H1_GOAL_BEFORE_HT"):
            ht = cur_period in ("H2","FT","HT") or (cur_period=="H1" and (cur_min or 0)>=45)
            if cur_period=="FT": result="win" if line_crossed else "lose"; fail="Not crossed by FT" if result=="lose" else None
            elif ht: result="win" if line_crossed else "lose"; fail="Not crossed by HT" if result=="lose" else None
            elif elapsed>65: result,fail="lose","HT timeout"
        elif action == "UNDER_HOLDS_TO_HT":
            ht = cur_period in ("H2","FT","HT") or (cur_period=="H1" and (cur_min or 0)>=45)
            if cur_period=="FT": result="win" if not line_crossed else "lose"; fail="Line crossed" if result=="lose" else None
            elif ht: result="win" if not line_crossed else "lose"; fail="Line crossed before HT" if result=="lose" else None
            elif elapsed>65: result,fail="lose","HT timeout"
        elif action in ("OVER_LINE_BEFORE_FT","GOAL_BY_FT"):
            if cur_period=="FT": result="win" if line_crossed else "lose"; fail="Not crossed by FT" if result=="lose" else None
            elif elapsed>35: result,fail="lose","FT timeout"
        if result:
            profit = round((float(entry_odd or 1)-1)*100,2) if result=="win" else -100.0
            emoji = "✅ WIN" if result=="win" else "❌ LOSE"
            log.info(f"{emoji} | {action} | {side}@{entry_odd} | {fail or 'resolved'} | P&L: ?{profit:+.0f}")
            try:
                conn.run("""UPDATE trades SET result=:a,resolved_at=NOW(),profit=:b,fail_reason=:c WHERE id=:d""",
                    a=result,b=profit,c=fail,d=tid)
                if result=="win":
                    conn.run("""UPDATE rules SET wins=wins+1,win_rate=ROUND((wins+1)::float/(wins+losses+1)*100,1),profit=profit+:a,updated_at=NOW() WHERE id=:b""",a=profit,b=rid)
                else:
                    conn.run("""UPDATE rules SET losses=losses+1,win_rate=CASE WHEN wins+losses+1>0 THEN ROUND(wins::float/(wins+losses+1)*100,1) ELSE 0 END,profit=profit+:a,updated_at=NOW() WHERE id=:b""",a=profit,b=rid)
            except Exception as e: log.debug(f"Validate update: {e}")

def collect():
    try:
        events = fetch_events()
        if not events: return
        conn = get_db()
        try:
            live_cnt = 0
            held_map = {}
            for event in events:
                p = parse_event(event)
                if not p["home"] or not p["away"]: continue
                mid = f"pg_{p['eid']}"
                live_cnt += 1
                try:
                    updated = conn.run("""UPDATE matches SET home=:b,away=:c,league=:d,minute=:e,
                        score_home=:f,score_away=:g,total_goals=:h,period=:i,updated_at=NOW()
                        WHERE mid=:a""",
                        a=mid,b=p["home"],c=p["away"],d=p["league"],e=p["minute"],
                        f=p["score_h"],g=p["score_a"],h=p["total"],i=p["period"])
                    if not updated:
                        conn.run("""INSERT INTO matches (mid,eid,home,away,league,minute,score_home,score_away,total_goals,period)
                            VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,:j)""",
                            a=mid,b=p["eid"],c=p["home"],d=p["away"],e=p["league"],
                            f=p["minute"],g=p["score_h"],h=p["score_a"],i=p["total"],j=p["period"])
                except Exception as me:
                    log.debug(f"Match upsert: {me}")
                    # Don't continue -- still fetch odds even if match upsert failed
                prev = last_goals.get(mid)
                if prev is not None and p["total"] > prev:
                    log.info(f"GOAL: {p['home']} vs {p['away']} {p['score_h']}-{p['score_a']} min:{p['minute']}")
                    try:
                        conn.run("""INSERT INTO goals (mid,minute,score_before,score_after,period,home,away,league)
                            VALUES (:a,:b,:c,:d,:e,:f,:g,:h)""",
                            a=mid,b=p["minute"],c=str(prev),d=f"{p['score_h']}-{p['score_a']}",
                            e=p["period"],f=p["home"],g=p["away"],h=p["league"])
                    except Exception as ge: log.error(f"Goal: {ge}")
                last_goals[mid] = p["total"]

                # Fetch odds from Bet365
                markets = []
                log.info(f"🔍 Fetching odds for {p['home']} vs {p['away']} (eid:{p['eid']})")
                try:
                    for bk in ["Bet365","Sbobet","1xbet","Unibet","Betfair Sportsbook"]:
                        r_odds = requests.get("https://api.odds-api.io/v3/odds",
                            params={"apiKey":ODDSAPI_KEY,"eventId":p["eid"],"bookmakers":bk},
                            timeout=5)
                        if r_odds.status_code != 200: continue
                        od = r_odds.json()
                        bk_markets = od.get("bookmakers",{}).get(bk,[]) or []
                        for mkt in bk_markets:
                            mname = mkt.get("name","")
                            if mname not in ("Totals","Totals HT","Goals Over/Under"): continue
                            mtype = "H1" if "HT" in mname else "FT"
                            for o in mkt.get("odds",[]):
                                try: line = float(o.get("hdp") or o.get("line") or 2.5) if str(o.get("hdp","")).strip() not in ("N/A","") else 2.5
                                except: line = 2.5
                                try: over = float(o.get("over") or 0) if str(o.get("over","")).strip() not in ("N/A","") else None
                                except: over = None
                                try: under = float(o.get("under") or 0) if str(o.get("under","")).strip() not in ("N/A","") else None
                                except: under = None
                                if over and over > 1:
                                    markets.append({"mtype":mtype,"line":line,"over":over,"under":under})
                                    k = ckey(mid,mtype,str(line))
                                    if k not in opening_cache:
                                        opening_cache[k] = {"over":over,"under":under}
                        if markets:
                            log.info(f"📊 Odds: {p['home']} vs {p['away']} -- {len(markets)} markets [{bk}]")
                            break
                except Exception as oe:
                    log.warning(f"📊 Odds error: {oe}")

                if markets:
                    # Always save opening odds (first time only)
                    save_opening_odds(conn,mid,p["home"],p["away"],p["league"],markets)
                    # Always save snapshot every 30s for goal analysis
                    for mkt in markets:
                        try:
                            conn.run("""INSERT INTO odds_snapshots 
                                (mid,minute,mtype,line,over_odd,under_odd)
                                VALUES (:a,:b,:c,:d,:e,:f)""",
                                a=mid,b=p["minute"],c=mkt["mtype"],
                                d=mkt["line"],e=mkt.get("over"),f=mkt.get("under"))
                        except: pass
                    # Save key minutes (70, 75, 80, 85, 90)
                    cur_min = p["minute"]
                    if cur_min in (70,75,80,85,90):
                        ft_snap = next((m for m in markets if m["mtype"]=="FT" and m["line"]==2.5),None)
                        try:
                            conn.run("""INSERT INTO key_minutes 
                                (mid,minute,score_home,score_away,over_ft,under_ft)
                                VALUES (:a,:b,:c,:d,:e,:f)
                                ON CONFLICT (mid,minute) DO NOTHING""",
                                a=mid,b=cur_min,c=p["score_h"],d=p["score_a"],
                                e=ft_snap.get("over") if ft_snap else None,
                                f=ft_snap.get("under") if ft_snap else None)
                        except: pass
                    # Fetch dangerous attacks from api-football (every ~60s to save quota)
                    if APIFOOTBALL_KEY and cur_min % 2 == 0:
                        fid = get_apifootball_id(p["home"],p["away"])
                        if fid:
                            stats = fetch_match_stats(fid)
                            if stats:
                                try:
                                    conn.run("""INSERT INTO match_stats 
                                        (mid,minute,dangerous_attacks_home,dangerous_attacks_away,
                                        shots_home,shots_away,possession_home)
                                        VALUES (:a,:b,:c,:d,:e,:f,:g)""",
                                        a=mid,b=cur_min,
                                        c=stats["dangerous_attacks_home"],
                                        d=stats["dangerous_attacks_away"],
                                        e=stats["shots_home"],f=stats["shots_away"],
                                        g=stats["possession_home"])
                                    log.debug(f"Stats saved: {p['home']} DA={stats['dangerous_attacks_home']}/{stats['dangerous_attacks_away']}")
                                except: pass
                    # Run rules
                    check_rules(conn,mid,p["home"],p["away"],p["league"],
                               p["minute"],p["score_h"],p["score_a"],p["period"],
                               markets,held_map)
            validate_trades(conn)
            log.info(f"Saved | live:{live_cnt}/{len(events)}")
        finally: release_db(conn)
    except Exception as e: log.error(f"Collect: {e}")

def collector_loop():
    if not DATABASE_URL or not ODDSAPI_KEY:
        log.warning("Missing env vars -- collector paused")
        return
    time.sleep(5)
    while True:
        try:
            collect()
        except Exception as e:
            log.error(f"Collector: {e}")
        time.sleep(POLL_INTERVAL)

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>PapaGoal</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Inter:wght@300;400;600;700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#0A0F1E;--bg2:#0F172A;--card:#131929;--card2:#1a2235;--border:#1e2d45;--border2:#243452;--blue:#3B82F6;--green:#10B981;--red:#EF4444;--yellow:#F59E0B;--purple:#8B5CF6;--text:#E2E8F0;--muted:#64748B}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;display:flex}
.sidebar{width:220px;min-height:100vh;background:var(--bg2);border-right:1px solid var(--border);display:flex;flex-direction:column;position:fixed;top:0;left:0;bottom:0;z-index:100}
.logo{padding:20px 16px;border-bottom:1px solid var(--border)}
.logo-main{font-family:'JetBrains Mono',monospace;font-size:17px;font-weight:700;color:#fff;letter-spacing:2px}
.logo-main span{color:var(--blue)}
.logo-sub{font-size:10px;color:var(--muted);margin-top:2px}
.nav{flex:1;padding:12px 8px}
.nav-item{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:8px;font-size:13px;color:var(--muted);cursor:pointer;transition:all 0.15s;margin-bottom:2px;border:none;background:none;width:100%;text-align:left;font-family:'Inter',sans-serif}
.nav-item:hover{background:var(--card);color:var(--text)}
.nav-item.active{background:rgba(59,130,246,0.15);color:var(--blue)}
.main{margin-left:220px;flex:1}
.page{display:none;padding:24px;max-width:1300px}
.page.active{display:block}
.ph{margin-bottom:20px;display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px}
.pt{font-size:22px;font-weight:700}
.ps{font-size:12px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-top:4px}
.sr{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px}
.sc{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px}
.sn{font-size:26px;font-weight:900;font-family:'JetBrains Mono',monospace}
.sl{font-size:11px;color:var(--muted);margin-top:4px}
.stit{font-size:11px;letter-spacing:3px;color:var(--muted);text-transform:uppercase;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border)}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;margin-bottom:10px;transition:border-color 0.2s}
.card.goal{border-color:rgba(59,130,246,0.5);background:linear-gradient(135deg,rgba(59,130,246,0.05),var(--card))}
.card.win{border-color:rgba(16,185,129,0.5)}
.card.lose{border-color:rgba(239,68,68,0.4)}
.card.hot{border-color:rgba(59,130,246,0.8);box-shadow:0 0 15px rgba(59,130,246,0.15)}
.ctop{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;gap:8px}
.mn{font-size:15px;font-weight:700}
.ml{font-size:11px;color:var(--muted);margin-top:2px}
.bgs{display:flex;gap:5px;align-items:center;flex-wrap:wrap}
.bg{padding:3px 8px;border-radius:5px;font-size:11px;font-weight:600;font-family:'JetBrains Mono',monospace}
.bgb{background:rgba(59,130,246,0.15);color:var(--blue);border:1px solid rgba(59,130,246,0.3)}
.bgg{background:rgba(16,185,129,0.12);color:var(--green);border:1px solid rgba(16,185,129,0.3)}
.bgr{background:rgba(239,68,68,0.12);color:var(--red);border:1px solid rgba(239,68,68,0.3)}
.bgy{background:rgba(245,158,11,0.12);color:var(--yellow);border:1px solid rgba(245,158,11,0.3)}
.bgp{background:rgba(139,92,246,0.12);color:var(--purple);border:1px solid rgba(139,92,246,0.3)}
.or{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}
.ot{background:var(--card2);border:1px solid var(--border2);border-radius:6px;padding:5px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;display:flex;flex-direction:column;align-items:center;gap:1px;min-width:60px}
.ol{font-size:9px;color:var(--muted);letter-spacing:1px}
.ov{font-size:13px;font-weight:700}
.rec-box{background:rgba(59,130,246,0.08);border:1px solid rgba(59,130,246,0.3);border-radius:8px;padding:12px;margin-bottom:8px}
.rec-title{font-size:13px;font-weight:700;color:var(--blue);margin-bottom:6px}
.rec-row{display:flex;gap:16px;font-size:12px;color:var(--muted);flex-wrap:wrap}
.rec-val{color:var(--text);font-weight:600;font-family:'JetBrains Mono',monospace}
.pbar{height:4px;background:var(--border2);border-radius:2px;margin:6px 0;overflow:hidden}
.pfill{height:100%;border-radius:2px}
.pb6{height:6px;background:var(--card2);border-radius:3px;overflow:hidden;margin:3px 0}
.pf6{height:100%;border-radius:3px}
.status-badge{padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;font-family:'JetBrains Mono',monospace}
.s-validated{background:rgba(16,185,129,0.15);color:var(--green)}
.s-promising{background:rgba(59,130,246,0.15);color:var(--blue)}
.s-testing{background:rgba(245,158,11,0.15);color:var(--yellow)}
.s-rejected{background:rgba(239,68,68,0.15);color:var(--red)}
.s-dangerous{background:rgba(239,68,68,0.2);color:var(--red)}
.s-active{background:rgba(139,92,246,0.15);color:var(--purple)}
.toggle{padding:4px 12px;border-radius:6px;font-size:11px;font-weight:700;cursor:pointer;border:1px solid transparent;font-family:'JetBrains Mono',monospace;transition:all 0.2s}
.ton{background:rgba(16,185,129,0.15);color:var(--green);border-color:rgba(16,185,129,0.3)!important}
.toff{background:rgba(255,255,255,0.05);color:var(--muted);border-color:var(--border)!important}
.abtn{background:rgba(139,92,246,0.1);border:1px solid rgba(139,92,246,0.3);color:var(--purple);border-radius:8px;padding:9px 18px;font-size:13px;font-family:'Inter',sans-serif;font-weight:600;cursor:pointer;transition:all 0.2s}
.abtn:hover{background:rgba(139,92,246,0.2)}
.abtn:disabled{opacity:0.5;cursor:not-allowed}
.otg{display:grid;grid-template-columns:repeat(5,1fr);gap:6px;margin-top:10px}
.otc{background:var(--card2);border-radius:6px;padding:6px;text-align:center}
.empty{text-align:center;padding:60px 20px;color:var(--muted)}
.ldot{width:8px;height:8px;border-radius:50%;background:var(--blue);animation:blink 1.2s infinite;display:inline-block;margin-right:6px}
.upd{font-size:11px;color:var(--muted);font-family:'JetBrains Mono',monospace}
.tc{display:grid;grid-template-columns:1fr 1fr;gap:16px}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.2}}
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-track{background:var(--bg)}::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
@media(max-width:900px){.sidebar{width:52px}.main{margin-left:52px}.sidebar .nav-item span:last-child,.logo-sub,.logo-main{display:none}.sr{grid-template-columns:repeat(2,1fr)}.tc{grid-template-columns:1fr}.otg{grid-template-columns:repeat(3,1fr)}}
</style></head>
<body>
<div class="sidebar">
  <div class="logo"><div class="logo-main">PAPA<span>GOAL</span></div><div class="logo-sub">READ THE MARKET</div></div>
  <nav class="nav">
    <button class="nav-item active" onclick="show('live',this)"><span>📡</span><span>Live Dashboard</span></button>
    <button class="nav-item" onclick="show('goals',this)"><span>⚽</span><span>Goals</span></button>
    <button class="nav-item" onclick="show('trades',this)"><span>📈</span><span>Simulation</span></button>
    <button class="nav-item" onclick="show('obs',this)"><span>🔥</span><span>Observations</span></button>
    <button class="nav-item" onclick="show('rules',this)"><span>📋</span><span>Rules Engine</span></button>
    <button class="nav-item" onclick="show('analytics',this)"><span>📊</span><span>Analytics</span></button>
    <button class="nav-item" onclick="show('ai',this)"><span>🤖</span><span>AI Insights</span></button>
    <button class="nav-item" onclick="show('debug',this)"><span>🔧</span><span>API Debug</span></button>
  </nav>
</div>
<div class="main">

<div class="page active" id="p-live">
  <div class="ph"><div><div class="pt"><span class="ldot"></span>Live Dashboard</div><div class="ps">Don't predict football. Read the market.</div></div><div class="upd" id="upd">Updating...</div></div>
  <div class="sr">
    <div class="sc"><div class="sn" style="color:var(--blue)" id="sl">--</div><div class="sl">Live Matches</div></div>
    <div class="sc"><div class="sn" style="color:var(--green)" id="sh">--</div><div class="sl">Active Signals</div></div>
    <div class="sc"><div class="sn" style="color:var(--yellow)" id="sg">--</div><div class="sl">Goals Today</div></div>
    <div class="sc"><div class="sn" style="color:var(--purple)" id="st">--</div><div class="sl">Open Trades</div></div>
  </div>
  <div class="stit">🎯 Active Recommendations</div>
  <div id="live-cards"><div class="empty"><div style="font-size:42px">⚽</div><div>No active signals -- waiting for live matches</div></div></div>
  <div class="stit" style="margin-top:20px">? All Live Matches</div>
  <div id="all-matches"><div class="empty" style="padding:20px">Loading matches...</div></div>
</div>

<div class="page" id="p-goals">
  <div class="ph"><div><div class="pt">⚽ Goals Detected</div><div class="ps">Odds before each goal - core learning data</div></div></div>
  <div id="goals-list"><div class="empty"><div style="font-size:42px">⚽</div><div>Loading goals...</div></div></div>
</div>

<div class="page" id="p-trades">
  <div class="ph"><div><div class="pt">? Simulation</div><div class="ps">Paper Trading - measuring rule accuracy</div></div></div>
  <div id="trades-content"><div class="empty"><div style="font-size:42px">?</div><div>Loading...</div></div></div>
</div>

<div class="page" id="p-obs">
  <div class="ph"><div><div class="pt">? Observations</div><div class="ps">All signals from last 3 hours</div></div></div>
  <div id="obs-list"><div class="empty"><div style="font-size:42px">?</div><div>Loading...</div></div></div>
</div>

<div class="page" id="p-rules">
  <div class="ph">
    <div><div class="pt">? Rules Engine</div><div class="ps">Rule lifecycle ? hit rates ? AI suggestions</div></div>
    <button class="abtn" onclick="runAIRules()" id="ai-rules-btn">🤖 AI: Improve Rules</button>
  </div>
  <div class="sr">
    <div class="sc"><div class="sn" style="color:var(--green)" id="ra">--</div><div class="sl">Active Rules</div></div>
    <div class="sc"><div class="sn" style="color:var(--blue)" id="rv">--</div><div class="sl">Validated</div></div>
    <div class="sc"><div class="sn" style="color:var(--yellow)" id="rt">--</div><div class="sl">Total Signals</div></div>
    <div class="sc"><div class="sn" style="color:var(--purple)" id="rp">--</div><div class="sl">Dummy Profit</div></div>
  </div>
  <div id="rules-list"><div class="empty"><div style="font-size:42px">?</div><div>Loading...</div></div></div>
</div>

<div class="page" id="p-analytics">
  <div class="ph"><div><div class="pt">📊 Analytics</div><div class="ps">Pattern analysis & performance metrics</div></div></div>
  <div id="analytics-content"><div class="empty"><div style="font-size:42px">📊</div><div>Loading...</div></div></div>
</div>

<div class="page" id="p-ai">
  <div class="ph">
    <div><div class="pt">🤖 AI Insights</div><div class="ps">Claude analyzes patterns & suggests rules</div></div>
    <button class="abtn" onclick="runAI()" id="ai-btn">🤖 Run Analysis</button>
  </div>
  <div id="ai-content"><div class="empty"><div style="font-size:42px">🤖</div><div>Click Run Analysis to get insights</div></div></div>
</div>

<div class="page" id="p-debug">
  <div class="ph">
    <div><div class="pt">? API Debug</div><div class="ps">Raw response from odds-api.io -- diagnose parsing issues</div></div>
    <button class="abtn" onclick="loadDebug()">? Fetch Now</button>
  </div>
  <div id="debug-content"><div class="empty"><div style="font-size:42px">?</div><div>Click Fetch Now to inspect the API response</div></div></div>
</div>

</div>
<script>
let cur='live';
const statusClass={'VALIDATED':'s-validated','PROMISING':'s-promising','TESTING':'s-testing','ACTIVE':'s-active','REJECTED':'s-rejected','DANGEROUS':'s-dangerous'};

function show(p,btn){
  document.querySelectorAll('.page').forEach(x=>x.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(x=>x.classList.remove('active'));
  document.getElementById('p-'+p).classList.add('active');
  if(btn) btn.classList.add('active');
  cur=p;
  const fn={goals:loadGoals,trades:loadTrades,obs:loadObs,rules:loadRules,analytics:loadAnalytics,ai:loadAI,debug:loadDebug};
  if(fn[p]) fn[p]();
}

async function loadLive(){
  try{
    const[st,obs,ai,matches]=await Promise.all([
      fetch('/api/stats').then(r=>r.json()),
      fetch('/api/signals').then(r=>r.json()),
      fetch('/api/ai_live').then(r=>r.json()),
      fetch('/api/matches').then(r=>r.json())
    ]);
    document.getElementById('sl').textContent=st.live||0;
    document.getElementById('sh').textContent=st.signals||0;
    document.getElementById('sg').textContent=st.goals_today||0;
    document.getElementById('st').textContent=st.open_trades||0;
    document.getElementById('upd').textContent='Updated: '+new Date().toLocaleTimeString();
    const aiMap={};ai.forEach(a=>aiMap[a.match_id]=a.analysis);
    const el=document.getElementById('live-cards');
    if(!obs.length){
      el.innerHTML='<div class="empty"><div style="font-size:36px">✅</div><div>No active signals yet</div></div>';
    } else {
      const bm={};
      obs.forEach(o=>{if(!bm[o.match_id]) bm[o.match_id]={...o,signals:[]};bm[o.match_id].signals.push(o);});
      el.innerHTML=Object.values(bm).map(m=>{
        const ai=aiMap[m.match_id]?`<div style="background:rgba(59,130,246,0.06);border:1px solid rgba(59,130,246,0.2);border-radius:8px;padding:10px;margin-top:8px;font-size:13px;line-height:1.6;color:#94a3b8"><div style="font-size:10px;letter-spacing:2px;color:var(--blue);margin-bottom:4px">🤖 CLAUDE AI</div>${aiMap[m.match_id]}</div>`:'';
        const sigs=m.signals.map(s=>`<div class="rec-box">
          <div class="rec-title">🎯 ${s.action_type} ? ${s.rule_name}</div>
          <div class="rec-row">
            <span>Market: <span class="rec-val">${s.market_type} ${s.line}</span></span>
            <span>Side: <span class="rec-val">${(s.selected_side||'').toUpperCase()}</span></span>
            <span>Odd: <span class="rec-val" style="color:var(--yellow)">${s.entry_odd||'--'}</span></span>
            <span>Gap: <span class="rec-val" style="color:${(s.gap||0)>0?'var(--green)':'var(--red)'}">${s.gap||0}</span></span>
            <span>Pressure: <span class="rec-val">${s.pressure||0}%</span></span>
            <span style="color:var(--green)">? Paper Trade Created</span>
          </div>
        </div>`).join('');
        return `<div class="card hot">
          <div class="ctop">
            <div><div class="mn">${m.home_team} vs ${m.away_team}</div><div class="ml">${m.league||''}</div></div>
            <div class="bgs">
              ${m.minute>0?`<span class="bg bgb">? ${m.minute}'</span>`:''}
              ${m.score&&m.score!='0-0'?`<span class="bg bgy">${m.score}</span>`:''}
              <span class="bg bgg">🎯 SIGNAL</span>
            </div>
          </div>${sigs}${ai}</div>`;
      }).join('');
    }
    // Show all matches
    const mel=document.getElementById('all-matches');
    if(!matches.length){
      mel.innerHTML='<div style="color:var(--muted);font-size:13px;padding:20px;text-align:center">⚽ No live matches right now -- check back when European leagues are playing (afternoons)</div>';
    } else {
      mel.innerHTML='<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px">'+
        matches.map(m=>`<div class="card" style="padding:12px;cursor:pointer" onclick="toggleMatchOdds('${m.mid}',this)">
          <div style="font-size:13px;font-weight:700;margin-bottom:4px">${m.home||m.home_team||'?'} vs ${m.away||m.away_team||'?'}</div>
          <div style="font-size:10px;color:var(--muted);margin-bottom:6px">${m.league||'Unknown League'}</div>
          <div class="bgs">
            <span class="bg bgb">? ${m.minute}'</span>
            <span class="bg bgy">${m.score_home}-${m.score_away}</span>
            <span class="bg ${m.period==='H1'?'bgb':m.period==='H2'?'bgp':'bgg'}">${m.period}</span>
          </div>
          <div class="match-odds-panel" style="display:none;margin-top:8px;border-top:1px solid var(--border);padding-top:8px">
            <div style="font-size:10px;color:var(--muted);margin-bottom:4px">📊 Opening Odds</div>
            <div class="odds-loading" style="font-size:11px;color:var(--muted)">Loading...</div>
          </div>
        </div>`).join('')+'</div>';
    }
  }catch(e){console.error(e);}
}

async function toggleMatchOdds(mid, card){
  const panel = card.querySelector('.match-odds-panel');
  if(panel.style.display==='none'){
    panel.style.display='block';
    const loader = panel.querySelector('.odds-loading');
    try{
      const data = await fetch('/api/opening_odds?mid='+mid).then(r=>r.json());
      if(!data.length){ loader.textContent='No opening odds yet'; return; }
      // Group by mtype
      const h1 = data.filter(o=>o.mtype==='H1');
      const ft = data.filter(o=>o.mtype==='FT');
      const row = (o) => `<div style="display:flex;justify-content:space-between;font-size:11px;padding:2px 0">
        <span style="color:var(--muted)">${o.mtype} ${o.line}</span>
        <span style="color:var(--green)">O: ${o.over_open||'--'}</span>
        <span style="color:var(--red)">U: ${o.under_open||'--'}</span>
      </div>`;
      loader.innerHTML = [...h1,...ft].map(row).join('');
    }catch(e){ loader.textContent='Error loading odds'; }
  } else {
    panel.style.display='none';
  }
}

async function loadGoals(){
  const goals=await fetch('/api/goals').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('goals-list');
  if(!goals.length){el.innerHTML='<div class="empty"><div style="font-size:42px">⚽</div><div>No goals yet</div></div>';return;}
  el.innerHTML=goals.map(g=>{
    const snap30 = g.odds_30s||[];
    const snap60 = g.odds_60s||[];
    const fmtOdds = (snaps) => {
      if(!snaps.length) return '<span style="color:var(--muted)">no data</span>';
      return snaps.filter(s=>s.mtype==='FT'||s.mtype==='H1').slice(0,4).map(s=>
        `<div style="font-size:11px;font-family:monospace">
          <span style="color:var(--muted)">${s.mtype} ${s.line}</span>
          <span style="color:var(--green)"> O:${s.over?.toFixed(2)||'--'}</span>
          <span style="color:var(--red)"> U:${s.under?.toFixed(2)||'--'}</span>
        </div>`
      ).join('') || '<span style="color:var(--muted)">no data</span>';
    };
    return `<div class="card win">
      <div class="ctop">
        <div><div class="mn">${g.home||g.home_team||'?'} vs ${g.away||g.away_team||'?'}</div><div class="ml">${g.league||''} ? ${g.period||'FT'}</div></div>
        <div style="font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--green)">⚽ Min ${g.minute}</div>
      </div>
      <div style="font-size:12px;color:var(--muted);margin-bottom:8px">${g.score_before||'?'} ? ${g.score_after||'?'}</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
        <div style="background:var(--bg);border-radius:6px;padding:8px">
          <div style="font-size:10px;color:var(--muted);margin-bottom:4px">~30s before</div>
          ${fmtOdds(snap30)}
        </div>
        <div style="background:var(--bg);border-radius:6px;padding:8px">
          <div style="font-size:10px;color:var(--muted);margin-bottom:4px">~60s before</div>
          ${fmtOdds(snap60)}
        </div>
      </div>
    </div>`;
  }).join('');
}

async function loadTrades(){
  const trades=await fetch('/api/trades').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('trades-content');
  const pend=trades.filter(t=>t.result==='pending');
  const wins=trades.filter(t=>t.result==='win');
  const lose=trades.filter(t=>t.result==='lose');
  const total=wins.length+lose.length;
  const pct=total>0?Math.round(wins.length/total*100):0;
  const profit=trades.reduce((s,t)=>s+(t.dummy_profit_loss||0),0);
  el.innerHTML=`
    <div class="sr">
      <div class="sc"><div class="sn" style="color:var(--yellow)">${pend.length}</div><div class="sl">? Pending</div></div>
      <div class="sc"><div class="sn" style="color:var(--green)">${wins.length}</div><div class="sl">✅ Win</div></div>
      <div class="sc"><div class="sn" style="color:var(--red)">${lose.length}</div><div class="sl">❌ Lose</div></div>
      <div class="sc"><div class="sn" style="color:${profit>=0?'var(--green)':'var(--red)'}">${pct}% ? ?${profit.toFixed(0)}</div><div class="sl">Hit Rate ? P&L</div></div>
    </div>
    <div class="stit">All Trades (${trades.length})</div>
    ${!trades.length?'<div class="empty"><div style="font-size:42px">?</div><div>No trades yet</div></div>':
      trades.map(t=>{
        const rc=t.result==='pending'?'bgy':t.result==='win'?'bgg':'bgr';
        const rl=t.result==='pending'?'? PENDING':t.result==='win'?'✅ WIN':'❌ LOSE';
        const bc=t.result==='pending'?'var(--yellow)':t.result==='win'?'var(--green)':'var(--red)';
        return `<div class="card" style="border-color:${bc}33">
          <div class="ctop">
            <div><div class="mn">${t.home||t.home_team||'?'} vs ${t.away||t.away_team||'?'}</div>
            <div class="ml">${t.rule_name} ? ${(t.mtype||'FT')==='H1'?'First Half':'Full Match'} ? Line ${t.line||''} ? ${(t.side||'').toUpperCase()==='OVER'?'? OVER':'? UNDER'}</div></div>
            <div class="bgs">
              ${(t.minute_entry||t.entry_min)>0?`<span class="bg bgb">? ${t.minute_entry||t.entry_min}'</span>`:''}
              <span class="bg ${rc}">${rl}</span>
            </div>
          </div>
          <div class="or">
            <div class="ot"><div class="ol">ENTRY ODD</div><div class="ov" style="color:var(--yellow)">${t.entry_odd||'--'}</div></div>
            <div class="ot"><div class="ol">EXPECTED</div><div class="ov">${t.expected_odd||'--'}</div></div>
            <div class="ot"><div class="ol">GAP</div><div class="ov" style="color:var(--blue)">${t.gap||0}</div></div>
            <div class="ot"><div class="ol">PRESSURE</div><div class="ov">${t.pressure||t.pressure_score||0}%</div></div>
            ${t.result!=='pending'?`<div class="ot"><div class="ol">P&L</div><div class="ov" style="color:${(t.profit||t.dummy_profit_loss||0)>=0?'var(--green)':'var(--red)'}">?${(t.profit||t.dummy_profit_loss||0).toFixed(0)}</div></div>`:''}
          </div>
          <div style="font-size:11px;color:var(--muted)">${(()=>{const m={'OVER_LINE_WITHIN_10M':'🎯 Goal in 10min','OVER_LINE_WITHIN_5M':'🎯 Goal in 5min','UNDER_HOLDS_10M':'🛡 No goal 10min','H1_OVER_LINE_BEFORE_HT':'🎯 Goal before HT','UNDER_HOLDS_TO_HT':'🛡 No goal to HT','OVER_LINE_BEFORE_FT':'🎯 Goal before FT','OVER_LINE_WITHIN_15M':'🎯 Goal in 15min'};return m[t.action_type]||t.action_type;})()} ? Window: ${t.val_window||'10m'} ? Score: ${t.score_entry||'--'}</div>
          ${(t.fail_reason||t.failure_reason)?`<div style="font-size:11px;color:var(--red);margin-top:4px">${t.fail_reason||t.failure_reason}</div>`:""}
        </div>`;
      }).join('')}`;
}

async function loadObs(){
  const obs=await fetch('/api/observations').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('obs-list');
  if(!obs.length){el.innerHTML='<div class="empty"><div style="font-size:42px">?</div><div>No observations</div></div>';return;}
  el.innerHTML=obs.map(o=>`
    <div class="card">
      <div class="ctop">
        <div><div class="mn">${o.home||o.home_team||'?'} vs ${o.away||o.away_team||'?'}</div>
        <div class="ml">${o.rule_name} ? ${o.league||''}</div></div>
        <div class="bgs">
          ${o.minute>0?`<span class="bg bgb">? ${o.minute}'</span>`:''}
          <span class="bg bgy">${o.mtype||o.market_type||'FT'} ${o.line||''}</span>
          <span class="bg bgp">${o.action_type}</span>
        </div>
      </div>
      <div class="or">
        <div class="ot"><div class="ol">OVER</div><div class="ov">${o.over_odd||'--'}</div></div>
        <div class="ot"><div class="ol">EXPECTED</div><div class="ov">${o.expected_odd||'--'}</div></div>
        <div class="ot"><div class="ol">GAP</div><div class="ov" style="color:var(--blue)">${o.gap||0}</div></div>
        <div class="ot"><div class="ol">PRESSURE</div><div class="ov">${o.pressure||o.pressure_score||0}%</div></div>
        <div class="ot"><div class="ol">CONF</div><div class="ov">${o.confidence||o.confidence_estimate||50}%</div></div>
      </div>
      <div style="font-size:12px;color:var(--muted)">${o.reason||''}</div>
    </div>`).join('');
}

async function loadRules(){
  const rules=await fetch('/api/rules').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('rules-list');
  document.getElementById('ra').textContent=rules.filter(r=>r.is_active).length;
  document.getElementById('rv').textContent=rules.filter(r=>r.status==='VALIDATED').length;
  document.getElementById('rt').textContent=rules.reduce((s,r)=>s+(r.total_signals||0),0);
  const prof=rules.reduce((s,r)=>s+(r.profit||r.dummy_profit||0),0);
  document.getElementById('rp').textContent=(prof>=0?'+':'')+'?'+prof.toFixed(0);
  if(!rules.length){el.innerHTML='<div class="empty">No rules</div>';return;}
  _rulesCache=rules;
  el.innerHTML=rules.map(r=>{
    const wr=parseFloat(r.win_rate||0);
    const wc=wr>=60?'var(--green)':wr>=45?'var(--yellow)':'var(--red)';
    const resolved=(r.win_count||0)+(r.lose_count||0);
    const pending=(r.total_signals||0)-resolved;
    const prof=r.profit||r.dummy_profit||0;
    const sideLabel=r.selected_side==='under'?'? UNDER':'? OVER';
    const sideColor=r.selected_side==='under'?'var(--purple)':'var(--green)';
    // Conditions summary
    const oddRange=r.side==='under'
      ?`Under ${r.under_min||r.under_odd_min||'?'}-${r.under_max||r.under_odd_max||'?'}`
      :`Over ${r.over_min||r.over_odd_min||'?'}-${r.over_max||r.over_odd_max||'?'}`;
    return `<div class="card" style="border-color:${r.is_active?'var(--border2)':'var(--border)'}">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;gap:8px">
        <div style="flex:1">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;flex-wrap:wrap">
            <span style="font-size:14px;font-weight:700;color:${r.is_active?'var(--text)':'var(--muted)'}">${r.source==='ai'?'🤖 ':'? '}${r.rule_name}</span>
            <span class="status-badge ${statusClass[r.status]||'s-active'}">${r.status}</span>
            <span style="font-size:11px;font-weight:700;color:${sideColor};font-family:'JetBrains Mono',monospace">${sideLabel}</span>
          </div>
          <div style="font-size:11px;color:var(--muted);margin-bottom:6px">${r.description||''}</div>
          <div style="font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace;display:flex;gap:12px;flex-wrap:wrap">
            <span>? min ${r.min_min||r.minute_min||"🏠"}-${r.min_max||r.minute_max||"🏠"}</span>
            <span>📊 ${r.mtype||r.market_type||"FT"} ${r.line_min||"🏠"}-${r.line_max||"🏠"}</span>
            <span>? ${oddRange}</span>
            <span>? window: ${r.val_window||r.validation_window||"🏠"}</span>
            <span>🎯 ${r.action_type}</span>
          </div>
        </div>
        <div style="display:flex;gap:6px">
          <button class="toggle ${r.is_active?'ton':'toff'}" onclick="toggleRule(${r.id},${!r.is_active})">${r.is_active?'ON':'OFF'}</button>
          <button onclick="editRuleById(${r.id})" style="padding:4px 10px;background:rgba(99,179,237,0.15);border:1px solid rgba(99,179,237,0.3);border-radius:6px;color:var(--blue);font-size:11px;cursor:pointer">??</button>
        </div>
      </div>
      <div style="background:var(--bg2);border-radius:8px;padding:10px;margin-top:8px">
        <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px;text-align:center;margin-bottom:8px">
          <div><div style="font-size:18px;font-weight:900;font-family:'JetBrains Mono',monospace;color:var(--blue)">${r.total_signals||0}</div><div style="font-size:10px;color:var(--muted)">SIGNALS</div></div>
          <div><div style="font-size:18px;font-weight:900;font-family:'JetBrains Mono',monospace;color:var(--green)">${r.wins||r.win_count||0}</div><div style="font-size:10px;color:var(--muted)">WON</div></div>
          <div><div style="font-size:18px;font-weight:900;font-family:'JetBrains Mono',monospace;color:var(--red)">${r.losses||r.lose_count||0}</div><div style="font-size:10px;color:var(--muted)">LOST</div></div>
          <div><div style="font-size:18px;font-weight:900;font-family:'JetBrains Mono',monospace;color:var(--yellow)">${pending}</div><div style="font-size:10px;color:var(--muted)">PENDING</div></div>
          <div><div style="font-size:18px;font-weight:900;font-family:'JetBrains Mono',monospace;color:${prof>=0?'var(--green)':'var(--red)'}">${prof>=0?'+':''}?${prof.toFixed(0)}</div><div style="font-size:10px;color:var(--muted)">P&L</div></div>
        </div>
        <div style="display:flex;align-items:center;gap:10px">
          <div style="flex:1"><div class="pb6"><div class="pf6" style="width:${Math.min(100,wr)}%;background:${wc}"></div></div></div>
          <span style="font-size:13px;font-family:'JetBrains Mono',monospace;font-weight:700;color:${wc};width:44px;text-align:right">${wr.toFixed(1)}%</span>
          <span style="font-size:10px;color:var(--muted)">${resolved} resolved</span>
        </div>
      </div>
    </div>`;
  }).join('');
}

async function toggleRule(name,state){
  try{await fetch('/api/rules/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({rule_name:name,is_active:state})});loadRules();}catch(e){console.error(e);}
}

async function runAIRules(){
  const btn=document.getElementById('ai-rules-btn');
  btn.disabled=true;btn.textContent='🤖 Analyzing...';
  try{
    const r=await fetch('/api/ai_rules',{method:'POST'});
    const d=await r.json();
    if(d.error)alert('Error: '+d.error);
    else{alert(`AI done! ${d.new_rules||0} new rule suggestions added.`);loadRules();}
  }catch(e){alert('Error');}
  btn.disabled=false;btn.textContent='🤖 AI: Improve Rules';
}

async function loadAnalytics(){
  const data=await fetch('/api/analytics').then(r=>r.json()).catch(()=>({}));
  const el=document.getElementById('analytics-content');
  const targets=[
    {l:"Goals collected",v:data.goals||data.total_goals||0,t:500,c:"var(--green)"},
    {l:"Snapshots saved",v:data.snapshots||data.total_snapshots||0,t:50000,c:"var(--blue)"},
    {l:"Paper trades",v:data.trades||data.total_trades||0,t:200,c:"var(--purple)"},
    {l:"Observations",v:data.obs||data.total_obs||0,t:1000,c:"var(--yellow)"}
  ];
  el.innerHTML=`
    <div class="sr">
      <div class="sc"><div class="sn" style="color:var(--green)">${data.goals||data.total_goals||0}</div><div class="sl">Goals</div></div>
      <div class="sc"><div class="sn" style="color:var(--blue)">${(data.snapshots||data.total_snapshots||0).toLocaleString()}</div><div class="sl">Snapshots</div></div>
      <div class="sc"><div class="sn" style="color:var(--yellow)">${data.obs||data.total_obs||0}</div><div class="sl">Observations</div></div>
      <div class="sc"><div class="sn" style="color:${(data.hit_rate||data.success_rate||0)>=55?'var(--green)':'var(--red)'}">${data.hit_rate||data.success_rate||0}%</div><div class="sl">Hit Rate</div></div>
    </div>
    <div class="tc">
      <div class="card">
        <div class="stit">Collection Progress</div>
        ${targets.map(t=>`
          <div style="display:flex;justify-content:space-between;margin-top:12px;font-size:12px">
            <span style="color:var(--muted)">${t.l}</span>
            <span style="color:${t.c};font-family:'JetBrains Mono',monospace">${t.v||0} / ${t.t}</span>
          </div>
          <div class="pb6"><div class="pf6" style="width:${Math.min(100,(t.v||0)/t.t*100)}%;background:${t.c}"></div></div>
        `).join('')}
      </div>
      <div class="card">
        <div class="stit">Top Rules by Signals</div>
        ${(data.top_rules||[]).map(r=>`
          <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px">
            <span style="color:var(--muted)">${r.name||r.rule_name||'?'}</span>
            <span style="color:var(--blue);font-family:'JetBrains Mono',monospace">${r.cnt||r.total_signals||0} signals</span>
          </div>`).join('')}
      </div>
    </div>`;
}

async function loadAI(){
  const ins=await fetch('/api/insights').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('ai-content');
  if(!ins.length){el.innerHTML='<div class="empty"><div style="font-size:42px">🤖</div><div>Click Run Analysis to get insights</div></div>';return;}
  el.innerHTML=ins.map(i=>`<div class="card">
    <div style="font-size:13px;font-weight:700;color:var(--purple);margin-bottom:4px">? Market Analysis</div>
    <div style="font-size:11px;color:var(--muted);margin-bottom:10px;font-family:'JetBrains Mono',monospace">${new Date(i.created_at).toLocaleString()} ? ${i.goals_analyzed||0} goals ? ${i.rules_analyzed||0} rules</div>
    <div style="font-size:13px;line-height:1.7;color:#94a3b8;white-space:pre-line">${i.content}</div>
  </div>`).join('');
}

async function runAI(){
  const btn=document.getElementById('ai-btn');btn.disabled=true;btn.textContent='? Analyzing...';
  try{
    const r=await fetch('/api/run_ai',{method:'POST'});
    const d=await r.json();
    if(d.error)btn.textContent='❌ '+d.error;
    else{await loadAI();btn.textContent='✅ Done';}
  }catch(e){btn.textContent='❌ Error';}
  setTimeout(()=>{btn.disabled=false;btn.textContent='🤖 Run Analysis';},3000);
}

let _rulesCache = [];

function editRuleById(id){
  const r = _rulesCache.find(x=>x.id===id);
  if(r) editRule(r);
  else fetch('/api/rules').then(res=>res.json()).then(rules=>{
    _rulesCache=rules;
    const found=rules.find(x=>x.id===id);
    if(found) editRule(found);
  });
}

function editRule(r){
  // Parse rule object
  let rule = r;
  if(typeof r === 'string') try{ rule=JSON.parse(r); }catch(e){ return; }
  
  // Set form values
  document.getElementById('ar-name').value = rule.rule_name||'';
  document.getElementById('ar-rule-id').value = rule.id||'';
  
  // mtype
  document.getElementById('ar-mtype').value = rule.mtype||'FT';
  
  // side
  document.getElementById('ar-side').value = rule.side||'over';
  
  // minutes - find closest match
  const minMin = rule.min_min||0, minMax = rule.min_max||90;
  const minSel = document.getElementById('ar-minutes');
  for(let opt of minSel.options){
    const [a,b] = opt.value.split('-').map(Number);
    if(Math.abs(a-minMin)<10 && Math.abs(b-minMax)<10){ minSel.value=opt.value; break; }
  }
  
  // odds
  const ovMin = rule.over_min||rule.under_min||0;
  const ovMax = rule.over_max||rule.under_max||9.99;
  const oddSel = document.getElementById('ar-odds');
  for(let opt of oddSel.options){
    const [a,b] = opt.value.split('-').map(Number);
    if(Math.abs(a-ovMin)<0.5){ oddSel.value=opt.value; break; }
  }
  
  // window
  document.getElementById('ar-window').value = rule.val_window||'10m';
  
  // line
  const lineSel = document.getElementById('ar-line');
  const lMin = rule.line_min||0.5, lMax = rule.line_max||3.5;
  for(let opt of lineSel.options){
    const [a,b] = opt.value.split('-').map(Number);
    if(Math.abs(a-lMin)<0.1 && Math.abs(b-lMax)<0.1){ lineSel.value=opt.value; break; }
  }
  
  document.getElementById('ar-modal-title').textContent = '?? Edit Rule';
  document.getElementById('ar-save-btn').textContent = '💾 Update Rule';
  document.getElementById('add-rule-modal').style.display='flex';
  updatePreview();
}

function openAddRule(){
  document.getElementById('ar-rule-id').value = '';
  document.getElementById('ar-name').value = '';
  document.getElementById('ar-modal-title').textContent = '? Add New Rule';
  document.getElementById('ar-save-btn').textContent = '💾 Save Rule';
  document.getElementById('ar-mtype').value = 'FT';
  document.getElementById('ar-side').value = 'over';
  document.getElementById('ar-minutes').value = '1-20';
  document.getElementById('ar-odds').value = '1.60-2.00';
  document.getElementById('ar-window').value = '10m';
  document.getElementById('ar-line').value = '1.5-1.5';
  document.getElementById('add-rule-modal').style.display='flex';
  updatePreview();
}
function closeAddRule(){
  document.getElementById('add-rule-modal').style.display='none';
}
function updatePreview(){
  const name=document.getElementById('ar-name').value||'New Rule';
  const mtype=document.getElementById('ar-mtype').value;
  const side=document.getElementById('ar-side').value;
  const mins=document.getElementById('ar-minutes').value;
  const odds=document.getElementById('ar-odds').value;
  const win=document.getElementById('ar-window').value;
  const line=document.getElementById('ar-line').value;
  const sideText=side==='over'?'🎯 Goal expected':'🛡 No goal expected';
  document.getElementById('ar-preview').innerHTML=
    `<b style="color:var(--text)">${name}</b><br>
    ${sideText} ? ${mtype} ? Line ${line.split('-')[0]} ? Min ${mins} ? Over ${odds} ? Check: ${win}`;
}
// Update preview on change
setTimeout(()=>{
  ['ar-name','ar-mtype','ar-side','ar-minutes','ar-odds','ar-window','ar-line'].forEach(id=>{
    const el=document.getElementById(id);
    if(el) el.addEventListener('input',updatePreview),el.addEventListener('change',updatePreview);
  });
},500);

async function saveRule(){
  const name=document.getElementById('ar-name').value.trim();
  if(!name){alert('Please enter a rule name');return;}
  const ruleId=document.getElementById('ar-rule-id').value;
  const mtype=document.getElementById('ar-mtype').value;
  const side=document.getElementById('ar-side').value;
  const [minMin,minMax]=document.getElementById('ar-minutes').value.split('-').map(Number);
  const [ovMin,ovMax]=document.getElementById('ar-odds').value.split('-').map(Number);
  const win=document.getElementById('ar-window').value;
  const [lineMin,lineMax]=document.getElementById('ar-line').value.split('-').map(Number);
  
  const actionMap={
    'over':{'5m':'OVER_LINE_WITHIN_5M','10m':'OVER_LINE_WITHIN_10M','15m':'OVER_LINE_WITHIN_15M','HT':'H1_OVER_LINE_BEFORE_HT','FT':'OVER_LINE_BEFORE_FT'},
    'under':{'5m':'UNDER_HOLDS_10M','10m':'UNDER_HOLDS_10M','15m':'UNDER_HOLDS_10M','HT':'UNDER_HOLDS_TO_HT','FT':'UNDER_HOLDS_10M'}
  };
  const action=actionMap[side][win]||'OVER_LINE_WITHIN_10M';
  
  const body={
    rule_name:name,
    description:`${mtype} Over ${ovMin}-${ovMax} at min ${minMin}-${minMax}`,
    mtype,side,
    min_min:minMin,min_max:minMax,
    over_min:side==='over'?ovMin:null,
    over_max:side==='over'?ovMax:null,
    under_min:side==='under'?ovMin:null,
    under_max:side==='under'?ovMax:null,
    line_min:lineMin,line_max:lineMax,
    action_type:action,
    val_window:win
  };
  const url = ruleId ? '/api/rules/edit' : '/api/rules/add';
  if(ruleId) body.id = parseInt(ruleId);
  try{
    const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d=await r.json();
    if(d.status==='ok'){closeAddRule();loadRules();}
    else alert('Error: '+d.error);
  }catch(e){alert('Error: '+e.message);}
}

async function loadDebug(){
  const el=document.getElementById('debug-content');
  el.innerHTML='<div class="empty">? Fetching from odds-api.io...</div>';
  try{
    const d=await fetch('/api/debug_odds').then(r=>r.json());
    const tried=(d.tried||[]).map(t=>`
      <div style="display:flex;gap:10px;align-items:center;padding:6px 0;border-bottom:1px solid var(--border);font-size:12px;flex-wrap:wrap">
        <span style="color:${t.has_totals?'var(--green)':'var(--muted)'}">${t.has_totals?'✅':'--'}</span>
        <span style="font-weight:600">${t.home||t.home_team||'?'} vs ${t.away||t.away_team||'?'}</span>
        <span style="color:var(--muted);font-size:10px">${t.league}</span>
        <span style="color:var(--blue);font-family:monospace;font-size:11px">[${(t.markets||[]).join(', ')}]</span>
      </div>`).join('');
    const ft=d.found_totals;
    const ftHtml=ft?`
      <div class="card" style="margin-top:10px;border-color:rgba(52,211,153,0.4)">
        <div class="stit">✅ Found Totals -- ${ft.event?.home} vs ${ft.event?.away}</div>
        ${(ft.all_markets||[]).filter(m=>m.name.includes('Total')).map(m=>`
          <div style="margin:6px 0">
            <div style="font-size:11px;color:var(--blue);margin-bottom:4px">${m.name}</div>
            ${(m.odds||[]).map(o=>`
              <div style="font-size:11px;font-family:monospace;color:var(--muted)">
                Line <b style="color:var(--text)">${o.hdp}</b> -- 
                Over <b style="color:var(--green)">${o.over}</b> / 
                Under <b style="color:var(--red)">${o.under}</b>
              </div>`).join('')}
          </div>`).join('')}
      </div>`:'<div style="color:var(--red);padding:10px">No Totals found in current events</div>';
    el.innerHTML=`
      <div class="card">
        <div class="stit">${d.events_count||0} Live Events Scanned -- API Key: ${d.api_key_set?'✅':'❌'}</div>
        ${tried}
      </div>
      ${ftHtml}`;
  }catch(e){el.innerHTML=`<div class="empty">Error: ${e.message}</div>`;}
}

async function auto(){if(cur==='live') await loadLive();}
loadLive();setInterval(auto,20000);
</script></body></html>"""

@app.route("/")
def index(): return render_template_string(HTML)

@app.route("/health")
def health(): return jsonify({"status":"ok","version":"v7"})

@app.route("/api/matches")
def api_matches():
    try:
        conn = get_db()
        try:
            rows = conn.run("""SELECT mid,home,away,league,minute,score_home,score_away,total_goals,period,updated_at
                FROM matches WHERE updated_at>NOW()-INTERVAL '10 minutes'
                ORDER BY updated_at DESC LIMIT 100""")
            return jsonify([{"mid":r[0],"home":r[1],"away":r[2],"home_team":r[1],"away_team":r[2],
                "league":r[3],"minute":r[4],
                "score_home":r[5],"score_away":r[6],"total_goals":r[7],"period":r[8],"updated_at":str(r[9])} for r in rows])
        finally: release_db(conn)
    except Exception as e: log.error(f"api_matches: {e}"); return jsonify([])

@app.route("/api/stats")
def api_stats():
    try:
        conn = get_db()
        try:
            live  = conn.run("SELECT COUNT(*) FROM matches WHERE updated_at>NOW()-INTERVAL '3 minutes'")[0][0]
            sigs  = conn.run("SELECT COUNT(*) FROM observations WHERE detected_at>NOW()-INTERVAL '30 minutes'")[0][0]
            goals = conn.run("SELECT COUNT(*) FROM goals WHERE goal_time>NOW()-INTERVAL '24 hours'")[0][0]
            open_ = conn.run("SELECT COUNT(*) FROM trades WHERE result='pending'")[0][0]
            return jsonify({"live":live,"signals":sigs,"goals_today":goals,"open_trades":open_})
        finally: release_db(conn)
    except: return jsonify({"live":0,"signals":0,"goals_today":0,"open_trades":0})

@app.route("/api/signals")
def api_signals():
    try:
        conn = get_db()
        try:
            rows = conn.run("""SELECT DISTINCT ON (mid,rule_id) mid,home,away,league,rule_name,minute,score,
                mtype,line,over_odd,under_odd,expected_odd,gap,pressure,confidence,action_type,selected_side,entry_odd
                FROM observations WHERE detected_at>NOW()-INTERVAL '30 minutes'
                ORDER BY mid,rule_id,detected_at DESC LIMIT 50""")
            cols=["mid","home","away","league","rule_name","minute","score","mtype","line",
                  "over_odd","under_odd","expected_odd","gap","pressure","confidence","action_type","side","entry_odd"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result:
                r["home_team"]=r["home"]
                r["away_team"]=r["away"]
                r["selected_side"]=r["side"]
                r["market_type"]=r["mtype"]
            return jsonify(result)
        finally: release_db(conn)
    except: return jsonify([])

@app.route("/api/goals")
def api_goals():
    try:
        conn = get_db()
        try:
            rows = conn.run("SELECT mid,minute,score_before,score_after,period,home,away,league,goal_time FROM goals ORDER BY goal_time DESC LIMIT 50")
            result = []
            for r in rows:
                mid,minute,sb,sa,period,home,away,league,gt = r
                snap30 = conn.run("""SELECT mtype,line,over_odd,under_odd FROM odds_snapshots
                    WHERE mid=:a AND minute>=:b AND minute<=:c
                    ORDER BY saved_at DESC LIMIT 10""",
                    a=mid,b=max(0,minute-2),c=minute)
                snap60 = conn.run("""SELECT mtype,line,over_odd,under_odd FROM odds_snapshots
                    WHERE mid=:a AND minute>=:b AND minute<=:c
                    ORDER BY saved_at DESC LIMIT 10""",
                    a=mid,b=max(0,minute-3),c=minute-1)
                # Odds velocity
                vel_snaps = conn.run("""SELECT over_odd FROM odds_snapshots
                    WHERE mid=:a AND mtype='FT' AND line=2.5
                    AND minute>=:b AND minute<=:c ORDER BY saved_at DESC LIMIT 6""",
                    a=mid,b=max(0,minute-4),c=minute)
                velocity,vel_dir = 0,"unknown"
                if len(vel_snaps)>=2:
                    try:
                        n=float(vel_snaps[0][0] or 0); o=float(vel_snaps[-1][0] or 0)
                        if n and o:
                            velocity=round(n-o,3)
                            vel_dir="rising_fast" if velocity>0.15 else "rising" if velocity>0.05 else "dropping_fast" if velocity<-0.15 else "dropping" if velocity<-0.05 else "stable"
                    except: pass
                # Dangerous attacks
                da=conn.run("""SELECT dangerous_attacks_home,dangerous_attacks_away,shots_home,shots_away
                    FROM match_stats WHERE mid=:a AND minute<=:b ORDER BY saved_at DESC LIMIT 1""",
                    a=mid,b=minute)
                da_data={"home":int(da[0][0]),"away":int(da[0][1]),"shots_home":int(da[0][2]),"shots_away":int(da[0][3])} if da else None
                # Key minutes
                km=conn.run("""SELECT minute,score_home,score_away,over_ft FROM key_minutes
                    WHERE mid=:a AND minute<=:b ORDER BY minute DESC LIMIT 3""",a=mid,b=minute)
                result.append({
                    "mid":mid,"minute":minute,"score_before":sb,"score_after":sa,
                    "period":period,"home":home,"away":away,"league":league,
                    "goal_time":str(gt),"home_team":home,"away_team":away,
                    "odds_30s":[{"mtype":s[0],"line":float(s[1]),"over":float(s[2]) if s[2] else None,"under":float(s[3]) if s[3] else None} for s in snap30],
                    "odds_60s":[{"mtype":s[0],"line":float(s[1]),"over":float(s[2]) if s[2] else None,"under":float(s[3]) if s[3] else None} for s in snap60],
                    "had_snapshots":len(snap30)>0,
                    "velocity":velocity,"vel_direction":vel_dir,"dangerous_attacks":da_data,
                    "key_minutes":[{"minute":k[0],"score":f"{k[1]}-{k[2]}","over_ft":float(k[3]) if k[3] else None} for k in km]
                })
            return jsonify(result)
        finally: release_db(conn)
    except Exception as e:
        log.error(f"api_goals: {e}")
        return jsonify([])

@app.route("/api/trades")
def api_trades():
    try:
        conn = get_db()
        try:
            rows = conn.run("""SELECT home,away,league,rule_name,mtype,line,side,action_type,
                entry_odd,expected_odd,gap,pressure,validation_window,result,profit,fail_reason,
                created_at,entry_min,score_entry FROM trades ORDER BY created_at DESC LIMIT 100""")
            cols=["home","away","league","rule_name","mtype","line","side","action_type",
                  "entry_odd","expected_odd","gap","pressure","val_window","result","profit",
                  "fail_reason","created_at","minute_entry","score_entry"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result:
                r["created_at"]=str(r["created_at"])
                r["home_team"]=r["home"]
                r["away_team"]=r["away"]
                r["validation_window"]=r["val_window"]
            return jsonify(result)
        finally: release_db(conn)
    except: return jsonify([])

@app.route("/api/observations")
def api_observations():
    try:
        conn = get_db()
        try:
            rows = conn.run("""SELECT mid,home,away,league,rule_name,minute,score,mtype,line,
                over_odd,under_odd,expected_odd,gap,pressure,confidence,action_type,reason,detected_at
                FROM observations WHERE detected_at>NOW()-INTERVAL '3 hours'
                ORDER BY detected_at DESC LIMIT 100""")
            cols=["mid","home","away","league","rule_name","minute","score","mtype","line",
                  "over_odd","under_odd","expected_odd","gap","pressure","confidence","action_type","reason","detected_at"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result: r["detected_at"]=str(r["detected_at"])
            return jsonify(result)
        finally: release_db(conn)
    except: return jsonify([])

@app.route("/api/rules")
def api_rules():
    try:
        conn = get_db()
        try:
            rows = conn.run("""SELECT id,rule_name,description,source,mtype,line_min,line_max,min_min,min_max,
                over_min,over_max,under_min,under_max,action_type,side,val_window,status,is_active,
                total_signals,wins,losses,win_rate,profit,created_at FROM rules ORDER BY total_signals DESC""")
            cols=["id","rule_name","description","source","mtype","line_min","line_max","min_min","min_max",
                  "over_min","over_max","under_min","under_max","action_type","side","val_window","status","is_active",
                  "total_signals","wins","losses","win_rate","profit","created_at"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result: r["created_at"]=str(r["created_at"])
            return jsonify(result)
        finally: release_db(conn)
    except: return jsonify([])

@app.route("/api/rules/edit", methods=["POST"])
def api_rules_edit():
    try:
        d=request.json
        conn=get_db()
        try:
            conn.run("""UPDATE rules SET
                rule_name=:a,description=:b,mtype=:c,
                line_min=:d,line_max=:e,min_min=:f,min_max=:g,
                over_min=:h,over_max=:i,under_min=:j,under_max=:k,
                action_type=:l,side=:m,val_window=:n
                WHERE id=:o""",
                a=d["rule_name"],b=d.get("description",""),c=d.get("mtype","FT"),
                d=d.get("line_min",0.5),e=d.get("line_max",3.5),
                f=d.get("min_min",0),g=d.get("min_max",90),
                h=d.get("over_min"),i=d.get("over_max"),
                j=d.get("under_min"),k=d.get("under_max"),
                l=d.get("action_type","OVER_LINE_WITHIN_10M"),
                m=d.get("side","over"),n=d.get("val_window","10m"),
                o=d["id"])
            return jsonify({"status":"ok"})
        finally: release_db(conn)
    except Exception as e:
        log.error(f"Edit rule: {e}")
        return jsonify({"error":str(e)}),500

@app.route("/api/rules/add", methods=["POST"])
def api_rules_add():
    try:
        d=request.json
        conn=get_db()
        try:
            conn.run("""INSERT INTO rules 
                (rule_name,description,source,mtype,line_min,line_max,min_min,min_max,
                over_min,over_max,under_min,under_max,held_min,action_type,side,val_window,status,is_active)
                VALUES (:a,:b,'manual',:c,:d,:e,:f,:g,:h,:i,:j,:k,0,:l,:m,:n,'TESTING',TRUE)""",
                a=d["rule_name"],b=d.get("description",""),c=d.get("mtype","FT"),
                d=d.get("line_min",0.5),e=d.get("line_max",3.5),
                f=d.get("min_min",0),g=d.get("min_max",90),
                h=d.get("over_min"),i=d.get("over_max"),
                j=d.get("under_min"),k=d.get("under_max"),
                l=d.get("action_type","OVER_LINE_WITHIN_10M"),
                m=d.get("side","over"),n=d.get("val_window","10m"))
            return jsonify({"status":"ok"})
        finally: release_db(conn)
    except Exception as e:
        log.error(f"Add rule: {e}")
        return jsonify({"error":str(e)}),500

@app.route("/api/rules/toggle", methods=["POST"])
def api_rules_toggle():
    try:
        data=request.json
        log.info(f"Toggle: {data}")
        conn=get_db()
        try:
            rule_id = data.get("id") or data.get("rule_name")
            conn.run("UPDATE rules SET is_active=:a WHERE id=:b",
                a=data["is_active"], b=rule_id)
            return jsonify({"status":"ok"})
        finally: release_db(conn)
    except Exception as e:
        log.error(f"Toggle error: {e}")
        return jsonify({"error":str(e)}),500

@app.route("/api/analytics")
def api_analytics():
    try:
        conn=get_db()
        try:
            goals=conn.run("SELECT COUNT(*) FROM goals")[0][0]
            trades=conn.run("SELECT COUNT(*) FROM trades")[0][0]
            obs=conn.run("SELECT COUNT(*) FROM observations")[0][0]
            try: snaps=conn.run("SELECT COUNT(*) FROM odds_snapshots")[0][0]
            except: snaps=0
            wins=conn.run("SELECT COUNT(*) FROM trades WHERE result='win'")[0][0]
            done=conn.run("SELECT COUNT(*) FROM trades WHERE result!='pending'")[0][0]
            rate=round(wins/done*100) if done>0 else 0
            top=conn.run("SELECT rule_name,COUNT(*) cnt FROM trades GROUP BY rule_name ORDER BY cnt DESC LIMIT 8")
            return jsonify({"goals":goals,"snapshots":snaps,"trades":trades,"obs":obs,"hit_rate":rate,
                           "top_rules":[{"name":r[0],"cnt":r[1]} for r in top]})
        finally: release_db(conn)
    except Exception as e:
        log.error(f"Analytics: {e}")
        return jsonify({"goals":0,"snapshots":0,"trades":0,"obs":0,"hit_rate":0})

@app.route("/api/debug_odds")
def api_debug_odds():
    """Debug: find a live event that has Totals (Over/Under) odds"""
    try:
        r = requests.get("https://api.odds-api.io/v3/events",
            params={"apiKey":ODDSAPI_KEY,"sport":"football","status":"live","limit":50},
            timeout=15)
        raw = r.json()
        events = raw if isinstance(raw,list) else raw.get("data",[])
        result = {"events_status":r.status_code,"events_count":len(events),"api_key_set":bool(ODDSAPI_KEY)}

        found_totals = None
        tried = []
        for ev in events[:30]:
            eid = str(ev.get("id",""))
            league = ev.get("league",{}).get("name","") if isinstance(ev.get("league"),dict) else ""
            r2 = requests.get("https://api.odds-api.io/v3/odds",
                params={"apiKey":ODDSAPI_KEY,"eventId":eid,"bookmakers":"Bet365"},
                timeout=8)
            data = {}
            try: data = r2.json()
            except: pass
            bk = data.get("bookmakers",{})
            markets = bk.get("Bet365",[]) if isinstance(bk,dict) else []
            mnames = [m.get("name","") for m in markets] if markets else []
            has_totals = any("Total" in n or "O/U" in n for n in mnames)
            tried.append({"eid":eid,"home":ev.get("home"),"away":ev.get("away"),
                         "league":league,"markets":mnames,"has_totals":has_totals})
            if has_totals and not found_totals:
                found_totals = {"event":ev,"odds_raw":data,"all_markets":markets}

        result["tried"] = tried[:15]
        result["found_totals"] = found_totals
        return jsonify(result)
    except Exception as e:
        return jsonify({"error":str(e)})

@app.route("/api/opening_odds")
def api_opening_odds():
    try:
        mid = request.args.get("mid")
        conn=get_db()
        try:
            if mid:
                rows=conn.run("""SELECT mid,home,away,league,mtype,line,over_open,under_open
                    FROM opening_odds WHERE mid=:a ORDER BY mtype,line""",a=mid)
            else:
                rows=conn.run("""SELECT mid,home,away,league,mtype,line,over_open,under_open
                    FROM opening_odds ORDER BY saved_at DESC LIMIT 500""")
            return jsonify([{"mid":r[0],"home":r[1],"away":r[2],"league":r[3],
                "mtype":r[4],"line":float(r[5]),
                "over_open":float(r[6]) if r[6] else None,
                "under_open":float(r[7]) if r[7] else None} for r in rows])
        finally: release_db(conn)
    except Exception as e: return jsonify({"error":str(e)}),500

@app.route("/api/insights")
def api_insights():
    try:
        conn=get_db()
        try:
            rows=conn.run("SELECT itype,content,goals_n,rules_n,created_at FROM insights WHERE itype='market_analysis' ORDER BY created_at DESC LIMIT 10")
            return jsonify([{"itype":r[0],"content":r[1],"goals_n":r[2],"rules_n":r[3],"created_at":str(r[4])} for r in rows])
        finally: release_db(conn)
    except: return jsonify([])

@app.route("/api/ai_live")
def api_ai_live():
    return jsonify([])

@app.route("/api/run_ai", methods=["POST"])
def api_run_ai():
    if not ANTHROPIC_API_KEY: return jsonify({"error":"No ANTHROPIC_API_KEY"}),400
    try:
        conn=get_db()
        try:
            goals=conn.run("SELECT minute,score_before,period,mid FROM goals ORDER BY goal_time DESC LIMIT 200")
            rules=conn.run("SELECT rule_name,status,total_signals,win_rate,profit FROM rules ORDER BY total_signals DESC")
            trades=conn.run("SELECT result,COUNT(*) FROM trades WHERE result!='pending' GROUP BY result")

            # Get odds snapshots before goals -- the core learning data
            goal_odds = []
            for g in goals[:50]:
                mid, minute = g[3], g[0]
                snaps = conn.run("""SELECT mtype,line,over_odd,under_odd FROM odds_snapshots
                    WHERE mid=:a AND minute>=:b AND minute<=:c
                    ORDER BY saved_at DESC LIMIT 6""",
                    a=mid, b=max(0,minute-2), c=minute)
                if snaps:
                    goal_odds.append({
                        "minute": minute,
                        "score_before": g[1],
                        "period": g[2],
                        "odds_before": [{"mtype":s[0],"line":float(s[1]),"over":float(s[2]) if s[2] else None,"under":float(s[3]) if s[3] else None} for s in snaps]
                    })

            prompt = f"""You are PapaGoal AI -- expert in football betting market patterns.

MISSION: Find what odds patterns ALWAYS appear before goals.

Data: {len(goals)} goals total, {len(goal_odds)} goals with odds snapshots.

GOALS WITH ODDS BEFORE THEM:
{goal_odds[:30]}

RULES PERFORMANCE:
{[(r[0],r[1],r[2],r[3]) for r in rules]}

TRADES: {[(r[0],r[1]) for r in trades]}

ANALYZE:
1. What Over/Under odds levels appear most before goals? (e.g. "Over 0.5 H1 between 1.4-1.8 in min 25-40")
2. What odds levels appear before goals in final minutes (80-90)?
3. Is there a pattern between score and odds before goals?
4. What odds level means "goal very likely" vs "goal unlikely"?
5. Suggest 2 specific rule improvements based on this data.

Be specific with numbers. Focus on patterns that repeat across multiple goals."""

            resp=requests.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key":ANTHROPIC_API_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
                json={"model":"claude-sonnet-4-5","max_tokens":1500,"messages":[{"role":"user","content":prompt}]},
                timeout=30)
            if resp.status_code==200:
                text=resp.json()["content"][0]["text"]
                conn.run("INSERT INTO insights (itype,content,goals_n,rules_n) VALUES ('goal_pattern_analysis',:a,:b,:c)",
                    a=text,b=len(goals),c=len(rules))
                return jsonify({"status":"ok"})
            return jsonify({"error":f"Claude API returned {resp.status_code}: {resp.text[:200]}"}),500
        finally: release_db(conn)
    except Exception as e:
        log.error(f"run_ai: {e}")
        return jsonify({"error":str(e)}),500

@app.route("/api/ai_rules", methods=["POST"])
def api_ai_rules():
    if not ANTHROPIC_API_KEY: return jsonify({"error":"No API key"}),400
    try:
        data_req = request.get_json(silent=True) or {}
        user_instruction = data_req.get("instruction","")
        conn=get_db()
        try:
            goals=conn.run("SELECT minute,period,score_before FROM goals ORDER BY goal_time DESC LIMIT 200")
            rules=conn.run("SELECT rule_name,total_signals,wins,losses,win_rate FROM rules ORDER BY total_signals DESC")
            obs=conn.run("SELECT minute,mtype,line,over_odd,under_odd FROM observations ORDER BY detected_at DESC LIMIT 200")

            minute_dist = {}
            for g in goals:
                if g[0]:
                    bucket = (g[0]//5)*5
                    minute_dist[bucket] = minute_dist.get(bucket,0)+1

            json_example = ('{"new_rules":[{"rule_name":"name","description":"desc","mtype":"FT",'
                           '"line_min":0.5,"line_max":2.5,"min_min":17,"min_max":20,'
                           '"over_min":1.50,"over_max":1.60,"under_min":null,"under_max":null,'
                           '"held_min":0,"action_type":"OVER_LINE_WITHIN_10M","side":"over","val_window":"10m"}],'
                           '"insights":"2 sentences"}')

            default_instr = "Find patterns at key minutes (18,30,78,84,85+). Stable odds at 1.5 or 2.5 for 1-2min = goal. Fast rising = no goal."
            instr = user_instruction if user_instruction else default_instr

            prompt = (
                "You are PapaGoal AI. Find Over/Under mispricings in football.\n\n"
                "PATTERNS TO FIND:\n"
                "1. Stable odds (1.5 or 2.5) for 1-2min = goal imminent -> OVER\n"
                "2. Fast rising odds = no goal -> UNDER\n"
                "3. Key minutes: 18, 30, 45, 78, 84, 85+\n\n"
                "Actions: OVER_LINE_WITHIN_5M, OVER_LINE_WITHIN_10M, OVER_LINE_WITHIN_15M, "
                "H1_OVER_LINE_BEFORE_HT, UNDER_HOLDS_10M, UNDER_HOLDS_TO_HT, OVER_LINE_BEFORE_FT\n"
                "mtype: FT=full match, H1=first half. val_window: 5m,10m,15m,HT,FT\n\n"
                f"Goals: {len(goals)} total. Distribution: {dict(sorted(minute_dist.items()))}\n"
                f"Rules: {[(r[0],r[2],r[3],str(r[4])+'%') for r in rules]}\n"
                f"Odds sample (min,mtype,line,over,under): {[(o[0],o[1],o[2],o[3],o[4]) for o in obs[:20]]}\n\n"
                f"Request: {instr}\n\n"
                f"Suggest 1-2 rules. Return ONLY JSON: {json_example}"
            )
            resp=requests.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key":ANTHROPIC_API_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
                json={"model":"claude-sonnet-4-5","max_tokens":1000,"messages":[{"role":"user","content":prompt}]},timeout=30)
            if resp.status_code==200:
                text=resp.json()["content"][0]["text"]
                text=re.sub(r'```json\s*','',text)
                text=re.sub(r'```\s*','',text)
                m=re.search(r'\{.*\}',text,re.DOTALL)
                if m:
                    try: data=json.loads(m.group())
                    except:
                        m2=re.search(r'"new_rules"\s*:\s*(\[.*?\])',text,re.DOTALL)
                        data={"new_rules":json.loads(m2.group(1))} if m2 else {"new_rules":[]}
                    count=0
                    for nr in data.get("new_rules",[]):
                        try:
                            conn.run("""INSERT INTO rules (rule_name,description,source,mtype,line_min,line_max,min_min,min_max,
                                over_min,over_max,under_min,under_max,held_min,action_type,side,val_window,status,is_active)
                                VALUES (:a,:b,'ai',:c,:d,:e,:f,:g,:h,:i,:j,:k,:l,:m,:n,:o,'AI_HYPOTHESIS',FALSE)
                                ON CONFLICT DO NOTHING""",
                                a=nr["rule_name"],b=nr.get("description",""),c=nr.get("mtype","FT"),
                                d=nr.get("line_min",0.5),e=nr.get("line_max",3.5),f=nr.get("min_min",0),g=nr.get("min_max",90),
                                h=nr.get("over_min"),i=nr.get("over_max"),j=nr.get("under_min"),k=nr.get("under_max"),
                                l=nr.get("held_min",0),m=nr.get("action_type","OVER_LINE_WITHIN_10M"),
                                n=nr.get("side","over"),o=nr.get("val_window","10m"))
                            count+=1
                        except: pass
                    return jsonify({"status":"ok","new_rules":count})
            return jsonify({"error":f"Claude {resp.status_code}"}),500
        finally: release_db(conn)
    except Exception as e: return jsonify({"error":str(e)}),500


def api_ai_rules():
    if not ANTHROPIC_API_KEY: return jsonify({"error":"No API key"}),400
    try:
        conn=get_db()
        try:
            goals=conn.run("SELECT minute,period FROM goals ORDER BY goal_time DESC LIMIT 100")
            rules=conn.run("SELECT rule_name,total_signals,win_rate FROM rules ORDER BY total_signals DESC")
            json_example = '{"new_rules":[{"rule_name":"name","description":"desc","mtype":"FT","line_min":0.5,"line_max":2.5,"min_min":17,"min_max":20,"over_min":1.50,"over_max":1.60,"under_min":null,"under_max":null,"held_min":0,"action_type":"OVER_LINE_WITHIN_10M","side":"over","val_window":"10m"}],"insights":"2 sentences"}'
            prompt=f"""Suggest new PapaGoal rules. {len(goals)} goals. Rules: {[(r[0],r[1],r[2]) for r in rules]}.
PapaGoal Rule System Guide:
- side="over": bet that Over line will be crossed (goal will happen)
- side="under": bet that Under line will hold (no goal will happen)
- action_type options:
  * OVER_LINE_WITHIN_10M: goal expected in next 10 minutes
  * H1_OVER_LINE_BEFORE_HT: goal expected before half time
  * UNDER_HOLDS_10M: no goal for next 10 minutes
  * UNDER_HOLDS_TO_HT: no goal until half time
  * OVER_LINE_BEFORE_FT: goal expected before full time end
  * OVER_LINE_WITHIN_15M: goal expected in next 15 minutes
- val_window: "10m","15m","5m","HT","FT"
- mtype: "FT"=full match odds, "H1"=first half odds only
- over_min/over_max: trigger when Over odd is in this range
- under_min/under_max: trigger when Under odd is in this range
- The goal is to find mispriced odds -- when market odds suggest something different than what we expect

Return ONLY JSON: {json_example}"""
            resp=requests.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key":ANTHROPIC_API_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
                json={"model":"claude-sonnet-4-5","max_tokens":800,"messages":[{"role":"user","content":prompt}]},timeout=30)
            if resp.status_code==200:
                text=resp.json()["content"][0]["text"]
                # Clean markdown code blocks
                text=re.sub(r'```json\s*','',text)
                text=re.sub(r'```\s*','',text)
                m=re.search(r'\{.*\}',text,re.DOTALL)
                if m:
                    try:
                        data=json.loads(m.group())
                    except:
                        # Try to find just the new_rules array
                        m2=re.search(r'"new_rules"\s*:\s*(\[.*?\])',text,re.DOTALL)
                        data={"new_rules":json.loads(m2.group(1))} if m2 else {"new_rules":[]}
                    count=0
                    for nr in data.get("new_rules",[]):
                        try:
                            conn.run("""INSERT INTO rules (rule_name,description,source,mtype,line_min,line_max,min_min,min_max,
                                over_min,over_max,under_min,under_max,held_min,action_type,side,val_window,status,is_active)
                                VALUES (:a,:b,'ai',:c,:d,:e,:f,:g,:h,:i,:j,:k,:l,:m,:n,:o,'AI_HYPOTHESIS',FALSE)
                                ON CONFLICT DO NOTHING""",
                                a=nr["rule_name"],b=nr.get("description",""),c=nr.get("mtype","FT"),
                                d=nr.get("line_min",0.5),e=nr.get("line_max",3.5),f=nr.get("min_min",0),g=nr.get("min_max",90),
                                h=nr.get("over_min"),i=nr.get("over_max"),j=nr.get("under_min"),k=nr.get("under_max"),
                                l=nr.get("held_min",0),m=nr.get("action_type","OVER_LINE_WITHIN_10M"),
                                n=nr.get("side","over"),o=nr.get("val_window","10m"))
                            count+=1
                        except: pass
                    return jsonify({"status":"ok","new_rules":count})
            return jsonify({"error":f"Claude {resp.status_code}"}),500
        finally: release_db(conn)
    except Exception as e: return jsonify({"error":str(e)}),500

init_db()
threading.Thread(target=collector_loop,daemon=True).start()
log.info("PapaGoal v7 started")

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=PORT,debug=False)
