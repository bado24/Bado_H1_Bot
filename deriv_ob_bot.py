#!/usr/bin/env python3
"""
Bot Telegram OB H1 / FVG / Retest M15 / Confirmation M15 — données Deriv.
ALERTES UNIQUEMENT : le bot ne passe AUCUN ordre.

Variables d'environnement :
  TELEGRAM_TOKEN  (obligatoire)  token donné par @BotFather
  CHAT_ID         (recommandé)   ton chat id (le bot te le donne si absent)
  TZ_OFFSET       (optionnel)    décalage horaire affiché, ex: 1 pour UTC+1
  DERIV_APP_ID    (optionnel)    défaut 1089
"""
import os, json, time, csv, math, threading, traceback
import datetime as dt
import requests

try:
    import websocket  # pip install websocket-client
except ImportError:
    websocket = None

# ───────────────────────── CONFIG ─────────────────────────
TOKEN = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")
APP_ID = os.getenv("DERIV_APP_ID", "1089")
WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
API = f"https://api.telegram.org/bot{TOKEN}"

SYMBOLS = {  # nom affiché -> symbole API Deriv
    "XAUUSD": "frxXAUUSD", "XAGUSD": "frxXAGUSD", "BTCUSD": "cryBTCUSD",
    "EURUSD": "frxEURUSD", "USDCAD": "frxUSDCAD", "USDJPY": "frxUSDJPY",
}
# Taille de contrat par lot (À VÉRIFIER dans les specs du symbole sur ton MT5 Deriv)
CONTRACT = {"frxXAUUSD": 100, "frxXAGUSD": 5000, "cryBTCUSD": 1}
DEFAULT_CONTRACT = 100000
DEC = {"XAUUSD": 2, "XAGUSD": 3, "BTCUSD": 2, "EURUSD": 5, "USDCAD": 5, "USDJPY": 3}

CFG = dict(
    impulse_atr=1.5,     # impulsion minimale (en ATR H1) pour valider un OB
    max_zone_atr=2.5,    # OB trop grand = ignoré
    require_fvg=False,   # True = n'accepter que les OB qui laissent un FVG
    ob_max_age_h=96,     # durée de vie d'un OB (heures)
    retest_window=24,    # bougies M15 max entre retest et confirmation
    sl_buffer_atr=0.05,  # marge du SL sous/sur la zone (en ATR H1)
    max_sl_atr=3.0,      # SL max autorisé (en ATR H1)
    rr=[1, 2, 3, 4],
)
DATA_DIR = os.getenv("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)
STATE_FILE = os.path.join(DATA_DIR, "state.json")
JOURNAL = os.path.join(DATA_DIR, "journal.csv")
RUN_MINUTES = float(os.getenv("RUN_MINUTES", "0"))   # 0 = tourne sans limite (Colab) ; GitHub = 345
DEADLINE = time.time() + RUN_MINUTES * 60 if RUN_MINUTES else None
LOCK = threading.Lock()
STATE = {"running": False, "capital": 1000.0, "risk": 1.0, "sync": False, "offset": None}
ENG = {}
BT = {"busy": False}
REJECTS = {"n": 0}

# ───────────────────────── DERIV ─────────────────────────
def ws_call(req, retries=3):
    err = None
    for _ in range(retries):
        try:
            ws = websocket.create_connection(WS_URL, timeout=20)
            ws.send(json.dumps(req))
            r = json.loads(ws.recv())
            ws.close()
            if "error" in r:
                raise RuntimeError(r["error"]["message"])
            return r
        except Exception as e:
            err = e
            time.sleep(2)
    raise err


def fetch(sym, gran, count=500, end="latest"):
    r = ws_call({"ticks_history": sym, "adjust_start_time": 1, "count": count,
                 "end": end, "style": "candles", "granularity": gran})
    return [dict(t=int(c["epoch"]), o=float(c["open"]), h=float(c["high"]),
                 l=float(c["low"]), c=float(c["close"])) for c in r.get("candles", [])]


def fetch_history(sym, gran, days):
    """Remonte le temps par blocs de 5000 bougies."""
    start = int(time.time()) - days * 86400
    out, end, prev_first = [], "latest", None
    while True:
        chunk = fetch(sym, gran, 5000, end)
        if not chunk or (prev_first is not None and chunk[0]["t"] >= prev_first):
            break
        out = chunk + out
        prev_first = chunk[0]["t"]
        if prev_first <= start:
            break
        end = prev_first - 1
        time.sleep(0.3)
    seen, res = set(), []
    for c in out:
        if c["t"] >= start and c["t"] not in seen:
            seen.add(c["t"])
            res.append(c)
    return sorted(res, key=lambda c: c["t"])


def closed(cs, gran):
    now = time.time()
    return [c for c in cs if c["t"] + gran <= now]

# ───────────────────────── INDICATEURS ─────────────────────────
def ema(vals, n):
    k, e, out = 2 / (n + 1), None, []
    for v in vals:
        e = v if e is None else e + k * (v - e)
        out.append(e)
    return out


def prep(cs, trend=False):
    prev, trs = None, []
    for c in cs:
        tr = c["h"] - c["l"] if prev is None else max(c["h"] - c["l"], abs(c["h"] - prev), abs(c["l"] - prev))
        trs.append(tr)
        prev = c["c"]
    for c, a in zip(cs, ema(trs, 27)):  # ≈ ATR Wilder 14
        c["atr"] = a
    if trend:
        cl = [c["c"] for c in cs]
        for c, a, b in zip(cs, ema(cl, 50), ema(cl, 200)):
            c["e50"], c["e200"] = a, b


def session(t):
    h = dt.datetime.fromtimestamp(t, dt.timezone.utc).hour
    if h < 7: return "Asie"
    if h < 12: return "Londres"
    if h < 16: return "Londres/New York"
    if h < 21: return "New York"
    return "Hors session"


def trend_of(h, d):
    c, a, b = h["c"], h["e50"], h["e200"]
    if c > a > b: return "haussière", d == "buy"
    if c < a < b: return "baissière", d == "sell"
    return "range", False


def calc_lot(sym, entry, sl, capital, risk_pct):
    per_lot = CONTRACT.get(sym, DEFAULT_CONTRACT) * abs(entry - sl)
    if sym.startswith("frxUSD"):      # USDJPY, USDCAD : gain exprimé en devise cotée
        per_lot /= entry
    risk = capital * risk_pct / 100
    lot = math.floor(risk / per_lot * 100) / 100 if per_lot > 0 else 0
    return lot, round(lot * per_lot, 2), round(risk, 2)

# ───────────────────────── CONFIRMATION M15 ─────────────────────────
def confirm(m15, j, ob):
    bull = ob["d"] == "buy"
    c, p = m15[j], m15[j - 1]
    if (c["c"] > c["o"]) != bull:
        return None
    # la zone doit avoir été touchée récemment
    last3 = m15[j - 2:j + 1]
    if bull and min(x["l"] for x in last3) > ob["hi"] + 0.25 * ob["atr"]: return None
    if not bull and max(x["h"] for x in last3) < ob["lo"] - 0.25 * ob["atr"]: return None
    body, rng = abs(c["c"] - c["o"]), (c["h"] - c["l"]) or 1e-9
    # 1) Englobante
    if (p["c"] < p["o"]) == bull and (p["c"] != p["o"]):
        if bull and c["c"] >= p["o"] and c["o"] <= p["c"]: return "Englobante haussière"
        if not bull and c["c"] <= p["o"] and c["o"] >= p["c"]: return "Englobante baissière"
    # 2) Morning / Evening Star
    a = m15[j - 2]
    ab, mb = abs(a["c"] - a["o"]), abs(p["c"] - p["o"])
    mid = (a["o"] + a["c"]) / 2
    if ab >= 0.5 * c["atr"] and mb <= 0.35 * ab:
        if bull and a["c"] < a["o"] and c["c"] > mid: return "Morning Star"
        if not bull and a["c"] > a["o"] and c["c"] < mid: return "Evening Star"
    # 3) Rejet avec mèche
    wick = (min(c["o"], c["c"]) - c["l"]) if bull else (c["h"] - max(c["o"], c["c"]))
    if wick >= 2 * body and wick >= 0.5 * rng:
        return "Rejet avec mèche"
    return None

# ───────────────────────── MOTEUR ─────────────────────────
class Engine:
    def __init__(self, sym, d=None):
        d = d or {}
        self.sym = sym
        self.obs = d.get("obs", [])
        self.last_m15 = d.get("last_m15", 0)
        self.last_h1 = d.get("last_h1", 0)
        self.seen = d.get("seen", [])

    def to_dict(self):
        return dict(obs=self.obs, last_m15=self.last_m15, last_h1=self.last_h1, seen=self.seen[-300:])

    # A. détection OB H1 (appelé à chaque clôture H1 d'index k)
    def detect(self, h1, k, ev):
        for i in (k - 1, k - 2, k - 3):
            if i < 1 or h1[i]["c"] == h1[i]["o"]:
                continue
            o, atr = h1[i], h1[i]["atr"]
            bull = o["c"] < o["o"]                       # OB haussier = dernière bougie baissière
            d = "buy" if bull else "sell"
            nxt = h1[i + 1]
            if bull != (nxt["c"] > nxt["o"]):
                continue
            oid = f"{self.sym}|{d}|{o['t']}"
            if oid in self.seen:
                continue
            last = min(i + 3, k)
            win = h1[i + 1:last + 1]
            if bull:
                disp = max(x["h"] for x in win) - o["l"]
                ok = max(x["c"] for x in win) > o["h"] and min(x["c"] for x in win) >= o["l"]
            else:
                disp = o["h"] - min(x["l"] for x in win)
                ok = min(x["c"] for x in win) < o["l"] and max(x["c"] for x in win) <= o["h"]
            if not ok or disp < CFG["impulse_atr"] * atr or (o["h"] - o["l"]) > CFG["max_zone_atr"] * atr:
                continue
            fvg = None
            for m in (i, i + 1):
                if m + 2 <= last:
                    a, b = h1[m], h1[m + 2]
                    if bull and b["l"] > a["h"]: fvg = [a["h"], b["l"]]
                    if not bull and b["h"] < a["l"]: fvg = [b["h"], a["l"]]
            self.seen.append(oid)
            if CFG["require_fvg"] and not fvg:
                ev.append(dict(type="reject", t=h1[k]["t"] + 3600, sym=self.sym, d=d,
                               reasons=["OB sans FVG (filtre actif)"]))
                continue
            tl, al = trend_of(h1[k], d)
            ob = dict(id=oid, d=d, hi=o["h"], lo=o["l"], t=o["t"], det=h1[k]["t"] + 3600,
                      disp=round(disp / atr, 2), fvg=fvg, atr=atr, st="new", ret_t=None,
                      trend=tl, aligned=al)
            self.obs.append(ob)
            ev.append(dict(type="ob", t=ob["det"], sym=self.sym, d=d, ob=ob))

    # B + C. retest et confirmation M15
    def process(self, m15, j, h1k, ev):
        c = m15[j]
        for ob in self.obs:
            if ob["st"] not in ("new", "retested") or c["t"] < ob["det"]:
                continue
            bull = ob["d"] == "buy"
            if c["t"] - ob["det"] > CFG["ob_max_age_h"] * 3600:
                ob["st"] = "expired"; continue
            if (bull and c["c"] < ob["lo"]) or (not bull and c["c"] > ob["hi"]):
                ob["st"] = "invalid"; continue          # zone cassée
            touch = c["l"] <= ob["hi"] if bull else c["h"] >= ob["lo"]
            if ob["st"] == "new" and touch:
                ob["st"], ob["ret_t"] = "retested", c["t"]
                ev.append(dict(type="retest", t=c["t"] + 900, sym=self.sym, d=ob["d"], ob=ob))
            if ob["st"] == "retested":
                if (c["t"] - ob["ret_t"]) / 900 > CFG["retest_window"]:
                    ob["st"] = "expired"; continue
                pat = confirm(m15, j, ob)
                if pat:
                    ev.append(self.signal(ob, m15, j, h1k, pat))
                    ob["st"] = "done"                   # une seule entrée par configuration

    def signal(self, ob, m15, j, h1k, pat):
        bull = ob["d"] == "buy"
        c = m15[j]
        entry = c["c"]
        seg = [x for x in m15[max(0, j - 40):j + 1] if x["t"] >= ob["ret_t"]]
        buf = CFG["sl_buffer_atr"] * ob["atr"]
        sl = (min(ob["lo"], min(x["l"] for x in seg)) - buf) if bull else (max(ob["hi"], max(x["h"] for x in seg)) + buf)
        R = abs(entry - sl)
        lot, risk_real, risk_t = calc_lot(SYMBOLS[self.sym], entry, sl, STATE["capital"], STATE["risk"]) \
            if R > 0 else (0, 0, 0)
        reasons = []
        if R <= 0: reasons.append("SL invalide")
        if R > CFG["max_sl_atr"] * ob["atr"]: reasons.append(f"SL trop éloigné (> {CFG['max_sl_atr']} ATR H1)")
        if lot < 0.01: reasons.append("Lot < 0.01 : SL trop éloigné pour ton risque max")
        tl, al = trend_of(h1k, ob["d"])
        base = dict(t=c["t"] + 900, sym=self.sym, d=ob["d"], ob=ob, pattern=pat, j=j)
        if reasons:
            return dict(base, type="reject", reasons=reasons)
        fvg15 = (c["l"] > m15[j - 2]["h"]) if bull else (c["h"] < m15[j - 2]["l"])
        return dict(base, type="signal", entry=entry, sl=sl, R=R,
                    tps={r: entry + (R * r if bull else -R * r) for r in CFG["rr"]},
                    lot=lot, risk=risk_real, risk_target=risk_t, session=session(c["t"] + 900),
                    trend=tl, aligned=al, fvg15=fvg15)

    def run(self, h1, m15):
        ev, hi = [], 0
        for j, c in enumerate(m15):
            if c["t"] <= self.last_m15:
                continue
            close_t = c["t"] + 900
            while hi < len(h1) and h1[hi]["t"] + 3600 <= close_t:
                if h1[hi]["t"] > self.last_h1:
                    self.detect(h1, hi, ev)
                    self.last_h1 = h1[hi]["t"]
                    self.obs = [o for o in self.obs if o["st"] in ("new", "retested")]
                hi += 1
            if j >= 2 and hi > 0:
                self.process(m15, j, h1[hi - 1], ev)
            self.last_m15 = c["t"]
        self.obs = [o for o in self.obs if o["st"] in ("new", "retested")]
        return ev

# ───────────────────────── BACKTEST (V5) ─────────────────────────
def evaluate(sig, m15, horizon=300):
    bull = sig["d"] == "buy"
    res = {r: None for r in CFG["rr"]}
    for c in m15[sig["j"] + 1: sig["j"] + 1 + horizon]:
        slhit = c["l"] <= sig["sl"] if bull else c["h"] >= sig["sl"]
        for r in res:
            if res[r] is None and not slhit:
                tp = sig["tps"][r]
                if (c["h"] >= tp) if bull else (c["l"] <= tp):
                    res[r] = "W"
        if slhit:  # SL et TP dans la même bougie = perte (prudent)
            for r in res:
                if res[r] is None: res[r] = "L"
            break
        if all(v is not None for v in res.values()):
            break
    return res


def stats(results):
    out = []
    for r in CFG["rr"]:
        w = sum(1 for x in results if x[r] == "W")
        l = sum(1 for x in results if x[r] == "L")
        n = w + l
        out.append((r, n, (w / n * 100) if n else 0.0, ((w * r - l) / n) if n else 0.0))
    return out


def backtest(days, label):
    if BT["busy"]:
        send("Un backtest est déjà en cours."); return
    BT["busy"] = True
    try:
        send(f"⏳ Backtest {label} lancé (1 à 3 min)…")
        STATE_BT = dict(STATE, capital=1e9)
        all_res, aligned_res, counter_res, lines = [], [], [], []
        for name, sym in SYMBOLS.items():
            try:
                h1 = closed(fetch_history(sym, 3600, days + 20), 3600)
                m15 = closed(fetch_history(sym, 900, days), 900)
                if len(h1) < 250 or len(m15) < 50:
                    lines.append(f"{name}: données insuffisantes"); continue
                prep(h1, True); prep(m15)
                saved = dict(STATE); STATE.update(capital=1e9)
                try:
                    ev = Engine(name).run(h1, m15)
                finally:
                    STATE.update(saved)
                obs = [e for e in ev if e["type"] == "ob"]
                rets = [e for e in ev if e["type"] == "retest"]
                sigs = [e for e in ev if e["type"] == "signal"]
                rej = [e for e in ev if e["type"] == "reject"]
                res = [evaluate(s, m15) for s in sigs]
                for s, r in zip(sigs, res):
                    all_res.append(r)
                    (aligned_res if s["aligned"] else counter_res).append(r)
                st = stats(res)
                wr = " · ".join(f"1:{r} {w:.0f}%" for r, n, w, _ in st)
                lines.append(f"<b>{name}</b> — {len(obs)} OB · {len(rets)} retests · {len(sigs)} signaux · {len(rej)} rejetés\n   WR {wr}")
            except Exception as e:
                lines.append(f"{name}: erreur ({e})")
        tot = stats(all_res)
        txt = f"📊 <b>Backtest {label}</b>\n\n" + "\n".join(lines)
        txt += f"\n\n<b>TOTAL — {len(all_res)} signaux</b>\n"
        txt += "\n".join(f"1:{r}  WR {w:.0f}%  espérance {e:+.2f}R  ({n} résolus)" for r, n, w, e in tot)
        a, b = stats(aligned_res), stats(counter_res)
        txt += (f"\n\nTendance H1 alignée : {len(aligned_res)} signaux, 1:2 → WR {a[1][2]:.0f}% ({a[1][3]:+.2f}R)"
                f"\nContre-tendance/range : {len(counter_res)} signaux, 1:2 → WR {b[1][2]:.0f}% ({b[1][3]:+.2f}R)")
        txt += "\n\n<i>Indicatif : spread/slippage non inclus, SL+TP dans la même bougie = perte.</i>"
        send(txt)
    except Exception as e:
        send(f"Backtest échoué : {e}")
    finally:
        BT["busy"] = False

# ───────────────────────── NEWS ─────────────────────────
_news = {"t": 0, "data": []}


def news_near(name, t):
    try:
        if time.time() - _news["t"] > 3600:
            _news["data"] = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=10).json()
            _news["t"] = time.time()
        cur = {"USD"} if name in ("XAUUSD", "XAGUSD", "BTCUSD") else {name[:3], name[3:]}
        out = []
        for e in _news["data"]:
            if e.get("impact") != "High" or e.get("country") not in cur:
                continue
            et = dt.datetime.fromisoformat(e["date"]).timestamp()
            if -1800 <= et - t <= 7200:
                out.append(f"{e['country']} {fmt_time(et)} {e['title']}")
        return out
    except Exception:
        return []

# ───────────────────────── TELEGRAM ─────────────────────────
KEYBOARD = {"keyboard": [
    [{"text": "▶️ Start"}, {"text": "⏹ Stop"}],
    [{"text": "📊 6 mois"}, {"text": "📊 3 mois"}],
    [{"text": "📊 1 mois"}, {"text": "📊 3 semaines"}],
    [{"text": "ℹ️ Statut"}]], "resize_keyboard": True}


def tg(method, **kw):
    return requests.post(f"{API}/{method}", json=kw, timeout=40).json()


def send(text, chat=None):
    try:
        tg("sendMessage", chat_id=chat or CHAT_ID, text=text[:4000], parse_mode="HTML", reply_markup=KEYBOARD)
    except Exception as e:
        print("Telegram error:", e)


def fmt_time(t):
    off = float(os.getenv("TZ_OFFSET", "0"))
    return dt.datetime.fromtimestamp(t + off * 3600, dt.timezone.utc).strftime("%d/%m %H:%M")


def px(name, v):
    return f"{v:.{DEC.get(name, 5)}f}"


def journal(e, extra=""):
    new = not os.path.exists(JOURNAL)
    with open(JOURNAL, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new: w.writerow(["heure", "symbole", "evenement", "direction", "details"])
        det = extra or (" | ".join(e.get("reasons", [])) if e["type"] == "reject" else e.get("pattern", ""))
        ob = e.get("ob")
        if ob: det += f" [OB {px(e['sym'], ob['lo'])}-{px(e['sym'], ob['hi'])}]"
        w.writerow([fmt_time(e["t"]), e["sym"], e["type"], e["d"], det])


def handle_event(e):
    n, ob = e["sym"], e.get("ob")
    journal(e)
    arrow = "🟢 BUY" if e["d"] == "buy" else "🔴 SELL"
    if e["type"] == "ob":
        fvg = f"oui ({px(n, min(ob['fvg']))} – {px(n, max(ob['fvg']))})" if ob["fvg"] else "non"
        send(f"🟦 <b>OB H1 détecté — {n}</b>\n{arrow} (zone {'demande' if e['d']=='buy' else 'offre'})\n"
             f"Zone : {px(n, ob['lo'])} – {px(n, ob['hi'])}\nImpulsion : {ob['disp']} ATR · FVG : {fvg}\n"
             f"Tendance H1 : {ob['trend']}{' (alignée)' if ob['aligned'] else ''}\n"
             f"Session : {session(e['t'])}\n\n👉 Fais ta propre analyse. Je te préviens au retest M15.")
    elif e["type"] == "retest":
        send(f"🟨 <b>Retest M15 — {n}</b>\nLe prix est revenu dans l'OB {arrow}\n"
             f"Zone : {px(n, ob['lo'])} – {px(n, ob['hi'])}\n\n⏳ Prépare-toi, j'attends la confirmation "
             f"(englobante / star / rejet). <b>N'entre pas au simple contact.</b>")
    elif e["type"] == "reject":
        REJECTS["n"] += 1
    elif e["type"] == "signal":
        news = news_near(n, e["t"])
        tps = "\n".join(f"TP 1:{r} : {px(n, v)}" for r, v in e["tps"].items())
        send(f"{arrow} — <b>{n}</b>   ✅ Configuration détectée\n\n"
             f"Timeframe : H1 / M15\nConfirmation : {e['pattern']}{' + FVG M15' if e['fvg15'] else ''}\n"
             f"Entrée : {px(n, e['entry'])} (clôture M15)\nSL : {px(n, e['sl'])}\n{tps}\n\n"
             f"Lot : {e['lot']:.2f} (risque ${e['risk']} / cible ${e['risk_target']})\n"
             f"Session : {e['session']} · Tendance H1 : {e['trend']}{' (alignée)' if e['aligned'] else ' (non alignée)'}\n"
             + ("⚠️ News : " + " ; ".join(news) + "\n" if news else "")
             + "\n🟠 En attente de validation humaine")

# ───────────────────────── BOUCLE LIVE ─────────────────────────
def scan_once(silent=False):
    for name, sym in SYMBOLS.items():
        try:
            h1 = closed(fetch(sym, 3600, 500), 3600)
            m15 = closed(fetch(sym, 900, 400), 900)
            if len(h1) < 60 or len(m15) < 10:
                continue
            prep(h1, True); prep(m15)
            eng = ENG.setdefault(name, Engine(name))
            first = eng.last_m15 == 0
            ev = eng.run(h1, m15)
            if silent or first:
                continue                      # mise à jour silencieuse (pas de flood)
            for e in ev:
                handle_event(e)
        except Exception:
            traceback.print_exc()
    save_state()


def worker():
    next_scan = 0
    while True:
        try:
            if STATE["running"]:
                if time.time() >= next_scan:
                    scan_once(silent=STATE.pop("sync", False))
                    next_scan = (int(time.time()) // 900 + 1) * 900 + 10   # après chaque clôture M15
            else:
                next_scan = 0
        except Exception:
            traceback.print_exc()
        time.sleep(5)


def save_state():
    with LOCK:
        d = dict(STATE, engines={k: v.to_dict() for k, v in ENG.items()})
        d.pop("sync", None)
        with open(STATE_FILE, "w") as f:
            json.dump(d, f)


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            d = json.load(open(STATE_FILE))
            for k in ("running", "capital", "risk", "offset"):
                if k in d: STATE[k] = d[k]
            for k, v in d.get("engines", {}).items():
                ENG[k] = Engine(k, v)
        except Exception:
            pass


def status():
    lines = [f"{'🟢 ACTIF' if STATE['running'] else '🔴 ARRÊTÉ'} · capital ${STATE['capital']:.0f} · risque {STATE['risk']}%"]
    for n, e in ENG.items():
        for ob in e.obs:
            lines.append(f"• {n} {ob['d'].upper()} {px(n, ob['lo'])}–{px(n, ob['hi'])} "
                         f"({'en attente de retest' if ob['st']=='new' else 'retesté, attend confirmation'})")
    if len(lines) == 1: lines.append("Aucun OB actif.")
    lines.append(f"Signaux rejetés (session) : {REJECTS['n']}")
    return "\n".join(lines)


def on_message(chat, text):
    global CHAT_ID
    t = text.lower().strip()
    if not CHAT_ID:
        send(f"Ton chat id est <code>{chat}</code>. Mets-le dans la variable CHAT_ID puis relance le bot.", chat); return
    if str(chat) != str(CHAT_ID):
        return
    if "stop" in t:
        STATE["running"] = False; save_state()
        send("⏹ Bot arrêté. Plus aucun signal. Appuie sur Start pour reprendre.")
    elif "start" in t:
        STATE["running"], STATE["sync"] = True, True; save_state()
        send("▶️ Bot démarré. Synchronisation en cours (aucun ancien signal ne sera renvoyé)…")
    elif "6 mois" in t: threading.Thread(target=backtest, args=(180, "6 mois"), daemon=True).start()
    elif "3 mois" in t: threading.Thread(target=backtest, args=(90, "3 mois"), daemon=True).start()
    elif "1 mois" in t: threading.Thread(target=backtest, args=(30, "1 mois"), daemon=True).start()
    elif "3 sem" in t: threading.Thread(target=backtest, args=(21, "3 semaines"), daemon=True).start()
    elif "statut" in t: send(status())
    elif t.startswith("/journal"):
        if os.path.exists(JOURNAL):
            requests.post(f"{API}/sendDocument", data={"chat_id": CHAT_ID}, files={"document": open(JOURNAL, "rb")}, timeout=60)
        else: send("Journal vide.")
    elif t.startswith("/capital"):
        try: STATE["capital"] = float(t.split()[1]); save_state(); send(f"Capital : ${STATE['capital']:.0f}")
        except Exception: send("Usage : /capital 500")
    elif t.startswith("/risque"):
        try: STATE["risk"] = float(t.split()[1].replace(",", ".")); save_state(); send(f"Risque : {STATE['risk']}%")
        except Exception: send("Usage : /risque 1")
    else:
        send("Boutons : Start · Stop · Backtests · Statut\nCommandes : /capital 500 · /risque 1 · /journal")


def poll():
    while DEADLINE is None or time.time() < DEADLINE:
        try:
            r = tg("getUpdates", offset=STATE["offset"], timeout=30)
            for u in r.get("result", []):
                STATE["offset"] = u["update_id"] + 1
                m = u.get("message")
                if m and "text" in m:
                    on_message(m["chat"]["id"], m["text"])
            if r.get("result"):
                save_state()
        except Exception:
            traceback.print_exc()
            time.sleep(5)


def main():
    if not TOKEN:
        raise SystemExit("TELEGRAM_TOKEN manquant")
    load_state()
    threading.Thread(target=worker, daemon=True).start()
    if CHAT_ID and not STATE["running"] and DEADLINE is None:
        send("🤖 Bot prêt. Appuie sur ▶️ Start.")
    print("Bot en écoute…")
    poll()
    save_state()
    print("Arrêt planifié, état sauvegardé.")


if __name__ == "__main__":
    main()
