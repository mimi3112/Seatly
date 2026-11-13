"""
Seatly - a Tatkal-style flash-release seat booking demo.

What it demonstrates
  * A public page lists two demo "slots" and lets anyone book a seat on either.
  * NAIVE-DEMO books with a deliberately buggy read-then-write path (SELECT seats_left,
    check it, THEN write) - the classic race condition. A short, clearly-labelled
    "simulated processing delay" (payment/validation step) widens the race window so a
    modest concurrent load test reliably shows overselling, instead of needing an
    enormous number of requests to hit a microsecond-wide race by chance.
  * ATOMIC-DEMO books with a single conditional UPDATE ... WHERE seats_left > 0,
    inside a transaction, checked by affected-row count - the correct, race-proof way.
    seats_left can be read directly off the dashboard: negative = oversold (a bug was
    allowed to happen), never-negative = the fix held under the same load.
  * Every booking attempt (success or sold-out) is logged to a file on a secondary EBS
    volume (the disaster-recovery demo target) and successful bookings are written to RDS.
  * A successful booking can fetch a generated ticket; in AWS mode it's rendered, uploaded
    to a private S3 bucket, and served back through a short-lived pre-signed URL.

Runs in two modes, exactly like the admin needs for local development vs. the real deploy:
  * AWS mode   : SSM_PREFIX is set. DB host/password/admin password come from SSM
                 Parameter Store via the instance role. Tickets go to S3.
  * Local mode : SSM_PREFIX unset -> SQLite + a local folder, tickets render inline.
"""
import json
import os
import secrets
import socket
import sqlite3
import time
from datetime import datetime, timezone
from functools import wraps

from flask import Flask, Response, jsonify, redirect, render_template_string, request
from werkzeug.exceptions import HTTPException

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REGION = os.getenv("AWS_REGION", "ap-south-1")
DB_NAME = os.getenv("DB_NAME", "seatly")             # database name inside RDS (older builds used "flashbook")
SSM_PREFIX = os.getenv("SSM_PREFIX", "")            # e.g. "/seatly"
USE_MYSQL = bool(SSM_PREFIX)
LOG_DIR = os.getenv("LOG_DIR", os.path.join(BASE_DIR, "logs"))
LOG_MOUNT = os.getenv("LOG_MOUNT", "")              # e.g. "/data": writes require it to be a live mount
TICKET_BUCKET = os.getenv("TICKET_BUCKET", "")
HOST = socket.gethostname()

# How long the naive path sleeps between its read and its write, simulating a realistic
# processing step (card auth, fraud check) that most naive implementations have in
# practice. This is what makes the race reliably visible with ~20-50 concurrent requests
# instead of requiring thousands to win a true microsecond-wide race by chance.
NAIVE_RACE_WINDOW_SECONDS = float(os.getenv("NAIVE_RACE_WINDOW_SECONDS", "0.35"))

app = Flask(__name__)


class StorageError(RuntimeError):
    """Raised when the EBS log volume is missing/unwritable (safe to show to the user)."""


class SlotError(RuntimeError):
    """User-facing booking failure (not found / sold out)."""


# --------------------------------------------------------------------------- secrets
_secret_cache = {}


def secret(name, local_default=""):
    if not SSM_PREFIX:
        return os.getenv(name.upper(), local_default)
    if name not in _secret_cache:
        import boto3                     # only needed in AWS mode
        ssm = boto3.client("ssm", region_name=REGION)
        resp = ssm.get_parameter(Name=f"{SSM_PREFIX}/{name}", WithDecryption=True)
        _secret_cache[name] = resp["Parameter"]["Value"]
    return _secret_cache[name]


# --------------------------------------------------------------------------- database
DDL_MYSQL = [
    """CREATE TABLE IF NOT EXISTS slots (
         id VARCHAR(40) PRIMARY KEY, name VARCHAR(160) NOT NULL, mode VARCHAR(10) NOT NULL,
         seats_total INT NOT NULL, seats_left INT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS bookings (
         id INT AUTO_INCREMENT PRIMARY KEY, slot_id VARCHAR(40) NOT NULL,
         passenger_name VARCHAR(120) NOT NULL, pnr VARCHAR(16) NOT NULL, mode VARCHAR(10) NOT NULL,
         booked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, served_by VARCHAR(64),
         INDEX idx_slot (slot_id))""",
]
DDL_SQLITE = [
    """CREATE TABLE IF NOT EXISTS slots (
         id TEXT PRIMARY KEY, name TEXT NOT NULL, mode TEXT NOT NULL,
         seats_total INTEGER NOT NULL, seats_left INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS bookings (
         id INTEGER PRIMARY KEY AUTOINCREMENT, slot_id TEXT NOT NULL,
         passenger_name TEXT NOT NULL, pnr TEXT NOT NULL, mode TEXT NOT NULL,
         booked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, served_by TEXT)""",
]
DEFAULT_SLOTS = [
    ("NAIVE-DEMO", "Tatkal Demo - naive booking (will oversell under load)", "naive"),
    ("ATOMIC-DEMO", "Tatkal Demo - atomic booking (race-proof)", "atomic"),
]
DEFAULT_SEATS = int(os.getenv("DEMO_SEATS", "3"))


def get_conn(autocommit=True):
    if USE_MYSQL:
        import pymysql
        return pymysql.connect(
            host=secret("db_host"), user="admin", password=secret("db_password"),
            database=DB_NAME, autocommit=autocommit, connect_timeout=5,
            cursorclass=pymysql.cursors.DictCursor,
        )
    conn = sqlite3.connect(os.getenv("SQLITE_PATH", os.path.join(BASE_DIR, "local.db")), timeout=10)
    conn.row_factory = sqlite3.Row
    if autocommit:
        conn.isolation_level = None      # sqlite's equivalent of autocommit
    return conn


def _clean(row):
    return {k: (v.isoformat(sep=" ") if hasattr(v, "isoformat") else v) for k, v in dict(row).items()}


def run(sql, params=(), fetch=False):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql if USE_MYSQL else sql.replace("%s", "?"), params)
        rows = [_clean(r) for r in cur.fetchall()] if fetch else None
        return rows
    finally:
        conn.close()


_db_ready = False


def ensure_db():
    global _db_ready
    if _db_ready:
        return
    for stmt in (DDL_MYSQL if USE_MYSQL else DDL_SQLITE):
        run(stmt)
    existing = {r["id"] for r in run("SELECT id FROM slots", fetch=True)}
    for slot_id, name, mode in DEFAULT_SLOTS:
        if slot_id not in existing:
            run("INSERT INTO slots (id, name, mode, seats_total, seats_left) VALUES (%s,%s,%s,%s,%s)",
                (slot_id, name, mode, DEFAULT_SEATS, DEFAULT_SEATS))
    _db_ready = True


def reset_slot(slot_id, seats):
    run("UPDATE slots SET seats_total=%s, seats_left=%s WHERE id=%s", (seats, seats, slot_id))
    run("DELETE FROM bookings WHERE slot_id=%s", (slot_id,))


def gen_pnr():
    return "SL" + secrets.token_hex(4).upper()


# --------------------------------------------------------------------------- booking logic (the point of the project)
def book_naive(slot_id, passenger):
    """Deliberately buggy: reads seats_left, decides, THEN writes - two separate
    statements with a gap between them, so two concurrent requests can both read
    'seats available' before either one writes its booking."""
    conn = get_conn(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("SELECT seats_left FROM slots WHERE id=%s" if USE_MYSQL else
                     "SELECT seats_left FROM slots WHERE id=?", (slot_id,))
        row = cur.fetchone()
        if row is None:
            raise SlotError("slot not found")
        seats_left = row["seats_left"] if USE_MYSQL else row["seats_left"]
        conn.commit()                    # release any lock while we "process payment"
        if seats_left <= 0:
            raise SlotError("sold out")

        time.sleep(NAIVE_RACE_WINDOW_SECONDS)   # <-- the simulated processing delay (the bug's window)

        cur.execute("UPDATE slots SET seats_left = seats_left - 1 WHERE id=%s" if USE_MYSQL else
                     "UPDATE slots SET seats_left = seats_left - 1 WHERE id=?", (slot_id,))
        pnr = gen_pnr()
        cur.execute("INSERT INTO bookings (slot_id, passenger_name, pnr, mode, served_by) VALUES (%s,%s,%s,'naive',%s)"
                    if USE_MYSQL else
                    "INSERT INTO bookings (slot_id, passenger_name, pnr, mode, served_by) VALUES (?,?,?,'naive',?)",
                    (slot_id, passenger, pnr, HOST))
        booking_id = cur.lastrowid
        conn.commit()
        return pnr, booking_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def book_atomic(slot_id, passenger):
    """Correct: ONE conditional UPDATE decides and writes in a single atomic statement.
    Under InnoDB/SQLite's row-level locking, two concurrent requests against the same
    row serialize automatically - the second one simply sees seats_left already
    decremented (or zero) and fails cleanly, instead of racing."""
    conn = get_conn(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("UPDATE slots SET seats_left = seats_left - 1 WHERE id=%s AND seats_left > 0" if USE_MYSQL else
                     "UPDATE slots SET seats_left = seats_left - 1 WHERE id=? AND seats_left > 0", (slot_id,))
        if cur.rowcount == 0:
            conn.rollback()
            # distinguish "sold out" from "no such slot" for a clearer error message
            cur.execute("SELECT 1 FROM slots WHERE id=%s" if USE_MYSQL else
                         "SELECT 1 FROM slots WHERE id=?", (slot_id,))
            raise SlotError("sold out" if cur.fetchone() else "slot not found")
        pnr = gen_pnr()
        cur.execute("INSERT INTO bookings (slot_id, passenger_name, pnr, mode, served_by) VALUES (%s,%s,%s,'atomic',%s)"
                    if USE_MYSQL else
                    "INSERT INTO bookings (slot_id, passenger_name, pnr, mode, served_by) VALUES (?,?,?,'atomic',?)",
                    (slot_id, passenger, pnr, HOST))
        booking_id = cur.lastrowid
        conn.commit()
        return pnr, booking_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# --------------------------------------------------------------------------- EBS raw log
def check_volume():
    if LOG_MOUNT and not os.path.ismount(LOG_MOUNT):
        raise StorageError(f"Disk not found: log volume {LOG_MOUNT} is not mounted")
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
    except OSError as exc:
        raise StorageError(f"Disk I/O error on log volume: {exc.strerror}") from exc


def append_log(record):
    check_volume()
    try:
        with open(os.path.join(LOG_DIR, "bookings.log"), "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
    except OSError as exc:
        raise StorageError(f"Disk I/O error on log volume: {exc.strerror}") from exc


def tail_log(n=20):
    check_volume()
    path = os.path.join(LOG_DIR, "bookings.log")
    try:
        if not os.path.exists(path):
            return []
        with open(path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            fh.seek(max(0, fh.tell() - 65536))
            return fh.read().decode("utf-8", "replace").splitlines()[-n:]
    except OSError as exc:
        raise StorageError(f"Disk I/O error on log volume: {exc.strerror}") from exc


# --------------------------------------------------------------------------- helpers
def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        import hmac
        auth = request.authorization
        ok = bool(auth) and auth.username == "admin" and hmac.compare_digest(
            (auth.password or "").encode(), secret("admin_password", "admin").encode())
        if not ok:
            return Response("Authentication required", 401,
                            {"WWW-Authenticate": 'Basic realm="Seatly admin"'})
        return fn(*args, **kwargs)
    return wrapper


def render_ticket_text(booking):
    return (
        f"SEATLY E-TICKET\n"
        f"===================\n"
        f"PNR:        {booking['pnr']}\n"
        f"Passenger:  {booking['passenger_name']}\n"
        f"Slot:       {booking['slot_id']}\n"
        f"Booked at:  {booking['booked_at']}\n"
        f"Booking id: {booking['id']}\n"
        f"Served by:  {booking['served_by']}\n"
    )


def upload_ticket(booking):
    """AWS mode only: renders a tiny PDF (falls back to plain text if fpdf2 isn't
    installed) and uploads it to the private ticket bucket. Returns a pre-signed URL."""
    import boto3
    body = render_ticket_text(booking)
    content_type, ext = "text/plain", "txt"
    try:
        from fpdf import FPDF
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Helvetica", size=12)
        for line in body.splitlines():
            pdf.cell(0, 8, text=line, new_x="LMARGIN", new_y="NEXT")
        body = bytes(pdf.output())
        content_type, ext = "application/pdf", "pdf"
    except Exception:
        body = body.encode("utf-8")       # fpdf2 missing or failed - plain-text ticket is still a real ticket

    key = f"tickets/{booking['id']}.{ext}"
    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(Bucket=TICKET_BUCKET, Key=key, Body=body, ContentType=content_type)
    return s3.generate_presigned_url("get_object", Params={"Bucket": TICKET_BUCKET, "Key": key}, ExpiresIn=300)


@app.errorhandler(Exception)
def on_error(exc):
    if isinstance(exc, HTTPException):
        return exc
    if isinstance(exc, SlotError):
        return jsonify(ok=False, error=str(exc)), 409 if str(exc) == "sold out" else 404
    app.logger.exception("unhandled error")
    msg = str(exc) if isinstance(exc, StorageError) else "Internal error"
    return Response(f"500 Server Error: {msg}\n(served by {HOST})\n", 500, mimetype="text/plain")


# --------------------------------------------------------------------------- routes
@app.get("/health")
def health():
    # Deliberately light: no DB / disk access, so a broken disk never makes the ASG kill the node.
    return Response("ok\n", mimetype="text/plain")


@app.get("/api/state")
def api_state():
    ensure_db()
    slots = run("SELECT id, name, mode, seats_total, seats_left FROM slots ORDER BY id", fetch=True)
    for s in slots:
        s["booked"] = run("SELECT COUNT(*) AS n FROM bookings WHERE slot_id=%s", (s["id"],), fetch=True)[0]["n"]
        s["oversold"] = max(0, s["booked"] - s["seats_total"])
    return jsonify(slots=slots, host=HOST)


@app.post("/book/<slot_id>")
def book(slot_id):
    ensure_db()
    data = request.get_json(silent=True) or {}
    passenger = (request.form.get("passenger_name") or data.get("passenger_name") or "Guest").strip()[:120]
    rows = run("SELECT mode FROM slots WHERE id=%s", (slot_id,), fetch=True)
    if not rows:
        return jsonify(ok=False, error="slot not found"), 404
    mode = rows[0]["mode"]
    booker = book_naive if mode == "naive" else book_atomic
    t0 = time.perf_counter()
    try:
        pnr, booking_id = booker(slot_id, passenger)
        ok, error = True, None
    except SlotError as exc:
        pnr, booking_id, ok, error = None, None, False, str(exc)
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)

    append_log({                         # EBS - every attempt, win or lose
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "slot_id": slot_id, "mode": mode, "passenger": passenger,
        "ok": ok, "pnr": pnr, "error": error, "elapsed_ms": elapsed_ms, "served_by": HOST,
    })
    status = 200 if ok else (409 if error == "sold out" else 404)
    return jsonify(ok=ok, pnr=pnr, booking_id=booking_id, error=error, mode=mode,
                   served_by=HOST, elapsed_ms=elapsed_ms), status


@app.get("/ticket/<int:booking_id>")
def ticket(booking_id):
    ensure_db()
    rows = run("SELECT * FROM bookings WHERE id=%s", (booking_id,), fetch=True)
    if not rows:
        return Response("Booking not found\n", 404, mimetype="text/plain")
    booking = rows[0]
    if SSM_PREFIX and TICKET_BUCKET:
        return redirect(upload_ticket(booking), 302)
    return Response(render_ticket_text(booking), mimetype="text/plain")


@app.post("/admin/reset")
@require_admin
def admin_reset():
    ensure_db()
    seats = max(1, min(int(request.form.get("seats", DEFAULT_SEATS)), 5000))
    target = request.form.get("slot_id", "both")
    for slot_id, _, _ in DEFAULT_SLOTS:
        if target in ("both", slot_id):
            reset_slot(slot_id, seats)
    return redirect("/admin", 303)


@app.get("/api/bookings")
@require_admin
def api_bookings():
    ensure_db()
    return jsonify(run("SELECT * FROM bookings ORDER BY id DESC LIMIT 300", fetch=True))


@app.get("/api/burn")
@require_admin
def burn():
    """CPU load generator, for an additional pure-ASG-scaling demo independent of the
    booking race (admin only, capped at 500 ms per call)."""
    import hashlib
    try:
        ms = max(1, min(int(request.args.get("ms", "150")), 500))
    except ValueError:
        ms = 150
    end, buf, n = time.perf_counter() + ms / 1000, os.urandom(1 << 20), 0
    while time.perf_counter() < end:
        hashlib.sha256(buf).digest()
        n += 1
    return jsonify(host=HOST, iterations=n)


@app.get("/admin")
@require_admin
def admin():
    ensure_db()
    slots = run("SELECT id, name, mode, seats_total, seats_left FROM slots ORDER BY id", fetch=True)
    for s in slots:
        s["booked"] = run("SELECT COUNT(*) AS n FROM bookings WHERE slot_id=%s", (s["id"],), fetch=True)[0]["n"]
        s["oversold"] = s["booked"] - s["seats_total"]
    recent = run("SELECT * FROM bookings ORDER BY id DESC LIMIT 20", fetch=True)
    ebs_lines = tail_log(20)              # 500s here when the EBS volume is gone (DR demo)
    return render_template_string(ADMIN_HTML, slots=slots, recent=recent, ebs_lines=ebs_lines,
                                   host=HOST, default_seats=DEFAULT_SEATS)


@app.get("/")
def home():
    ensure_db()
    return render_template_string(HOME_HTML)


HOME_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Seatly - Tatkal-style booking demo</title>
<style>
 :root{--bg:#f6f7f9;--card:#fff;--ink:#1b2430;--mut:#667085;--acc:#2457d6;--bad:#d1293d;--good:#1f9d6b;--line:#e4e7ec}
 *{box-sizing:border-box} body{margin:0;font:15px/1.5 system-ui,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--ink)}
 header{background:#16233b;color:#fff;padding:14px 24px} header h1{font-size:18px;margin:0 0 4px}
 header p{margin:0;font-size:13px;opacity:.8}
 main{max-width:900px;margin:20px auto;padding:0 16px;display:grid;gap:16px;grid-template-columns:1fr 1fr}
 .card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:18px}
 .card h2{margin:0 0 4px;font-size:16px} .tag{display:inline-block;font-size:11px;padding:2px 8px;border-radius:999px;margin-bottom:8px}
 .tag.naive{background:#fde3e6;color:var(--bad)} .tag.atomic{background:#e3f7ee;color:var(--good)}
 .seats{font-size:34px;font-weight:700;margin:6px 0} .seats.neg{color:var(--bad)}
 .sub{color:var(--mut);font-size:13px} input{width:100%;padding:9px;border:1px solid var(--line);border-radius:6px;font:inherit;margin:10px 0}
 button{width:100%;background:var(--acc);color:#fff;border:0;border-radius:6px;padding:10px;font:inherit;cursor:pointer}
 .result{margin-top:10px;font-size:13px;padding:8px 10px;border-radius:6px;display:none}
 .result.ok{background:#e3f7ee;color:var(--good);display:block} .result.err{background:#fde3e6;color:var(--bad);display:block}
 footer{max-width:900px;margin:0 auto 40px;padding:0 16px;color:var(--mut);font-size:12px}
 button:disabled{background:#98a2b3;cursor:not-allowed}
 .soldout{display:inline-block;background:var(--bad);color:#fff;font-size:12px;font-weight:700;padding:2px 10px;border-radius:999px;margin-bottom:6px}
</style></head><body>
<header><h1>Seatly</h1><p>Flash-release seat booking - book a seat on either demo slot below.</p></header>
<main id="main"></main>
<footer>Two identical-looking forms, two very different booking engines underneath. Watch what happens to "seats left" under a concurrent load test.</footer>
<script>
async function refresh() {
  const res = await fetch('/api/state'); const data = await res.json();
  const main = document.getElementById('main');
  data.slots.forEach(s => {
    let card = document.getElementById('card-' + s.id);
    if (!card) {
      // Build each card ONCE so the input box and the result message are not destroyed on every refresh.
      card = document.createElement('div');
      card.className = 'card'; card.id = 'card-' + s.id;
      card.innerHTML = `
        <span class="tag ${s.mode}">${s.mode}</span>
        <h2>${s.id}</h2>
        <div class="sub">${s.name}</div>
        <div class="seats" id="seats-${s.id}"></div>
        <div class="sub" id="booked-${s.id}"></div>
        <form onsubmit="return book(event, '${s.id}')">
          <input name="passenger_name" placeholder="Passenger name" required autocomplete="off">
          <button type="submit">Book a seat</button>
        </form>
        <div class="result" id="result-${s.id}"></div>`;
      main.appendChild(card);
    }
    // Only the numbers are updated on each refresh.
    const soldOut = s.seats_left <= 0;
    const seatsEl = document.getElementById('seats-' + s.id);
    seatsEl.className = 'seats ' + (s.seats_left < 0 ? 'neg' : '');
    seatsEl.innerHTML = (soldOut ? '<span class="soldout">SOLD OUT</span><br>' : '') +
      s.seats_left + ' / ' + s.seats_total + ' seats left';
    document.getElementById('booked-' + s.id).textContent =
      s.booked + ' booked' + (s.oversold > 0 ? ' - OVERSOLD by ' + s.oversold : '');
    const btn = card.querySelector('button');
    btn.disabled = soldOut;
    btn.textContent = soldOut ? 'Sold out' : 'Book a seat';
  });
}
async function book(ev, slotId) {
  ev.preventDefault();
  const name = ev.target.passenger_name.value;
  const res = await fetch('/book/' + slotId, {method:'POST', body:new URLSearchParams({passenger_name:name})});
  const text = await res.text(); let data;
  try { data = JSON.parse(text); } catch (e) { data = {ok:false, error:'HTTP ' + res.status + ' - ' + text.split(String.fromCharCode(10))[0]}; }
  const el = document.getElementById('result-' + slotId);
  el.className = 'result ' + (data.ok ? 'ok' : 'err');
  el.innerHTML = data.ok
    ? `Booked! PNR ${data.pnr} - <a href="/ticket/${data.booking_id}" target="_blank">view ticket</a>`
    : (data.error === 'sold out' ? 'Sold out - no seats left.' : `Failed: ${data.error}`);
  if (data.ok) ev.target.reset();
  await refresh();
}
refresh(); setInterval(refresh, 2000);
</script>
</body></html>"""

ADMIN_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Seatly admin</title>
<style>
 :root{--bg:#f6f7f9;--card:#fff;--ink:#1b2430;--mut:#667085;--acc:#2457d6;--bad:#d1293d;--good:#1f9d6b;--line:#e4e7ec}
 *{box-sizing:border-box} body{margin:0;font:15px/1.5 system-ui,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--ink)}
 header{background:#16233b;color:#fff;padding:14px 24px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}
 header h1{font-size:18px;margin:0} header span{font-size:13px;opacity:.8}
 main{max-width:1100px;margin:20px auto;padding:0 16px;display:grid;gap:16px}
 .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px}
 .card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;overflow:auto}
 h2{font-size:15px;margin:0 0 10px} table{border-collapse:collapse;width:100%;font-size:13px}
 th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);vertical-align:top;word-break:break-all}
 th{color:var(--mut);font-weight:600} label{display:block;font-size:13px;color:var(--mut);margin:8px 0 3px}
 input,select{width:100%;padding:8px;border:1px solid var(--line);border-radius:6px;font:inherit}
 button{margin-top:12px;background:var(--acc);color:#fff;border:0;border-radius:6px;padding:9px 14px;font:inherit;cursor:pointer}
 .big{font-size:30px;font-weight:700} .big.bad{color:var(--bad)} .big.good{color:var(--good)}
 pre{margin:0;font-size:12px;white-space:pre-wrap;word-break:break-all;background:#0f172a;color:#d1d5db;padding:10px;border-radius:8px}
</style></head><body>
<header><h1>Seatly admin</h1><span>served by {{ host }}</span></header>
<main>
 <div class="grid">
  {% for s in slots %}
  <div class="card">
   <h2>{{ s.id }} ({{ s.mode }})</h2>
   <div class="big {{ 'bad' if s.seats_left < 0 else 'good' }}">{{ s.seats_left }} / {{ s.seats_total }}</div>
   <p>{{ s.booked }} booked{% if s.oversold > 0 %} - <b>OVERSOLD by {{ s.oversold }}</b>{% endif %}</p>
  </div>
  {% endfor %}
  <div class="card"><h2>Reset for the next run</h2>
   <form method="post" action="/admin/reset">
    <label>Slot</label>
    <select name="slot_id"><option value="both">Both</option><option value="NAIVE-DEMO">NAIVE-DEMO</option><option value="ATOMIC-DEMO">ATOMIC-DEMO</option></select>
    <label>Seats</label><input name="seats" type="number" value="{{ default_seats }}" min="1" max="5000">
    <button type="submit">Reset</button>
   </form>
  </div>
 </div>
<div class="card"><h2>Live demo tools</h2>
  <p>Fires many booking requests at the same instant from this browser tab (no terminal needed).</p>
  <label>Simultaneous requests per slot</label>
  <input id="rushN" type="number" value="10" min="2" max="60">
  <button onclick="rush('NAIVE-DEMO')">Rush NAIVE-DEMO</button>
  <button onclick="rush('ATOMIC-DEMO')">Rush ATOMIC-DEMO</button>
  <button onclick="rush('BOTH')">Rush BOTH</button>
  <div id="rushOut" style="margin-top:10px;font-size:14px"></div>
  <hr style="margin:16px 0;border:0;border-top:1px solid var(--line)">
  <p>CPU load for the Auto Scaling demo (open this page in 2 or 3 tabs for more load).</p>
  <button onclick="loadStart()">Start CPU load</button>
  <button onclick="loadStop()">Stop CPU load</button>
  <div id="loadOut" style="margin-top:10px;font-size:14px">idle</div>
 </div>
 <div class="card"><h2>Recent bookings - table bookings (RDS)</h2>
  <table><tr><th>#</th><th>Time (UTC)</th><th>Slot</th><th>Mode</th><th>Passenger</th><th>PNR</th><th>Served by</th></tr>
  {% for r in recent %}<tr><td>{{ r.id }}</td><td>{{ r.booked_at }}</td><td>{{ r.slot_id }}</td>
   <td>{{ r.mode }}</td><td>{{ r.passenger_name }}</td><td>{{ r.pnr }}</td><td>{{ r.served_by }}</td></tr>
  {% else %}<tr><td colspan="7">Nothing yet</td></tr>{% endfor %}</table>
 </div>
 <div class="card"><h2>Raw attempt log - /data/logs/bookings.log (EBS)</h2>
  <pre>{% for l in ebs_lines %}{{ l }}
{% else %}(empty){% endfor %}</pre>
 </div>
</main>
<script>
async function one(slot, i) {
  try {
    const r = await fetch('/book/' + slot, {method:'POST', body:new URLSearchParams({passenger_name:'Rush-' + i})});
    const t = await r.text(); let d;
    try { d = JSON.parse(t); } catch (e) { d = {ok:false, error:'HTTP ' + r.status}; }
    return d.ok ? 'ok' : (d.error || 'error');
  } catch (e) { return 'network error'; }
}
async function rush(which) {
  const n = Math.max(2, Math.min(60, parseInt(document.getElementById('rushN').value) || 10));
  const slots = which === 'BOTH' ? ['NAIVE-DEMO', 'ATOMIC-DEMO'] : [which];
  document.getElementById('rushOut').textContent = 'Firing ' + n + ' simultaneous requests per slot...';
  const jobs = [];
  slots.forEach(s => { for (let i = 1; i <= n; i++) { jobs.push(one(s, i).then(res => ({s: s, res: res}))); } });
  const all = await Promise.all(jobs);
  const lines = slots.map(s => {
    const rs = all.filter(x => x.s === s).map(x => x.res);
    const ok = rs.filter(x => x === 'ok').length, sold = rs.filter(x => x === 'sold out').length;
    return '<b>' + s + '</b>: ' + ok + ' booked, ' + sold + ' sold out, ' + (rs.length - ok - sold) + ' errors (of ' + rs.length + ')';
  });
  sessionStorage.setItem('rushOut', lines.join('<br>'));
  location.reload();
}
const savedRush = sessionStorage.getItem('rushOut');
if (savedRush) { document.getElementById('rushOut').innerHTML = 'Last rush result:<br>' + savedRush; sessionStorage.removeItem('rushOut'); }
let loadOn = false, loadCount = 0;
async function loadWorker() {
  while (loadOn) {
    try { await fetch('/api/burn?ms=500'); } catch (e) { await new Promise(r => setTimeout(r, 500)); }
    loadCount++;
    if (loadOn) document.getElementById('loadOut').textContent = 'CPU load RUNNING - ' + loadCount + ' requests sent';
  }
}
function loadStart() { if (loadOn) return; loadOn = true; loadCount = 0; for (let i = 0; i < 6; i++) loadWorker(); }
function loadStop() { loadOn = false; document.getElementById('loadOut').textContent = 'Stopped after ' + loadCount + ' requests'; }
</script>
</body></html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False, threaded=True)
