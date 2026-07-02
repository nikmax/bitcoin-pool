#!/usr/bin/env python3
import json, os, time, threading, secrets, copy, hashlib, traceback
from pathlib import Path
import requests
from flask import Flask, jsonify, request, render_template_string, redirect, Response

import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))
from shared.bitcoin import make_job_from_template, build_block_hex
from shared.config import load_json_config, save_json_config
from shared.protocol import STATUS_REGISTERED, STATUS_OFFLINE, STATUS_MINING, STATUS_FOUND

CONFIG_PATH = Path(os.environ.get("MINER_MASTER_CONFIG", "config.json"))
app = Flask(__name__)
lock = threading.RLock()
LOG_DIR = Path(os.environ.get("MINER_LOG_DIR", "logs"))
ROUND_LOG = LOG_DIR / "rounds.jsonl"
BLOCK_LOG = LOG_DIR / "blocks.jsonl"
EVENT_LOG = LOG_DIR / "events.jsonl"
BENCH_LOG = LOG_DIR / "benchmarks.jsonl"
LOG_DIR.mkdir(parents=True, exist_ok=True)

ERROR_LOG = LOG_DIR / "errors.log"

def log_exception(context, exc):
    tb = traceback.format_exc()
    line = f"[{now_iso()}] {context}: {repr(exc)}\n{tb}\n"
    try:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    print(line, flush=True)
    try:
        with lock:
            STATE["last_error"] = f"{context}: {exc}"
            STATE.setdefault("logs", []).append({"ts": time.strftime("%H:%M:%S"), "msg": f"ERROR {context}: {exc}"})
            STATE["logs"] = STATE["logs"][-200:]
    except Exception:
        pass

@app.errorhandler(Exception)
def handle_uncaught_exception(exc):
    log_exception(f"HTTP {request.method} {request.path}", exc)
    return jsonify({"ok": False, "error": str(exc), "type": exc.__class__.__name__}), 500

STATE = {
    "running": False,
    "started_at": None,
    "template_id": None,
    "height": None,
    "previousblockhash": None,
    "workers": {},
    "total_hashrate_hs": 0.0,
    "total_hashes": 0,
    "last_error": None,
    "last_submit_result": None,
    "found": False,
    "logs": [],
    "verify_started_at": None,
    "verified_total_hashes": 0,
}
JOBS = {}
BENCHMARK = {
    "running": False,
    "benchmark_id": None,
    "started_at_ts": None,
    "started_at": None,
    "duration_seconds": 0,
    "end_at_ts": None,
    "ended_at": None,
    "end_reason": None,
    "valid_finds": 0,
    "finds": [],
    "last_result": None,
}
ACTIVE_TEMPLATE = None
PAYOUT_SCRIPT = None
EXTRANONCE = 0
CURRENT_ROUND = None
COMPLETED_BLOCKS = []
FINALIZED_ROUND_IDS = set()
FINALIZING_ROUND_IDS = set()

def log(msg):
    line = {"ts": time.strftime("%H:%M:%S"), "msg": str(msg)}
    with lock:
        STATE.setdefault("logs", []).append(line)
        STATE["logs"] = STATE["logs"][-200:]
    print(f"[{line['ts']}] {line['msg']}")



def append_jsonl(path, obj):
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception as e:
        print(f"JSONL log error {path}: {e}")

def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")

def round_summary_locked(round_obj):
    if not round_obj:
        return None
    r = copy.deepcopy(round_obj)
    total = sum(int(p.get("verified_hashes", 0) or 0) for p in r.get("participants", {}).values())
    r["verified_hashes_total"] = total
    for p in r.get("participants", {}).values():
        hashes = int(p.get("verified_hashes", 0) or 0)
        p["share_percent"] = (hashes / total * 100.0) if total > 0 else 0.0
        # vorbereitetes Abrechnungsfeld: standardmäßig nicht bezahlt
        p.setdefault("paid", "no")
    return r

def add_round_event_locked(event_type, worker_id=None, **extra):
    ev = {"ts": now_iso(), "event": event_type, "worker_id": worker_id, **extra}
    if CURRENT_ROUND is not None:
        CURRENT_ROUND.setdefault("events", []).append(ev)
        CURRENT_ROUND["events"] = CURRENT_ROUND["events"][-500:]
    append_jsonl(EVENT_LOG, {"round_id": CURRENT_ROUND.get("round_id") if CURRENT_ROUND else None, **ev})

def ensure_participant_locked(worker_id):
    if CURRENT_ROUND is None or not worker_id:
        return None
    parts = CURRENT_ROUND.setdefault("participants", {})
    w = STATE.get("workers", {}).get(worker_id, {})
    if worker_id in parts:
        if w.get("name") and parts[worker_id].get("name") in (None, "", worker_id):
            parts[worker_id]["name"] = w.get("name")
        if w.get("gpu_device") and not parts[worker_id].get("gpu_device"):
            parts[worker_id]["gpu_device"] = w.get("gpu_device")
        return parts[worker_id]
    if worker_id not in parts:
        parts[worker_id] = {
            "worker_id": worker_id,
            "name": w.get("name", worker_id),
            "backend": w.get("backend"),
            "gpu_device": w.get("gpu_device"),
            "verified_hashes": 0,
            "active_seconds": 0.0,
            "heartbeat_count": 0,
            "join_count": 1,
            "disconnects": 0,
            "first_seen": now_iso(),
            "last_seen": now_iso(),
            "last_status": w.get("status"),
            "last_heartbeat_seq": None,
            "paid": "no",
        }
        add_round_event_locked("joined", worker_id, name=parts[worker_id]["name"], gpu_device=parts[worker_id].get("gpu_device"))
    return parts[worker_id]

def start_round_locked(round_id, tmpl, template_id=None, force_new=False):
    global CURRENT_ROUND
    # Eine Runde ist normalerweise ein Chain-Tip (height + previousblockhash).
    # Bei manuellem START darf aber bewusst eine neue Runde entstehen, auch wenn
    # Bitcoin Core dasselbe Template zurückgibt. Dadurch sieht man im Dashboard
    # klar: Stop/Start = neue Messrunde / neue Template-ID / ExtraNonce ab 1.
    chain_tip = f"{tmpl.get('height')}:{tmpl.get('previousblockhash')}"
    current_chain_tip = CURRENT_ROUND.get("chain_tip") if CURRENT_ROUND else None

    if CURRENT_ROUND and current_chain_tip == chain_tip and not force_new:
        CURRENT_ROUND["template_id"] = template_id or CURRENT_ROUND.get("template_id")
        CURRENT_ROUND["transactions"] = len(tmpl.get("transactions", []))
        STATE["current_round"] = round_summary_locked(CURRENT_ROUND)
        return

    if CURRENT_ROUND:
        CURRENT_ROUND["ended_at"] = now_iso()
        CURRENT_ROUND["end_reason"] = "manual_restart" if current_chain_tip == chain_tip and force_new else "new_chain_tip"
        append_jsonl(ROUND_LOG, round_summary_locked(CURRENT_ROUND))

    CURRENT_ROUND = {
        "round_id": round_id,
        "chain_tip": chain_tip,
        "network": cfg().get("network", "unknown"),
        "height": tmpl.get("height"),
        "previousblockhash": tmpl.get("previousblockhash"),
        "started_at": now_iso(),
        "ended_at": None,
        "end_reason": None,
        "template_id": template_id or round_id,
        "transactions": len(tmpl.get("transactions", [])),
        "participants": {},
        "events": [],
    }
    STATE["current_round"] = round_summary_locked(CURRENT_ROUND)
    append_jsonl(EVENT_LOG, {"round_id": round_id, "ts": now_iso(), "event": "round_started", "height": tmpl.get("height"), "previousblockhash": tmpl.get("previousblockhash"), "force_new": bool(force_new)})

def add_worker_delta_locked(worker_id, data):
    p = ensure_participant_locked(worker_id)
    if not p:
        return
    seq = data.get("heartbeat_seq")
    if seq is not None:
        try:
            seq = int(seq)
        except Exception:
            seq = None
    last_seq = p.get("last_heartbeat_seq")
    if seq is not None and last_seq is not None and seq <= last_seq:
        return
    status = str(data.get("status", ""))
    delta_hashes = int(data.get("last_interval_hashes", 0) or 0)
    delta_seconds = float(data.get("last_interval_seconds", 0.0) or 0.0)
    if status in (STATUS_MINING, STATUS_FOUND) and delta_hashes > 0 and delta_seconds > 0:
        p["verified_hashes"] = int(p.get("verified_hashes", 0) or 0) + delta_hashes
        p["active_seconds"] = float(p.get("active_seconds", 0.0) or 0.0) + delta_seconds
        p["heartbeat_count"] = int(p.get("heartbeat_count", 0) or 0) + 1
    if seq is not None:
        p["last_heartbeat_seq"] = seq
    p["last_seen"] = now_iso()
    prev = p.get("last_status")
    p["last_status"] = status
    if prev and prev != status:
        add_round_event_locked("status_change", worker_id, old=prev, new=status)
    STATE["current_round"] = round_summary_locked(CURRENT_ROUND)

def finalize_round_for_block_locked(worker_id, job_id, nonce, submit_result):
    global CURRENT_ROUND, COMPLETED_BLOCKS
    if CURRENT_ROUND is None:
        return None
    round_id = CURRENT_ROUND.get("round_id")
    if round_id in FINALIZED_ROUND_IDS or CURRENT_ROUND.get("finalized"):
        # Late duplicate find for an already closed round. Keep it as event only.
        add_round_event_locked("late_block_found_ignored", worker_id, job_id=job_id, nonce=nonce, submitblock=submit_result)
        return STATE.get("last_round") or round_summary_locked(CURRENT_ROUND)
    ensure_participant_locked(worker_id)
    CURRENT_ROUND["ended_at"] = now_iso()
    CURRENT_ROUND["end_reason"] = "block_found"
    CURRENT_ROUND["finder_worker_id"] = worker_id
    CURRENT_ROUND["found_job_id"] = job_id
    CURRENT_ROUND["found_nonce"] = nonce
    CURRENT_ROUND["submitblock"] = submit_result
    CURRENT_ROUND["finalized"] = True
    add_round_event_locked("block_found", worker_id, job_id=job_id, nonce=nonce, submitblock=submit_result)
    summary = round_summary_locked(CURRENT_ROUND)
    append_jsonl(ROUND_LOG, summary)
    append_jsonl(BLOCK_LOG, summary)
    COMPLETED_BLOCKS.append(summary)
    COMPLETED_BLOCKS = COMPLETED_BLOCKS[-100:]
    if round_id:
        FINALIZED_ROUND_IDS.add(round_id)
        FINALIZING_ROUND_IDS.discard(round_id)
    STATE["last_round"] = summary
    STATE["current_round"] = summary
    return summary


def benchmark_snapshot_locked():
    b = copy.deepcopy(BENCHMARK)
    if b.get("running") and b.get("started_at_ts"):
        now = time.time()
        b["elapsed_seconds"] = max(0.0, now - float(b.get("started_at_ts") or now))
        b["remaining_seconds"] = max(0.0, float(b.get("end_at_ts") or now) - now)
    else:
        b.setdefault("elapsed_seconds", 0.0)
        b.setdefault("remaining_seconds", 0.0)
    r = round_summary_locked(CURRENT_ROUND)
    parts = []
    total_hashes = 0
    if r:
        total_hashes = int(r.get("verified_hashes_total", 0) or 0)
        for p in r.get("participants", {}).values():
            parts.append({
                "worker_id": p.get("worker_id"),
                "name": p.get("name"),
                "gpu_device": p.get("gpu_device"),
                "verified_hashes": int(p.get("verified_hashes", 0) or 0),
                "active_seconds": float(p.get("active_seconds", 0.0) or 0.0),
                "share_percent": float(p.get("share_percent", 0.0) or 0.0),
            })
    elapsed = float(b.get("elapsed_seconds") or 0.0)
    b["verified_hashes_total"] = total_hashes
    b["verified_hashrate_hs"] = (total_hashes / elapsed) if elapsed > 0 else 0.0
    b["participants"] = sorted(parts, key=lambda x: x.get("verified_hashes", 0), reverse=True)
    return b

def finish_benchmark_locked(reason="finished"):
    if not BENCHMARK.get("running"):
        return BENCHMARK.get("last_result") or benchmark_snapshot_locked()
    BENCHMARK["running"] = False
    BENCHMARK["ended_at"] = now_iso()
    BENCHMARK["end_reason"] = reason
    result = benchmark_snapshot_locked()
    result["end_reason"] = reason
    BENCHMARK["last_result"] = result
    append_jsonl(BENCH_LOG, result)
    return result

def benchmark_watchdog():
    while True:
        time.sleep(1)
        with lock:
            if BENCHMARK.get("running") and time.time() >= float(BENCHMARK.get("end_at_ts") or 0):
                res = finish_benchmark_locked("duration_reached")
                STATE["running"] = False
                msg = f"Benchmark beendet: {res.get('verified_hashrate_hs',0):.0f} H/s, Funde {res.get('valid_finds',0)}"
            else:
                msg = None
        if msg:
            log(msg)

def load_config():
    return load_json_config(CONFIG_PATH)

def save_config(cfg):
    save_json_config(CONFIG_PATH, cfg)

def cfg():
    return load_config()

def sha256_hex(text):
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()

def dashboard_auth_enabled():
    return bool(cfg().get("dashboard_auth_enabled", False))

def dashboard_auth_ok():
    if not dashboard_auth_enabled():
        return True
    auth = request.authorization
    if not auth:
        return False
    c = cfg()
    expected_user = str(c.get("dashboard_user", "admin"))
    expected_hash = str(c.get("dashboard_password_hash", ""))
    # Backward-compatible fallback for local testing only; prefer dashboard_password_hash.
    if not expected_hash and c.get("dashboard_password"):
        expected_hash = sha256_hex(c.get("dashboard_password"))
    return secrets.compare_digest(str(auth.username or ""), expected_user) and bool(expected_hash) and secrets.compare_digest(sha256_hex(auth.password or ""), expected_hash)

def require_dashboard_auth(fn):
    def wrapper(*args, **kwargs):
        if dashboard_auth_ok():
            return fn(*args, **kwargs)
        return Response(
            "Login erforderlich",
            401,
            {"WWW-Authenticate": 'Basic realm="Miner Master Dashboard"'}
        )
    wrapper.__name__ = fn.__name__
    return wrapper

def rpc(method, params=None, timeout=60):
    c = cfg()
    r = requests.post(c["rpc_url"], auth=(c["rpc_user"], c["rpc_pass"]), json={
        "jsonrpc": "1.0", "id": "miner-master", "method": method, "params": params or []
    }, timeout=timeout)

    # Bitcoin Core may return JSON-RPC errors with HTTP 500.
    # Parse JSON first so the dashboard/logs show the real RPC error
    # instead of only "500 Server Error".
    try:
        data = r.json()
    except Exception:
        r.raise_for_status()
        raise

    if data.get("error"):
        err = data["error"]
        if isinstance(err, dict):
            raise RuntimeError(f"RPC {method} failed: {err.get('code')} - {err.get('message')}")
        raise RuntimeError(f"RPC {method} failed: {err}")

    r.raise_for_status()
    return data["result"]

def token_ok():
    c = cfg()
    expected = c.get("worker_token", "")
    got = request.headers.get("Authorization", "")
    if got.startswith("Bearer "):
        got = got[7:]
    if not got and request.is_json:
        got = (request.json or {}).get("worker_token", "")
    return bool(expected) and secrets.compare_digest(got, expected)

def refresh_template(force=False):
    global ACTIVE_TEMPLATE, PAYOUT_SCRIPT, EXTRANONCE
    c = cfg()
    if PAYOUT_SCRIPT is None or force:
        info = rpc("getaddressinfo", [c["mining_address"]])
        PAYOUT_SCRIPT = info["scriptPubKey"]
    tmpl = rpc("getblocktemplate", [{"rules": ["segwit"]}])
    template_id = f"{tmpl['height']}:{tmpl['previousblockhash']}:{tmpl.get('longpollid','')}"
    chain_tip = f"{tmpl['height']}:{tmpl['previousblockhash']}"
    round_id = f"{chain_tip}:manual-{int(time.time()*1000)}" if force else chain_tip
    with lock:
        if force or STATE.get("template_id") != template_id:
            ACTIVE_TEMPLATE = tmpl
            STATE.update({
                "template_id": template_id,
                "round_id": round_id,
                "height": tmpl["height"],
                "previousblockhash": tmpl["previousblockhash"],
                "transactions": len(tmpl.get("transactions", [])),
            })
            if force:
                EXTRANONCE = 0
            start_round_locked(round_id, tmpl, template_id=template_id, force_new=force)
    return ACTIVE_TEMPLATE

def template_watcher():
    while True:
        time.sleep(3)
        with lock:
            running = STATE["running"]
        if not running:
            continue
        try:
            refresh_template(force=False)
        except Exception as e:
            with lock:
                STATE["last_error"] = f"Template watcher: {e}"

def alloc_job(worker_id):
    global EXTRANONCE
    tmpl = refresh_template(force=False)
    with lock:
        EXTRANONCE += 1
        extranonce = EXTRANONCE
    job = make_job_from_template(tmpl, PAYOUT_SCRIPT, extranonce)
    job_id = f"{STATE['template_id']}:{worker_id}:{extranonce}"
    job["job_id"] = job_id
    job["template_id"] = STATE["template_id"]
    job["round_id"] = STATE.get("round_id")
    with lock:
        JOBS[job_id] = job
    public = {k: job[k] for k in ["job_id", "template_id", "round_id", "height", "transactions", "previousblockhash", "bits", "target_hex", "header_prefix_hex", "extranonce"]}
    public["max_nonce"] = 0xffffffff
    public["recommended_batch_size"] = int(cfg().get("gpu_batch_size", 262144))
    return public

def worker_timeouts():
    c = cfg()
    offline_after = int(c.get("worker_offline_after_seconds", 20))
    remove_after = int(c.get("worker_remove_after_seconds", 120))
    return offline_after, remove_after

def cleanup_workers():
    now = time.time()
    offline_after, remove_after = worker_timeouts()
    with lock:
        for wid, w in list(STATE["workers"].items()):
            age = now - float(w.get("last_seen", 0) or 0)
            if age > remove_after:
                del STATE["workers"][wid]
            elif age > offline_after:
                if w.get("online") or w.get("status") != STATUS_OFFLINE:
                    p = ensure_participant_locked(wid)
                    if p:
                        p["disconnects"] = int(p.get("disconnects", 0) or 0) + 1
                    add_round_event_locked("offline", wid, age_seconds=int(age))
                w["status"] = STATUS_OFFLINE
                w["hashrate_hs"] = 0.0
                w["verified_hashrate_hs"] = 0.0
                w["online"] = False
            else:
                if not w.get("online") and w.get("status") == STATUS_OFFLINE:
                    p = ensure_participant_locked(wid)
                    if p:
                        p["join_count"] = int(p.get("join_count", 0) or 0) + 1
                    add_round_event_locked("resumed", wid)
                w["online"] = True
        STATE["current_round"] = round_summary_locked(CURRENT_ROUND)

def snapshot():
    cleanup_workers()
    with lock:
        s = json.loads(json.dumps(STATE))
    now = time.time()
    c = cfg()
    stale_after = float(c.get("worker_hashrate_stale_after_seconds", 8))
    total = 0.0
    online_count = 0
    for w in s["workers"].values():
        age = now - float(w.get("last_seen", 0) or 0)
        w["age_seconds"] = int(age)
        st = str(w.get("status", ""))
        non_mining = st in ("thermal_pause", "thermal_stop", "local_stop", "idle", "stopping", "panic_stop") or st.startswith("thermal")
        if age > stale_after or non_mining:
            w["hashrate_hs"] = 0.0
            w["verified_hashrate_hs"] = 0.0
            if non_mining:
                w["last_interval_hashes"] = 0
                w["last_interval_seconds"] = 0.0
        if w.get("online"):
            online_count += 1
            total += float(w.get("verified_hashrate_hs", 0.0) or 0.0)
    s["total_hashrate_hs"] = total
    s["online_workers"] = online_count
    s["runtime_seconds"] = int(now - s["started_at"]) if s.get("started_at") else 0
    s["verified_total_hashes"] = sum(int(w.get("total_hashes", 0) or 0) for w in s["workers"].values() if w.get("online"))
    s["verify_worker_count"] = len(s.get("workers", {}))
    with lock:
        s["current_round"] = round_summary_locked(CURRENT_ROUND)
        s["recent_blocks"] = copy.deepcopy(COMPLETED_BLOCKS[-20:])
        s["benchmark"] = benchmark_snapshot_locked()
    return s

@app.route("/")
@require_dashboard_auth
def index():
    return render_template_string(HTML)

@app.route("/api/status")
@app.route("/status")
@require_dashboard_auth
def api_status():
    return jsonify(snapshot())

@app.route("/start", methods=["POST", "GET"])
@require_dashboard_auth
def start():
    try:
        refresh_template(force=True)
        with lock:
            STATE.update({"running": True, "found": False, "started_at": time.time(), "last_error": None, "last_submit_result": None})
            log(f"Master gestartet: Height {STATE.get('height')}")
        return jsonify({"ok": True, "message": "Master läuft. Worker können Jobs holen."})
    except Exception as e:
        with lock:
            STATE["last_error"] = str(e)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/stop", methods=["POST", "GET"])
@require_dashboard_auth
def stop():
    with lock:
        STATE["running"] = False
        for w in STATE.get("workers", {}).values():
            w["hashrate_hs"] = 0.0
            w["verified_hashrate_hs"] = 0.0
            w["status"] = "stopping"
    log("Master gestoppt")
    return jsonify({"ok": True, "message": "Mining gestoppt."})

@app.route("/panic", methods=["POST", "GET"])
@require_dashboard_auth
def panic():
    with lock:
        STATE["running"] = False
        STATE["last_error"] = "PANIC STOP gedrückt: Master verteilt keine Jobs mehr."
        for w in STATE.get("workers", {}).values():
            w["hashrate_hs"] = 0.0
            w["verified_hashrate_hs"] = 0.0
            w["status"] = "panic_stop"
    log("PANIC STOP")
    return jsonify({"ok": True, "message": "PANIC STOP: Master gestoppt. Worker stoppen spätestens beim nächsten Heartbeat/Jobwechsel."})

@app.route("/api/worker/register", methods=["POST"])
def worker_register():
    if not token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = request.json or {}
    wid = str(data.get("worker_id") or data.get("name") or secrets.token_hex(4))
    name = str(data.get("name") or wid)
    now = time.time()
    offline_after, _remove_after = worker_timeouts()
    with lock:
        # Ein aktiver worker_name darf nur einmal im Cluster vorkommen.
        # Gleicher worker_name + gleiche worker_id = Reconnect desselben Workers.
        # Gleicher worker_name + andere worker_id = zweite Instanz/Fehlkonfiguration -> ablehnen.
        for other_id, other in STATE.get("workers", {}).items():
            if other_id == wid:
                continue
            if str(other.get("name") or other_id) != name:
                continue
            age = now - float(other.get("last_seen", 0) or 0)
            if other.get("online") and age <= offline_after:
                msg = f"Workername bereits aktiv: {name} ({other_id})"
                log(msg)
                return jsonify({"ok": False, "error": msg, "code": "worker_name_already_active", "active_worker_id": other_id}), 409

        STATE["workers"].setdefault(wid, {})
        STATE["workers"][wid].update({
            "worker_id": wid,
            "name": name,
            "backend": data.get("backend"),
            "gpu_device": data.get("gpu_device"),
            "gpus": data.get("gpus", []),
            "last_seen": now,
            "online": True,
            "hashrate_hs": 0.0,
            "status": STATUS_REGISTERED,
        })
        if STATE.get("running"):
            ensure_participant_locked(wid)
        log(f"Worker registriert: {wid} {data.get('gpu_device','')}")
    return jsonify({"ok": True, "worker_id": wid})

@app.route("/api/worker/job", methods=["POST"])
def worker_job():
    if not token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = request.json or {}
    wid = data.get("worker_id", "unknown")
    worker_name = data.get("name") or data.get("worker_name")
    with lock:
        running = STATE["running"]
        STATE["workers"].setdefault(wid, {"worker_id": wid})
        update = {"last_seen": time.time(), "online": True, "status": "waiting_job"}
        if worker_name:
            update["name"] = str(worker_name)
        STATE["workers"][wid].update(update)
    if not running:
        return jsonify({"ok": True, "running": False})
    try:
        job = alloc_job(wid)
        with lock:
            STATE["workers"][wid].update({"status": STATUS_MINING, "job_id": job["job_id"], "height": job["height"]})
            ensure_participant_locked(wid)
            log(f"Job an {wid}: Height {job['height']} ExtraNonce {job['extranonce']}")
        return jsonify({"ok": True, "running": True, "job": job})
    except Exception as e:
        with lock:
            STATE["last_error"] = str(e)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/worker/heartbeat", methods=["POST"])
def worker_heartbeat():
    try:
        if not token_ok():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        data = request.get_json(silent=True) or {}
        wid = data.get("worker_id", "unknown")
        worker_name = data.get("name") or data.get("worker_name")
        with lock:
            w = STATE["workers"].setdefault(wid, {"worker_id": wid})
            status = data.get("status", "mining")
            try:
                hashrate = float(data.get("hashrate_hs", 0.0) or 0.0)
            except Exception:
                hashrate = 0.0
            try:
                verified = float(data.get("verified_hashrate_hs", data.get("hashrate_hs", 0.0)) or 0.0)
            except Exception:
                verified = 0.0
            if str(status).startswith("thermal") or status in ("local_stop", "idle", "stopping", "panic_stop"):
                hashrate = 0.0
                verified = 0.0
            update_payload = {
                "last_seen": time.time(),
                "online": True,
                "hashrate_hs": hashrate,
                "verified_hashrate_hs": verified,
                "total_hashes": int(data.get("total_hashes", w.get("total_hashes", 0)) or 0),
                "nonce": data.get("nonce"),
                "completed_batches": int(data.get("completed_batches", w.get("completed_batches", 0) or 0) or 0),
                "last_interval_hashes": int(data.get("last_interval_hashes", 0) or 0),
                "last_interval_seconds": float(data.get("last_interval_seconds", 0.0) or 0.0),
                "job_id": data.get("job_id", w.get("job_id")),
                "backend": data.get("backend", w.get("backend")),
                "gpu_device": data.get("gpu_device", w.get("gpu_device")),
                "gpu_metrics": data.get("gpu_metrics", w.get("gpu_metrics")),
                "status": status,
                "batch_size": data.get("batch_size", w.get("batch_size")),
                "local_size": data.get("local_size", w.get("local_size")),
                "heartbeat_seq": data.get("heartbeat_seq", w.get("heartbeat_seq")),
                "last_update": time.strftime("%H:%M:%S"),
            }
            if worker_name:
                update_payload["name"] = str(worker_name)
            w.update(update_payload)
            if data.get("job_id") and str(data.get("job_id", "")).startswith(str(STATE.get("template_id", ""))):
                add_worker_delta_locked(wid, data)
            running = STATE["running"]
            template_id = STATE["template_id"]
        return jsonify({"ok": True, "running": running, "template_id": template_id})
    except Exception as e:
        # Production API should never return Flask's HTML 500 page to workers.
        with lock:
            STATE["last_error"] = f"heartbeat error: {e}"
        print("Heartbeat error:", repr(e))
        return jsonify({"ok": False, "error": f"heartbeat error: {e}"}), 500

@app.route("/api/worker/found", methods=["POST"])
def worker_found():
    if not token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    found_worker_id = data.get("worker_id")
    found_worker_name = data.get("name") or data.get("worker_name")
    if found_worker_id and found_worker_name:
        with lock:
            STATE["workers"].setdefault(found_worker_id, {"worker_id": found_worker_id})["name"] = str(found_worker_name)
    job_id = data.get("job_id")
    try:
        nonce = int(data.get("nonce"))
    except Exception:
        return jsonify({"ok": False, "error": "invalid nonce"}), 400
    with lock:
        job = JOBS.get(job_id)
    if not job:
        return jsonify({"ok": False, "error": "unknown job"}), 404

    job_round_id = job.get("round_id") or f"{job.get('height')}:{job.get('previousblockhash')}"

    try:
        with lock:
            benchmark_running = bool(BENCHMARK.get("running"))
        if benchmark_running:
            with lock:
                BENCHMARK["valid_finds"] = int(BENCHMARK.get("valid_finds", 0) or 0) + 1
                find = {"ts": now_iso(), "worker_id": data.get("worker_id"), "job_id": job_id, "nonce": nonce, "height": job.get("height")}
                BENCHMARK.setdefault("finds", []).append(find)
                BENCHMARK["finds"] = BENCHMARK["finds"][-200:]
                add_round_event_locked("benchmark_valid_find", data.get("worker_id"), job_id=job_id, nonce=nonce)
                log(f"Benchmark-Fund von {data.get('worker_id')}: nicht submitted, nonce={nonce}")
            return jsonify({"ok": True, "benchmark": True, "submitted": False})

        # A round may receive late/duplicate found messages from multiple workers.
        # Only the first one is allowed to submit/finalize. Others are logged and ignored.
        with lock:
            if job_round_id in FINALIZED_ROUND_IDS or job_round_id in FINALIZING_ROUND_IDS:
                add_round_event_locked("late_block_found_ignored", data.get("worker_id"), job_id=job_id, nonce=nonce)
                return jsonify({"ok": True, "ignored": True, "reason": "round already finalized/finalizing"})
            FINALIZING_ROUND_IDS.add(job_round_id)
            was_running = bool(STATE.get("running"))

        block_hex = build_block_hex(job, nonce)
        result = rpc("submitblock", [block_hex])

        # Bitcoin Core returns None/null only when the block was accepted. Any string
        # such as "unexpected-witness", "bad-txnmrklroot" or "high-hash" is a
        # rejection and must not finalize the round as a found block.
        if result is not None:
            with lock:
                FINALIZING_ROUND_IDS.discard(job_round_id)
                STATE.update({
                    "found": False,
                    "running": was_running,
                    "last_submit_result": result,
                    "last_rejected_block": {
                        "ts": now_iso(),
                        "worker_id": data.get("worker_id"),
                        "job_id": job_id,
                        "nonce": nonce,
                        "submitblock": result,
                    },
                })
                add_round_event_locked("block_rejected", data.get("worker_id"), job_id=job_id, nonce=nonce, submitblock=result)
                log(f"Block abgelehnt von {data.get('worker_id')}: submitblock={result}")
            # Keep mining. Force a template refresh so workers do not keep submitting
            # against a problematic/stale job, but do not close the current round.
            if was_running:
                try:
                    refresh_template(force=True)
                except Exception as e:
                    with lock:
                        STATE["last_error"] = f"Template refresh after rejected block: {e}"
                    log(f"Template refresh after rejected block fehlgeschlagen: {e}")
            return jsonify({"ok": True, "accepted": False, "submitblock": result, "running": was_running})

        with lock:
            STATE.update({"found": True, "running": was_running, "last_submit_result": result})
            summary = finalize_round_for_block_locked(data.get("worker_id"), job_id, nonce, result)
            log(f"Blockfund von {data.get('worker_id')}: submitblock={result}")

        # Production mode should continue after a valid block. Refresh immediately so
        # workers receive the next chain tip instead of the master staying stopped.
        if was_running:
            try:
                refresh_template(force=True)
                with lock:
                    log(f"Nächste Runde gestartet: Height {STATE.get('height')}")
            except Exception as e:
                with lock:
                    STATE["last_error"] = f"Template refresh after block: {e}"
                log(f"Template refresh after block fehlgeschlagen: {e}")

        return jsonify({"ok": True, "accepted": True, "submitblock": result, "round": summary, "running": was_running})
    except Exception as e:
        with lock:
            FINALIZING_ROUND_IDS.discard(job_round_id)
            STATE["last_error"] = str(e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/worker/status", methods=["GET", "POST"])
def worker_safe_status():
    """Token-geschützter Status für Worker-Dashboards.

    Wichtig: Das Master-Dashboard kann per Basic-Login geschützt sein. Worker
    sollen diese Login-Daten NICHT kennen. Deshalb gibt es diesen kleinen
    maschinenlesbaren Status über den bestehenden worker_token.
    """
    if not token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    s = snapshot()
    return jsonify({
        "ok": True,
        "running": bool(s.get("running")),
        "height": s.get("height"),
        "template_id": s.get("template_id"),
        "round_id": s.get("round_id"),
        "total_hashrate_hs": float(s.get("total_hashrate_hs", 0.0) or 0.0),
        "online_workers": int(s.get("online_workers", 0) or 0),
    })

@app.route("/api/rpc_test")
@require_dashboard_auth
def api_rpc_test():
    try:
        info = rpc("getblockchaininfo")
        mining = rpc("getmininginfo")
        return jsonify({"ok": True, "chain": info.get("chain"), "blocks": info.get("blocks"), "headers": info.get("headers"), "difficulty": mining.get("difficulty"), "networkhashps": mining.get("networkhashps")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/regtest/newaddress", methods=["POST"])
@require_dashboard_auth
def api_regtest_newaddress():
    global PAYOUT_SCRIPT
    try:
        addr = rpc("getnewaddress", ["miner-v036", "bech32"])
        c = cfg()
        c["mining_address"] = addr
        save_config(c)
        PAYOUT_SCRIPT = None
        log(f"Regtest-Adresse gesetzt: {addr}")
        return jsonify({"ok": True, "mining_address": addr})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/regtest/generate", methods=["POST"])
@require_dashboard_auth
def api_regtest_generate():
    try:
        data = request.json or {}
        blocks = int(data.get("blocks", 1))
        addr = data.get("address") or cfg().get("mining_address")
        res = rpc("generatetoaddress", [blocks, addr], timeout=120)
        log(f"Regtest generatetoaddress {blocks}: {res[-1] if res else 'ok'}")
        refresh_template(force=True)
        return jsonify({"ok": True, "blocks": res})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500



@app.route("/api/benchmark/start", methods=["POST", "GET"])
@require_dashboard_auth
def api_benchmark_start():
    try:
        data = request.json or {} if request.is_json else {}
        duration = int(data.get("duration_seconds") or request.args.get("seconds") or cfg().get("benchmark_round_seconds", 60))
        duration = max(5, min(duration, 86400))
        refresh_template(force=True)
        with lock:
            BENCHMARK.clear()
            BENCHMARK.update({
                "running": True,
                "benchmark_id": f"bench-{int(time.time())}",
                "started_at_ts": time.time(),
                "started_at": now_iso(),
                "duration_seconds": duration,
                "end_at_ts": time.time() + duration,
                "ended_at": None,
                "end_reason": None,
                "valid_finds": 0,
                "finds": [],
                "last_result": None,
            })
            STATE.update({"running": True, "found": False, "started_at": time.time(), "last_error": None, "last_submit_result": None})
            log(f"Benchmark gestartet: {duration}s Height {STATE.get('height')}")
        return jsonify({"ok": True, "benchmark": snapshot().get("benchmark")})
    except Exception as e:
        with lock:
            STATE["last_error"] = str(e)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/benchmark/stop", methods=["POST", "GET"])
@require_dashboard_auth
def api_benchmark_stop():
    with lock:
        res = finish_benchmark_locked("manual_stop")
        STATE["running"] = False
    log("Benchmark manuell gestoppt")
    return jsonify({"ok": True, "benchmark": res})

@app.route("/api/benchmark/status")
@require_dashboard_auth
def api_benchmark_status():
    with lock:
        b = benchmark_snapshot_locked()
        out = {"ok": True, "benchmark": b}
        # Backward-compatible flattening for simple frontends/scripts.
        out.update(b)
        return jsonify(out)

@app.route("/benchmarks")
@require_dashboard_auth
def benchmarks_page():
    return render_template_string(BENCHMARK_HTML)


def read_jsonl_tail(path, limit=500):
    rows = []
    try:
        if not Path(path).exists():
            return rows
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()[-int(limit):]
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    except Exception as e:
        print(f"read_jsonl_tail error {path}: {e}")
    return rows

@app.route("/api/worker/history", methods=["GET", "POST"])
def worker_history():
    """Token-geschützte Block-/Round-Historie für ein Worker-Dashboard.

    Der Worker bekommt keine Master-Dashboard-Login-Daten. Diese API nutzt nur
    den bestehenden worker_token und liefert nur die Beteiligungen des Workers.
    """
    if not token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    wid = str(data.get("worker_id") or request.args.get("worker_id") or "")
    name = str(data.get("name") or request.args.get("name") or "")
    limit = int(data.get("limit") or request.args.get("limit") or 100)
    with lock:
        blocks = list(COMPLETED_BLOCKS[-max(limit, 1):])
    if len(blocks) < limit:
        # Nach Master-Neustart kommen alte Rounds aus blocks.jsonl.
        seen = {b.get("round_id") for b in blocks}
        for b in read_jsonl_tail(BLOCK_LOG, limit * 3):
            if b.get("round_id") not in seen:
                blocks.append(b)
                seen.add(b.get("round_id"))
    matches = []
    total_worker_hashes = 0
    total_cluster_hashes = 0
    for b in blocks:
        parts = b.get("participants", {})
        if isinstance(parts, dict):
            plist = list(parts.values())
        else:
            plist = list(parts or [])
        worker_part = None
        for p in plist:
            if (wid and str(p.get("worker_id")) == wid) or (name and str(p.get("name")) == name):
                worker_part = dict(p)
                break
        if not worker_part:
            continue
        cluster_hashes = int(b.get("verified_hashes_total") or sum(int(p.get("verified_hashes", 0) or 0) for p in plist))
        worker_hashes = int(worker_part.get("verified_hashes", 0) or 0)
        share = float(worker_part.get("share_percent") or ((worker_hashes / cluster_hashes * 100.0) if cluster_hashes > 0 else 0.0))
        worker_part.setdefault("paid", "no")
        total_worker_hashes += worker_hashes
        total_cluster_hashes += cluster_hashes
        matches.append({
            "round_id": b.get("round_id"),
            "height": b.get("height"),
            "started_at": b.get("started_at"),
            "ended_at": b.get("ended_at"),
            "end_reason": b.get("end_reason"),
            "submitblock": b.get("submitblock"),
            "finder_worker_id": b.get("finder_worker_id"),
            "cluster_verified_hashes": cluster_hashes,
            "worker_verified_hashes": worker_hashes,
            "worker_active_seconds": float(worker_part.get("active_seconds", 0.0) or 0.0),
            "worker_share_percent": share,
            "paid": worker_part.get("paid", "no"),
            "worker": worker_part,
        })
    matches.sort(key=lambda x: str(x.get("ended_at") or x.get("started_at") or ""), reverse=True)
    return jsonify({
        "ok": True,
        "worker_id": wid,
        "name": name,
        "blocks": matches[:limit],
        "summary": {
            "block_count": len(matches),
            "worker_verified_hashes": total_worker_hashes,
            "cluster_verified_hashes": total_cluster_hashes,
            "overall_share_percent": (total_worker_hashes / total_cluster_hashes * 100.0) if total_cluster_hashes > 0 else 0.0,
        }
    })

@app.route("/api/rounds")
@require_dashboard_auth
def api_rounds():
    with lock:
        return jsonify({"ok": True, "current_round": round_summary_locked(CURRENT_ROUND), "recent_blocks": copy.deepcopy(COMPLETED_BLOCKS[-100:])})

@app.route("/blocks")
@require_dashboard_auth
def blocks_page():
    return render_template_string(BLOCKS_HTML)

@app.route("/verify")
@require_dashboard_auth
def verify_page():
    return render_template_string(VERIFY_HTML)

@app.route("/regtest")
@require_dashboard_auth
def regtest_page():
    return render_template_string(REGTEST_HTML, config=json.dumps(cfg(), indent=2, ensure_ascii=False))

@app.route("/config", methods=["GET", "POST"])
@require_dashboard_auth
def config_page():
    if request.method == "POST":
        try:
            new_cfg = json.loads(request.form.get("config", "{}"))
            save_config(new_cfg)
            return redirect("/config?saved=1")
        except Exception as e:
            return f"Config Fehler: {e}", 400
    return render_template_string(CONFIG_HTML, config=json.dumps(cfg(), indent=2, ensure_ascii=False))

HTML = r'''
<!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Miner Master Production Dashboard</title><style>
body{font-family:system-ui,Arial;background:#0b1020;color:#e8eefc;margin:0}header{padding:16px 22px;background:#111936;border-bottom:1px solid #26345f}.wrap{padding:18px;max-width:1400px;margin:auto}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}.card{background:#111936;border:1px solid #26345f;border-radius:14px;padding:14px;margin-bottom:12px}.label{color:#9fb0d0;font-size:.85rem}.value{font-size:1.5rem;font-weight:700}button,a.button{background:#2563eb;color:white;border:0;border-radius:10px;padding:10px 14px;text-decoration:none;margin-right:8px;cursor:pointer}.danger{background:#dc2626}.ok{color:#34d399}.bad{color:#fb7185}.warn{color:#fbbf24}table{width:100%;border-collapse:collapse}td,th{padding:8px;border-bottom:1px solid #26345f;text-align:left;vertical-align:top}pre{white-space:pre-wrap}.bar{height:8px;background:#26345f;border-radius:8px;overflow:hidden}.bar>span{display:block;height:8px;background:#34d399}.small{font-size:.85rem;color:#9fb0d0}.log{height:170px;overflow:auto;background:#0b1020;border-radius:10px;padding:10px}.pill{display:inline-block;border-radius:999px;padding:2px 8px;background:#26345f;font-size:.8rem}
</style></head>
<body><header><b>Bitcoin Miner Master Production Dashboard</b> <span id="run"></span></header><div class="wrap">
<div class="card"><button onclick="post('/start')">Start</button><button class="danger" onclick="post('/stop')">Stop</button><button class="danger" onclick="post('/panic')">NOT-AUS</button><a class="button" href="/config">Config</a><a class="button" href="/blocks">Blocks/Rounds</a><span id="msg" class="small"></span></div>
<div class="grid"><div class="card"><div class="label">Gesamt-Hashrate</div><div id="hr" class="value">—</div></div><div class="card"><div class="label">Worker online</div><div id="wc" class="value">—</div></div><div class="card"><div class="label">Blockhöhe</div><div id="height" class="value">—</div></div><div class="card"><div class="label">Laufzeit</div><div id="rt" class="value">—</div></div></div>
<div class="card"><h3>Worker</h3><table><thead><tr><th>Name</th><th>Status</th><th>GPU</th><th>Hashrate</th><th>Verifikation</th><th>GPU-Metriken</th><th>Tuning</th><th>Alter</th></tr></thead><tbody id="workers"></tbody></table></div>
<div class="card"><h3>Leistungsverteilung</h3><div id="bars"></div></div>
<div class="card"><h3>Aktuelle Round</h3><pre id="round"></pre></div><div class="card"><h3>Live-Log</h3><div id="logs" class="log"></div></div>
<div class="card"><h3>Fehler / Submit</h3><pre id="err"></pre></div>
</div><script>
function esc(x){return String(x??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}
function fmt(n){n=Number(n||0); if(n>1e9)return (n/1e9).toFixed(2)+' GH/s'; if(n>1e6)return (n/1e6).toFixed(2)+' MH/s'; if(n>1e3)return (n/1e3).toFixed(2)+' kH/s'; return n.toFixed(0)+' H/s'}
function dur(s){s=s||0;let h=Math.floor(s/3600),m=Math.floor((s%3600)/60),x=s%60;return `${h}h ${m}m ${x}s`}
async function post(u){let r=await fetch(u,{method:'POST'});document.getElementById('msg').textContent=await r.text();tick()}
function metric(w){let g=(w.gpu_metrics||[])[0]||{}; if(!g.name)return '<span class="small">—</span>'; return `<div>${esc(g.temp_c)}°C · ${esc(g.util_percent)}% · ${esc(g.power_w)}W · ${esc(g.pstate)}</div><div class="small">VRAM ${esc(g.mem_used_mb)}/${esc(g.mem_total_mb)} MB</div>`}
function tuning(w){return `<span class="pill">Batch ${esc(w.batch_size||'—')}</span> <span class="pill">Local ${esc(w.local_size||'—')}</span><div class="small">Nonce ${esc(w.nonce||'')}</div>`}
async function tick(){let s=await (await fetch('/api/status')).json();document.getElementById('run').innerHTML=s.running?'<span class="ok">● RUNNING</span>':'<span class="bad">● STOPPED</span>';document.getElementById('hr').textContent=fmt(s.total_hashrate_hs);document.getElementById('height').textContent=s.height||'—';document.getElementById('rt').textContent=dur(s.runtime_seconds);let ws=Object.values(s.workers||{});document.getElementById('wc').textContent=(s.online_workers||0)+' / '+ws.length;document.getElementById('workers').innerHTML=ws.map(w=>`<tr><td><b>${esc(w.name||w.worker_id)}</b><div class="small">${esc(w.worker_id)}</div></td><td>${w.online?'<span class="ok">online</span>':'<span class="bad">offline</span>'}<br>${esc(w.status||'')}</td><td>${esc(w.gpu_device||'')}<div class="small">${esc(w.backend||'')}</div></td><td><b>${fmt(w.verified_hashrate_hs||w.hashrate_hs||0)}</b><div class="small">Anzeige ${fmt(w.hashrate_hs||0)}</div></td><td><div>${Number(w.last_interval_hashes||0).toLocaleString()} Nonces</div><div class="small">in ${Number(w.last_interval_seconds||0).toFixed(3)}s · Batches ${Number(w.completed_batches||0).toLocaleString()}</div></td><td>${metric(w)}</td><td>${tuning(w)}</td><td>${w.age_seconds||0}s</td></tr>`).join('');let max=Math.max(1,...ws.map(w=>Number((w.verified_hashrate_hs||w.hashrate_hs)||0)));document.getElementById('bars').innerHTML=ws.map(w=>`<div style="margin:8px 0"><b>${esc(w.name||w.worker_id)}</b> <span class="small">${fmt(w.hashrate_hs||0)}</span><div class="bar"><span style="width:${Math.max(1,100*Number((w.verified_hashrate_hs||w.hashrate_hs)||0)/max)}%"></span></div></div>`).join('');document.getElementById('logs').innerHTML=(s.logs||[]).slice(-80).map(l=>`<div><span class="small">${esc(l.ts)}</span> ${esc(l.msg)}</div>`).join('');let lg=document.getElementById('logs');lg.scrollTop=lg.scrollHeight;document.getElementById('round').textContent=JSON.stringify(s.current_round||{},null,2);document.getElementById('err').textContent=JSON.stringify({last_error:s.last_error,last_submit_result:s.last_submit_result},null,2)}
setInterval(tick,1000);tick();</script></body></html>
'''

VERIFY_HTML = r'''<!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Verify</title><style>body{font-family:system-ui;background:#0b1020;color:#e8eefc;margin:20px;line-height:1.45}.card{background:#111936;border:1px solid #26345f;border-radius:14px;padding:14px;margin:12px 0}table{width:100%;border-collapse:collapse}td,th{padding:8px;border-bottom:1px solid #26345f;text-align:left}button,a{background:#2563eb;color:white;border:0;border-radius:10px;padding:10px 14px;text-decoration:none;margin-right:8px}.small{color:#9fb0d0;font-size:.9rem}pre{background:#0b1020;border-radius:10px;padding:10px;white-space:pre-wrap}.ok{color:#34d399}.warn{color:#fbbf24}.bad{color:#fb7185}</style></head><body><h1>Production Dashboard</h1><p><a href="/">Zurück</a></p><div class="card"><h2>Cluster-Verifikation</h2><p>Diese Seite zeigt die Hashrate aus abgeschlossenen Worker-Intervallen: <code>last_interval_hashes / last_interval_seconds</code>. Das ist die maßgebliche Zahl, nicht eine optimistische Anzeige.</p><table><tbody id="summary"></tbody></table></div><div class="card"><h2>Worker-Rohdaten</h2><table><thead><tr><th>Worker</th><th>Status</th><th>Instant</th><th>Verified</th><th>Intervall-Nonces</th><th>Intervall-Sekunden</th><th>Batches</th><th>Abweichung</th></tr></thead><tbody id="workers"></tbody></table></div><div class="card"><h2>Bewertung</h2><pre id="judge"></pre></div><script>function esc(x){return String(x??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}function fmt(n){n=Number(n||0); if(n>1e9)return (n/1e9).toFixed(2)+' GH/s'; if(n>1e6)return (n/1e6).toFixed(2)+' MH/s'; if(n>1e3)return (n/1e3).toFixed(2)+' kH/s'; return n.toFixed(0)+' H/s'}async function tick(){let s=await(await fetch('/api/status')).json();let ws=Object.values(s.workers||{});let verified=ws.reduce((a,w)=>a+Number(w.verified_hashrate_hs||0),0);let instant=ws.reduce((a,w)=>a+Number(w.hashrate_hs||0),0);document.getElementById('summary').innerHTML=`<tr><td>Cluster verified</td><td><b>${fmt(verified)}</b></td></tr><tr><td>Cluster instant</td><td>${fmt(instant)}</td></tr><tr><td>Worker</td><td>${s.online_workers||0} online / ${ws.length} gesamt</td></tr><tr><td>Height</td><td>${esc(s.height||'—')}</td></tr>`;document.getElementById('workers').innerHTML=ws.map(w=>{let ih=Number(w.hashrate_hs||0), vh=Number(w.verified_hashrate_hs||0);let diff=vh?((ih-vh)/vh*100):0;let cls=Math.abs(diff)>15?'warn':'ok';return `<tr><td><b>${esc(w.name||w.worker_id)}</b><div class="small">${esc(w.gpu_device||'')}</div></td><td>${esc(w.status||'')}</td><td>${fmt(ih)}</td><td><b>${fmt(vh)}</b></td><td>${Number(w.last_interval_hashes||0).toLocaleString()}</td><td>${Number(w.last_interval_seconds||0).toFixed(3)}</td><td>${Number(w.completed_batches||0).toLocaleString()}</td><td class="${cls}">${diff.toFixed(1)}%</td></tr>`}).join('');let msg=[];for(let w of ws){let ih=Number(w.hashrate_hs||0), vh=Number(w.verified_hashrate_hs||0);if(vh>0 && Math.abs((ih-vh)/vh)>0.15)msg.push(`${w.name||w.worker_id}: Instant und Verified weichen deutlich ab.`);if(Number(w.last_interval_seconds||0)<0.5)msg.push(`${w.name||w.worker_id}: Messintervall sehr kurz; Wert kann springen.`)}if(!msg.length)msg.push('Keine auffällige Abweichung zwischen Instant und Verified.');document.getElementById('judge').textContent=msg.join('\n')}setInterval(tick,1000);tick()</script></body></html>'''


BENCHMARK_HTML = r"""<!doctype html>
<html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Benchmark Lab</title>
<style>
body{font-family:system-ui;background:#0b1020;color:#e8eefc;margin:20px;line-height:1.45}
.card{background:#111936;border:1px solid #26345f;border-radius:14px;padding:14px;margin:12px 0}
table{width:100%;border-collapse:collapse}td,th{padding:8px;border-bottom:1px solid #26345f;text-align:left}
button,a{background:#2563eb;color:white;border:0;border-radius:10px;padding:10px 14px;text-decoration:none;margin:4px;display:inline-block}
button.stop{background:#dc2626}.small{color:#9fb0d0;font-size:.9rem}pre{background:#0b1020;border-radius:10px;padding:10px;white-space:pre-wrap}.ok{color:#34d399}.warn{color:#fbbf24}.bad{color:#fb7185}
input{background:#0b1020;color:#e8eefc;border:1px solid #26345f;border-radius:8px;padding:8px;width:110px}
</style></head><body>
<h1>Production Dashboard</h1>
<p><a href="/">Dashboard</a><a href="/verify">Verify</a><a href="/blocks">Blocks/Rounds</a></p>
<div class="card">
<h2>Benchmark-Runde</h2>
<p>Im Benchmark-Modus laufen die Worker für eine feste Dauer weiter. Regtest-Funde werden gezählt, aber nicht submitted und beenden die Runde nicht.</p>
<label>Dauer Sekunden: <input id="duration" type="number" value="300" min="5" max="86400"></label>
<button onclick="startBench()">Benchmark starten</button>
<button class="stop" onclick="stopBench()">Benchmark stoppen</button>
<p id="stateHint" class="small">Benchmark gestoppt.</p><pre id="action"></pre>
</div>
<div class="card"><h2>Aktueller Benchmark</h2><table><tbody id="benchSummary"></tbody></table></div>
<div class="card"><h2>Teilnehmer / Beiträge</h2><table><thead><tr><th>Worker</th><th>Hashes</th><th>Aktive Zeit</th><th>Ø verified</th><th>Anteil</th></tr></thead><tbody id="parts"></tbody></table></div>
<div class="card"><h2>Gültige Regtest-Funde während Benchmark</h2><pre id="finds"></pre></div>
<div class="card"><h2>Letzter abgeschlossener Benchmark</h2><pre id="last"></pre></div>
<script>
function esc(x){return String(x??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}
function fmt(n){n=Number(n||0); if(n>1e9)return (n/1e9).toFixed(2)+' GH/s'; if(n>1e6)return (n/1e6).toFixed(2)+' MH/s'; if(n>1e3)return (n/1e3).toFixed(2)+' kH/s'; return n.toFixed(0)+' H/s'}
function fmtHash(n){n=Number(n||0); if(n>1e12)return (n/1e12).toFixed(2)+' TH'; if(n>1e9)return (n/1e9).toFixed(2)+' GH'; if(n>1e6)return (n/1e6).toFixed(2)+' MH'; return n.toLocaleString()}
async function post(url, body){
  const action=document.getElementById('action');
  action.textContent='Request läuft...';
  try{
    let r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})});
    let txt=await r.text();
    let j; try{j=JSON.parse(txt)}catch(e){j={ok:r.ok, raw:txt}}
    action.textContent=JSON.stringify(j,null,2);
    await tick();
    return j;
  }catch(e){
    action.textContent='FEHLER: '+e;
    return {ok:false,error:String(e)};
  }
}
async function startBench(){
  let d=Number(document.getElementById('duration').value||300);
  document.getElementById('stateHint').textContent='Starte Benchmark...';
  await post('/api/benchmark/start',{duration_seconds:d});
}
async function stopBench(){
  document.getElementById('stateHint').textContent='Stoppe Benchmark...';
  await post('/api/benchmark/stop',{});
}
async function tick(){
let res=await(await fetch('/api/benchmark/status?ts='+Date.now(), {cache:'no-store'})).json();
let b=res.benchmark||res;
let running=b.running?'RUNNING':'STOPPED';let elapsed=Number(b.elapsed_seconds||0), rem=Number(b.remaining_seconds||0);
document.getElementById('stateHint').innerHTML=b.running?'Benchmark läuft. Worker sollten Jobs erhalten und Hashes melden.':'Benchmark gestoppt. Klicke „Benchmark starten“.';
document.getElementById('benchSummary').innerHTML=`<tr><td>Status</td><td><b class="${b.running?'ok':'warn'}">${running}</b></td></tr><tr><td>Benchmark ID</td><td>${esc(b.benchmark_id||'—')}</td></tr><tr><td>Zeit</td><td>${elapsed.toFixed(1)}s elapsed / ${rem.toFixed(1)}s remaining</td></tr><tr><td>Verified Cluster</td><td><b>${fmt(b.verified_hashrate_hs)}</b></td></tr><tr><td>Total Hashes</td><td>${fmtHash(b.verified_hashes_total)}</td></tr><tr><td>Valid Finds</td><td>${Number(b.valid_finds||0)}</td></tr>`;
let ps=b.participants||[];document.getElementById('parts').innerHTML=ps.map(p=>{let avg=Number(p.active_seconds||0)>0?Number(p.verified_hashes||0)/Number(p.active_seconds||0):0;return `<tr><td><b>${esc(p.name||p.worker_id)}</b><div class="small">${esc(p.gpu_device||'')}</div></td><td>${fmtHash(p.verified_hashes)}</td><td>${Number(p.active_seconds||0).toFixed(1)}s</td><td>${fmt(avg)}</td><td>${Number(p.share_percent||0).toFixed(2)}%</td></tr>`}).join('');
document.getElementById('finds').textContent=JSON.stringify(b.finds||[],null,2);document.getElementById('last').textContent=JSON.stringify(b.last_result||{},null,2)}
setInterval(tick,1000);tick();
</script></body></html>"""

BLOCKS_HTML = '<!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Blocks/Rounds</title><style>body{font-family:system-ui;background:#0b1020;color:#e8eefc;margin:20px;line-height:1.45}.card{background:#111936;border:1px solid #26345f;border-radius:14px;padding:14px;margin:12px 0}table{width:100%;border-collapse:collapse}td,th{padding:8px;border-bottom:1px solid #26345f;text-align:left}a{background:#2563eb;color:white;border:0;border-radius:10px;padding:10px 14px;text-decoration:none;margin-right:8px}.small{color:#9fb0d0;font-size:.9rem}pre{background:#0b1020;border-radius:10px;padding:10px;white-space:pre-wrap}</style></head><body><h1>Blocks & Rounds</h1><p><a href="/">Zurück</a><a href="/verify">Verify</a></p><div class="card"><h2>Aktuelle Runde</h2><div id="current"></div></div><div class="card"><h2>Gefundene Blöcke / abgeschlossene Runden</h2><div id="blocks"></div></div><script>function esc(x){return String(x??\'\').replace(/[&<>]/g,c=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\'}[c]))}function fmtHash(n){n=Number(n||0); if(n>1e12)return (n/1e12).toFixed(2)+\' TH\'; if(n>1e9)return (n/1e9).toFixed(2)+\' GH\'; if(n>1e6)return (n/1e6).toFixed(2)+\' MH\'; return n.toLocaleString()}function renderRound(r){if(!r)return \'<p>Keine aktive Runde.</p>\';let parts=Object.values(r.participants||{}).sort((a,b)=>Number(b.verified_hashes||0)-Number(a.verified_hashes||0));let rows=parts.map(p=>`<tr><td><b>${esc(p.name||p.worker_id)}</b><div class="small">${esc(p.worker_id)} · ${esc(p.gpu_device||\'\')}</div></td><td>${fmtHash(p.verified_hashes)}</td><td>${Number(p.active_seconds||0).toFixed(1)}s</td><td>${Number(p.share_percent||0).toFixed(2)}%</td><td>${Number(p.join_count||0)}</td><td>${Number(p.disconnects||0)}</td><td>${esc(p.last_status||\'\')}</td></tr>`).join(\'\');return `<p><b>Round:</b> ${esc(r.round_id)}<br><b>Height:</b> ${esc(r.height)} · <b>Start:</b> ${esc(r.started_at)} · <b>Ende:</b> ${esc(r.ended_at||\'läuft\')} · <b>Grund:</b> ${esc(r.end_reason||\'\')}</p><p><b>Total verified:</b> ${fmtHash(r.verified_hashes_total)}</p><table><thead><tr><th>Worker</th><th>Verified Hashes</th><th>Aktive Zeit</th><th>Anteil</th><th>Joins</th><th>Disconnects</th><th>Status</th></tr></thead><tbody>${rows}</tbody></table><details><summary>Events</summary><pre>${esc(JSON.stringify(r.events||[],null,2))}</pre></details>`}async function tick(){let d=await(await fetch(\'/api/rounds\')).json();document.getElementById(\'current\').innerHTML=renderRound(d.current_round);let blocks=(d.recent_blocks||[]).slice().reverse();document.getElementById(\'blocks\').innerHTML=blocks.length?blocks.map(renderRound).join(\'\'):\'<p>Noch keine gespeicherten Blockfunde.</p>\'}setInterval(tick,3000);tick()</script></body></html>'

REGTEST_HTML = r'''<!doctype html><html lang="de"><head><meta charset="utf-8"><title>Regtest</title><style>body{font-family:system-ui;background:#0b1020;color:#e8eefc;margin:20px;line-height:1.45}button,a{background:#2563eb;color:white;border:0;border-radius:10px;padding:10px 14px;text-decoration:none;margin-right:8px}pre{background:#111936;border:1px solid #26345f;border-radius:12px;padding:12px;white-space:pre-wrap}.card{background:#111936;border:1px solid #26345f;border-radius:14px;padding:14px;margin:12px 0}</style></head><body><h1>Regtest-Test</h1><p><a href="/">Zurück</a></p><div class="card"><h2>1. Bitcoin Core prüfen</h2><button onclick="rpcTest()">RPC testen</button><pre id="rpc"></pre></div><div class="card"><h2>2. Regtest-Adresse erzeugen</h2><p>Erzeugt über Bitcoin Core eine neue bech32-Adresse und speichert sie als <code>mining_address</code> in der Master-Config.</p><button onclick="newAddress()">Neue Regtest-Adresse setzen</button><pre id="addr"></pre></div><div class="card"><h2>3. Optional: Core-Testblock erzeugen</h2><p>Damit prüfst du, ob dein Regtest-Knoten grundsätzlich Blöcke erzeugen kann. Das ist unabhängig vom GPU-Miner.</p><button onclick="genBlock()">1 Block mit Bitcoin Core erzeugen</button><pre id="gen"></pre></div><div class="card"><h2>4. Miner-Test</h2><p>Danach auf der Hauptseite <b>Start</b> drücken. Auf Regtest sollte der Cluster sehr schnell einen echten gültigen Block finden und per <code>submitblock</code> einreichen.</p><pre>Aktuelle Config:
{{config}}</pre></div><div class="card"><h2>Start-Kommandos für bitcoind</h2><pre>bitcoind -regtest -daemon \
  -server=1 \
  -rpcuser=bitcoinrpc \
  -rpcpassword=change-me \
  -rpcbind=127.0.0.1 \
  -rpcallowip=127.0.0.1

# Master config.json:
&quot;rpc_url&quot;: &quot;http://127.0.0.1:18443&quot;,
&quot;rpc_user&quot;: &quot;bitcoinrpc&quot;,
&quot;rpc_pass&quot;: &quot;change-me&quot;</pre></div><script>async function show(id,p){let el=document.getElementById(id);try{let r=await p;el.textContent=JSON.stringify(await r.json(),null,2)}catch(e){el.textContent=String(e)}}function rpcTest(){show('rpc', fetch('/api/rpc_test'))}function newAddress(){show('addr', fetch('/api/regtest/newaddress',{method:'POST'}))}function genBlock(){show('gen', fetch('/api/regtest/generate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({blocks:1})}))}</script></body></html>'''

CONFIG_HTML = r'''<!doctype html><html lang="de"><head><meta charset="utf-8"><title>Config</title><style>body{font-family:system-ui;background:#0b1020;color:#e8eefc;margin:20px}textarea{width:100%;height:70vh;background:#111936;color:#e8eefc;border:1px solid #26345f;border-radius:12px;padding:12px}button,a{background:#2563eb;color:white;border:0;border-radius:10px;padding:10px 14px;text-decoration:none}</style></head><body><h1>Master config.json</h1><form method="post"><textarea name="config">{{config}}</textarea><br><br><button>Speichern</button> <a href="/">Zurück</a></form></body></html>'''

if __name__ == "__main__":
    c = cfg()
    threading.Thread(target=template_watcher, daemon=True).start()
    # Benchmark watchdog disabled in production release
    app.run(host=c.get("web_host", "0.0.0.0"), port=int(c.get("web_port", 8080)), threaded=True)
