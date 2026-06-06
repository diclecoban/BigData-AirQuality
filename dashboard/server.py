"""
Istanbul Air Quality Dashboard — Backend Server
===============================================
Flask API + Server-Sent Events (SSE) bridge between the pipeline and the browser.

Endpoints
---------
GET /                       → serves index.html
GET /api/stations           → station averages (from CSV)
GET /api/daily              → daily pollutant averages
GET /api/hourly             → 24-h diurnal AQI pattern
GET /api/latest             → latest reading per station
GET /api/models             → ML model performance metrics
GET /api/weather            → recent weather data
GET /api/status             → pipeline connection status
GET/POST /api/pipeline-mode → validate a requested client stream mode
GET /stream/live?mode=...   → SSE stream scoped to one client-selected mode

Live data strategy
------------------
1. Try to connect to Kafka (configurable via KAFKA_BOOTSTRAP env var).
2. If Kafka is reachable → consume from 'air_quality_normalized' topic.
3. CSV replay is generated only while at least one replay client is connected.
"""

from __future__ import annotations

import csv
import json
import os
import queue
import random
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from flask import Flask, Response, jsonify, send_from_directory, request

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent        # dashboard/

# DATA_ROOT: in Docker the volume is mounted at /app/data;
# locally (running server.py directly) it's ../data relative to the script.
_docker_data = Path("/app/data")
DATA_ROOT   = _docker_data if _docker_data.exists() else BASE_DIR.parent / "data"
RAW_DIR    = DATA_ROOT / "raw"
REPORT_DIR = DATA_ROOT / "reports"

AQ_CSV_PATH      = RAW_DIR / "airquality_historical.csv"
WEATHER_CSV_PATH = RAW_DIR / "weather_historical.csv"
MODELS_JSON_PATH = REPORT_DIR / "evaluation_summary.csv"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "air_quality_normalized")
WEATHER_TOPIC   = os.getenv("WEATHER_TOPIC", "weather_normalized")
PORT            = int(os.getenv("DASHBOARD_PORT", "8766"))
IN_DOCKER       = _docker_data.exists()
SPARK_STATUS_URL = os.getenv(
    "SPARK_STATUS_URL", "http://spark-master:8080" if IN_DOCKER else "http://localhost:8080"
)
MLFLOW_STATUS_URL = os.getenv(
    "MLFLOW_STATUS_URL", "http://mlflow:5000" if IN_DOCKER else "http://localhost:5001"
)

# How many SSE clients can queue before we drop messages
SSE_QUEUE_SIZE = 50

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
kafka_status   = {"connected": False, "checked_at": None, "broker": KAFKA_BOOTSTRAP}
_sse_clients: list[dict] = []
_sse_lock = threading.Lock()
_service_cache = {}
_service_cache_lock = threading.Lock()
_aq_cache = {"mtime": None, "rows": []}
_aq_cache_lock = threading.Lock()
_weather_cache = {"mtime": None, "rows": []}
_weather_cache_lock = threading.Lock()
_analytics_cache = {}
_analytics_cache_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__, static_folder=str(BASE_DIR), static_url_path="")


# ── API helpers ─────────────────────────────────────────────────────────────

def _load_aq_csv() -> list[dict]:
    if not AQ_CSV_PATH.exists():
        return []
    mtime = AQ_CSV_PATH.stat().st_mtime_ns
    with _aq_cache_lock:
        if _aq_cache["mtime"] != mtime:
            with open(AQ_CSV_PATH, newline="", encoding="utf-8") as f:
                _aq_cache["rows"] = list(csv.DictReader(f))
            _aq_cache["mtime"] = mtime
        return _aq_cache["rows"]


def _load_weather_csv() -> list[dict]:
    if not WEATHER_CSV_PATH.exists():
        return []
    mtime = WEATHER_CSV_PATH.stat().st_mtime_ns
    with _weather_cache_lock:
        if _weather_cache["mtime"] != mtime:
            with open(WEATHER_CSV_PATH, newline="", encoding="utf-8") as f:
                _weather_cache["rows"] = list(csv.DictReader(f))
            _weather_cache["mtime"] = mtime
        return _weather_cache["rows"]


def _get_cached_analytics(name: str):
    if not AQ_CSV_PATH.exists():
        return None
    mtime = AQ_CSV_PATH.stat().st_mtime_ns
    with _analytics_cache_lock:
        entry = _analytics_cache.get(name)
        return entry["value"] if entry and entry["mtime"] == mtime else None


def _set_cached_analytics(name: str, value):
    if AQ_CSV_PATH.exists():
        with _analytics_cache_lock:
            _analytics_cache[name] = {"mtime": AQ_CSV_PATH.stat().st_mtime_ns, "value": value}
    return value


def _has_replay_clients() -> bool:
    with _sse_lock:
        return any(client["mode"] == "csv_replay" for client in _sse_clients)


def _replay_client_count() -> int:
    with _sse_lock:
        return sum(client["mode"] == "csv_replay" for client in _sse_clients)


def _float_or_none(v: str) -> float | None:
    try:
        return float(v) if v.strip() else None
    except (ValueError, AttributeError):
        return None


def _http_reachable(url: str, timeout: float = 2.0) -> bool:
    """Return True if the URL responds (any HTTP status counts as reachable)."""
    try:
        req = urllib.request.Request(url, method="HEAD")
        urllib.request.urlopen(req, timeout=timeout)
        return True
    except urllib.error.HTTPError:
        return True  # Got an HTTP response → service is running
    except Exception:
        return False


def _cached_reachable(name: str, url: str, ttl: float = 30.0) -> bool:
    """Check URL reachability, caching the result for `ttl` seconds."""
    now = time.time()
    with _service_cache_lock:
        entry = _service_cache.get(name)
        if entry is not None and now - entry["ts"] < ttl:
            return entry["ok"]
    ok = _http_reachable(url)
    with _service_cache_lock:
        _service_cache[name] = {"ok": ok, "ts": time.time()}
    return ok


# ── Routes ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(str(BASE_DIR), "index.html")


@app.route("/pipeline.html")
def pipeline():
    return send_from_directory(str(BASE_DIR), "pipeline.html")


@app.route("/api/status")
def api_status():
    spark_ok = _cached_reachable("spark", SPARK_STATUS_URL)
    mlflow_ok = _cached_reachable("mlflow", MLFLOW_STATUS_URL)
    return jsonify({
        "kafka": kafka_status,
        "server_time": datetime.now(timezone.utc).isoformat(),
        "data_source": "kafka" if kafka_status["connected"] else "offline",
        "replay_clients": _replay_client_count(),
        "aq_csv_exists": AQ_CSV_PATH.exists(),
        "weather_csv_exists": WEATHER_CSV_PATH.exists(),
        "spark_reachable": spark_ok,
        "mlflow_reachable": mlflow_ok,
    })


@app.route("/api/pipeline-mode", methods=["GET", "POST"])
def pipeline_mode():
    if request.method == "POST":
        data = request.get_json(force=True, silent=True) or {}
        mode = data.get("mode")
        if mode not in {"csv_replay", "realtime"}:
            return jsonify({"ok": False, "error": "mode must be csv_replay or realtime"}), 400
        return jsonify({"mode": mode, "ok": True, "scope": "client"})
    return jsonify({"modes": ["csv_replay", "realtime"], "scope": "client"})


@app.route("/api/stations")
def api_stations():
    cached = _get_cached_analytics("stations")
    if cached is not None:
        return jsonify(cached)
    rows = _load_aq_csv()
    agg: dict[str, dict] = {}
    for r in rows:
        name = r.get("station_name", "").strip()
        if not name:
            continue
        if name not in agg:
            agg[name] = {
                "name": name,
                "district": r.get("district", "").replace(" - Turkey", "").strip(),
                "lat": _float_or_none(r.get("latitude", "")),
                "lon": _float_or_none(r.get("longitude", "")),
                "aqi_vals": [], "pm10_vals": [], "no2_vals": [],
            }
        for field, key in [("aqi", "aqi_vals"), ("pm10", "pm10_vals"), ("no2", "no2_vals")]:
            v = _float_or_none(r.get(field, ""))
            if v is not None:
                agg[name][key].append(v)

    result = []
    for name, d in agg.items():
        def avg(lst):
            return round(sum(lst) / len(lst), 2) if lst else None
        result.append({
            "name": name,
            "district": d["district"],
            "lat": d["lat"],
            "lon": d["lon"],
            "aqi": avg(d["aqi_vals"]),
            "pm10": avg(d["pm10_vals"]),
            "no2": avg(d["no2_vals"]),
        })

    result.sort(key=lambda x: (x["aqi"] or 0), reverse=True)
    return jsonify(_set_cached_analytics("stations", result))


@app.route("/api/daily")
def api_daily():
    cached = _get_cached_analytics("daily")
    if cached is not None:
        return jsonify(cached)
    rows = _load_aq_csv()
    daily: dict[str, dict] = {}
    for r in rows:
        day = r.get("timestamp", "")[:10]
        if not day:
            continue
        if day not in daily:
            daily[day] = {"pm10": [], "no2": [], "so2": [], "o3": [], "aqi": []}
        for field in ["pm10", "no2", "so2", "o3", "aqi"]:
            v = _float_or_none(r.get(field, ""))
            if v is not None:
                daily[day][field].append(v)

    result = {}
    for day, d in sorted(daily.items()):
        result[day] = {k: round(sum(v) / len(v), 2) if v else None for k, v in d.items()}
    return jsonify(_set_cached_analytics("daily", result))


@app.route("/api/hourly")
def api_hourly():
    cached = _get_cached_analytics("hourly")
    if cached is not None:
        return jsonify(cached)
    rows = _load_aq_csv()
    hourly: dict[int, list] = {h: [] for h in range(24)}
    for r in rows:
        ts = r.get("timestamp", "")
        try:
            hour = int(ts[11:13]) if len(ts) > 12 else 0
        except ValueError:
            continue
        v = _float_or_none(r.get("aqi", ""))
        if v is not None:
            hourly[hour].append(v)

    result = {str(h): round(sum(v) / len(v), 2) if v else 0 for h, v in sorted(hourly.items())}
    return jsonify(_set_cached_analytics("hourly", result))


@app.route("/api/latest")
def api_latest():
    cached = _get_cached_analytics("latest")
    if cached is not None:
        return jsonify(cached)
    rows = _load_aq_csv()
    latest: dict[str, dict] = {}
    for r in rows:
        name = r.get("station_name", "").strip()
        ts   = r.get("timestamp", "")
        if name and (name not in latest or ts > latest[name]["timestamp"]):
            latest[name] = {
                "station": name,
                "district": r.get("district", "").replace(" - Turkey", "").strip(),
                "timestamp": ts,
                "aqi":  _float_or_none(r.get("aqi",  "")),
                "pm10": _float_or_none(r.get("pm10", "")),
                "pm25": _float_or_none(r.get("pm25", "")),
                "no2":  _float_or_none(r.get("no2",  "")),
                "so2":  _float_or_none(r.get("so2",  "")),
                "o3":   _float_or_none(r.get("o3",   "")),
                "lat":  _float_or_none(r.get("latitude",  "")),
                "lon":  _float_or_none(r.get("longitude", "")),
                "source": r.get("source", "ibb"),
            }
    return jsonify(_set_cached_analytics("latest", list(latest.values())))


@app.route("/api/models")
def api_models():
    """Return only metrics produced by an evaluation run.

    No fallback metrics are embedded here: an absent report means the models
    have not been evaluated in the current deployment.
    """
    if not MODELS_JSON_PATH.exists():
        return jsonify([])

    models = []
    with open(MODELS_JSON_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                model_id = row["model"]
                models.append({
                    "name": model_id.replace("baseline_", "").replace("_", " ").title(),
                    "model_id": model_id,
                    "rmse": float(row["rmse"]),
                    "mae": float(row["mae"]),
                    "r2": float(row["r2"]),
                    "best": False,
                })
            except (KeyError, TypeError, ValueError):
                continue

    if models:
        min(models, key=lambda model: model["rmse"])["best"] = True
    return jsonify(models)


@app.route("/api/weather")
def api_weather():
    return jsonify([{
        "timestamp":   row.get("timestamp", ""),
        "temperature": _float_or_none(row.get("temperature", "")),
        "humidity":    _float_or_none(row.get("humidity", "")),
        "wind_speed":  _float_or_none(row.get("wind_speed", "")),
        "pressure":    _float_or_none(row.get("pressure", "")),
    } for row in _load_weather_csv()[:48]])


# ── SSE ─────────────────────────────────────────────────────────────────────

def _broadcast(event: str, data: dict, mode: str | None = None):
    """Push an SSE event to matching clients, or all clients when mode is None."""
    msg = f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    with _sse_lock:
        for client in _sse_clients:
            if mode is not None and client["mode"] != mode:
                continue
            try:
                client["queue"].put_nowait(msg)
            except queue.Full:
                # Keep the connection alive and favor the newest state.
                try:
                    client["queue"].get_nowait()
                    client["queue"].put_nowait(msg)
                except (queue.Empty, queue.Full):
                    pass


def _sse_generator(client: dict) -> Iterator[str]:
    # Send connection confirmation
    source = client["mode"]
    if source == "realtime" and not kafka_status["connected"]:
        source = "offline"
    elif source == "csv_replay" and not AQ_CSV_PATH.exists():
        source = "offline"
    yield f"event: connected\ndata: {json.dumps({'broker': KAFKA_BOOTSTRAP, 'source': source})}\n\n"
    try:
        while True:
            try:
                msg = client["queue"].get(timeout=25)
                yield msg
            except queue.Empty:
                # Keep-alive ping
                yield ": ping\n\n"
    except GeneratorExit:
        with _sse_lock:
            if client in _sse_clients:
                _sse_clients.remove(client)


@app.route("/stream/live")
def stream_live():
    mode = request.args.get("mode", "realtime")
    if mode not in {"csv_replay", "realtime"}:
        return jsonify({"error": "mode must be csv_replay or realtime"}), 400
    client = {"queue": queue.Queue(maxsize=SSE_QUEUE_SIZE), "mode": mode}
    with _sse_lock:
        _sse_clients.append(client)

    return Response(
        _sse_generator(client),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )


# ── Kafka consumer thread ────────────────────────────────────────────────────

def _try_kafka_connection() -> bool:
    try:
        from kafka import KafkaConsumer
        from kafka.errors import NoBrokersAvailable
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=5000,
            connections_max_idle_ms=10000,
        )
        consumer.close()
        return True
    except Exception:
        return False


def _kafka_consumer_thread():
    """Long-running thread: connect to Kafka and forward messages as SSE events."""
    try:
        from kafka import KafkaConsumer
        from kafka.errors import NoBrokersAvailable, KafkaError
    except ImportError:
        kafka_status["connected"] = False
        kafka_status["checked_at"] = datetime.now(timezone.utc).isoformat()
        print("[Kafka] kafka-python is not installed; realtime stream is unavailable.")
        _broadcast("status", {"type": "kafka_offline", "reason": "kafka-python is not installed"})
        return

    print(f"[Kafka] Attempting to connect to {KAFKA_BOOTSTRAP}...")
    msg_count = 0

    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="dashboard-sync-consumer",
                auto_offset_reset="latest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                request_timeout_ms=10000,
            )
            kafka_status["connected"] = True
            kafka_status["checked_at"] = datetime.now(timezone.utc).isoformat()
            print(f"[Kafka] Connected to {KAFKA_BOOTSTRAP}, topic: {KAFKA_TOPIC}")

            _broadcast("status", {"type": "kafka_connected", "broker": KAFKA_BOOTSTRAP})

            for msg in consumer:
                data = msg.value
                data["_kafka_offset"] = msg.offset
                data["_kafka_partition"] = msg.partition
                data["_received_at"] = datetime.now(timezone.utc).isoformat()
                data["_source_mode"] = "kafka"
                msg_count += 1
                _broadcast("reading", data, mode="realtime")
        except (NoBrokersAvailable, KafkaError, Exception) as e:
            kafka_status["connected"] = False
            kafka_status["checked_at"] = datetime.now(timezone.utc).isoformat()
            print(f"[Kafka] Not available ({type(e).__name__}). Replay remains inactive.")
            _broadcast("status", {"type": "kafka_offline", "reason": str(e)[:100]})
            time.sleep(30)  # retry every 30s
        finally:
            if consumer is not None:
                consumer.close()


def _csv_replay_thread():
    """Replay one same-time snapshot containing every station per interval."""
    print("[Replay] CSV replay thread started (inactive until a replay client connects).")
    snapshot_idx = 0
    msg_count = 0
    rows_mtime = None
    station_rows: dict[str, list[dict]] = {}
    while True:
        if not _has_replay_clients():
            time.sleep(2)
            continue

        rows = _load_aq_csv()
        if not rows:
            time.sleep(2)
            continue

        current_mtime = AQ_CSV_PATH.stat().st_mtime_ns
        if rows_mtime != current_mtime:
            station_rows = {}
            for row in rows:
                name = row.get("station_name", "").strip()
                if name:
                    station_rows.setdefault(name, []).append(row)
            rows_mtime = current_mtime
            snapshot_idx = 0

        snapshot = [
            station[snapshot_idx % len(station)]
            for station in station_rows.values()
            if station
        ]
        snapshot_idx += 1

        # Slightly randomize values to make it look live
        def jitter(v, pct=0.05):
            if v is None:
                return None
            return round(float(v) * (1 + random.uniform(-pct, pct)), 2)

        for row in snapshot:
            data = {
                "station_id":   row.get("station_id", ""),
                "station_name": row.get("station_name", "").strip(),
                "district":     row.get("district", "").replace(" - Turkey", "").strip(),
                "source":       row.get("source", "ibb"),
                "timestamp":    datetime.now(timezone.utc).isoformat(),
                "latitude":     _float_or_none(row.get("latitude", "")),
                "longitude":    _float_or_none(row.get("longitude", "")),
                "pm10":         jitter(_float_or_none(row.get("pm10", ""))),
                "pm25":         jitter(_float_or_none(row.get("pm25", ""))),
                "no2":          jitter(_float_or_none(row.get("no2",  ""))),
                "so2":          jitter(_float_or_none(row.get("so2",  ""))),
                "o3":           jitter(_float_or_none(row.get("o3",   ""))),
                "aqi":          jitter(_float_or_none(row.get("aqi",  ""))),
                "_source_mode": "csv_replay",
                "_seq": msg_count,
                "_snapshot": snapshot_idx - 1,
            }
            msg_count += 1
            _broadcast("reading", data, mode="csv_replay")

        # Broadcast a synthetic "weather" reading every 20 messages
        if snapshot_idx % 20 == 0:
            wrows = _load_weather_csv()
            if wrows:
                wr = wrows[snapshot_idx % len(wrows)]
                _broadcast("weather", {
                    "timestamp":   datetime.now(timezone.utc).isoformat(),
                    "temperature": _float_or_none(wr.get("temperature", "")),
                    "humidity":    _float_or_none(wr.get("humidity", "")),
                    "wind_speed":  _float_or_none(wr.get("wind_speed", "")),
                    "pressure":    _float_or_none(wr.get("pressure", "")),
                    "_source_mode": "csv_replay",
                }, mode="csv_replay")

        time.sleep(1.0)


# ── Startup ─────────────────────────────────────────────────────────────────

def _start_background_threads():
    connected = _try_kafka_connection()
    kafka_status["connected"] = connected
    kafka_status["checked_at"] = datetime.now(timezone.utc).isoformat()

    # Always start both threads; each self-manages based on mode
    t = threading.Thread(target=_kafka_consumer_thread, daemon=True)
    t.start()
    t2 = threading.Thread(target=_csv_replay_thread, daemon=True)
    t2.start()
    print(f"[Server] Started. Kafka: {'connected' if connected else 'unavailable'}. Replay: inactive.")


if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════════════════╗
║  Istanbul Air Quality Dashboard Server               ║
║  http://localhost:{PORT}                              ║
║  Kafka: {KAFKA_BOOTSTRAP:<38}      ║
╚══════════════════════════════════════════════════════╝
""")
    _start_background_threads()
    app.run(host="0.0.0.0", port=PORT, threaded=True, debug=False)
