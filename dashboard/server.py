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
GET/POST /api/pipeline-mode → inspect or change Kafka/CSV replay mode
GET /stream/live            → SSE stream of Kafka or explicitly enabled replay messages

Live data strategy
------------------
1. Try to connect to Kafka (configurable via KAFKA_BOOTSTRAP env var).
2. If Kafka is reachable → consume from 'air_quality_normalized' topic.
3. CSV replay remains inactive until enabled through /api/pipeline-mode.
"""

from __future__ import annotations

import csv
import json
import os
import queue
import random
import threading
import time
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

# How many SSE clients can queue before we drop messages
SSE_QUEUE_SIZE = 50

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
kafka_status   = {"connected": False, "checked_at": None, "broker": KAFKA_BOOTSTRAP}
_sse_queues: list[queue.Queue] = []
_sse_lock = threading.Lock()
_replay_active = False  # controlled by /api/pipeline-mode

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__, static_folder=str(BASE_DIR), static_url_path="")


# ── API helpers ─────────────────────────────────────────────────────────────

def _load_aq_csv() -> list[dict]:
    rows = []
    if not AQ_CSV_PATH.exists():
        return rows
    with open(AQ_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def _float_or_none(v: str) -> float | None:
    try:
        return float(v) if v.strip() else None
    except (ValueError, AttributeError):
        return None


# ── Routes ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(str(BASE_DIR), "index.html")


@app.route("/pipeline.html")
def pipeline():
    return send_from_directory(str(BASE_DIR), "pipeline.html")


@app.route("/api/status")
def api_status():
    return jsonify({
        "kafka": kafka_status,
        "server_time": datetime.now(timezone.utc).isoformat(),
        "data_source": "csv_replay" if _replay_active else ("kafka" if kafka_status["connected"] else "offline"),
        "pipeline_mode": "csv_replay" if _replay_active else "realtime",
        "aq_csv_exists": AQ_CSV_PATH.exists(),
        "weather_csv_exists": WEATHER_CSV_PATH.exists(),
    })


@app.route("/api/pipeline-mode", methods=["GET", "POST"])
def pipeline_mode():
    global _replay_active
    if request.method == "POST":
        data = request.get_json(force=True, silent=True) or {}
        mode = data.get("mode", "realtime")
        _replay_active = (mode == "csv_replay")
        print(f"[Mode] Pipeline mode → {'csv_replay' if _replay_active else 'realtime'}")
        return jsonify({"mode": "csv_replay" if _replay_active else "realtime", "ok": True})
    return jsonify({"mode": "csv_replay" if _replay_active else "realtime"})


@app.route("/api/stations")
def api_stations():
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
    return jsonify(result)


@app.route("/api/daily")
def api_daily():
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
    return jsonify(result)


@app.route("/api/hourly")
def api_hourly():
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
    return jsonify(result)


@app.route("/api/latest")
def api_latest():
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
    return jsonify(list(latest.values()))


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
    rows = []
    if not WEATHER_CSV_PATH.exists():
        return jsonify([])
    with open(WEATHER_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 48:
                break
            rows.append({
                "timestamp":   row.get("timestamp", ""),
                "temperature": _float_or_none(row.get("temperature", "")),
                "humidity":    _float_or_none(row.get("humidity", "")),
                "wind_speed":  _float_or_none(row.get("wind_speed", "")),
                "pressure":    _float_or_none(row.get("pressure", "")),
            })
    return jsonify(rows)


# ── SSE ─────────────────────────────────────────────────────────────────────

def _broadcast(event: str, data: dict):
    """Push an SSE event to all connected clients."""
    msg = f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    dead = []
    with _sse_lock:
        for q in _sse_queues:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _sse_queues.remove(q)


def _sse_generator(q: queue.Queue) -> Iterator[str]:
    # Send connection confirmation
    source = "csv_replay" if _replay_active else ("kafka" if kafka_status["connected"] else "offline")
    yield f"event: connected\ndata: {json.dumps({'broker': KAFKA_BOOTSTRAP, 'source': source})}\n\n"
    try:
        while True:
            try:
                msg = q.get(timeout=25)
                yield msg
            except queue.Empty:
                # Keep-alive ping
                yield ": ping\n\n"
    except GeneratorExit:
        with _sse_lock:
            if q in _sse_queues:
                _sse_queues.remove(q)


@app.route("/stream/live")
def stream_live():
    q: queue.Queue = queue.Queue(maxsize=SSE_QUEUE_SIZE)
    with _sse_lock:
        _sse_queues.append(q)

    return Response(
        _sse_generator(q),
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
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable, KafkaError

    print(f"[Kafka] Attempting to connect to {KAFKA_BOOTSTRAP}...")
    msg_count = 0

    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="dashboard-sync-consumer",
                auto_offset_reset="latest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=2000,
                request_timeout_ms=10000,
            )
            kafka_status["connected"] = True
            kafka_status["checked_at"] = datetime.now(timezone.utc).isoformat()
            print(f"[Kafka] Connected to {KAFKA_BOOTSTRAP}, topic: {KAFKA_TOPIC}")

            _broadcast("status", {"type": "kafka_connected", "broker": KAFKA_BOOTSTRAP})

            for msg in consumer:
                if _replay_active:
                    continue  # replay mode active — discard Kafka messages
                data = msg.value
                data["_kafka_offset"] = msg.offset
                data["_kafka_partition"] = msg.partition
                data["_received_at"] = datetime.now(timezone.utc).isoformat()
                data["_source_mode"] = "kafka"
                msg_count += 1
                _broadcast("reading", data)

        except (NoBrokersAvailable, KafkaError, Exception) as e:
            kafka_status["connected"] = False
            kafka_status["checked_at"] = datetime.now(timezone.utc).isoformat()
            print(f"[Kafka] Not available ({type(e).__name__}). Replay remains inactive.")
            _broadcast("status", {"type": "kafka_offline", "reason": str(e)[:100]})
            time.sleep(30)  # retry every 30s


def _csv_replay_thread():
    """Replay CSV rows only while _replay_active is enabled through the API."""
    print("[Replay] CSV replay thread started (inactive until toggled on).")
    rows = _load_aq_csv()
    if not rows:
        print("[Replay] No CSV data found, replay unavailable.")
        return

    idx = 0
    msg_count = 0
    while True:
        if not _replay_active:
            time.sleep(2)
            continue

        row = rows[idx % len(rows)]
        idx += 1

        # Slightly randomize values to make it look live
        def jitter(v, pct=0.05):
            if v is None:
                return None
            return round(float(v) * (1 + random.uniform(-pct, pct)), 2)

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
        }
        msg_count += 1

        _broadcast("reading", data)

        # Broadcast a synthetic "weather" reading every 20 messages
        if msg_count % 20 == 0 and WEATHER_CSV_PATH.exists():
            with open(WEATHER_CSV_PATH, newline="", encoding="utf-8") as f:
                wreader = csv.DictReader(f)
                wrows = list(wreader)
            if wrows:
                wr = wrows[msg_count % len(wrows)]
                _broadcast("weather", {
                    "timestamp":   datetime.now(timezone.utc).isoformat(),
                    "temperature": _float_or_none(wr.get("temperature", "")),
                    "humidity":    _float_or_none(wr.get("humidity", "")),
                    "wind_speed":  _float_or_none(wr.get("wind_speed", "")),
                    "pressure":    _float_or_none(wr.get("pressure", "")),
                    "_source_mode": "csv_replay",
                })

        time.sleep(0.6)  # ~1.7 messages/second — realistic feel


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
