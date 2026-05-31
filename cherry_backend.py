"""
Cherry Rewards — Backend Server for Render.com
pip install flask flask-cors paho-mqtt psycopg2-binary python-dotenv
"""

import os, json, threading, time, logging
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import paho.mqtt.client as mqtt

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
MQTT_BROKER   = os.getenv("MQTT_BROKER",   "broker.hivemq.com")
MQTT_PORT     = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER     = os.getenv("MQTT_USER",     "")
MQTT_PASS     = os.getenv("MQTT_PASS",     "")
MQTT_CLIENT_ID= os.getenv("MQTT_CLIENT_ID", f"cherry_backend_{int(time.time())}")
DATABASE_URL  = os.getenv("DATABASE_URL",  "")   # Render Postgres internal URL

# Topics
TOPIC_QR_SCAN    = "cherry/qr_scan"
TOPIC_LOGIN      = "cherry/login"
TOPIC_REWARDS    = "user_id/rewards"
TOPIC_EMAIL_QR   = "111aabc/email"

# ─── DATABASE ────────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def init_db():
    """Create tables if they don't exist."""
    if not DATABASE_URL:
        log.warning("DATABASE_URL not set — skipping DB init")
        return
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS geotags (
                        id      SERIAL PRIMARY KEY,
                        name    TEXT NOT NULL,
                        lat     DOUBLE PRECISION NOT NULL,
                        lng     DOUBLE PRECISION NOT NULL
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id          SERIAL PRIMARY KEY,
                        email       TEXT UNIQUE NOT NULL,
                        pin_hash    TEXT,
                        user_id     TEXT UNIQUE,
                        created_at  TIMESTAMP DEFAULT NOW()
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS rewards (
                        id          SERIAL PRIMARY KEY,
                        user_id     TEXT NOT NULL,
                        title       TEXT,
                        description TEXT,
                        expires_at  TEXT,
                        active      BOOLEAN DEFAULT TRUE
                    );
                """)
                conn.commit()
        log.info("Database tables ready.")
    except Exception as e:
        log.error(f"DB init error: {e}")


def seed_geotags_from_file(filepath="geotags.txt"):
    """
    Load geotags from a text file and insert into the DB.
    File format (one per line):  lat, lng, Store Name
    Example:
        -36.8485, 174.7633, Cherry – City Centre
        -36.8600, 174.7500, Cherry – Westfield
    """
    if not os.path.exists(filepath):
        log.info(f"Geotag file '{filepath}' not found — skipping seed.")
        return
    if not DATABASE_URL:
        log.warning("DATABASE_URL not set — cannot seed geotags")
        return
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                with open(filepath) as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        parts = [p.strip() for p in line.split(",", 2)]
                        if len(parts) < 3:
                            continue
                        lat, lng, name = float(parts[0]), float(parts[1]), parts[2]
                        cur.execute("""
                            INSERT INTO geotags (name, lat, lng)
                            VALUES (%s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """, (name, lat, lng))
                conn.commit()
        log.info("Geotags seeded from file.")
    except Exception as e:
        log.error(f"Geotag seed error: {e}")


# ─── GEOTAG ENDPOINT ─────────────────────────────────────────────────────────
@app.route("/geotags")
def geotags():
    """
    Returns stores within `radius` metres of (lat, lng).
    Query params: lat, lng, radius (default 8000 m)
    """
    try:
        user_lat = float(request.args.get("lat", 0))
        user_lng = float(request.args.get("lng", 0))
        radius_m = float(request.args.get("radius", 8000))
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid parameters"}), 400

    if not DATABASE_URL:
        # Return demo data when no DB
        return jsonify([
            {"lat": user_lat + 0.012, "lng": user_lng - 0.008, "name": "Cherry – City Centre"},
            {"lat": user_lat - 0.015, "lng": user_lng + 0.020, "name": "Cherry – Westfield"},
            {"lat": user_lat + 0.025, "lng": user_lng + 0.015, "name": "Cherry – Harbour View"},
        ])

    try:
        radius_deg = radius_m / 111_000          # rough degree conversion
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT name, lat, lng,
                           (6371000 * acos(
                               cos(radians(%s)) * cos(radians(lat)) *
                               cos(radians(lng) - radians(%s)) +
                               sin(radians(%s)) * sin(radians(lat))
                           )) AS distance_m
                    FROM geotags
                    WHERE lat BETWEEN %s AND %s
                      AND lng BETWEEN %s AND %s
                    HAVING (6371000 * acos(
                               cos(radians(%s)) * cos(radians(lat)) *
                               cos(radians(lng) - radians(%s)) +
                               sin(radians(%s)) * sin(radians(lat))
                           )) <= %s
                    ORDER BY distance_m
                    LIMIT 50;
                """, (
                    user_lat, user_lng, user_lat,
                    user_lat - radius_deg, user_lat + radius_deg,
                    user_lng - radius_deg, user_lng + radius_deg,
                    user_lat, user_lng, user_lat,
                    radius_m
                ))
                rows = cur.fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        log.error(f"Geotag query error: {e}")
        return jsonify({"error": "Database error"}), 500


# ─── HEALTH ──────────────────────────────────────────────────────────────────
@app.route("/")
def health():
    return jsonify({"status": "ok", "service": "Cherry Rewards Backend"})


# ─── MQTT CLIENT ─────────────────────────────────────────────────────────────
mqtt_client: mqtt.Client = None


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("MQTT connected")
        client.subscribe(TOPIC_QR_SCAN)
        client.subscribe(TOPIC_LOGIN)
    else:
        log.warning(f"MQTT connection failed rc={rc}")


def on_message(client, userdata, msg):
    topic   = msg.topic
    payload = msg.payload.decode("utf-8", errors="replace")
    log.info(f"MQTT ← {topic}: {payload[:120]}")

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        data = {"raw": payload}

    if topic == TOPIC_QR_SCAN:
        handle_qr_scan(data)
    elif topic == TOPIC_LOGIN:
        handle_login(data)


def handle_qr_scan(data: dict):
    """
    Received a scanned QR code from the frontend.
    Publishes decoded reward back to the app if we recognise the user.
    """
    qr_data = data.get("data", "")
    user_email = data.get("user", "anonymous")
    log.info(f"QR scan: user={user_email}  data={qr_data[:80]}")

    # Example: lookup reward for the scanned code
    reward = lookup_reward_for_qr(qr_data)
    if reward:
        mqtt_client.publish(TOPIC_REWARDS, json.dumps(reward))
        log.info(f"Published reward to {TOPIC_REWARDS}: {reward}")


def handle_login(data: dict):
    """
    Received a login/signup event from the frontend.
    Publishes user_id back so the QR card updates.
    """
    email = data.get("email", "")
    mode  = data.get("mode", "signin")
    log.info(f"Login event: mode={mode} email={email}")

    if not email:
        return

    user_id = derive_user_id(email)
    mqtt_client.publish(TOPIC_EMAIL_QR, json.dumps({"user_id": user_id, "email": email}))
    log.info(f"Published user_id={user_id} to {TOPIC_EMAIL_QR}")


# ─── HELPERS ─────────────────────────────────────────────────────────────────
def derive_user_id(email: str) -> str:
    """Deterministic short user ID from email (demo; use a DB lookup in production)."""
    import hashlib
    h = hashlib.sha256(email.lower().encode()).hexdigest()[:8].upper()
    return f"CHR-{h}"


def lookup_reward_for_qr(qr_data: str) -> dict | None:
    """
    Look up a reward from the DB (or return a demo reward).
    In production, parse qr_data as a JWT / voucher code and query rewards table.
    """
    # Demo: always return a reward
    return {
        "title":   "Free Espresso Shot",
        "desc":    "One complimentary espresso with any beverage purchase.",
        "exp":     "30 Jun 2026",
        "source":  qr_data[:40],
    }


def start_mqtt():
    global mqtt_client
    mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID, protocol=mqtt.MQTTv311)
    if MQTT_USER:
        mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    def _run():
        while True:
            try:
                mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
                mqtt_client.loop_forever()
            except Exception as e:
                log.error(f"MQTT error: {e} — retrying in 5s")
                time.sleep(5)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    log.info(f"MQTT thread started → {MQTT_BROKER}:{MQTT_PORT}")


# ─── STARTUP ─────────────────────────────────────────────────────────────────
with app.app_context():
    init_db()
    seed_geotags_from_file("geotags.txt")
    start_mqtt()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
