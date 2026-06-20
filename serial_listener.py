import serial
import json
import threading
import time
import uuid
import mysql.connector
import requests
import paho.mqtt.client as mqtt

SERIAL_PORT = "/dev/ttyUSB0"
BAUD_RATE   = 9600
FASTAPI_URL = "http://localhost:8000"
DB_CONFIG   = {
    "host":     "localhost",
    "user":     "admin",
    "password": "password",
    "database": "traffic_db"
}

# ── ThingsBoard MQTT ─────────────────────────────────────────────────
TB_HOST  = "mqtt.thingsboard.cloud"
TB_PORT  = 8883
TB_TOKEN = "zWjEDFArSqXqjSnOiNbu"
TB_TOPIC = "v1/devices/me/telemetry"
# ─────────────────────────────────────────────────────────────────────

# ── OpenWeatherMap ───────────────────────────────────────────────────
OWM_API_KEY     = "eb3a6fe66d217605ab08dd63e21514b2"
OWM_LAT         = 3.1390
OWM_LON         = 101.6869
_cached_weather = {"condition": "Clear", "fetched_at": 0}
# ─────────────────────────────────────────────────────────────────────

http_session          = requests.Session()
_last_broadcast_t     = 0.0
_last_arduino_t       = 0.0
_mqtt_publish_counter = 0

def get_db():
    return mysql.connector.connect(**DB_CONFIG)

# ── MQTT ─────────────────────────────────────────────────────────────

def connect_mqtt():
    client = mqtt.Client(client_id=f"traffic-{uuid.uuid4().hex[:8]}")
    client.username_pw_set(TB_TOKEN)
    client.tls_set()

    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            print("[MQTT] Connected to ThingsBoard")
        else:
            print(f"[MQTT] Connection failed rc={rc}")

    def on_disconnect(c, userdata, rc):
        if rc != 0:
            print(f"[MQTT] Disconnected rc={rc}, will auto-reconnect...")

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=0, max_delay=60)
    client.connect(TB_HOST, TB_PORT, keepalive=1200)
    client.loop_start()
    return client

def publish_state(mqtt_client, state):
    try:
        payload = {}
        for lane_data in state:
            lane = lane_data.get("lane", 0)
            payload[f"lane{lane}_phase"]         = lane_data.get("phase", "")
            payload[f"lane{lane}_remaining"]     = lane_data.get("remaining", 0)
            payload[f"lane{lane}_vehicle_count"] = lane_data.get("vehicle_count", 0)
            payload[f"lane{lane}_ped_status"]    = lane_data.get("ped_status", "IDLE")
            payload[f"lane{lane}_ped_countdown"] = lane_data.get("ped_countdown", 0)
            payload[f"lane{lane}_ped_waiting"]   = lane_data.get("ped_waiting", 0)
        mqtt_client.publish(TB_TOPIC, json.dumps(payload))
    except Exception as e:
        print(f"[MQTT ERROR] {e}")

def mqtt_keepalive(mqtt_client):
    while True:
        time.sleep(30)
        try:
            mqtt_client.publish(TB_TOPIC, json.dumps({"keepalive": 1}))
        except Exception as e:
            print(f"[MQTT KEEPALIVE ERROR] {e}")

# ── Weather ──────────────────────────────────────────────────────────

def get_weather_condition():
    global _cached_weather
    now = time.time()
    if now - _cached_weather["fetched_at"] < 600:
        return _cached_weather["condition"]
    try:
        url  = (
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?lat={OWM_LAT}&lon={OWM_LON}&appid={OWM_API_KEY}"
        )
        res       = http_session.get(url, timeout=5)
        data      = res.json()
        condition = data["weather"][0]["main"]
        _cached_weather = {"condition": condition, "fetched_at": now}
        print(f"[WEATHER] {condition}")
        return condition
    except Exception as e:
        print(f"[WEATHER ERROR] {e}")
        return _cached_weather["condition"]

# ── State helpers ────────────────────────────────────────────────────

def get_current_state(cursor):
    cursor.execute("SELECT * FROM realtime_state ORDER BY lane")
    columns = [desc[0] for desc in cursor.description]
    rows    = cursor.fetchall()
    state   = []
    for row in rows:
        d = dict(zip(columns, row))
        for k, v in d.items():
            if hasattr(v, 'isoformat'):
                d[k] = v.isoformat()
            elif isinstance(v, bytearray):
                d[k] = int.from_bytes(v, 'little')
        state.append(d)
    return state

def broadcast_async(state):
    global _last_broadcast_t
    _last_broadcast_t = time.time()
    def _do():
        try:
            http_session.post(
                f"{FASTAPI_URL}/internal/broadcast",
                json={"type": "state", "data": state},
                timeout=0.5
            )
        except Exception as e:
            print(f"[WS BROADCAST ERROR] {e}")
    threading.Thread(target=_do, daemon=True).start()

# ── Message handlers ─────────────────────────────────────────────────

def handle_state(data, cursor, mqtt_client):
    global _last_arduino_t, _mqtt_publish_counter
    _last_arduino_t = time.time()

    active_lane    = data.get("lane")
    phase          = data.get("phase")
    remaining      = data.get("remaining")
    ped_status     = data.get("ped")
    ped_countdown  = data.get("ped_countdown", 0)
    vehicle_counts = data.get("vehicle_counts", [0, 0, 0, 0])
    ped_waiting    = data.get("ped_waiting", False)
    is_yellow      = phase == "YELLOW"

    cursor.execute("""
        SELECT `key`, `value` FROM config
        WHERE `key` IN ('GREEN_MEDIUM', 'YELLOW_TIME')
    """)
    config       = {row[0]: row[1] for row in cursor.fetchall()}
    green_medium = config.get("GREEN_MEDIUM", 20)
    yellow_time  = config.get("YELLOW_TIME", 3)

    for lane in range(4):
        if lane == active_lane:
            lane_remaining = remaining
            lane_phase     = phase
        else:
            steps = (lane - active_lane) % 4
            if not is_yellow:
                lane_remaining = remaining + yellow_time + (steps - 1) * (green_medium + yellow_time)
            else:
                lane_remaining = remaining + (steps - 1) * (green_medium + yellow_time)
            lane_phase = "RED"

        lane_count = vehicle_counts[lane] if lane < len(vehicle_counts) else 0
        cursor.execute("""
            UPDATE realtime_state
            SET phase=%s, remaining=%s, vehicle_count=%s
            WHERE lane=%s
        """, (lane_phase, lane_remaining, lane_count, lane))

    if ped_status != "IDLE":
        cursor.execute("""
            UPDATE realtime_state
            SET ped_status=%s, ped_countdown=%s, ped_waiting=%s
        """, (ped_status, ped_countdown, ped_waiting))
    else:
        cursor.execute("""
            UPDATE realtime_state
            SET ped_status='IDLE', ped_countdown=0, ped_waiting=0
        """)

    state = get_current_state(cursor)
    broadcast_async(state)
    publish_state(mqtt_client, state)

def handle_vehicle_count(data, cursor):
    cursor.execute("""
        INSERT INTO phase_log (lane, vehicle_count) VALUES (%s, %s)
    """, (data.get("lane"), data.get("count")))

def handle_ped_detected(data):
    print(f"[PED] Person detected side {data.get('side')}")

def handle_ped_crossing_start():
    print("[PED] Crossing started")

def handle_ped_crossing_end(data, cursor):
    count    = data.get("count", 0)
    duration = data.get("duration")
    cursor.execute("""
        INSERT INTO ped_log (ped_count, duration_sec) VALUES (%s, %s)
    """, (count, duration))
    print(f"[PED] Crossing ended — {count} people, {duration}s")

def handle_ack(data):
    print(f"[ACK] Arduino applied: {data.get('key')} = {data.get('value')}")

# ── Pending commands ─────────────────────────────────────────────────

def flush_pending_commands(cursor, ser):
    cursor.execute("""
        SELECT id, `key`, `value` FROM pending_commands
        WHERE sent = 0 ORDER BY id ASC
    """)
    rows = cursor.fetchall()
    for row in rows:
        cmd_id, key, value = row
        cmd = json.dumps({"cmd": "SET", "key": key, "value": value})
        ser.write((cmd + "\n").encode())
        ser.flush()
        print(f"[SERIAL] Sent: {key} = {value}")
        cursor.execute(
            "UPDATE pending_commands SET sent=1 WHERE id=%s", (cmd_id,)
        )

# ── Analytics ────────────────────────────────────────────────────────

def run_analytics(cursor, ser):
    cursor.execute("SELECT `key`, `value` FROM config")
    config      = {row[0]: row[1] for row in cursor.fetchall()}
    count_heavy = config.get("COUNT_HEAVY", 10)
    green_heavy = config.get("GREEN_HEAVY", 30)

    # Rule 1 — heavy traffic AND bad weather (multi-source)
    weather     = get_weather_condition()
    bad_weather = weather in ("Rain", "Drizzle", "Fog", "Mist", "Thunderstorm", "Snow")

    cursor.execute("""
        SELECT lane, AVG(vehicle_count) as avg_count
        FROM (
            SELECT lane, vehicle_count FROM phase_log
            ORDER BY id DESC LIMIT 20
        ) recent
        GROUP BY lane
        HAVING avg_count > %s
    """, (count_heavy,))
    rows = cursor.fetchall()
    if rows and bad_weather:
        new_val = min(green_heavy + 5, 60)
        if new_val != green_heavy:
            cursor.execute(
                "UPDATE config SET `value`=%s WHERE `key`='GREEN_HEAVY'",
                (new_val,)
            )
            cmd = json.dumps({"cmd": "SET", "key": "GREEN_HEAVY", "value": new_val})
            ser.write((cmd + "\n").encode())
            ser.flush()
            print(f"[RULE 1] Heavy traffic + {weather} → GREEN_HEAVY set to {new_val}s")

    # Rule 2 — frequent pedestrians
    cursor.execute("""
        SELECT COUNT(*) FROM ped_log
        WHERE timestamp >= NOW() - INTERVAL 1 HOUR
    """)
    ped_count = cursor.fetchone()[0]
    if ped_count > 10:
        cursor.execute(
            "UPDATE config SET `value`=10 WHERE `key`='PED_COOLDOWN_SEC'"
        )
        cmd = json.dumps({"cmd": "SET", "key": "PED_COOLDOWN_SEC", "value": 10})
        ser.write((cmd + "\n").encode())
        ser.flush()
        print(f"[RULE 2] Frequent pedestrians → PED_COOLDOWN_SEC set to 10s")

# ── Main loop ────────────────────────────────────────────────────────

def main():
    print(f"Connecting to {SERIAL_PORT}...")
    ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=0)
    print("Serial connected!")

    conn   = get_db()
    cursor = conn.cursor()
    print("Database connected!")

    mqtt_client = connect_mqtt()

    threading.Thread(
        target=mqtt_keepalive,
        args=(mqtt_client,),
        daemon=True
    ).start()

    analytics_counter   = 0
    pending_cmd_counter = 0
    line_buffer         = b""

    while True:
        try:
            if ser.in_waiting > 0:
                chunk = ser.read(ser.in_waiting)
                line_buffer += chunk

                while b"\n" in line_buffer:
                    raw, line_buffer = line_buffer.split(b"\n", 1)
                    line = raw.decode("utf-8", errors="ignore").strip()
                    if not line:
                        continue

                    try:
                        data     = json.loads(line)
                        msg_type = data.get("type")

                        if msg_type == "STATE":
                            handle_state(data, cursor, mqtt_client)
                        elif msg_type == "EVENT":
                            event = data.get("event")
                            if event == "VEHICLE_COUNT":
                                handle_vehicle_count(data, cursor)
                            elif event == "PED_DETECTED":
                                handle_ped_detected(data)
                            elif event == "PED_CROSSING_START":
                                handle_ped_crossing_start()
                            elif event == "PED_CROSSING_END":
                                handle_ped_crossing_end(data, cursor)
                        elif msg_type == "ACK":
                            handle_ack(data)
                        elif msg_type == "debug":
                            print(f"[ARDUINO DEBUG] {data}")

                        conn.commit()

                        analytics_counter += 1
                        if analytics_counter >= 30:
                            analytics_counter = 0
                            run_analytics(cursor, ser)

                    except json.JSONDecodeError:
                        print(f"[WARN] Invalid JSON: {line}")
            else:
                time.sleep(0.01)

            pending_cmd_counter += 1
            if pending_cmd_counter >= 10:
                pending_cmd_counter = 0
                flush_pending_commands(cursor, ser)
                conn.commit()

            now = time.time()
            if now - _last_broadcast_t >= 1.5 and _last_arduino_t > 0:
                try:
                    state   = get_current_state(cursor)
                    elapsed = int(now - _last_arduino_t)
                    if elapsed > 0:
                        for lane_data in state:
                            rem = lane_data.get("remaining") or 0
                            lane_data["remaining"] = max(0, rem - elapsed)
                            pc = lane_data.get("ped_countdown") or 0
                            if pc > 0:
                                lane_data["ped_countdown"] = max(0, pc - elapsed)
                    broadcast_async(state)
                    publish_state(mqtt_client, state)
                except Exception as e:
                    print(f"[HEARTBEAT ERROR] {e}")

        except mysql.connector.Error as db_err:
            print(f"[DB ERROR] {db_err} — reconnecting...")
            try:
                conn   = get_db()
                cursor = conn.cursor()
                print("[DB] Reconnected!")
            except Exception as e:
                print(f"[DB] Reconnect failed: {e}")
        except Exception as e:
            print(f"[ERROR] {e}")

if __name__ == "__main__":
    main()