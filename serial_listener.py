import serial
import json
import mysql.connector
import requests

SERIAL_PORT = "/dev/ttyUSB0"
BAUD_RATE = 9600
FASTAPI_URL = "http://localhost:8000"
DB_CONFIG  = {
    "host":     "localhost",
    "user":     "admin",
    "password": "password",
    "database": "traffic_db"
}

def get_db():
    return mysql.connector.connect(**DB_CONFIG)

def broadcast_state(cursor):
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
    try:
        requests.post(
            f"{FASTAPI_URL}/internal/broadcast",
            json={"type": "state", "data": state},
            timeout=0.5
        )
    except Exception as e:
        print(f"[WS BROADCAST ERROR] {e}")

def handle_state(data, cursor):
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

    broadcast_state(cursor)

def handle_vehicle_count(data, cursor):
    cursor.execute("""
        INSERT INTO phase_log (lane, vehicle_count)
        VALUES (%s, %s)
    """, (data.get("lane"), data.get("count")))

def handle_ped_detected(data):
    print(f"[PED] Person detected side {data.get('side')}")

def handle_ped_crossing_start():
    print("[PED] Crossing started")

def handle_ped_crossing_end(data, cursor):
    count    = data.get("count", 0)
    duration = data.get("duration")
    cursor.execute("""
        INSERT INTO ped_log (ped_count, duration_sec)
        VALUES (%s, %s)
    """, (count, duration))
    print(f"[PED] Crossing ended — {count} people, {duration}s")

def handle_ack(data):
    print(f"[ACK] Arduino applied: {data.get('key')} = {data.get('value')}")

def flush_pending_commands(cursor, ser):
    cursor.execute("""
        SELECT id, `key`, `value` FROM pending_commands
        WHERE sent = 0
        ORDER BY id ASC
    """)
    rows = cursor.fetchall()
    for row in rows:
        cmd_id, key, value = row
        cmd = json.dumps({"cmd": "SET", "key": key, "value": value})
        ser.write((cmd + "\n").encode())
        ser.flush()
        print(f"[SERIAL] Sent: {key} = {value}")
        cursor.execute(
            "UPDATE pending_commands SET sent=1 WHERE id=%s",
            (cmd_id,)
        )

def run_analytics(cursor, ser):
    cursor.execute("SELECT `key`, `value` FROM config")
    config      = {row[0]: row[1] for row in cursor.fetchall()}
    count_heavy = config.get("COUNT_HEAVY", 10)
    green_heavy = config.get("GREEN_HEAVY", 30)

    cursor.execute("""
        SELECT lane, AVG(vehicle_count) as avg_count
        FROM (
            SELECT lane, vehicle_count
            FROM phase_log
            ORDER BY id DESC
            LIMIT 20
        ) recent
        GROUP BY lane
        HAVING avg_count > %s
    """, (count_heavy,))
    rows = cursor.fetchall()
    if rows:
        new_val = min(green_heavy + 5, 60)
        if new_val != green_heavy:
            cursor.execute(
                "UPDATE config SET `value`=%s WHERE `key`='GREEN_HEAVY'",
                (new_val,)
            )
            cmd = json.dumps({"cmd": "SET", "key": "GREEN_HEAVY", "value": new_val})
            ser.write((cmd + "\n").encode())
            ser.flush()
            print(f"[RULE 1] Heavy traffic → GREEN_HEAVY set to {new_val}s")

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

def main():
    print(f"Connecting to {SERIAL_PORT}...")
    ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
    print("Serial connected!")

    conn   = get_db()
    cursor = conn.cursor()
    print("Database connected!")

    analytics_counter = 0

    while True:
        try:
            flush_pending_commands(cursor, ser)
            conn.commit()

            line = ser.readline().decode("utf-8").strip()
            if not line:
                continue

            data     = json.loads(line)
            msg_type = data.get("type")

            if msg_type == "STATE":
                handle_state(data, cursor)
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