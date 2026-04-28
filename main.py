from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "admin",
    "password": "password",
    "database": "traffic_db"
}

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    return mysql.connector.connect(**DB_CONFIG)

class ConfigUpdate(BaseModel):
    key: str
    value: int

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        print(f"[WS] Client connected — total: {len(self.active)}")

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)
        print(f"[WS] Client disconnected — total: {len(self.active)}")

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except:
                dead.append(ws)
        for ws in dead:
            self.active.remove(ws)

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)

@app.post("/internal/broadcast")
async def internal_broadcast(data: dict):
    await manager.broadcast(data)
    return {"status": "ok"}

@app.get("/api/state")
def get_state():
    conn   = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM realtime_state ORDER BY lane")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

@app.get("/api/phase-log")
def get_phase_log(limit: int = 50):
    conn   = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM phase_log ORDER BY id DESC LIMIT %s", (limit,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

@app.get("/api/phase-log/summary")
def get_phase_summary():
    conn   = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            lane,
            COUNT(*)           AS total_cycles,
            SUM(vehicle_count) AS total_vehicles,
            AVG(vehicle_count) AS avg_vehicles,
            MAX(vehicle_count) AS max_vehicles,
            MIN(vehicle_count) AS min_vehicles
        FROM phase_log
        GROUP BY lane
        ORDER BY lane
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

@app.get("/api/ped-log")
def get_ped_log(limit: int = 50):
    conn   = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM ped_log ORDER BY id DESC LIMIT %s", (limit,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

@app.get("/api/ped-log/summary")
def get_ped_summary():
    conn   = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            COUNT(*)          AS total_crossings,
            SUM(ped_count)    AS total_pedestrians,
            AVG(ped_count)    AS avg_pedestrians,
            AVG(duration_sec) AS avg_duration,
            MAX(duration_sec) AS max_duration,
            MIN(duration_sec) AS min_duration
        FROM ped_log
    """)
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row

@app.get("/api/config")
def get_config():
    conn   = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM config")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

@app.post("/api/config")
def update_config(body: ConfigUpdate):
    valid_keys = [
        "GREEN_HEAVY", "GREEN_MEDIUM", "GREEN_LIGHT",
        "YELLOW_TIME", "PED_TIME", "PED_WARNING_SEC",
        "PED_COOLDOWN_SEC", "COUNT_HEAVY", "COUNT_MEDIUM"
    ]
    if body.key not in valid_keys:
        raise HTTPException(status_code=400, detail=f"Invalid key: {body.key}")

    conn   = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE config SET `value`=%s WHERE `key`=%s",
        (body.value, body.key)
    )
    cursor.execute(
        "INSERT INTO pending_commands (`key`, `value`) VALUES (%s, %s)",
        (body.key, body.value)
    )
    conn.commit()
    cursor.close()
    conn.close()

    print(f"[FASTAPI] Queued: {body.key} = {body.value}")
    return {"status": "ok", "key": body.key, "value": body.value}

@app.get("/api/alerts")
def get_alerts():
    conn   = get_db()
    cursor = conn.cursor(dictionary=True)
    alerts = []

    cursor.execute("""
        SELECT lane, AVG(vehicle_count) as avg_count
        FROM (
            SELECT lane, vehicle_count
            FROM phase_log
            ORDER BY id DESC
            LIMIT 20
        ) recent
        GROUP BY lane
    """)
    rows = cursor.fetchall()
    cursor.execute("SELECT `value` FROM config WHERE `key`='COUNT_HEAVY'")
    count_heavy = cursor.fetchone()["value"]

    for row in rows:
        if row["avg_count"] and row["avg_count"] > count_heavy:
            alerts.append({
                "type":    "HEAVY_TRAFFIC",
                "message": f"Lane {row['lane']} avg {row['avg_count']:.1f} vehicles — above threshold {count_heavy}",
                "lane":    row["lane"]
            })

    cursor.execute("""
        SELECT COUNT(*) as count FROM ped_log
        WHERE timestamp >= NOW() - INTERVAL 1 HOUR
    """)
    ped_count = cursor.fetchone()["count"]
    if ped_count > 10:
        alerts.append({
            "type":    "FREQUENT_PEDESTRIANS",
            "message": f"{ped_count} pedestrian crossings in last hour",
            "lane":    None
        })

    cursor.close()
    conn.close()
    return alerts