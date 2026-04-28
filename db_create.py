import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="password"
)
cursor = conn.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS traffic_db")
cursor.execute("USE traffic_db")

cursor.execute("""
CREATE TABLE IF NOT EXISTS realtime_state (
    lane          INT PRIMARY KEY,
    phase         VARCHAR(20),
    remaining     INT,
    ped_status    VARCHAR(20),
    ped_countdown INT DEFAULT 0,
    vehicle_count INT DEFAULT 0,
    ped_waiting   TINYINT DEFAULT 0,
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS phase_log (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    timestamp     DATETIME DEFAULT CURRENT_TIMESTAMP,
    lane          INT,
    vehicle_count INT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS ped_log (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP,
    ped_count    INT,
    duration_sec INT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS config (
    `key`      VARCHAR(50) PRIMARY KEY,
    `value`    INT NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS pending_commands (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    `key`      VARCHAR(50),
    `value`    INT,
    sent       TINYINT DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.executemany(
    "INSERT IGNORE INTO realtime_state (lane, phase, remaining, ped_status) VALUES (%s, %s, %s, %s)",
    [(0, 'RED', 0, 'IDLE'),
     (1, 'RED', 0, 'IDLE'),
     (2, 'RED', 0, 'IDLE'),
     (3, 'RED', 0, 'IDLE')]
)

cursor.executemany(
    "INSERT IGNORE INTO config (`key`, `value`) VALUES (%s, %s)",
    [('GREEN_HEAVY', 30),
     ('GREEN_MEDIUM', 20),
     ('GREEN_LIGHT', 10),
     ('YELLOW_TIME', 3),
     ('PED_TIME', 15),
     ('PED_WARNING_SEC', 5),
     ('PED_COOLDOWN_SEC', 15),
     ('COUNT_HEAVY', 10),
     ('COUNT_MEDIUM', 5)]
)

conn.commit()
cursor.close()
conn.close()
print("Database and tables created successfully!")