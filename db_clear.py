import mysql.connector
import subprocess

DB_CONFIG = {
    "host": "localhost",
    "user": "admin",
    "password": "password"
}

conn   = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

cursor.execute("DROP DATABASE IF EXISTS traffic_db")
print("Database dropped!")

conn.commit()
cursor.close()
conn.close()

subprocess.run(["python3", "db_create.py"])
print("Database recreated!")