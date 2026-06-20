import discord
from discord import app_commands
import mysql.connector
import json

DISCORD_TOKEN = "MTUxNzg4MzkzOTEyODIxMzUyNA.GYNMPq.ct5rJ6t_ejqALLWLpMLBLJUuu90jfTofRhsfTc"
GUILD_ID      = 1517883988037992588   # your server ID — get by right-clicking server icon with dev mode on

DB_CONFIG = {
    "host":     "localhost",
    "user":     "admin",
    "password": "password",
    "database": "traffic_db"
}

VALID_KEYS = [
    "GREEN_HEAVY", "GREEN_MEDIUM", "GREEN_LIGHT",
    "YELLOW_TIME", "PED_TIME", "PED_WARNING_SEC",
    "PED_COOLDOWN_SEC", "COUNT_HEAVY", "COUNT_MEDIUM"
]

def get_db():
    return mysql.connector.connect(**DB_CONFIG)

# ── Bot setup ────────────────────────────────────────────────────────

intents = discord.Intents.default()
client  = discord.Client(intents=intents)
tree    = app_commands.CommandTree(client)
GUILD   = discord.Object(id=GUILD_ID)

@client.event
async def on_ready():
    await tree.sync(guild=GUILD)   # instant sync to your server
    print(f"[DISCORD] Logged in as {client.user} — slash commands synced")

# ── /status ──────────────────────────────────────────────────────────

@tree.command(guild=GUILD, name="status", description="Show current lane phases and countdowns")
async def status(interaction: discord.Interaction):
    try:
        conn   = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM realtime_state ORDER BY lane")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        lines = ["**🚦 Current Intersection Status**"]
        for r in rows:
            phase = r.get("phase", "?")
            emoji = "🟢" if phase == "GREEN" else ("🟡" if phase == "YELLOW" else "🔴")
            ped   = f" | 🚶 {r.get('ped_status','IDLE')}" if r.get("ped_status") != "IDLE" else ""
            lines.append(
                f"{emoji} **Lane {r['lane']+1}**: {phase} — {r.get('remaining',0)}s | 🚗 {r.get('vehicle_count',0)} vehicles{ped}"
            )
        await interaction.response.send_message("\n".join(lines))

    except Exception as e:
        await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)

# ── /config ──────────────────────────────────────────────────────────

@tree.command(guild=GUILD, name="config", description="Show all current timing configuration values")
async def config(interaction: discord.Interaction):
    try:
        conn   = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM config ORDER BY `key`")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        lines = ["**⚙️ Traffic Timing Config**"]
        for r in rows:
            lines.append(f"`{r['key']}` = **{r['value']}**")
        await interaction.response.send_message("\n".join(lines))

    except Exception as e:
        await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)

# ── /set ─────────────────────────────────────────────────────────────

@tree.command(guild=GUILD, name="set", description="Update a traffic timing config value")
@app_commands.describe(
    key   = "Config key to update",
    value = "New integer value"
)
@app_commands.choices(key=[
    app_commands.Choice(name=k, value=k) for k in VALID_KEYS
])
async def set_config(interaction: discord.Interaction, key: str, value: int):
    try:
        conn   = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE config SET `value`=%s WHERE `key`=%s",
            (value, key)
        )
        cursor.execute(
            "INSERT INTO pending_commands (`key`, `value`) VALUES (%s, %s)",
            (key, value)
        )
        conn.commit()
        cursor.close()
        conn.close()

        await interaction.response.send_message(
            f"✅ Queued: `{key}` = **{value}** — Arduino will apply shortly"
        )
        print(f"[DISCORD CMD] Set {key} = {value}")

    except Exception as e:
        await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)

# ── /analytics ───────────────────────────────────────────────────────

@tree.command(guild=GUILD, name="analytics", description="Show vehicle and pedestrian analytics summary")
async def analytics(interaction: discord.Interaction):
    try:
        conn   = get_db()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT lane, SUM(vehicle_count) as total, AVG(vehicle_count) as avg, MAX(vehicle_count) as max
            FROM phase_log GROUP BY lane ORDER BY lane
        """)
        vehicle_rows = cursor.fetchall()

        cursor.execute("""
            SELECT COUNT(*) as crossings, SUM(ped_count) as total_peds,
                   AVG(duration_sec) as avg_duration
            FROM ped_log
        """)
        ped = cursor.fetchone()
        cursor.close()
        conn.close()

        lines = ["**📊 Analytics Summary**\n**🚗 Vehicle counts per lane:**"]
        for r in vehicle_rows:
            lines.append(
                f"Lane {r['lane']+1} — total: **{r['total']}** | avg: **{float(r['avg'] or 0):.1f}** | max: **{r['max']}**"
            )
        lines.append(f"\n**🚶 Pedestrian stats:**")
        lines.append(f"Total crossings: **{ped['crossings']}** | Total pedestrians: **{ped['total_peds'] or 0}** | Avg duration: **{float(ped['avg_duration'] or 0):.1f}s**")

        await interaction.response.send_message("\n".join(lines))

    except Exception as e:
        await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)

# ── Run ──────────────────────────────────────────────────────────────

client.run(DISCORD_TOKEN)