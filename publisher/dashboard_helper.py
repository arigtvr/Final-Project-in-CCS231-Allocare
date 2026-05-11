import os
import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATABASE_PATH = os.environ.get("DATABASE_PATH", str(BASE_DIR / "data" / "allocare.db"))


def init_db() -> None:
    db_dir = os.path.dirname(DATABASE_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS patient_admissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id TEXT NOT NULL,
                severity TEXT NOT NULL,
                facility_unit TEXT NOT NULL,
                patient_name TEXT,
                admission_notes TEXT,
                admitted_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bed_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bed_id TEXT NOT NULL,
                status TEXT NOT NULL,
                facility_type TEXT NOT NULL,
                updated_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS facility_capacities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                facility_unit TEXT NOT NULL UNIQUE,
                max_capacity INTEGER NOT NULL,
                updated_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS doctors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doctor_id TEXT NOT NULL UNIQUE,
                specialty TEXT NOT NULL,
                availability TEXT NOT NULL,
                doctor_name TEXT,
                registered_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT DEFAULT 'subscriber',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS consumed_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                delivery_tag TEXT,
                ack_status TEXT DEFAULT 'acknowledged',
                consumed_at TEXT NOT NULL,
                consumed_by TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                source_data TEXT,
                is_resolved INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                resolved_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS queue_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscriber_id INTEGER,
                queue_name TEXT NOT NULL,
                topic_filter TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                FOREIGN KEY (subscriber_id) REFERENCES subscribers(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS group_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_name TEXT NOT NULL,
                routing_key TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                payload TEXT,
                created_by TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_system_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_size INTEGER,
                event_category TEXT,
                file_extension TEXT,
                processed_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dashboard_event_metrics (
                metric_key TEXT PRIMARY KEY,
                metric_value INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_dashboard_data() -> dict:
    with get_db_connection() as conn:
        admissions = conn.execute(
            "SELECT patient_id, severity, facility_unit, created_at FROM patient_admissions ORDER BY id DESC LIMIT 6"
        ).fetchall()
        beds = conn.execute(
            "SELECT bed_id, status, facility_type, created_at FROM bed_updates ORDER BY id DESC LIMIT 6"
        ).fetchall()
        total_admissions = conn.execute("SELECT COUNT(*) AS count FROM patient_admissions").fetchone()["count"]
        total_doctors = conn.execute("SELECT COUNT(*) AS count FROM doctors").fetchone()["count"]
        severity_counts_rows = conn.execute(
            "SELECT severity, COUNT(*) AS count FROM patient_admissions GROUP BY severity"
        ).fetchall()
        bed_status_counts_rows = conn.execute(
            "SELECT status, COUNT(*) AS count FROM bed_updates GROUP BY status"
        ).fetchall()
        capacity_rows = conn.execute(
            "SELECT facility_unit, max_capacity, updated_by, created_at FROM facility_capacities ORDER BY facility_unit ASC"
        ).fetchall()
        group_messages = conn.execute(
            "SELECT group_name, title, message, created_by, created_at FROM group_messages ORDER BY id DESC LIMIT 6"
        ).fetchall()
        latest_beds = conn.execute(
            """
            SELECT b1.bed_id, b1.status, b1.facility_type, b1.created_at
            FROM bed_updates b1
            INNER JOIN (
                SELECT bed_id, MAX(id) AS latest_id
                FROM bed_updates
                GROUP BY bed_id
            ) latest ON latest.latest_id = b1.id
            """
        ).fetchall()
        last_updated_row = conn.execute(
            "SELECT MAX(created_at) AS created_at FROM (SELECT created_at FROM patient_admissions UNION ALL SELECT created_at FROM bed_updates)"
        ).fetchone()

    severity_counts = {level: 0 for level in ["low", "medium", "high", "critical"]}
    for row in severity_counts_rows:
        severity_counts[row["severity"]] = row["count"]

    bed_status_counts = {status: 0 for status in ["vacant", "occupied"]}
    for row in bed_status_counts_rows:
        if row["status"] in bed_status_counts:
            bed_status_counts[row["status"]] = row["count"]

    max_severity = max(severity_counts.values()) if severity_counts else 0
    if max_severity == 0:
        max_severity = 1

    capacity_lookup = {row["facility_unit"]: row["max_capacity"] for row in capacity_rows}
    occupied_lookup = {unit: 0 for unit in capacity_lookup}
    for row in latest_beds:
        facility_unit = row["facility_type"]
        if facility_unit in occupied_lookup and row["status"] == "occupied":
            occupied_lookup[facility_unit] += 1

    capacity_summary = []
    for row in capacity_rows:
        facility_unit = row["facility_unit"]
        max_capacity = row["max_capacity"]
        occupied_count = occupied_lookup.get(facility_unit, 0)
        ratio = round((occupied_count / max_capacity) * 100) if max_capacity else 0
        remaining = max_capacity - occupied_count
        capacity_summary.append(
            {
                "facility_unit": facility_unit,
                "max_capacity": max_capacity,
                "occupied_count": occupied_count,
                "remaining_capacity": remaining if remaining > 0 else 0,
                "occupancy_percent": ratio,
                "updated_by": row["updated_by"],
                "created_at": row["created_at"],
            }
        )

    total_capacity = sum(row["max_capacity"] for row in capacity_rows)
    total_occupied = sum(occupied_lookup.values())
    total_capacity_ratio = round((total_occupied / total_capacity) * 100) if total_capacity else 0

    severity_series = [
        {"label": level, "count": severity_counts[level], "percent": round((severity_counts[level] / max_severity) * 100)}
        for level in ["low", "medium", "high", "critical"]
    ]

    return {
        "total_admissions": total_admissions,
        "total_doctors": total_doctors,
        "severity_counts": severity_counts,
        "bed_status_counts": bed_status_counts,
        "facility_capacities": capacity_rows,
        "capacity_summary": capacity_summary,
        "total_capacity": total_capacity,
        "total_occupied": total_occupied,
        "total_capacity_ratio": total_capacity_ratio,
        "severity_series": severity_series,
        "max_severity": max_severity,
        "recent_admissions": admissions,
        "recent_beds": beds,
        "recent_group_messages": group_messages,
        "group_targets": {},
        "last_updated": last_updated_row["created_at"] if last_updated_row and last_updated_row["created_at"] else "No activity yet",
    }
