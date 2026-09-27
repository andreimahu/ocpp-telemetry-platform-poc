from __future__ import annotations

import os
from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row

app = FastAPI(title="OCPP latest-status read API", version="1.0.0")
DATABASE_URL = os.environ["DATABASE_URL"]

SELECT_COLUMNS = """
    charger_id, connector_id, status, error_code, reported_at,
    gateway_received_at, event_id
"""

def fetch_all(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as connection, connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchall()

def status_view(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "connectorId": row["connector_id"],
        "status": row["status"],
        "errorCode": row["error_code"],
        "reportedAt": row["reported_at"],
        "gatewayReceivedAt": row["gateway_received_at"],
        "eventId": row["event_id"],
    }

def history_view(row: dict[str, Any], latest_event_id: str) -> dict[str, Any]:
    return {
        "eventId": row["event_id"],
        "status": row["status"],
        "errorCode": row["error_code"],
        "reportedAt": row["reported_at"],
        "gatewayReceivedAt": row["gateway_received_at"],
        "processedAt": row["processed_at"],
        "isLatest": row["event_id"] == latest_event_id,
    }

def charger_view(charger_id: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    station = next((status_view(row) for row in rows if row["connector_id"] == 0), None)
    connectors = [status_view(row) for row in rows if row["connector_id"] != 0]
    return {"chargerId": charger_id, "stationStatus": station, "connectors": connectors}

@app.get("/health")
def health() -> dict[str, str]:
    fetch_all("SELECT 1")
    return {"status": "ok"}

@app.get("/chargers")
def chargers() -> dict[str, list[dict[str, Any]]]:
    rows = fetch_all(
        f"SELECT {SELECT_COLUMNS} FROM latest_status ORDER BY charger_id, connector_id"
    )
    charger_ids = list(dict.fromkeys(row["charger_id"] for row in rows))
    return {
        "chargers": [
            charger_view(
                charger_id, [row for row in rows if row["charger_id"] == charger_id]
            )
            for charger_id in charger_ids
        ]
    }

@app.get("/chargers/{charger_id}/status")
def charger_status(charger_id: str) -> dict[str, Any]:
    rows = fetch_all(
        f"SELECT {SELECT_COLUMNS} FROM latest_status "
        "WHERE charger_id = %s ORDER BY connector_id",
        (charger_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="charger not found")
    return charger_view(charger_id, rows)

@app.get("/chargers/{charger_id}/connectors/{connector_id}/events")
def connector_events(
    charger_id: str,
    connector_id: int,
    limit: int = Query(20, ge=1, le=50),
) -> dict[str, Any]:
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as connection, connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {SELECT_COLUMNS} FROM latest_status "
            "WHERE charger_id = %s AND connector_id = %s",
            (charger_id, connector_id),
        )
        latest = cursor.fetchone()
        if latest is None:
            raise HTTPException(status_code=404, detail="connector not found")

        cursor.execute(
            "SELECT count(*) AS count FROM status_history "
            "WHERE charger_id = %s AND connector_id = %s",
            (charger_id, connector_id),
        )
        history_count = cursor.fetchone()["count"]

        cursor.execute(
            """
            WITH recent AS (
                SELECT
                    event_id, status, error_code, reported_at,
                    gateway_received_at, processed_at
                FROM status_history
                WHERE charger_id = %s AND connector_id = %s
                ORDER BY processed_at DESC
                LIMIT %s
            )
            SELECT * FROM recent ORDER BY processed_at ASC
            """,
            (charger_id, connector_id, limit),
        )
        events = cursor.fetchall()

    return {
        "chargerId": charger_id,
        "connectorId": connector_id,
        "latest": status_view(latest),
        "historyCount": history_count,
        "events": [
            history_view(event, latest["event_id"])
            for event in events
        ],
    }
