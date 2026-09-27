from __future__ import annotations

import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Iterator

from confluent_kafka import Producer
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Response, status

from event import OCPP_STATUSES

CHARGERS = ("station-001", "station-002")
CONNECTORS = (1, 2)
TOPIC = os.getenv("KAFKA_TOPIC", "ocpp.status_notifications.v1")
TRANSITION_SECONDS = float(os.getenv("TRANSITION_SECONDS", "5"))
producer = Producer(
    {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "enable.idempotence": True,
        "acks": "all",
    }
)

def to_iso(timestamp: datetime) -> str:
    return timestamp.isoformat().replace("+00:00", "Z")

def build_envelope(
    charger_id: str,
    connector_id: int,
    connector_status: str,
    reported_at: datetime | None = None,
) -> dict:
    reported_at = reported_at or datetime.now(timezone.utc)
    return {
        "event_id": str(uuid.uuid4()),
        "charger_id": charger_id,
        "gateway_received_at": to_iso(datetime.now(timezone.utc)),
        "action": "StatusNotification",
        "payload": {
            "connectorId": connector_id,
            "errorCode": "NoError",
            "status": connector_status,
            "timestamp": to_iso(reported_at),
        },
    }

def publish(envelope: dict, copies: int = 1) -> None:
    value = json.dumps(envelope).encode()
    key = envelope["charger_id"].encode()
    for _ in range(copies):
        producer.produce(TOPIC, key=key, value=value)
    if producer.flush(5) > 0:
        raise RuntimeError("event could not be delivered to Kafka")

def emit(charger_id: str, connector_id: int, connector_status: str) -> dict:
    envelope = build_envelope(charger_id, connector_id, connector_status)
    publish(envelope)
    return envelope

def complete_transition(
    charger_id: str, connector_id: int, final_status: str
) -> None:
    time.sleep(TRANSITION_SECONDS)
    emit(charger_id, connector_id, final_status)

def validate(charger_id: str, connector_id: int) -> None:
    if charger_id not in CHARGERS:
        raise HTTPException(status_code=404, detail="unknown simulated charger")
    if connector_id not in CONNECTORS:
        raise HTTPException(status_code=404, detail="unknown simulated connector")

def seed() -> Iterator[dict]:
    for charger_id in CHARGERS:
        yield emit(charger_id, 0, "Available")
        for connector_id in CONNECTORS:
            yield emit(charger_id, connector_id, "Available")

@asynccontextmanager
async def lifespan(_: FastAPI):
    list(seed())
    yield
    producer.flush(5)

app = FastAPI(title="OCPP station simulator API", version="1.0.0", lifespan=lifespan)

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

@app.post(
    "/chargers/{charger_id}/connectors/{connector_id}/start",
    status_code=status.HTTP_202_ACCEPTED,
)
def start_charging(
    charger_id: str,
    connector_id: int,
    response: Response,
    background_tasks: BackgroundTasks,
) -> dict:
    validate(charger_id, connector_id)
    try:
        event = emit(charger_id, connector_id, "Preparing")
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    background_tasks.add_task(
        complete_transition, charger_id, connector_id, "Charging"
    )
    response.headers["Location"] = (
        f"/chargers/{charger_id}/connectors/{connector_id}"
    )
    return {
        "accepted": True,
        "eventId": event["event_id"],
        "status": "Preparing",
        "nextStatus": "Charging",
        "transitionSeconds": TRANSITION_SECONDS,
    }

@app.post(
    "/chargers/{charger_id}/connectors/{connector_id}/stop",
    status_code=status.HTTP_202_ACCEPTED,
)
def stop_charging(
    charger_id: str,
    connector_id: int,
    response: Response,
    background_tasks: BackgroundTasks,
) -> dict:
    validate(charger_id, connector_id)
    try:
        event = emit(charger_id, connector_id, "Finishing")
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    background_tasks.add_task(
        complete_transition, charger_id, connector_id, "Available"
    )
    response.headers["Location"] = (
        f"/chargers/{charger_id}/connectors/{connector_id}"
    )
    return {
        "accepted": True,
        "eventId": event["event_id"],
        "status": "Finishing",
        "nextStatus": "Available",
        "transitionSeconds": TRANSITION_SECONDS,
    }

@app.post(
    "/chargers/{charger_id}/connectors/{connector_id}/demo/duplicate",
    status_code=status.HTTP_202_ACCEPTED,
)
def publish_duplicate(
    charger_id: str,
    connector_id: int,
    connector_status: str = Query("Available", alias="status"),
) -> dict:
    validate(charger_id, connector_id)
    if connector_status not in OCPP_STATUSES:
        raise HTTPException(status_code=422, detail="invalid OCPP connector status")
    envelope = build_envelope(charger_id, connector_id, connector_status)
    try:
        publish(envelope, copies=2)
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    return {
        "accepted": True,
        "eventId": envelope["event_id"],
        "publishedCopies": 2,
        "status": connector_status,
    }

@app.post(
    "/chargers/{charger_id}/connectors/{connector_id}/demo/out-of-order",
    status_code=status.HTTP_202_ACCEPTED,
)
def publish_out_of_order(charger_id: str, connector_id: int) -> dict:
    validate(charger_id, connector_id)
    newer_reported_at = datetime.now(timezone.utc)
    newer = build_envelope(
        charger_id, connector_id, "Charging", reported_at=newer_reported_at
    )
    late = build_envelope(
        charger_id,
        connector_id,
        "Available",
        reported_at=newer_reported_at - timedelta(minutes=1),
    )
    try:
        publish(newer)
        publish(late)
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    return {
        "accepted": True,
        "newerEventId": newer["event_id"],
        "newerStatus": newer["payload"]["status"],
        "newerReportedAt": newer["payload"]["timestamp"],
        "lateEventId": late["event_id"],
        "lateStatus": late["payload"]["status"],
        "lateReportedAt": late["payload"]["timestamp"],
        "publishedOrder": ["newer", "late"],
    }
