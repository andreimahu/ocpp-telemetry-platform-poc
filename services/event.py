from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

OCPP_STATUSES = {
    "Available",
    "Preparing",
    "Charging",
    "SuspendedEVSE",
    "SuspendedEV",
    "Finishing",
    "Reserved",
    "Unavailable",
    "Faulted",
}

def parse_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be a valid ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed

@dataclass(frozen=True)
class StatusEvent:
    event_id: str
    charger_id: str
    connector_id: int
    status: str
    error_code: str
    reported_at: datetime | None
    gateway_received_at: datetime

    @classmethod
    def from_envelope(cls, value: Any) -> "StatusEvent":
        if not isinstance(value, dict):
            raise ValueError("event must be a JSON object")
        if value.get("action") != "StatusNotification":
            raise ValueError("action must be StatusNotification")

        event_id = value.get("event_id")
        charger_id = value.get("charger_id")
        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError("event_id must be a non-empty string")
        if not isinstance(charger_id, str) or not charger_id.strip():
            raise ValueError("charger_id must be a non-empty string")

        payload = value.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")
        connector_id = payload.get("connectorId")
        if isinstance(connector_id, bool) or not isinstance(connector_id, int):
            raise ValueError("connectorId must be an integer")
        if connector_id not in (0, 1, 2):
            raise ValueError("connectorId must be 0, 1, or 2")

        status = payload.get("status")
        if status not in OCPP_STATUSES:
            raise ValueError("status is not an OCPP 1.6 connector status")
        error_code = payload.get("errorCode")
        if not isinstance(error_code, str) or not error_code:
            raise ValueError("errorCode must be a non-empty string")

        gateway_received_at = parse_timestamp(
            value.get("gateway_received_at"), "gateway_received_at"
        )
        timestamp = payload.get("timestamp")
        reported_at = parse_timestamp(timestamp, "payload.timestamp") if timestamp else None

        return cls(
            event_id=event_id,
            charger_id=charger_id,
            connector_id=connector_id,
            status=status,
            error_code=error_code,
            reported_at=reported_at,
            gateway_received_at=gateway_received_at,
        )
