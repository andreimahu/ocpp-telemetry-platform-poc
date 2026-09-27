import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parents[1] / "services"))

from event import StatusEvent  # noqa: E402


def envelope(**payload_overrides):
    payload = {
        "connectorId": 1,
        "errorCode": "NoError",
        "status": "Available",
        "timestamp": "2026-09-25T10:05:00Z",
    }
    payload.update(payload_overrides)
    return {
        "event_id": "event-1",
        "charger_id": "station-001",
        "gateway_received_at": "2026-09-25T10:05:00.125Z",
        "action": "StatusNotification",
        "payload": payload,
    }


class StatusEventTest(unittest.TestCase):
    def test_parses_reported_timestamp(self):
        event = StatusEvent.from_envelope(envelope())
        self.assertEqual(event.connector_id, 1)
        self.assertEqual(
            event.reported_at.isoformat(), "2026-09-25T10:05:00+00:00"
        )

    def test_allows_missing_reported_timestamp(self):
        value = envelope()
        del value["payload"]["timestamp"]
        event = StatusEvent.from_envelope(value)
        self.assertIsNone(event.reported_at)
        self.assertEqual(
            event.gateway_received_at.isoformat(),
            "2026-09-25T10:05:00.125000+00:00",
        )

    def test_rejects_connector_outside_local_model(self):
        with self.assertRaisesRegex(ValueError, "connectorId"):
            StatusEvent.from_envelope(envelope(connectorId=3))

    def test_rejects_unknown_ocpp_status(self):
        with self.assertRaisesRegex(ValueError, "status"):
            StatusEvent.from_envelope(envelope(status="AlmostCharging"))

    def test_rejects_timestamp_without_timezone(self):
        with self.assertRaisesRegex(ValueError, "timezone"):
            StatusEvent.from_envelope(envelope(timestamp="2026-09-25T10:05:00"))


if __name__ == "__main__":
    unittest.main()
