from __future__ import annotations

import json
import logging
import os
import time
import psycopg

from confluent_kafka import Consumer, KafkaError
from event import StatusEvent

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("status-processor")

INSERT_HISTORY = """
    INSERT INTO status_history (
        event_id, charger_id, connector_id, status, error_code,
        reported_at, gateway_received_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id
"""

UPSERT_LATEST = """
    INSERT INTO latest_status (
        charger_id, connector_id, status, error_code, reported_at,
        gateway_received_at, event_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (charger_id, connector_id) DO UPDATE SET
        status = EXCLUDED.status,
        error_code = EXCLUDED.error_code,
        reported_at = EXCLUDED.reported_at,
        gateway_received_at = EXCLUDED.gateway_received_at,
        event_id = EXCLUDED.event_id
    WHERE COALESCE(EXCLUDED.reported_at, EXCLUDED.gateway_received_at)
       >= COALESCE(latest_status.reported_at, latest_status.gateway_received_at)
"""

def connect_database() -> psycopg.Connection:
    database_url = os.environ["DATABASE_URL"]
    while True:
        try:
            connection = psycopg.connect(database_url)
            logger.info("connected to PostgreSQL")
            return connection
        except psycopg.OperationalError as error:
            logger.warning("PostgreSQL unavailable: %s", error)
            time.sleep(2)

def persist(connection: psycopg.Connection, event: StatusEvent) -> bool:
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            INSERT_HISTORY,
            (
                event.event_id,
                event.charger_id,
                event.connector_id,
                event.status,
                event.error_code,
                event.reported_at,
                event.gateway_received_at,
            ),
        )
        if cursor.fetchone() is None:
            return False
        cursor.execute(
            UPSERT_LATEST,
            (
                event.charger_id,
                event.connector_id,
                event.status,
                event.error_code,
                event.reported_at,
                event.gateway_received_at,
                event.event_id,
            ),
        )
    return True

def main() -> None:
    topic = os.getenv("KAFKA_TOPIC", "ocpp.status_notifications.v1")
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": "status-processor-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    consumer.subscribe([topic])
    connection = connect_database()
    logger.info("consuming %s", topic)

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka error: %s", message.error())
                continue

            try:
                event = StatusEvent.from_envelope(json.loads(message.value()))
            except (ValueError, TypeError, json.JSONDecodeError) as error:
                logger.error(
                    "discarding invalid event at %s[%s]@%s: %s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    error,
                )
                consumer.commit(message=message, asynchronous=False)
                continue

            while True:
                try:
                    inserted = persist(connection, event)
                    consumer.commit(message=message, asynchronous=False)
                    logger.info(
                        "%s event=%s charger=%s connector=%s status=%s",
                        "stored" if inserted else "deduplicated",
                        event.event_id,
                        event.charger_id,
                        event.connector_id,
                        event.status,
                    )
                    break
                except psycopg.Error as error:
                    logger.exception("database write failed; retrying event: %s", error)
                    connection.close()
                    time.sleep(2)
                    connection = connect_database()
    except KeyboardInterrupt:
        logger.info("stopping")
    finally:
        connection.close()
        consumer.close()

if __name__ == "__main__":
    main()
