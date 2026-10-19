-- Schema v1: rider_updates with idempotent inserts and atomic NOTIFY.
-- The AFTER INSERT trigger fires pg_notify in the same transaction as the
-- INSERT, so persistence and fan-out are atomic.

DROP TRIGGER  IF EXISTS rider_updates_notify ON rider_updates;
DROP FUNCTION IF EXISTS notify_rider_update();
DROP TABLE    IF EXISTS rider_updates;

CREATE TABLE rider_updates (
    id            BIGSERIAL                    PRIMARY KEY,
    rider_name    TEXT                         NOT NULL,
    location      TEXT                         NOT NULL,
    partition     INTEGER                      NOT NULL,
    topic         TEXT                         NOT NULL,
    kafka_offset  BIGINT                       NOT NULL,
    received_at   TIMESTAMPTZ(6)               NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX rider_updates_dedup
    ON rider_updates (topic, partition, kafka_offset);

CREATE INDEX rider_updates_received_at_idx
    ON rider_updates (received_at DESC);

CREATE INDEX rider_updates_rider_name_idx
    ON rider_updates (rider_name);

CREATE OR REPLACE FUNCTION notify_rider_update() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'rider_updates_channel',
        json_build_object(
            'id',          NEW.id::text,
            'rider_name',  NEW.rider_name,
            'location',    NEW.location,
            'partition',   NEW.partition,
            'topic',       NEW.topic,
            'received_at', NEW.received_at
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER rider_updates_notify
    AFTER INSERT ON rider_updates
    FOR EACH ROW EXECUTE FUNCTION notify_rider_update();
