CREATE TABLE incidents (id uuid PRIMARY KEY, incident_type text NOT NULL, severity smallint NOT NULL CHECK (severity BETWEEN 1 AND 5), latitude double precision NOT NULL, longitude double precision NOT NULL, description text NOT NULL, status text NOT NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL, version bigint NOT NULL);
CREATE TABLE incident_status_history (id bigserial PRIMARY KEY, incident_id uuid NOT NULL REFERENCES incidents(id), from_status text, to_status text NOT NULL, reason text NOT NULL, occurred_at timestamptz NOT NULL);
CREATE TABLE idempotency_keys (key text PRIMARY KEY, incident_id uuid NOT NULL REFERENCES incidents(id), created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE outbox (event_id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, event_type text NOT NULL, envelope jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz);
CREATE INDEX outbox_unpublished_idx ON outbox(created_at) WHERE published_at IS NULL;
CREATE TABLE processed_events (consumer_name text NOT NULL, event_id uuid NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(consumer_name,event_id));

