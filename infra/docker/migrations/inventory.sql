CREATE TABLE resources (id text PRIMARY KEY, kind text NOT NULL, available boolean NOT NULL, reserved_for uuid, reservation_expires_at timestamptz, version bigint NOT NULL DEFAULT 1);
CREATE TABLE outbox (event_id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, event_type text NOT NULL, envelope jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz);
CREATE TABLE processed_events (consumer_name text NOT NULL, event_id uuid NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(consumer_name,event_id));

