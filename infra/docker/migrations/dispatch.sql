CREATE TABLE responders (id text PRIMARY KEY, name text NOT NULL, capabilities text[] NOT NULL, available boolean NOT NULL, latitude double precision NOT NULL, longitude double precision NOT NULL, workload integer NOT NULL DEFAULT 0, version bigint NOT NULL DEFAULT 1);
CREATE TABLE assignments (id uuid PRIMARY KEY, incident_id uuid NOT NULL, responder_id text NOT NULL REFERENCES responders(id), status text NOT NULL, score double precision NOT NULL, expires_at timestamptz NOT NULL, UNIQUE(incident_id) DEFERRABLE INITIALLY IMMEDIATE);
CREATE TABLE outbox (event_id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, event_type text NOT NULL, envelope jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz);
CREATE TABLE processed_events (consumer_name text NOT NULL, event_id uuid NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(consumer_name,event_id));

