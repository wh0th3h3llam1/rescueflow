CREATE TABLE notification_attempts (id bigserial PRIMARY KEY, event_id uuid NOT NULL, incident_id uuid NOT NULL, channel text NOT NULL, attempt integer NOT NULL, status text NOT NULL, failure_reason text, attempted_at timestamptz NOT NULL, UNIQUE(event_id,channel,attempt));
CREATE TABLE successful_notifications (event_id uuid NOT NULL, channel text NOT NULL, sent_at timestamptz NOT NULL, PRIMARY KEY(event_id,channel));
CREATE TABLE processed_events (consumer_name text NOT NULL, event_id uuid NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(consumer_name,event_id));
