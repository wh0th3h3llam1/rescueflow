CREATE DATABASE dispatch;
CREATE DATABASE inventory;
CREATE DATABASE notification;
\connect incident
\i /migrations/incident.sql
\connect dispatch
\i /migrations/dispatch.sql
\connect inventory
\i /migrations/inventory.sql
\connect notification
\i /migrations/notification.sql

