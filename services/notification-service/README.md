# Notification service boundary

Owns simulated email/SMS/push attempts and successful-delivery uniqueness. Temporary failures retry; exhausted events preserve failure metadata in DLQ.
