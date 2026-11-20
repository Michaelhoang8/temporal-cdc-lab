-- Run against results/store.sqlite using a SQLite client.
SELECT COUNT(*) AS accepted_events FROM inbox;
SELECT COUNT(*) AS history_intervals FROM customer_history;
SELECT COUNT(*) AS active_customers FROM active_customers;
SELECT reason, COUNT(*) AS distinct_candidates FROM quarantine GROUP BY reason;

-- Exact boundary: the new interval applies at its start.
SELECT * FROM customer_history
WHERE entity_id = 'customer-0001'
  AND valid_from <= '2025-01-16T00:00:00.000Z'
  AND (valid_to IS NULL OR '2025-01-16T00:00:00.000Z' < valid_to);

-- Compare business validity with operational ingestion time.
SELECT event_id, effective_at, received_at, ingest_sequence, source_sequence
FROM inbox WHERE entity_id = 'customer-0001'
ORDER BY ingest_sequence;

-- Superseded same-time candidates remain inspectable in the inbox.
SELECT event_id, source_sequence, plan, region
FROM inbox WHERE entity_id = 'customer-0001'
  AND effective_at = '2025-01-21T00:00:00.000Z'
ORDER BY source_sequence DESC, event_id COLLATE BINARY DESC;

PRAGMA integrity_check;
PRAGMA foreign_key_check;
