PRAGMA foreign_keys = ON;
PRAGMA journal_mode = DELETE;
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);
INSERT INTO schema_version SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM schema_version);

CREATE TABLE IF NOT EXISTS inbox (
  ingest_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  entity_id TEXT NOT NULL,
  effective_at TEXT NOT NULL,
  source_sequence INTEGER NOT NULL CHECK (source_sequence >= 0),
  operation TEXT NOT NULL CHECK (operation IN ('upsert', 'delete')),
  plan TEXT,
  region TEXT,
  canonical_json TEXT NOT NULL,
  content_sha256 TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  received_at TEXT NOT NULL,
  CHECK ((operation = 'delete' AND plan IS NULL AND region IS NULL)
      OR (operation = 'upsert' AND plan IS NOT NULL AND region IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS inbox_entity_time ON inbox(entity_id, effective_at, source_sequence, event_id);

CREATE TABLE IF NOT EXISTS quarantine (
  quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  candidate_json TEXT NOT NULL,
  candidate_sha256 TEXT NOT NULL,
  existing_sha256 TEXT,
  batch_id TEXT NOT NULL,
  received_at TEXT NOT NULL,
  UNIQUE (event_id, reason, candidate_sha256)
);

CREATE TABLE IF NOT EXISTS customer_history (
  entity_id TEXT NOT NULL,
  valid_from TEXT NOT NULL,
  valid_to TEXT,
  is_deleted INTEGER NOT NULL CHECK (is_deleted IN (0, 1)),
  plan TEXT,
  region TEXT,
  source_event_id TEXT NOT NULL REFERENCES inbox(event_id),
  source_sequence INTEGER NOT NULL,
  PRIMARY KEY (entity_id, valid_from),
  CHECK (valid_to IS NULL OR valid_to > valid_from),
  CHECK ((is_deleted = 1 AND plan IS NULL AND region IS NULL)
      OR (is_deleted = 0 AND plan IS NOT NULL AND region IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_interval ON customer_history(entity_id) WHERE valid_to IS NULL;
CREATE VIEW IF NOT EXISTS active_customers AS
  SELECT entity_id, plan, region, valid_from, source_event_id
  FROM customer_history WHERE valid_to IS NULL AND is_deleted = 0;
