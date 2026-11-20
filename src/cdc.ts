import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

export type Change = {
  eventId: string;
  entityId: string;
  effectiveAt: string;
  sourceSequence: number;
  operation: 'upsert' | 'delete';
  attributes: { plan: string; region: string } | null;
};

export type BatchResult = {
  accepted: number;
  duplicates: number;
  quarantined: number;
  rebuiltEntities: number;
};

export const digest = (text: string): string => createHash('sha256').update(text).digest('hex');
const canonicalTime = (value: unknown): value is string => typeof value === 'string'
  && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)
  && Number.isFinite(Date.parse(value)) && new Date(value).toISOString() === value;

export function validate(raw: unknown): Change {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw new Error('event_must_be_object');
  const event = raw as Record<string, unknown>;
  const keys = Object.keys(event).sort().join(',');
  if (keys !== 'attributes,effectiveAt,entityId,eventId,operation,sourceSequence') throw new Error('unexpected_fields');
  for (const name of ['eventId', 'entityId']) {
    if (typeof event[name] !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/.test(event[name] as string)) throw new Error('invalid_identity');
  }
  if (!canonicalTime(event.effectiveAt)) {
    throw new Error('effective_time_requires_canonical_utc');
  }
  if (!Number.isSafeInteger(event.sourceSequence) || (event.sourceSequence as number) < 0) throw new Error('invalid_source_sequence');
  if (event.operation !== 'upsert' && event.operation !== 'delete') throw new Error('invalid_operation');
  let attributes: { plan: string; region: string } | null = null;
  if (event.operation === 'delete') {
    if (event.attributes !== null) throw new Error('delete_requires_null_attributes');
  } else {
    if (!event.attributes || typeof event.attributes !== 'object' || Array.isArray(event.attributes)) throw new Error('invalid_attributes');
    const attrs = event.attributes as Record<string, unknown>;
    if (Object.keys(attrs).sort().join(',') !== 'plan,region') throw new Error('unexpected_attribute_fields');
    if (!['free', 'pro', 'enterprise'].includes(attrs.plan as string) || !['NA', 'EU', 'APAC'].includes(attrs.region as string)) {
      throw new Error('invalid_attribute_value');
    }
    attributes = { plan: attrs.plan as string, region: attrs.region as string };
  }
  return { eventId: event.eventId as string, entityId: event.entityId as string,
    effectiveAt: event.effectiveAt, sourceSequence: event.sourceSequence as number,
    operation: event.operation, attributes };
}

export function canonical(event: Change): string {
  // Explicit property order makes content identity independent of input object-key order.
  return JSON.stringify({ eventId: event.eventId, entityId: event.entityId,
    effectiveAt: event.effectiveAt, sourceSequence: event.sourceSequence,
    operation: event.operation, attributes: event.attributes === null ? null
      : { plan: event.attributes.plan, region: event.attributes.region } });
}

export function openStore(path: string): DatabaseSync {
  const db = new DatabaseSync(path);
  try {
    const existing = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'").get();
    if (existing) {
      const versions = db.prepare('SELECT version FROM schema_version').all();
      if (versions.length !== 1 || versions[0].version !== 1) throw new Error('unsupported_schema_version');
    } else if (db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'").get()) {
      throw new Error('unversioned_existing_database');
    }
    db.exec(readFileSync(new URL('../sql/schema.sql', import.meta.url), 'utf8'));
    return db;
  } catch (error) { db.close(); throw error; }
}

export function ingest(db: DatabaseSync, rows: unknown[], batchId: string,
  fault?: (phase: 'after_inbox' | 'before_commit') => void): BatchResult {
  const result: BatchResult = { accepted: 0, duplicates: 0, quarantined: 0, rebuiltEntities: 0 };
  const affected = new Set<string>();
  const receivedAt = new Date().toISOString(); // Operational arrival clock; never used as validity time.
  db.exec('BEGIN IMMEDIATE');
  try {
    const quarantine = db.prepare(`INSERT OR IGNORE INTO quarantine
      (event_id, reason, candidate_json, candidate_sha256, existing_sha256, batch_id, received_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)`);
    for (const raw of rows) {
      let event: Change;
      try { event = validate(raw); }
      catch (error) {
        const text = JSON.stringify(raw) ?? 'null';
        const id = raw && typeof raw === 'object' && typeof (raw as Record<string,unknown>).eventId === 'string'
          ? (raw as Record<string,unknown>).eventId as string : '';
        const reason = error instanceof Error ? error.message : 'invalid_event';
        quarantine.run(id, reason, text, digest(text), null, batchId, receivedAt);
        result.quarantined += 1;
        continue;
      }
      const text = canonical(event);
      const hash = digest(text);
      const existing = db.prepare('SELECT canonical_json, content_sha256 FROM inbox WHERE event_id = ?').get(event.eventId) as
        { canonical_json: string; content_sha256: string } | undefined;
      if (existing) {
        if (existing.canonical_json === text) result.duplicates += 1;
        else {
          quarantine.run(event.eventId, 'immutable_event_id_conflict', text, hash, existing.content_sha256, batchId, receivedAt);
          result.quarantined += 1;
        }
        continue;
      }
      db.prepare(`INSERT INTO inbox
        (event_id, entity_id, effective_at, source_sequence, operation, plan, region,
         canonical_json, content_sha256, batch_id, received_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(event.eventId, event.entityId,
        event.effectiveAt, event.sourceSequence, event.operation, event.attributes?.plan ?? null,
        event.attributes?.region ?? null, text, hash, batchId, receivedAt);
      result.accepted += 1;
      affected.add(event.entityId);
    }
    fault?.('after_inbox');
    const insert = db.prepare(`INSERT INTO customer_history
      (entity_id, valid_from, valid_to, is_deleted, plan, region, source_event_id, source_sequence)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const entity of affected) {
      // At one effective instant, largest source sequence then ASCII event ID wins.
      const accepted = db.prepare(`SELECT * FROM inbox WHERE entity_id = ?
        ORDER BY effective_at ASC, source_sequence DESC, event_id COLLATE BINARY DESC`).all(entity) as
        Array<Record<string, string | number | null>>;
      const winners = accepted.filter((row, index) => index === 0 || row.effective_at !== accepted[index - 1].effective_at);
      db.prepare('DELETE FROM customer_history WHERE entity_id = ?').run(entity);
      for (let index = 0; index < winners.length; index += 1) {
        const row = winners[index];
        insert.run(entity, row.effective_at, winners[index + 1]?.effective_at ?? null,
          row.operation === 'delete' ? 1 : 0, row.plan, row.region, row.event_id, row.source_sequence);
      }
    }
    result.rebuiltEntities = affected.size;
    fault?.('before_commit');
    db.exec('COMMIT');
    return result;
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
}

export function history(db: DatabaseSync): Array<Record<string, unknown>> {
  return db.prepare('SELECT * FROM customer_history ORDER BY entity_id, valid_from').all().map(row => ({ ...row }));
}

export function atTime(db: DatabaseSync, entity: string, instant: string): Record<string, unknown> | undefined {
  if (!canonicalTime(instant)) throw new Error('invalid_asof_time');
  const row = db.prepare(`SELECT * FROM customer_history
    WHERE entity_id = ? AND valid_from <= ? AND (valid_to IS NULL OR ? < valid_to)`)
    .get(entity, instant, instant) as Record<string, unknown> | undefined;
  return row ? { ...row } : undefined;
}
