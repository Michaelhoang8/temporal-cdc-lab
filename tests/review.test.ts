import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { atTime, history, ingest, openStore } from '../src/cdc.ts';

test('as-of queries reject expanded ISO years outside the event-time domain', () => {
  const db = openStore(':memory:');
  try {
    for (const instant of ['+010000-01-01T00:00:00.000Z', '-000001-01-01T00:00:00.000Z']) {
      assert.throws(() => atTime(db, 'entity', instant), /invalid_asof_time/);
    }
  } finally {
    db.close();
  }
});

test('opening an unsupported database version leaves its schema intact', () => {
  const folder = mkdtempSync(join(tmpdir(), 'cdc-review-'));
  const path = join(folder, 'future.sqlite');
  try {
    const original = new DatabaseSync(path);
    original.exec('CREATE TABLE schema_version (version INTEGER NOT NULL); INSERT INTO schema_version VALUES (999); CREATE TABLE future_data (value TEXT); INSERT INTO future_data VALUES (\'retain\');');
    const before = original.prepare('SELECT type, name, sql FROM sqlite_master ORDER BY name').all();
    original.close();
    assert.throws(() => openStore(path), /unsupported_schema_version/);
    const after = new DatabaseSync(path);
    try {
      assert.deepEqual(after.prepare('SELECT type, name, sql FROM sqlite_master ORDER BY name').all(), before);
      assert.equal(after.prepare('SELECT value FROM future_data').get()?.value, 'retain');
    } finally {
      after.close();
    }
  } finally {
    // mkdtempSync creates this exact dedicated review directory under the temp root.
    rmSync(folder, { recursive: true, force: true });
  }
});

test('same-instant event-ID ties use ASCII binary order independent of arrival order', () => {
  const ids = ['Z', '_', 'a', '9'];
  for (const order of [ids, [...ids].reverse(), ['_', '9', 'Z', 'a']]) {
    const db = openStore(':memory:');
    try {
      for (const eventId of order) {
        ingest(db, [{ eventId, entityId: 'case_test', effectiveAt: '2026-01-01T00:00:00.000Z',
          sourceSequence: 3, operation: eventId === 'a' ? 'delete' : 'upsert',
          attributes: eventId === 'a' ? null : { plan: 'pro', region: 'EU' } }], `batch_${eventId}`);
      }
      const rows = history(db);
      assert.equal(rows.length, 1);
      assert.equal(rows[0].source_event_id, 'a');
      assert.equal(rows[0].is_deleted, 1);
      assert.equal(atTime(db, 'case_test', '2025-12-31T23:59:59.999Z'), undefined);
      assert.equal(atTime(db, 'case_test', '2026-01-01T00:00:00.000Z')?.is_deleted, 1);
    } finally {
      db.close();
    }
  }
});
