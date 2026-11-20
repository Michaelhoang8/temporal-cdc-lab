import { test } from 'node:test';
import assert from 'node:assert/strict';
import { openStore, ingest, history, atTime, validate, canonical } from '../src/cdc.ts';
import type { Change } from '../src/cdc.ts';
import { referenceHistory, referenceAt } from '../src/reference.ts';
import { fixtures, shuffle } from '../scripts/generate.ts';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const event = (id: string, day: number, seq = day, plan: string | null = 'pro'): Change => ({
  eventId: id, entityId: 'customer', effectiveAt: new Date(Date.UTC(2025, 0, 1 + day)).toISOString(),
  sourceSequence: seq, operation: plan === null ? 'delete' : 'upsert',
  attributes: plan === null ? null : { plan, region: 'NA' },
});
const instant = (day: number) => new Date(Date.UTC(2025, 0, 1 + day)).toISOString();
const count = (db: ReturnType<typeof openStore>, table: string) => Number(db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get()!.n);

test('late arrival splits an earlier interval without changing later versions', () => {
  const db = openStore(':memory:');
  try {
    ingest(db, [event('start', 0, 1, 'free'), event('later', 20)], 'initial');
    const result = ingest(db, [event('late', 10, 30, 'enterprise')], 'late');
    assert.equal(result.rebuiltEntities, 1);
    assert.deepEqual(history(db).map(row => [row.valid_from, row.valid_to, row.plan]),
      [[instant(0), instant(10), 'free'], [instant(10), instant(20), 'enterprise'], [instant(20), null, 'pro']]);
  } finally { db.close(); }
});

test('half-open point-in-time boundaries include tombstones and recreation', () => {
  const db = openStore(':memory:');
  const events = [event('start', 0), event('deleted', 10, 2, null), event('recreated', 20)];
  try {
    ingest(db, events, 'boundaries');
    assert.equal(atTime(db, 'customer', instant(-1)), undefined);
    assert.equal(atTime(db, 'customer', '2025-01-10T23:59:59.999Z')!.is_deleted, 0);
    assert.equal(atTime(db, 'customer', instant(10))!.is_deleted, 1);
    assert.equal(atTime(db, 'customer', instant(20))!.is_deleted, 0);
    assert.equal(atTime(db, 'missing', instant(0)), undefined);
    assert.throws(() => atTime(db, 'customer', '2025-01-01'), /invalid_asof_time/);
  } finally { db.close(); }
});

test('same-time winner uses largest sequence then ASCII identity, independent of arrival', () => {
  const rows = [event('Z', 0, 5), event('a', 0, 5, 'enterprise'), event('z', 0, 4, 'free')];
  for (const order of [rows, [...rows].reverse()]) {
    const db = openStore(':memory:');
    try {
      for (const row of order) ingest(db, [row], 'tie');
      assert.equal(history(db).length, 1);
      assert.equal(history(db)[0].source_event_id, 'a');
      assert.deepEqual(history(db), referenceHistory(rows));
    } finally { db.close(); }
  }
});

test('identical duplicate with reordered JSON keys does not rebuild or overwrite receipt metadata', () => {
  const db = openStore(':memory:');
  try {
    const original = event('one', 0);
    ingest(db, [original], 'first');
    const { attributes, ...rest } = original;
    const duplicate = { attributes: { region: attributes!.region, plan: attributes!.plan }, ...rest };
    assert.deepEqual(ingest(db, [duplicate], 'repeat'), { accepted: 0, duplicates: 1, quarantined: 0, rebuiltEntities: 0 });
    assert.equal(count(db, 'inbox'), 1);
    assert.equal(db.prepare('SELECT batch_id FROM inbox').get()!.batch_id, 'first');
  } finally { db.close(); }
});

test('immutable ID conflicts are quarantined and original history is retained', () => {
  const db = openStore(':memory:');
  try {
    const original = event('one', 0);
    ingest(db, [original], 'first');
    const bad = { ...original, entityId: 'other', sourceSequence: 999 };
    assert.equal(ingest(db, [bad, bad], 'conflict').quarantined, 2);
    assert.equal(count(db, 'quarantine'), 1);
    assert.equal(count(db, 'inbox'), 1);
    assert.equal(history(db)[0].entity_id, 'customer');
    assert.equal(db.prepare('SELECT reason FROM quarantine').get()!.reason, 'immutable_event_id_conflict');
  } finally { db.close(); }
});

test('first-accepted conflict policy intentionally does not promise conflicting-order invariance', () => {
  const original = event('one', 0, 1, 'free');
  const candidate = event('one', 0, 2, 'enterprise');
  const outputs: unknown[] = [];
  for (const rows of [[original, candidate], [candidate, original]]) {
    const db = openStore(':memory:');
    try { ingest(db, rows, 'conflicting'); outputs.push(history(db)[0].plan); }
    finally { db.close(); }
  }
  assert.deepEqual(outputs, ['free', 'enterprise']);
});

for (const phase of ['after_inbox', 'before_commit'] as const) {
  test(`transaction rollback at ${phase} restores inbox, history and quarantine`, () => {
    const db = openStore(':memory:');
    try {
      ingest(db, [event('base', 0)], 'base');
      const before = history(db);
      assert.throws(() => ingest(db, [event('new', 10), { invalid: true }], 'fault', current => {
        if (current === phase) throw new Error('injected_failure');
      }), /injected_failure/);
      assert.deepEqual(history(db), before);
      assert.equal(count(db, 'inbox'), 1);
      assert.equal(count(db, 'quarantine'), 0);
      assert.equal(ingest(db, [event('new', 10)], 'retry').accepted, 1);
    } finally { db.close(); }
  });
}

test('validation quarantines malformed timestamps, schema and unsafe sequence values', () => {
  const base = event('valid', 0);
  const malformed = [null, [], {}, { ...base, effectiveAt: '2025-02-30T00:00:00.000Z' },
    { ...base, effectiveAt: '2025-01-01T01:00:00.000+01:00' }, { ...base, sourceSequence: Number.MAX_SAFE_INTEGER + 1 },
    { ...base, eventId: 'non-ascii-é' }, { ...base, attributes: { plan: 'pro', region: 'NA', extra: true } },
    { ...base, operation: 'delete' }, { ...base, extra: 1 }];
  const db = openStore(':memory:');
  try {
    assert.equal(ingest(db, malformed, 'invalid').quarantined, malformed.length);
    assert.equal(count(db, 'inbox'), 0);
    assert.equal(count(db, 'customer_history'), 0);
  } finally { db.close(); }
});

test('24 nonconflicting permutations and batch sizes reconcile to independent history and as-of oracle', () => {
  const data = fixtures(8);
  const expected = referenceHistory(data.canonical);
  for (let seed = 1; seed <= 6; seed += 1) {
    const order = shuffle([...data.canonical, ...data.canonical.slice(0, 7)], seed);
    for (const size of [1, 7, 31, 1000]) {
      const db = openStore(':memory:');
      try {
        for (let offset = 0; offset < order.length; offset += size) ingest(db, order.slice(offset, offset + size), `chunk-${offset}`);
        assert.deepEqual(history(db), expected);
        assert.equal(db.prepare(readFileSync(new URL('../sql/reconciliation.sql', import.meta.url), 'utf8')).all().length, 0);
        for (const entity of ['customer-0001', 'customer-0008']) {
          for (const day of [-1, 0, 10, 15, 20, 30, 35, 40, 60, 70]) {
            assert.equal(atTime(db, entity, instant(day))?.source_event_id, referenceAt(data.canonical, entity, instant(day))?.eventId);
          }
        }
      } finally { db.close(); }
    }
  }
});

test('file-backed close and reopen preserves canonical state and duplicate protection', () => {
  const dir = mkdtempSync(join(tmpdir(), 'temporal-cdc-test-'));
  const file = join(dir, 'store.sqlite');
  try {
    let db = openStore(file);
    ingest(db, [event('one', 0)], 'first');
    db.close();
    db = openStore(file);
    try {
      assert.equal(history(db)[0].source_event_id, 'one');
      assert.equal(ingest(db, [event('one', 0)], 'retry').duplicates, 1);
      assert.equal(db.prepare('PRAGMA integrity_check').get()!.integrity_check, 'ok');
    } finally { db.close(); }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('empty batch is a valid no-op', () => {
  const db = openStore(':memory:');
  try { assert.deepEqual(ingest(db, [], 'empty'), { accepted: 0, duplicates: 0, quarantined: 0, rebuiltEntities: 0 }); }
  finally { db.close(); }
});
