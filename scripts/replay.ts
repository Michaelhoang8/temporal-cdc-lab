import { readFileSync, writeFileSync, mkdirSync, existsSync, unlinkSync, statSync } from 'node:fs';
import { resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { platform, arch, cpus } from 'node:os';
import { createHash } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import assert from 'node:assert/strict';
import { openStore, ingest, history, atTime, digest, validate } from '../src/cdc.ts';
import { referenceHistory, referenceAt } from '../src/reference.ts';
import { shuffle } from './generate.ts';

const root = fileURLToPath(new URL('../', import.meta.url));
const args = process.argv.slice(2);
const flag = (name: string, fallback: string) => {
  const index = args.indexOf(name);
  if (index < 0) return fallback;
  if (!args[index + 1] || args[index + 1].startsWith('--')) throw new Error(`Missing ${name} value`);
  return args[index + 1];
};
const output = resolve(root, flag('--output', 'results'));
const batchSize = Number(flag('--batch-size', '40'));
if (!Number.isSafeInteger(batchSize) || batchSize < 1) throw new Error('batch-size must be a positive integer');
const quick = args.includes('--quick');
mkdirSync(output, { recursive: true });
const readLines = (name: string) => readFileSync(join(root, 'data', name), 'utf8').trim().split('\n').map(line => JSON.parse(line));
const canonical = readLines('canonical.jsonl').map(validate);
const arrivals = readLines('arrivals.jsonl');
const manifest = JSON.parse(readFileSync(join(root, 'data/manifest.json'), 'utf8'));
assert.equal(digest(readFileSync(join(root, 'data/canonical.jsonl'), 'utf8').replace(/\r\n/g, '\n')), manifest.canonicalSha256);
assert.equal(digest(readFileSync(join(root, 'data/arrivals.jsonl'), 'utf8').replace(/\r\n/g, '\n')), manifest.arrivalsSha256);
const expected = referenceHistory(canonical);
const semanticHash = (rows: unknown) => digest(JSON.stringify(rows));
const expectedHash = semanticHash(expected);
const writeJson = (name: string, data: unknown) => writeFileSync(join(output, name), JSON.stringify(data, null, 2) + '\n');
const fresh = (name: string) => {
  const path = join(output, name);
  // Only these lab-owned generated files are replaced, never a supplied database.
  if (existsSync(path)) unlinkSync(path);
  return openStore(path);
};
const db = fresh('store.sqlite');
const batches: Array<Record<string, unknown>> = [];
const totals = { accepted: 0, duplicates: 0, quarantined: 0, rebuiltEntities: 0 };
let before: Array<Record<string, unknown>> = [];
const started = performance.now();
for (const [phase, start, end] of [['initial', 0, manifest.initialCount], ['late-and-repeats', manifest.initialCount, arrivals.length]] as const) {
  for (let offset = start; offset < end; offset += batchSize) {
    const rows = arrivals.slice(offset, Math.min(end, offset + batchSize));
    const tick = performance.now();
    const result = ingest(db, rows, `${phase}-${offset}`);
    const elapsedMs = performance.now() - tick;
    batches.push({ phase, offset, rows: rows.length, ...result, elapsedMs });
    for (const key of Object.keys(totals) as Array<keyof typeof totals>) totals[key] += result[key];
  }
  if (phase === 'initial') before = history(db).filter(row => row.entity_id === 'customer-0001');
}
const replayMs = performance.now() - started;
const actual = history(db);
assert.deepEqual(actual, expected, 'Database history differs from independent reference');
const reconciliationSql = readFileSync(join(root, 'sql/reconciliation.sql'), 'utf8');
const violations = db.prepare(reconciliationSql).all();
assert.equal(violations.length, 0);
assert.equal(db.prepare('PRAGMA integrity_check').get()!.integrity_check, 'ok');
assert.equal(db.prepare('PRAGMA foreign_key_check').all().length, 0);
const entities = [...new Set(canonical.map(row => row.entityId))];
let asofChecks = 0;
for (const entity of entities) {
  for (const day of [-1, 0, 10, 15, 20, 30, 35, 40, 60, 70]) {
    const instant = new Date(Date.UTC(2025, 0, 1 + day)).toISOString();
    assert.equal(atTime(db, entity, instant)?.source_event_id, referenceAt(canonical, entity, instant)?.eventId);
    asofChecks += 1;
  }
}
const examples = [-1, 10, 15, 20, 30, 35, 40, 60].map(day => {
  const instant = new Date(Date.UTC(2025, 0, 1 + day)).toISOString();
  return { instant, result: atTime(db, 'customer-0001', instant) ?? null };
});
const quarantine = db.prepare(`SELECT event_id, reason, candidate_sha256, existing_sha256 FROM quarantine ORDER BY quarantine_id`).all();
const active = Number(db.prepare('SELECT COUNT(*) AS n FROM active_customers').get()!.n);
const receiptRange = db.prepare('SELECT MIN(received_at) AS firstReceivedAt, MAX(received_at) AS lastReceivedAt FROM inbox').get();
const retryStart = performance.now();
const repeat = ingest(db, arrivals, 'full-replay');
const repeatMs = performance.now() - retryStart;
assert.equal(repeat.accepted, 0);
assert.deepEqual(history(db), actual);
assert.equal(db.prepare('SELECT COUNT(*) AS n FROM quarantine').get()!.n, quarantine.length);
const sqliteVersion = db.prepare('SELECT sqlite_version() AS version').get()!.version;
db.close();
writeJson('history.json', actual);
writeJson('quarantine.json', quarantine);
writeJson('asof-examples.json', examples);
writeJson('timeline-example.json', { entity: 'customer-0001', before, after: actual.filter(row => row.entity_id === 'customer-0001') });
const columns = ['phase', 'offset', 'rows', 'accepted', 'duplicates', 'quarantined', 'rebuiltEntities', 'elapsedMs'];
writeFileSync(join(output, 'batch-timings.csv'), columns.join(',') + '\n' + batches.map(row => columns.map(key => row[key]).join(',')).join('\n') + '\n');

const timings: Array<Record<string, number | string>> = [];
for (let repetition = 1; repetition <= (quick ? 1 : 3); repetition += 1) {
  // Every size receives exactly the same order within a repetition. Order is
  // rotated across repetitions to avoid a fixed small-to-large measurement order.
  const sizes = [10, 80, 900];
  const schedule = sizes.slice(repetition % 3).concat(sizes.slice(0, repetition % 3));
  const input = shuffle([...canonical, ...canonical.slice(0, 100)], 1000 + repetition);
  for (const size of schedule) {
    const filename = `trial-${repetition}-${size}.sqlite`;
    const trial = fresh(filename);
    let rebuilt = 0;
    const tick = performance.now();
    for (let offset = 0; offset < input.length; offset += size) {
      rebuilt += ingest(trial, input.slice(offset, offset + size), `trial-${offset}`).rebuiltEntities;
    }
    const elapsed = performance.now() - tick;
    assert.equal(semanticHash(history(trial)), expectedHash);
    trial.close();
    timings.push({ repetition, batchSize: size, rows: input.length, transactions: Math.ceil(input.length / size),
      rebuiltEntities: rebuilt, elapsedMs: elapsed, semanticSha256: expectedHash });
    unlinkSync(join(output, filename));
  }
}
const timingColumns = ['repetition', 'batchSize', 'rows', 'transactions', 'rebuiltEntities', 'elapsedMs', 'semanticSha256'];
writeFileSync(join(output, 'replay-timings.csv'), timingColumns.join(',') + '\n' + timings.map(row => timingColumns.map(key => row[key]).join(',')).join('\n') + '\n');
const sources = ['src/cdc.ts', 'src/reference.ts', 'scripts/generate.ts', 'scripts/replay.ts', 'scripts/query.ts', 'sql/schema.sql', 'sql/reconciliation.sql'];
writeJson('environment.json', { measuredAt: new Date().toISOString(), node: process.version, sqlite: sqliteVersion,
  platform: platform(), architecture: arch(), cpuModel: cpus()[0]?.model, logicalCpus: cpus().length,
  journalMode: 'DELETE', synchronous: 'FULL', databaseType: 'file-backed', timingClock: 'performance.now',
  sourceHashEncoding: 'UTF-8, CRLF normalized to LF',
  sourceSha256: Object.fromEntries(sources.map(name => [name, digest(readFileSync(join(root, name), 'utf8').replace(/\r\n/g, '\n'))])),
});
const databaseBytes = statSync(join(output, 'store.sqlite')).size;
const databaseSha256 = createHash('sha256').update(readFileSync(join(output, 'store.sqlite'))).digest('hex');
writeJson('summary.json', { inputRows: arrivals.length, ...totals, entities: entities.length, historyRows: actual.length,
  tombstoneIntervals: actual.filter(row => row.is_deleted === 1).length, activeCustomers: active,
  distinctQuarantines: quarantine.length, reconciliationViolations: violations.length, asofChecks,
  referenceMatches: true, semanticSha256: expectedHash, databaseBytes, databaseSha256, replayMs,
  primaryBatchSize: batchSize, primaryTransactions: batches.length, ...receiptRange,
  repeat: { ...repeat, elapsedMs: repeatMs, historyUnchanged: true }, benchmark: timings,
  scope: 'Synthetic local file-backed replay; no network CDC connector, no production throughput claim.' });
console.log(JSON.stringify({ ...totals, historyRows: actual.length, asofChecks, output: output.split(/[\\/]/).pop(), replayMs }, null, 2));
