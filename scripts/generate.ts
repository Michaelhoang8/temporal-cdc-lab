import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { canonical, digest } from '../src/cdc.ts';
import type { Change } from '../src/cdc.ts';

export function shuffle<T>(rows: T[], seed: number): T[] {
  const result = [...rows];
  let state = seed >>> 0;
  for (let i = result.length - 1; i > 0; i -= 1) {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    const j = Math.floor(state / 4294967296 * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
}

export function fixtures(count = 80): { canonical: Change[]; arrivals: unknown[]; initialCount: number } {
  const all: Change[] = [];
  const initial: Change[] = [];
  const late: Change[] = [];
  for (let n = 1; n <= count; n += 1) {
    const entityId = `customer-${String(n).padStart(4, '0')}`;
    const event = (suffix: string, day: number, sequence: number, plan: string | null, region = 'NA'): Change => ({
      eventId: `${entityId}-${suffix}`, entityId,
      effectiveAt: new Date(Date.UTC(2025, 0, 1 + day)).toISOString(), sourceSequence: sequence,
      operation: plan === null ? 'delete' : 'upsert', attributes: plan === null ? null : { plan, region },
    });
    const base = [event('00', 0, 1, 'free'), event('10', 10, 2, 'pro'),
      event('20', 20, 3, 'pro', 'EU'), event('30', 30, 4, null),
      event('40', 40, 5, 'free', 'APAC'), event('50', 50, 6, 'enterprise'),
      event('60', 60, 7, n % 8 === 0 ? null : 'pro')];
    const corrections = [event('15-late', 15, 8, 'enterprise', 'EU'),
      event('20-rev', 20, 9, 'enterprise', 'APAC'), event('20-z', 20, 9, 'pro', 'APAC')];
    all.push(...base, ...corrections);
    initial.push(...base);
    late.push(...corrections);
  }
  const first = shuffle(initial, 481516);
  const duplicateRows = first.slice(0, Math.min(100, first.length));
  const arrivals: unknown[] = [...first, ...shuffle(late, 2342), ...duplicateRows];
  // Conflicts follow their genuine originals deliberately. Reversing conflicting
  // immutable IDs is a different accepted input set, not an order-invariance case.
  for (const original of first.slice(0, Math.min(12, first.length))) {
    arrivals.push({ ...original, sourceSequence: original.sourceSequence + 100 });
  }
  arrivals.push({ ...all[0], eventId: 'invalid-time', effectiveAt: '2025-01-01' });
  arrivals.push({ ...all[0], eventId: 'invalid-plan', attributes: { plan: 'unknown', region: 'NA' } });
  arrivals.push({ ...all[0], eventId: 'invalid-field', debug: true });
  return { canonical: all, arrivals, initialCount: first.length };
}

if (process.argv[1]?.endsWith('generate.ts')) {
  const data = fixtures();
  const destination = new URL('../data/', import.meta.url);
  mkdirSync(destination, { recursive: true });
  const canonicalText = data.canonical.map(canonical).join('\n') + '\n';
  const arrivalText = data.arrivals.map(row => JSON.stringify(row)).join('\n') + '\n';
  writeFileSync(new URL('canonical.jsonl', destination), canonicalText);
  writeFileSync(new URL('arrivals.jsonl', destination), arrivalText);
  writeFileSync(new URL('manifest.json', destination), JSON.stringify({ synthetic: true, generatorSeed: 481516,
    entities: 80, canonicalEvents: data.canonical.length, arrivalRows: data.arrivals.length,
    initialCount: data.initialCount, lateCorrectionRows: 240, identicalDuplicateRows: 100,
    conflictingRows: 12, invalidRows: 3, canonicalSha256: digest(canonicalText), arrivalsSha256: digest(arrivalText),
    timestampMeaning: 'effectiveAt is synthetic business validity time; received_at is measured replay wall time',
  }, null, 2) + '\n');
  console.log(`Generated ${data.canonical.length} canonical events and ${data.arrivals.length} arrivals.`);
}
