import type { Change } from './cdc.ts';

// An independent in-memory oracle. It uses set enumeration and a reducer rather
// than the database's ordered query / adjacent-row selection implementation.
export function referenceHistory(events: Change[]): Array<Record<string, unknown>> {
  const result: Array<Record<string, unknown>> = [];
  const entities = [...new Set(events.map(event => event.entityId))].sort();
  for (const entity of entities) {
    const candidates = events.filter(event => event.entityId === entity);
    const times = [...new Set(candidates.map(event => event.effectiveAt))].sort();
    for (let index = 0; index < times.length; index += 1) {
      const sameTime = candidates.filter(event => event.effectiveAt === times[index]);
      const winner = sameTime.reduce((best, event) =>
        event.sourceSequence > best.sourceSequence || event.sourceSequence === best.sourceSequence && event.eventId > best.eventId
          ? event : best);
      result.push({ entity_id: entity, valid_from: times[index], valid_to: times[index + 1] ?? null,
        is_deleted: winner.operation === 'delete' ? 1 : 0, plan: winner.attributes?.plan ?? null,
        region: winner.attributes?.region ?? null, source_event_id: winner.eventId, source_sequence: winner.sourceSequence });
    }
  }
  return result;
}

export function referenceAt(events: Change[], entity: string, instant: string): Change | undefined {
  // Point-in-time result directly from source events; no history intervals involved.
  const eligible = events.filter(event => event.entityId === entity && event.effectiveAt <= instant);
  if (!eligible.length) return undefined;
  return eligible.reduce((best, event) => event.effectiveAt > best.effectiveAt ||
    event.effectiveAt === best.effectiveAt && (event.sourceSequence > best.sourceSequence ||
    event.sourceSequence === best.sourceSequence && event.eventId > best.eventId) ? event : best);
}
