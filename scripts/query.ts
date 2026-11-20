import { DatabaseSync } from 'node:sqlite';
import { atTime } from '../src/cdc.ts';
const usage = 'Usage: npm run query -- <database.sqlite> <entity-id> <YYYY-MM-DDTHH:mm:ss.sssZ>';
const example = 'Example: npm run query -- results/store.sqlite customer-0001 2025-01-16T00:00:00.000Z';

function main(): void {
  const args = process.argv.slice(2);
  if (args.length === 1 && (args[0] === '--help' || args[0] === '-h')) {
    console.log(`${usage}\n${example}\nOpens an existing database read-only. Returns active, deleted, or not_yet_known.`);
    return;
  }
  if (args.length !== 3 || args.some(arg => arg.startsWith('--')) || !/^[A-Za-z0-9_-]{1,64}$/.test(args[1])) {
    console.error(`Invalid query arguments.\n${usage}`);
    process.exitCode = 2;
    return;
  }
  const [file, entity, instant] = args;
  let db: DatabaseSync | undefined;
  try {
    db = new DatabaseSync(file, { readOnly: true });
    const result = atTime(db, entity, instant);
    console.log(JSON.stringify({ entity, instant, status: !result ? 'not_yet_known' : result.is_deleted ? 'deleted' : 'active', result: result ?? null }, null, 2));
  } catch (error) {
    console.error(`Query failed: ${error instanceof Error ? error.message : 'unknown error'}`);
    process.exitCode = 2;
  } finally { db?.close(); }
}
main();
