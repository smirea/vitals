import fs from 'fs';
import path from 'path';

import { Database } from 'bun:sqlite';
import { drizzle, type BunSQLiteDatabase } from 'drizzle-orm/bun-sqlite';

import { schema } from 'server/db/schema.ts';
import env from 'server/env.ts';

export type VitalsDatabase = BunSQLiteDatabase<typeof schema> & {
	$client: Database;
};

const projectRoot = path.resolve(import.meta.dir, '..', '..', '..');
const defaultDbPath = path.isAbsolute(env.VITALS_DB_PATH)
	? env.VITALS_DB_PATH
	: path.resolve(projectRoot, env.VITALS_DB_PATH);

function ensureRuntimeDatabaseObjects(client: Database) {
	client.exec(`
		CREATE TRIGGER IF NOT EXISTS bloodwork_results_cleanup_measurements_after_delete
		AFTER DELETE ON bloodwork_results
		BEGIN
			DELETE FROM bloodwork_measurements
			WHERE id = OLD.measurement_id
				AND NOT EXISTS (
					SELECT 1
					FROM bloodwork_results
					WHERE measurement_id = OLD.measurement_id
				);
		END;
	`);
}

function createSqliteClient(dbPath: string): Database {
	fs.mkdirSync(path.dirname(dbPath), { recursive: true });

	const client = new Database(dbPath, {
		create: true,
		strict: true,
		safeIntegers: false,
	});

	client.exec('PRAGMA foreign_keys = ON');
	client.exec('PRAGMA journal_mode = WAL');
	ensureRuntimeDatabaseObjects(client);

	return client;
}

export function createDatabase(dbPath = defaultDbPath): VitalsDatabase {
	const client = createSqliteClient(dbPath);
	return drizzle(client, {
		schema,
		casing: 'snake_case',
	});
}

let database: VitalsDatabase | null = null;

export function getDatabase(): VitalsDatabase {
	if (!database) {
		database = createDatabase();
	}
	return database;
}
