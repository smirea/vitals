import fs from 'fs';
import path from 'path';

import { Database } from 'bun:sqlite';
import { drizzle, type BunSQLiteDatabase } from 'drizzle-orm/bun-sqlite';

import { PROJECT_DB_PATH } from 'scripts/project-paths.ts';
import { schema } from 'server/db/schema.ts';

export type VitalsDatabase = BunSQLiteDatabase<typeof schema> & {
	$client: Database;
};

function createSqliteClient(dbPath: string): Database {
	fs.mkdirSync(path.dirname(dbPath), { recursive: true });

	const client = new Database(dbPath, {
		create: true,
		strict: true,
		safeIntegers: false,
	});

	client.exec('PRAGMA foreign_keys = ON');
	client.exec('PRAGMA journal_mode = WAL');

	return client;
}

export function createDatabase(dbPath = PROJECT_DB_PATH): VitalsDatabase {
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
