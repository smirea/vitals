import path from 'path';
import { fileURLToPath } from 'url';

import { defineConfig } from 'drizzle-kit';

import env from './server/src/env.ts';

const projectRoot = path.dirname(fileURLToPath(import.meta.url));
const dbPath = path.isAbsolute(env.VITALS_DB_PATH)
	? env.VITALS_DB_PATH
	: path.resolve(projectRoot, env.VITALS_DB_PATH);

export default defineConfig({
	schema: './server/src/db/schema.ts',
	dialect: 'sqlite',
	dbCredentials: {
		url: dbPath,
	},
});
