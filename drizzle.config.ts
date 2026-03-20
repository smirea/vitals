import path from 'path';
import { fileURLToPath } from 'url';

import { defineConfig } from 'drizzle-kit';

const projectRoot = path.dirname(fileURLToPath(import.meta.url));
const configuredDbPath = process.env.VITALS_DB_PATH?.trim();
const dbPath = configuredDbPath
	? path.isAbsolute(configuredDbPath)
		? configuredDbPath
		: path.resolve(projectRoot, configuredDbPath)
	: path.join(projectRoot, 'data', 'vitals.sqlite');

export default defineConfig({
	schema: './server/src/db/schema.ts',
	dialect: 'sqlite',
	dbCredentials: {
		url: dbPath,
	},
});
