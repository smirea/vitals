import path from 'path';

import env from '../server/src/env.ts';

const PROJECT_ROOT = path.resolve(import.meta.dir, '..');

const PROJECT_DATA_DIR = path.isAbsolute(env.VITALS_DATA_DIR)
	? env.VITALS_DATA_DIR
	: path.resolve(PROJECT_ROOT, env.VITALS_DATA_DIR);

const PROJECT_TO_IMPORT_DIR = path.join(PROJECT_DATA_DIR, 'to-import');
const PROJECT_DB_PATH = path.isAbsolute(env.VITALS_DB_PATH)
	? env.VITALS_DB_PATH
	: path.resolve(PROJECT_ROOT, env.VITALS_DB_PATH);

export { PROJECT_ROOT, PROJECT_TO_IMPORT_DIR, PROJECT_DB_PATH };
