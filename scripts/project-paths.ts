import path from 'path';

const PROJECT_ROOT = path.resolve(import.meta.dir, '..');

const configuredDataDir = process.env.VITALS_DATA_DIR?.trim();
const PROJECT_DATA_DIR = configuredDataDir
	? path.isAbsolute(configuredDataDir)
		? configuredDataDir
		: path.resolve(PROJECT_ROOT, configuredDataDir)
	: path.join(PROJECT_ROOT, 'data');

const PROJECT_TO_IMPORT_DIR = path.join(PROJECT_DATA_DIR, 'to-import');
const configuredDbPath = process.env.VITALS_DB_PATH?.trim();
const PROJECT_DB_PATH = configuredDbPath
	? path.isAbsolute(configuredDbPath)
		? configuredDbPath
		: path.resolve(PROJECT_ROOT, configuredDbPath)
	: path.join(PROJECT_DATA_DIR, 'vitals.sqlite');

export { PROJECT_ROOT, PROJECT_TO_IMPORT_DIR, PROJECT_DB_PATH };
