import path from 'path';

const PROJECT_ROOT = path.resolve(import.meta.dir, '..');

const configuredDataDir = process.env.VITALS_DATA_DIR?.trim();
const PROJECT_DATA_DIR = configuredDataDir
    ? (path.isAbsolute(configuredDataDir)
        ? configuredDataDir
        : path.resolve(PROJECT_ROOT, configuredDataDir))
    : path.join(PROJECT_ROOT, 'data');

const PROJECT_TO_IMPORT_DIR = path.join(PROJECT_DATA_DIR, 'to-import');
const PROJECT_GLOSSARY_PATH = path.join(PROJECT_ROOT, 'server/src/bloodwork-glossary.json');

export {
    PROJECT_ROOT,
    PROJECT_DATA_DIR,
    PROJECT_TO_IMPORT_DIR,
    PROJECT_GLOSSARY_PATH,
};
