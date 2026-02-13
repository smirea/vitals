import fs from 'fs';
import path from 'path';

import { runDownloadDataSync } from '../../scripts/download-data.ts';
import { PROJECT_DATA_DIR } from '../../scripts/project-paths.ts';
import { bloodworkLabSchema, type BloodworkLab } from '../../scripts/bloodwork-schema.ts';

if (!process.env.API_PORT) throw new Error('process.env.API_PORT is not set');

const shouldRunStartupSync = process.env.VITALS_DISABLE_STARTUP_SYNC !== 'true';
if (shouldRunStartupSync) {
    await runDownloadDataSync();
}

const DATA_DIR = PROJECT_DATA_DIR;

function listBloodworkJsonFiles(rootDir: string): string[] {
    if (!fs.existsSync(rootDir)) return [];

    const stack = [rootDir];
    const files: string[] = [];

    while (stack.length > 0) {
        const current = stack.pop();
        if (!current) continue;

        for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
            const fullPath = path.join(current, entry.name);
            if (entry.isDirectory()) {
                stack.push(fullPath);
                continue;
            }
            if (entry.isFile() && /^bloodwork_.*\.json$/i.test(entry.name)) {
                files.push(fullPath);
            }
        }
    }

    return files.sort((left, right) => right.localeCompare(left));
}

function loadBloodworkLabs(): BloodworkLab[] {
    const files = listBloodworkJsonFiles(DATA_DIR);
    const labs: BloodworkLab[] = [];

    for (const filePath of files) {
        try {
            const raw = JSON.parse(fs.readFileSync(filePath, 'utf8')) as unknown;
            const parsed = bloodworkLabSchema.parse(raw);
            labs.push(parsed);
        } catch (error) {
            console.error(`Skipping invalid bloodwork file: ${filePath}`, error);
        }
    }

    return labs.sort((a, b) => b.date.localeCompare(a.date));
}

const server = Bun.serve({
    development: true,
    port: process.env.API_PORT,
    routes: {
        '/status': Response.json({ ok: true }),
        '/bloodwork': () => Response.json({ items: loadBloodworkLabs() }),
        '/*': Response.json({ ok: false, error: 'Not found' }, { status: 404 }),
    },
});

console.log('Server running at:', server.url);
