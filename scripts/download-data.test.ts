import fs from 'fs';
import os from 'os';
import path from 'path';

import { describe, expect, test } from 'bun:test';

import { shouldDownload, toLocalPath } from './download-data.ts';

describe('toLocalPath', () => {
    test('maps object key to data directory path', () => {
        const localPath = toLocalPath('vitals/bloodwork_2025-01-01_lab.json', 'vitals');
        expect(localPath.endsWith(path.join('data', 'bloodwork_2025-01-01_lab.json'))).toBe(true);
    });
});

describe('shouldDownload', () => {
    test('returns true when no cached state exists', () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'vitals-sync-'));
        const localPath = path.join(tmpDir, 'file.json');
        const value = shouldDownload({
            object: {
                ETag: '"abc"',
                LastModified: new Date('2026-01-01T00:00:00.000Z'),
                Size: 10,
            },
            stateEntry: undefined,
            localPath,
        });

        expect(value).toBe(true);
    });

    test('returns false when state and local file match', () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'vitals-sync-'));
        const localPath = path.join(tmpDir, 'file.json');
        fs.writeFileSync(localPath, '{}', 'utf8');

        const value = shouldDownload({
            object: {
                ETag: '"abc"',
                LastModified: new Date('2026-01-01T00:00:00.000Z'),
                Size: 10,
            },
            stateEntry: {
                etag: 'abc',
                lastModified: '2026-01-01T00:00:00.000Z',
                size: 10,
            },
            localPath,
        });

        expect(value).toBe(false);
    });
});
