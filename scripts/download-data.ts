import fs from 'fs';
import path from 'path';

import { GetObjectCommand, ListObjectsV2Command, S3Client, type _Object } from '@aws-sdk/client-s3';

import { createScript } from './createScript.ts';

const DEFAULT_BUCKET = 'stefan-life';
const DEFAULT_PREFIX = 'vitals';
const DEFAULT_OUTPUT_DIR = path.resolve(process.cwd(), 'data');
const SYNC_STATE_FILE = path.resolve(DEFAULT_OUTPUT_DIR, '.s3-sync-state.json');

type SyncStateEntry = {
    etag: string;
    lastModified: string;
    size: number;
};

type SyncState = {
    objects: Record<string, SyncStateEntry>;
};

type SyncSummary = {
    downloaded: number;
    skipped: number;
    scanned: number;
};

function requireEnv(name: string): string {
    const value = process.env[name]?.trim();
    if (!value) {
        throw new Error(`Missing required environment variable: ${name}`);
    }
    return value;
}

function normalizeEtag(etag: string | undefined): string {
    return (etag ?? '').replaceAll('"', '');
}

function normalizeLastModified(value: Date | undefined): string {
    return value ? value.toISOString() : '';
}

function readSyncState(): SyncState {
    if (!fs.existsSync(SYNC_STATE_FILE)) {
        return { objects: {} };
    }
    const raw = fs.readFileSync(SYNC_STATE_FILE, 'utf8');
    const parsed = JSON.parse(raw) as SyncState;
    return {
        objects: parsed.objects ?? {},
    };
}

function writeSyncState(state: SyncState): void {
    fs.mkdirSync(path.dirname(SYNC_STATE_FILE), { recursive: true });
    fs.writeFileSync(SYNC_STATE_FILE, JSON.stringify(state, null, 4), 'utf8');
}

function toLocalPath(key: string, prefix: string): string {
    const normalizedPrefix = prefix.replace(/^\/+|\/+$/g, '');
    const prefixWithSlash = normalizedPrefix ? `${normalizedPrefix}/` : '';
    const relativeKey = prefixWithSlash && key.startsWith(prefixWithSlash)
        ? key.slice(prefixWithSlash.length)
        : key;

    if (!relativeKey || relativeKey.includes('..')) {
        throw new Error(`Invalid S3 object key for local mapping: ${key}`);
    }

    return path.join(DEFAULT_OUTPUT_DIR, relativeKey);
}

function shouldDownload({
    object,
    stateEntry,
    localPath,
}: {
    object: _Object;
    stateEntry: SyncStateEntry | undefined;
    localPath: string;
}): boolean {
    if (!stateEntry) return true;
    if (!fs.existsSync(localPath)) return true;

    const remoteEtag = normalizeEtag(object.ETag);
    const remoteLastModified = normalizeLastModified(object.LastModified);
    const remoteSize = object.Size ?? 0;

    return (
        stateEntry.etag !== remoteEtag ||
        stateEntry.lastModified !== remoteLastModified ||
        stateEntry.size !== remoteSize
    );
}

async function downloadObject({
    s3Client,
    bucket,
    key,
    destinationPath,
}: {
    s3Client: S3Client;
    bucket: string;
    key: string;
    destinationPath: string;
}): Promise<void> {
    const response = await s3Client.send(
        new GetObjectCommand({
            Bucket: bucket,
            Key: key,
        }),
    );

    if (!response.Body) {
        throw new Error(`S3 object body is empty: s3://${bucket}/${key}`);
    }

    const body = Buffer.from(await response.Body.transformToByteArray());
    fs.mkdirSync(path.dirname(destinationPath), { recursive: true });
    fs.writeFileSync(destinationPath, body);
}

async function listAllObjects({
    s3Client,
    bucket,
    prefix,
}: {
    s3Client: S3Client;
    bucket: string;
    prefix: string;
}): Promise<_Object[]> {
    const collected: _Object[] = [];
    let continuationToken: string | undefined;

    do {
        const page = await s3Client.send(
            new ListObjectsV2Command({
                Bucket: bucket,
                Prefix: prefix ? `${prefix.replace(/^\/+|\/+$/g, '')}/` : undefined,
                ContinuationToken: continuationToken,
            }),
        );

        if (page.Contents) {
            collected.push(...page.Contents.filter(item => !!item.Key));
        }
        continuationToken = page.IsTruncated ? page.NextContinuationToken : undefined;
    } while (continuationToken);

    return collected;
}

async function runDataSyncInternal(): Promise<SyncSummary> {
    const region = process.env.AWS_REGION?.trim() || process.env.AWS_DEFAULT_REGION?.trim();
    if (!region) {
        throw new Error('Missing required environment variable: AWS_REGION (or AWS_DEFAULT_REGION)');
    }

    const accessKeyId = requireEnv('AWS_ACCESS_KEY_ID');
    const secretAccessKey = requireEnv('AWS_SECRET_ACCESS_KEY');
    const sessionToken = process.env.AWS_SESSION_TOKEN?.trim();

    const bucket = process.env.VITALS_S3_BUCKET?.trim() || DEFAULT_BUCKET;
    const prefix = process.env.VITALS_S3_PREFIX?.trim() || DEFAULT_PREFIX;
    const s3Client = new S3Client({
        region,
        credentials: {
            accessKeyId,
            secretAccessKey,
            sessionToken: sessionToken || undefined,
        },
    });
    const existingState = readSyncState();
    const nextState: SyncState = { objects: { ...existingState.objects } };

    const objects = await listAllObjects({
        s3Client,
        bucket,
        prefix,
    });

    let downloaded = 0;
    let skipped = 0;

    for (const object of objects) {
        const key = object.Key;
        if (!key) continue;

        const localPath = toLocalPath(key, prefix);
        const existingEntry = existingState.objects[key];
        const shouldFetch = shouldDownload({
            object,
            stateEntry: existingEntry,
            localPath,
        });

        if (shouldFetch) {
            await downloadObject({
                s3Client,
                bucket,
                key,
                destinationPath: localPath,
            });
            downloaded++;
        } else {
            skipped++;
        }

        nextState.objects[key] = {
            etag: normalizeEtag(object.ETag),
            lastModified: normalizeLastModified(object.LastModified),
            size: object.Size ?? 0,
        };
    }

    writeSyncState(nextState);

    return {
        downloaded,
        skipped,
        scanned: objects.length,
    };
}

async function runDownloadDataSync(): Promise<SyncSummary> {
    const summary = await runDataSyncInternal();
    console.info(
        `Data sync finished: scanned=${summary.scanned}, downloaded=${summary.downloaded}, skipped=${summary.skipped}`,
    );
    return summary;
}

export {
    runDownloadDataSync,
    toLocalPath,
    shouldDownload,
};

if (import.meta.main) {
    createScript(async () => {
        await runDownloadDataSync();
    });
}
