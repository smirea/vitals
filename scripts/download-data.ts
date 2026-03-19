import fs from 'fs';
import path from 'path';

import { GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';

import { buildDatabaseS3Key, createS3Client } from 'scripts/aws.ts';
import { createScript } from 'scripts/createScript.ts';
import { PROJECT_DB_PATH } from 'scripts/project-paths.ts';

const DEFAULT_BUCKET = 'stefan-life';
const DEFAULT_PREFIX = 'vitals';

type SyncSummary = {
    downloaded: boolean;
    key: string;
    destinationPath: string;
};

function shouldDownloadDatabaseSnapshot(args: {
    destinationPath: string;
    remoteSize: number | undefined;
    remoteLastModified: Date | undefined;
}): boolean {
    if (!fs.existsSync(args.destinationPath)) {
        return true;
    }

    const stats = fs.statSync(args.destinationPath);
    if (args.remoteSize !== undefined && args.remoteSize !== stats.size) {
        return true;
    }

    if (!args.remoteLastModified) {
        return false;
    }

    return args.remoteLastModified.getTime() > stats.mtime.getTime();
}

async function downloadDatabaseSnapshot(args: {
    bucket: string;
    key: string;
    destinationPath: string;
}): Promise<SyncSummary> {
    const s3Client = createS3Client();
    const head = await s3Client.send(new HeadObjectCommand({
        Bucket: args.bucket,
        Key: args.key,
    }));

    const shouldDownload = shouldDownloadDatabaseSnapshot({
        destinationPath: args.destinationPath,
        remoteSize: head.ContentLength,
        remoteLastModified: head.LastModified,
    });

    if (!shouldDownload) {
        return {
            downloaded: false,
            key: args.key,
            destinationPath: args.destinationPath,
        };
    }

    const response = await s3Client.send(new GetObjectCommand({
        Bucket: args.bucket,
        Key: args.key,
    }));

    if (!response.Body) {
        throw new Error(`S3 object body is empty: s3://${args.bucket}/${args.key}`);
    }

    const body = Buffer.from(await response.Body.transformToByteArray());
    fs.mkdirSync(path.dirname(args.destinationPath), { recursive: true });
    fs.writeFileSync(args.destinationPath, body);

    if (head.LastModified) {
        fs.utimesSync(args.destinationPath, new Date(), head.LastModified);
    }

    return {
        downloaded: true,
        key: args.key,
        destinationPath: args.destinationPath,
    };
}

async function runDownloadDataSync(): Promise<SyncSummary> {
    const bucket = process.env.VITALS_S3_BUCKET?.trim() || DEFAULT_BUCKET;
    const prefix = process.env.VITALS_S3_PREFIX?.trim() || DEFAULT_PREFIX;
    const key = buildDatabaseS3Key(prefix, PROJECT_DB_PATH);
    const summary = await downloadDatabaseSnapshot({
        bucket,
        key,
        destinationPath: PROJECT_DB_PATH,
    });

    if (summary.downloaded) {
        console.info(`Downloaded s3://${bucket}/${summary.key} -> ${summary.destinationPath}`);
    } else {
        console.info(`SQLite snapshot already up to date at ${summary.destinationPath}`);
    }

    return summary;
}

export {
    downloadDatabaseSnapshot,
    runDownloadDataSync,
    shouldDownloadDatabaseSnapshot,
};

if (import.meta.main) {
    createScript(async () => {
        await runDownloadDataSync();
    });
}
