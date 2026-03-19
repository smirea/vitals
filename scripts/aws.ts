import path from 'path';

import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';

import { PROJECT_DB_PATH } from 'scripts/project-paths.ts';
import { getDatabase, type VitalsDatabase } from 'server/db/client.ts';

const DEFAULT_S3_BUCKET = 'stefan-life';
const DEFAULT_S3_PREFIX = 'vitals';

function requireEnv(name: string): string {
    const value = process.env[name]?.trim();
    if (!value) {
        throw new Error(`Missing required environment variable: ${name}`);
    }
    return value;
}

export function resolveAwsCredentials(): {
    region: string;
    accessKeyId: string;
    secretAccessKey: string;
    sessionToken?: string;
} {
    const region = process.env.AWS_REGION?.trim() || process.env.AWS_DEFAULT_REGION?.trim();
    if (!region) {
        throw new Error('Missing required environment variable: AWS_REGION (or AWS_DEFAULT_REGION)');
    }

    return {
        region,
        accessKeyId: requireEnv('AWS_ACCESS_KEY_ID'),
        secretAccessKey: requireEnv('AWS_SECRET_ACCESS_KEY'),
        sessionToken: process.env.AWS_SESSION_TOKEN?.trim() || undefined,
    };
}

export function createS3Client(): S3Client {
    const credentials = resolveAwsCredentials();

    return new S3Client({
        region: credentials.region,
        credentials: {
            accessKeyId: credentials.accessKeyId,
            secretAccessKey: credentials.secretAccessKey,
            sessionToken: credentials.sessionToken,
        },
    });
}

export function createS3ClientIfNeeded(options: {
    skipUpload: boolean;
}): { s3Client: S3Client | null; s3Bucket: string; s3Prefix: string } {
    const s3Bucket = process.env.VITALS_S3_BUCKET?.trim() || DEFAULT_S3_BUCKET;
    const s3Prefix = process.env.VITALS_S3_PREFIX?.trim() || DEFAULT_S3_PREFIX;

    if (options.skipUpload) {
        return { s3Client: null, s3Bucket, s3Prefix };
    }

    return {
        s3Client: createS3Client(),
        s3Bucket,
        s3Prefix,
    };
}

export function buildDatabaseS3Key(prefix: string, dbPath = PROJECT_DB_PATH): string {
    const normalizedPrefix = prefix.replace(/^\/+|\/+$/g, '');
    const dbFileName = path.basename(dbPath);

    return normalizedPrefix ? `${normalizedPrefix}/${dbFileName}` : dbFileName;
}

export async function uploadDatabaseSnapshot(args: {
    s3Client: S3Client | null;
    s3Bucket: string;
    s3Prefix: string;
    db?: VitalsDatabase;
    dbPath?: string;
}): Promise<string | null> {
    const { s3Client, s3Bucket, s3Prefix } = args;
    if (!s3Client) {
        return null;
    }

    const db = args.db ?? getDatabase();
    const dbPath = args.dbPath ?? PROJECT_DB_PATH;

    db.$client.exec('PRAGMA wal_checkpoint(TRUNCATE)');

    const key = buildDatabaseS3Key(s3Prefix, dbPath);
    const body = Buffer.from(await Bun.file(dbPath).arrayBuffer());

    await s3Client.send(
        new PutObjectCommand({
            Bucket: s3Bucket,
            Key: key,
            Body: body,
            ContentType: 'application/x-sqlite3',
        }),
    );

    return key;
}
