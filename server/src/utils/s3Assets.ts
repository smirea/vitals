import { Readable } from 'stream';
import path from 'path';
import { createHash } from 'crypto';

import {
	GetObjectCommand,
	HeadObjectCommand,
	PutObjectCommand,
	S3Client,
	type GetObjectCommandOutput,
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

import env from 'server/env.ts';
import { s3PathUtil } from 'shared/s3PathUtil.ts';

const s3Client = new S3Client({
	region: env.AWS_REGION,
	credentials: {
		accessKeyId: env.AWS_ACCESS_KEY_ID,
		secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
		sessionToken: env.AWS_SESSION_TOKEN,
	},
});

export type AssetBody = Buffer | Uint8Array;

export function createVitalsS3Path(tableName: string, ...parts: string[]) {
	return s3PathUtil.join(
		env.VITALS_S3_BUCKET,
		env.VITALS_S3_PREFIX,
		env.AWS_PREFIX,
		tableName,
		...parts,
	);
}

export function createContentAddressedS3Path(input: {
	tableName: string;
	fileName: string;
	body: AssetBody;
	parts?: string[];
}) {
	const sha256 = createHash('sha256').update(input.body).digest('hex');
	return createVitalsS3Path(
		input.tableName,
		...(input.parts ?? []),
		sha256,
		ensureSafeFileName(input.fileName),
	);
}

export function ensureSafeFileName(fileName: string) {
	const parsed = path.parse(fileName.trim() || 'asset');
	const name = (parsed.name || 'asset')
		.replace(/[^\d A-Za-z._-]+/g, '-')
		.replace(/\s+/g, '-')
		.replace(/-+/g, '-')
		.replace(/^-|-$/g, '');
	const extension = parsed.ext.replace(/[^\d.A-Za-z]/g, '');
	return `${name || 'asset'}${extension}`;
}

export async function uploadS3Asset(input: {
	s3Path: string;
	body: AssetBody;
	contentType: string;
}) {
	await s3Client.send(
		new PutObjectCommand({
			...s3PathUtil.parse(input.s3Path),
			Body: input.body,
			ContentType: input.contentType,
			ContentLength: input.body.byteLength,
		}),
	);
}

export async function assertS3AssetExists(s3Path: string) {
	await s3Client.send(new HeadObjectCommand(s3PathUtil.parse(s3Path)));
}

export function getS3Asset(input: { s3Path: string; range?: string | null }) {
	return s3Client.send(
		new GetObjectCommand({
			...s3PathUtil.parse(input.s3Path),
			Range: input.range ?? undefined,
		}),
	);
}

export function getSignedS3AssetUrl(s3Path: string) {
	return getSignedUrl(s3Client, new GetObjectCommand(s3PathUtil.parse(s3Path)), {
		expiresIn: 10 * 60,
	});
}

export function s3BodyToReadableStream(body: GetObjectCommandOutput['Body']) {
	if (!body) {
		throw new Error('S3 object response did not include a body.');
	}
	if (body instanceof Readable) {
		return Readable.toWeb(body) as unknown as ReadableStream;
	}
	const streamBody = body as {
		transformToWebStream?: () => ReadableStream;
	};
	if (typeof streamBody.transformToWebStream === 'function') {
		return streamBody.transformToWebStream();
	}
	if (body instanceof Blob) {
		return body.stream();
	}

	throw new Error(`Unsupported S3 body type: ${Object.prototype.toString.call(body)}`);
}
