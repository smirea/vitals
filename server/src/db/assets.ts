import { eq } from 'drizzle-orm';

import type { VitalsDatabase } from 'server/db/client.ts';
import { diaryVoiceMemos, labDocuments, pillImages } from 'server/db/schema.ts';
import type { AssetKind, AssetTable, DiaryVoiceMemoAssetKind } from 'server/utils/assetUrls.ts';

export type ResolvedAsset = {
	s3Path: string;
	fileName: string;
	mimeType: string;
	sizeBytes: number;
};

export function resolveAssetByRecord(
	db: VitalsDatabase,
	table: AssetTable,
	id: number,
	kind?: AssetKind,
): ResolvedAsset | null {
	switch (table) {
		case 'lab_documents':
			return resolveLabDocumentAsset(db, id);
		case 'pill_images':
			return resolvePillImageAsset(db, id);
		case 'diary_voice_memos':
			return resolveDiaryVoiceMemoAsset(
				db,
				id,
				kind === 'video' || kind === 'audio' ? kind : 'audio',
			);
	}
}

function resolveLabDocumentAsset(db: VitalsDatabase, id: number): ResolvedAsset | null {
	const row = db
		.select({
			fileName: labDocuments.fileName,
			mimeType: labDocuments.mimeType,
			s3Path: labDocuments.s3Path,
			sizeBytes: labDocuments.sizeBytes,
		})
		.from(labDocuments)
		.where(eq(labDocuments.id, id))
		.get();

	return row ?? null;
}

function resolvePillImageAsset(db: VitalsDatabase, id: number): ResolvedAsset | null {
	const row = db
		.select({
			fileName: pillImages.fileName,
			mimeType: pillImages.mimeType,
			s3Path: pillImages.s3Path,
			sizeBytes: pillImages.sizeBytes,
		})
		.from(pillImages)
		.where(eq(pillImages.id, id))
		.get();

	return row ?? null;
}

function resolveDiaryVoiceMemoAsset(
	db: VitalsDatabase,
	id: number,
	kind: DiaryVoiceMemoAssetKind,
): ResolvedAsset | null {
	const row = db
		.select({
			fileName: diaryVoiceMemos.fileName,
			mimeType: diaryVoiceMemos.mimeType,
			audioS3Path: diaryVoiceMemos.audioS3Path,
			audioSizeBytes: diaryVoiceMemos.audioSizeBytes,
			videoFileName: diaryVoiceMemos.videoFileName,
			videoMimeType: diaryVoiceMemos.videoMimeType,
			videoS3Path: diaryVoiceMemos.videoS3Path,
			videoSizeBytes: diaryVoiceMemos.videoSizeBytes,
		})
		.from(diaryVoiceMemos)
		.where(eq(diaryVoiceMemos.id, id))
		.get();

	if (!row) {
		return null;
	}

	if (kind === 'video') {
		if (!row.videoS3Path || !row.videoFileName || !row.videoMimeType) {
			return null;
		}
		return {
			s3Path: row.videoS3Path,
			fileName: row.videoFileName,
			mimeType: row.videoMimeType,
			sizeBytes: row.videoSizeBytes,
		};
	}

	return {
		s3Path: row.audioS3Path,
		fileName: row.fileName,
		mimeType: row.mimeType,
		sizeBytes: row.audioSizeBytes,
	};
}
