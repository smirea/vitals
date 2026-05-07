import fs from 'fs';
import path from 'path';

import { generateText } from 'ai';
import { asc, desc, eq, inArray, isNull, ne, or } from 'drizzle-orm';
import { z } from 'zod';

import type { VitalsDatabase } from 'server/db/client.ts';
import { ensureTagsByNames } from 'server/db/tags.ts';
import {
	diaryEntries,
	diaryEntryTags,
	diaryVoiceMemos,
	type DiaryEntryRow,
	type DiaryEntryTagRow,
	type DiaryVoiceMemoRow,
	locations,
	type LocationRow,
	tags,
	type TagRow,
} from 'server/db/schema.ts';
import models, { transcribeAudioWithElevenLabs } from 'server/utils/models.ts';

const optionalLocationNumberSchema = z.number().finite().nullable().optional();
const NEARBY_LOCATION_DISTANCE_METERS = 100;
const NOMINATIM_REVERSE_URL = 'https://nominatim.openstreetmap.org/reverse';
const projectRoot = path.resolve(import.meta.dir, '..', '..', '..');
const DIARY_RECOVERY_ROOT = path.join(projectRoot, 'data', 'diary');

const nominatimReverseResponseSchema = z.object({
	display_name: z.string().optional(),
	address: z
		.object({
			city: z.string().optional(),
			town: z.string().optional(),
			village: z.string().optional(),
			municipality: z.string().optional(),
			hamlet: z.string().optional(),
			county: z.string().optional(),
			state: z.string().optional(),
			country: z.string().optional(),
			country_code: z.string().optional(),
		})
		.passthrough(),
});

export const diaryLocationInputSchema = z.object({
	capturedAt: z.string().trim().min(1),
	latitude: z.number().finite().min(-90).max(90),
	longitude: z.number().finite().min(-180).max(180),
	accuracy: optionalLocationNumberSchema,
	altitude: optionalLocationNumberSchema,
	altitudeAccuracy: optionalLocationNumberSchema,
	heading: optionalLocationNumberSchema,
	speed: optionalLocationNumberSchema,
});

export const diaryCreateEntryInputSchema = z.object({
	notes: z.string().trim().min(1),
	tagNames: z.array(z.string().trim().min(1)).max(50).default([]),
	location: diaryLocationInputSchema,
});

export const diaryUploadVoiceMemoInputSchema = z.object({
	notes: z.string().trim().optional().default(''),
	transcript: z.string().trim().optional().default(''),
	fileName: z.string().trim().min(1),
	mimeType: z.string().trim().min(1),
	dataBase64: z.string().trim().min(1),
	durationSeconds: z.number().finite().positive().nullable().optional(),
	tagNames: z.array(z.string().trim().min(1)).max(50).default([]),
	location: diaryLocationInputSchema,
});

export const diaryStartVoiceMemoDraftInputSchema = diaryUploadVoiceMemoInputSchema.omit({
	dataBase64: true,
	durationSeconds: true,
});

export const diaryAppendVoiceMemoDraftInputSchema = z.object({
	recoveryId: z.string().trim().min(1),
	dataBase64: z.string().trim().min(1),
});

export const diaryFinishVoiceMemoDraftInputSchema = z.object({
	recoveryId: z.string().trim().min(1),
	transcript: z.string().trim().optional().default(''),
	durationSeconds: z.number().finite().positive().nullable().optional(),
});

export const diaryProcessVoiceMemoRecoveryInputSchema = z.object({
	recoveryId: z.string().trim().min(1),
});

export const diaryProcessVoiceMemoInputSchema = z.object({
	voiceMemoId: z.number().int().positive(),
	transcript: z.string().trim().optional(),
});

export const diaryFailVoiceMemoInputSchema = z.object({
	voiceMemoId: z.number().int().positive(),
	error: z.string().trim().min(1),
});

export const diaryDeleteVoiceMemoInputSchema = z.object({
	voiceMemoId: z.number().int().positive(),
});

export const diaryAddEntryTagsInputSchema = z.object({
	entryId: z.number().int().positive(),
	tagNames: z.array(z.string().trim().min(1)).min(1).max(50),
});

export const diaryAddVoiceMemoTagsInputSchema = z.object({
	voiceMemoId: z.number().int().positive(),
	tagNames: z.array(z.string().trim().min(1)).min(1).max(50),
});

type DiaryReadDb = Pick<VitalsDatabase, 'select'>;
type DiaryWriteDb = Pick<VitalsDatabase, 'select' | 'insert' | 'update'>;

type DiaryRecord = ReturnType<typeof buildDiaryPayload>[number];
type DiaryVoiceMemoRecoveryRecord = {
	id: string;
	createdAt: string;
	updatedAt: string;
	status:
		| 'audio_saved'
		| 'recording'
		| 'saving_to_database'
		| 'database_saved'
		| 'transcribing'
		| 'summarizing'
		| 'completed'
		| 'failed';
	audioPath: string | null;
	audioDeletedAt: string | null;
	fileName: string;
	mimeType: string;
	durationSeconds: number | null;
	audioBytes: number;
	notes: string;
	tagNames: string[];
	location: z.infer<typeof diaryLocationInputSchema>;
	transcript: string | null;
	summary: string | null;
	entryId: number | null;
	voiceMemoId: number | null;
	error: string | null;
	steps: Array<{
		at: string;
		status: DiaryVoiceMemoRecoveryRecord['status'];
		details?: Record<string, unknown>;
	}>;
};

function normalizeOptionalText(value: string | null | undefined) {
	const trimmed = value?.trim() ?? '';
	return trimmed.length > 0 ? trimmed : null;
}

function nullableNumber(value: number | null | undefined): number | null {
	return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function getErrorMessage(error: unknown) {
	return error instanceof Error ? error.message : String(error);
}

function getDiaryRecoveryPaths(createdAt: string, fileName: string) {
	const day = createdAt.slice(0, 10);
	const id = createdAt.replace(/[:.]/g, '-');
	const dirPath = path.join(DIARY_RECOVERY_ROOT, day);
	const audioPath = path.join(dirPath, fileName);
	const metadataPath = path.join(dirPath, `${id}.json`);

	return {
		id,
		dirPath,
		audioPath,
		metadataPath,
	};
}

function writeDiaryRecoveryRecord(metadataPath: string, record: DiaryVoiceMemoRecoveryRecord) {
	fs.writeFileSync(metadataPath, `${JSON.stringify(record, null, 2)}\n`);
}

function createDiaryVoiceMemoRecovery(
	input: z.infer<typeof diaryUploadVoiceMemoInputSchema>,
	audioData: Buffer,
) {
	const createdAt = new Date().toISOString();
	const paths = getDiaryRecoveryPaths(createdAt, input.fileName.trim());
	fs.mkdirSync(paths.dirPath, { recursive: true });
	fs.writeFileSync(paths.audioPath, audioData);

	const record: DiaryVoiceMemoRecoveryRecord = {
		id: paths.id,
		createdAt,
		updatedAt: createdAt,
		status: 'audio_saved',
		audioPath: paths.audioPath,
		audioDeletedAt: null,
		fileName: input.fileName.trim(),
		mimeType: input.mimeType.trim(),
		durationSeconds: nullableNumber(input.durationSeconds),
		audioBytes: audioData.byteLength,
		notes: input.notes.trim(),
		tagNames: input.tagNames,
		location: input.location,
		transcript: normalizeOptionalText(input.transcript),
		summary: null,
		entryId: null,
		voiceMemoId: null,
		error: null,
		steps: [
			{
				at: createdAt,
				status: 'audio_saved',
				details: {
					audioPath: paths.audioPath,
					audioBytes: audioData.byteLength,
				},
			},
		],
	};
	writeDiaryRecoveryRecord(paths.metadataPath, record);

	return {
		...paths,
		record,
	};
}

function createDiaryVoiceMemoDraftRecovery(
	input: z.infer<typeof diaryStartVoiceMemoDraftInputSchema>,
) {
	const createdAt = new Date().toISOString();
	const paths = getDiaryRecoveryPaths(createdAt, input.fileName.trim());
	fs.mkdirSync(paths.dirPath, { recursive: true });
	fs.writeFileSync(paths.audioPath, '');

	const record: DiaryVoiceMemoRecoveryRecord = {
		id: paths.id,
		createdAt,
		updatedAt: createdAt,
		status: 'recording',
		audioPath: paths.audioPath,
		audioDeletedAt: null,
		fileName: input.fileName.trim(),
		mimeType: input.mimeType.trim(),
		durationSeconds: null,
		audioBytes: 0,
		notes: input.notes.trim(),
		tagNames: input.tagNames,
		location: input.location,
		transcript: normalizeOptionalText(input.transcript),
		summary: null,
		entryId: null,
		voiceMemoId: null,
		error: null,
		steps: [
			{
				at: createdAt,
				status: 'recording',
				details: {
					audioPath: paths.audioPath,
				},
			},
		],
	};
	writeDiaryRecoveryRecord(paths.metadataPath, record);

	return {
		...paths,
		record,
	};
}

function updateDiaryVoiceMemoRecovery(
	metadataPath: string,
	record: DiaryVoiceMemoRecoveryRecord,
	status: DiaryVoiceMemoRecoveryRecord['status'],
	updates: Partial<
		Omit<DiaryVoiceMemoRecoveryRecord, 'id' | 'createdAt' | 'updatedAt' | 'status' | 'steps'>
	> = {},
	details: Record<string, unknown> = {},
) {
	const updatedAt = new Date().toISOString();
	const nextRecord: DiaryVoiceMemoRecoveryRecord = {
		...record,
		...updates,
		status,
		updatedAt,
		steps: [
			...record.steps,
			{
				at: updatedAt,
				status,
				details,
			},
		],
	};
	writeDiaryRecoveryRecord(metadataPath, nextRecord);
	return nextRecord;
}

function deleteDiaryRecoveryAudio(
	metadataPath: string,
	record: DiaryVoiceMemoRecoveryRecord,
	audioPath: string,
) {
	fs.unlinkSync(audioPath);
	return updateDiaryVoiceMemoRecovery(
		metadataPath,
		record,
		'database_saved',
		{
			audioPath: null,
			audioDeletedAt: new Date().toISOString(),
		},
		{
			deletedAudioPath: audioPath,
		},
	);
}

function findDiaryRecoveryByVoiceMemoId(voiceMemoId: number) {
	if (!fs.existsSync(DIARY_RECOVERY_ROOT)) {
		return null;
	}

	for (const day of fs.readdirSync(DIARY_RECOVERY_ROOT)) {
		const dayPath = path.join(DIARY_RECOVERY_ROOT, day);
		if (!fs.statSync(dayPath).isDirectory()) {
			continue;
		}

		for (const fileName of fs.readdirSync(dayPath)) {
			if (!fileName.endsWith('.json')) {
				continue;
			}

			const metadataPath = path.join(dayPath, fileName);
			const record = JSON.parse(
				fs.readFileSync(metadataPath, 'utf8'),
			) as DiaryVoiceMemoRecoveryRecord;
			if (record.voiceMemoId === voiceMemoId) {
				return {
					metadataPath,
					record,
				};
			}
		}
	}

	return null;
}

function readDiaryRecoveryRecord(metadataPath: string) {
	return JSON.parse(fs.readFileSync(metadataPath, 'utf8')) as DiaryVoiceMemoRecoveryRecord;
}

function findDiaryRecoveryById(recoveryId: string) {
	const metadataPath = path.join(
		DIARY_RECOVERY_ROOT,
		recoveryId.slice(0, 10),
		`${recoveryId}.json`,
	);
	if (!fs.existsSync(metadataPath)) {
		throw new Error(`Diary recovery ${recoveryId} does not exist.`);
	}

	return {
		metadataPath,
		record: readDiaryRecoveryRecord(metadataPath),
	};
}

function getDiaryRecoveryMetadataRecords() {
	if (!fs.existsSync(DIARY_RECOVERY_ROOT)) {
		return [];
	}

	const records: Array<{
		metadataPath: string;
		record: DiaryVoiceMemoRecoveryRecord;
	}> = [];

	for (const day of fs.readdirSync(DIARY_RECOVERY_ROOT)) {
		const dayPath = path.join(DIARY_RECOVERY_ROOT, day);
		if (!fs.statSync(dayPath).isDirectory()) {
			continue;
		}

		for (const fileName of fs.readdirSync(dayPath)) {
			if (!fileName.endsWith('.json')) {
				continue;
			}

			const metadataPath = path.join(dayPath, fileName);
			records.push({
				metadataPath,
				record: readDiaryRecoveryRecord(metadataPath),
			});
		}
	}

	return records;
}

function buildDiaryPayload(args: {
	entryRows: DiaryEntryRow[];
	locationRows: LocationRow[];
	voiceMemoRows: Array<Omit<DiaryVoiceMemoRow, 'audioData'>>;
	entryTagRows: DiaryEntryTagRow[];
	tagRows: TagRow[];
}) {
	const locationsById = new Map(args.locationRows.map(row => [row.id, row]));
	const tagsById = new Map(args.tagRows.map(row => [row.id, row]));
	const voiceMemosByEntryId = new Map<
		number,
		Array<{
			id: number;
			createdAt: string;
			fileName: string;
			mimeType: string;
			durationSeconds: number | null;
			transcriptionStatus: DiaryVoiceMemoRow['transcriptionStatus'];
			transcript: string | null;
			transcriptLanguage: string | null;
			transcriptionDurationSeconds: number | null;
			transcriptionError: string | null;
			processedAt: string | null;
		}>
	>();
	const tagsByEntryId = new Map<number, TagRow[]>();

	for (const row of args.voiceMemoRows) {
		const list = voiceMemosByEntryId.get(row.entryId) ?? [];
		list.push({
			id: row.id,
			createdAt: row.createdAt,
			fileName: row.fileName,
			mimeType: row.mimeType,
			durationSeconds: row.durationSeconds,
			transcriptionStatus: row.transcriptionStatus,
			transcript: row.transcript,
			transcriptLanguage: row.transcriptLanguage,
			transcriptionDurationSeconds: row.transcriptionDurationSeconds,
			transcriptionError: row.transcriptionError,
			processedAt: row.processedAt,
		});
		voiceMemosByEntryId.set(row.entryId, list);
	}

	for (const row of args.entryTagRows) {
		const tag = tagsById.get(row.tagId);
		if (!tag) {
			continue;
		}

		const list = tagsByEntryId.get(row.entryId) ?? [];
		list.push(tag);
		tagsByEntryId.set(row.entryId, list);
	}

	return args.entryRows.map(row => {
		const location = locationsById.get(row.locationId);
		if (!location) {
			throw new Error(`Diary entry ${row.id} is missing its location.`);
		}
		if (!location.name || !location.city || !location.country) {
			throw new Error(`Diary entry ${row.id} location is missing its reverse geocode name.`);
		}

		return {
			id: row.id,
			createdAt: row.createdAt,
			notes: row.notes,
			summary: row.summary,
			location: {
				...location,
				name: location.name,
				city: location.city,
				country: location.country,
			},
			tags: (tagsByEntryId.get(row.id) ?? []).sort((left, right) =>
				left.name.localeCompare(right.name),
			),
			voiceMemos: voiceMemosByEntryId.get(row.id) ?? [],
		};
	});
}

function getDiaryRecords(db: DiaryReadDb, entryIds?: number[]) {
	const shouldFilterByIds = Array.isArray(entryIds);
	if (shouldFilterByIds && entryIds.length === 0) {
		return [] satisfies DiaryRecord[];
	}

	const entryRowsQuery = db.select().from(diaryEntries).$dynamic();
	const entryRows = (
		shouldFilterByIds ? entryRowsQuery.where(inArray(diaryEntries.id, entryIds)) : entryRowsQuery
	)
		.orderBy(desc(diaryEntries.createdAt), desc(diaryEntries.id))
		.all();
	const resolvedEntryIds = entryRows.map(row => row.id);

	if (resolvedEntryIds.length === 0) {
		return [] satisfies DiaryRecord[];
	}

	const locationRows = db
		.select()
		.from(locations)
		.where(
			inArray(
				locations.id,
				entryRows.map(row => row.locationId),
			),
		)
		.orderBy(asc(locations.id))
		.all();

	const voiceMemoRows = db
		.select({
			id: diaryVoiceMemos.id,
			entryId: diaryVoiceMemos.entryId,
			createdAt: diaryVoiceMemos.createdAt,
			fileName: diaryVoiceMemos.fileName,
			mimeType: diaryVoiceMemos.mimeType,
			durationSeconds: diaryVoiceMemos.durationSeconds,
			transcriptionStatus: diaryVoiceMemos.transcriptionStatus,
			transcript: diaryVoiceMemos.transcript,
			transcriptLanguage: diaryVoiceMemos.transcriptLanguage,
			transcriptionDurationSeconds: diaryVoiceMemos.transcriptionDurationSeconds,
			transcriptionError: diaryVoiceMemos.transcriptionError,
			processedAt: diaryVoiceMemos.processedAt,
		})
		.from(diaryVoiceMemos)
		.where(inArray(diaryVoiceMemos.entryId, resolvedEntryIds))
		.orderBy(asc(diaryVoiceMemos.entryId), asc(diaryVoiceMemos.createdAt), asc(diaryVoiceMemos.id))
		.all();

	const entryTagRows = db
		.select()
		.from(diaryEntryTags)
		.where(inArray(diaryEntryTags.entryId, resolvedEntryIds))
		.orderBy(asc(diaryEntryTags.entryId), asc(diaryEntryTags.tagId))
		.all();
	const tagIds = [...new Set(entryTagRows.map(row => row.tagId))];
	const tagRows =
		tagIds.length === 0
			? []
			: db
					.select()
					.from(tags)
					.where(inArray(tags.id, tagIds))
					.orderBy(asc(tags.name), asc(tags.id))
					.all();

	return buildDiaryPayload({
		entryRows,
		locationRows,
		voiceMemoRows,
		entryTagRows,
		tagRows,
	});
}

function getDiaryRecord(db: DiaryReadDb, entryId: number) {
	return getDiaryRecords(db, [entryId])[0] ?? null;
}

function getPendingVoiceMemoRecords(db: DiaryReadDb) {
	const voiceMemoRows = db
		.select()
		.from(diaryVoiceMemos)
		.where(ne(diaryVoiceMemos.transcriptionStatus, 'completed'))
		.orderBy(desc(diaryVoiceMemos.createdAt), desc(diaryVoiceMemos.id))
		.all();
	const entryIds = [...new Set(voiceMemoRows.map(row => row.entryId))];

	if (entryIds.length === 0) {
		return [];
	}

	const entryRows = db
		.select()
		.from(diaryEntries)
		.where(inArray(diaryEntries.id, entryIds))
		.orderBy(desc(diaryEntries.createdAt), desc(diaryEntries.id))
		.all();
	const entryRowsById = new Map(entryRows.map(row => [row.id, row]));

	const locationRows = db
		.select()
		.from(locations)
		.where(
			inArray(
				locations.id,
				entryRows.map(row => row.locationId),
			),
		)
		.orderBy(asc(locations.id))
		.all();
	const locationsById = new Map(locationRows.map(row => [row.id, row]));

	const entryTagRows = db
		.select()
		.from(diaryEntryTags)
		.where(inArray(diaryEntryTags.entryId, entryIds))
		.orderBy(asc(diaryEntryTags.entryId), asc(diaryEntryTags.tagId))
		.all();
	const tagIds = [...new Set(entryTagRows.map(row => row.tagId))];
	const tagRows =
		tagIds.length === 0
			? []
			: db
					.select()
					.from(tags)
					.where(inArray(tags.id, tagIds))
					.orderBy(asc(tags.name), asc(tags.id))
					.all();
	const tagsById = new Map(tagRows.map(row => [row.id, row]));
	const tagsByEntryId = new Map<number, TagRow[]>();

	for (const row of entryTagRows) {
		const tag = tagsById.get(row.tagId);
		if (!tag) {
			continue;
		}

		const list = tagsByEntryId.get(row.entryId) ?? [];
		list.push(tag);
		tagsByEntryId.set(row.entryId, list);
	}

	return voiceMemoRows.map(row => {
		const entry = entryRowsById.get(row.entryId);
		if (!entry) {
			throw new Error(`Voice memo ${row.id} is missing diary entry ${row.entryId}.`);
		}

		const location = locationsById.get(entry.locationId);
		if (!location) {
			throw new Error(`Diary entry ${entry.id} is missing its location.`);
		}

		return {
			id: row.id,
			entryId: row.entryId,
			createdAt: row.createdAt,
			fileName: row.fileName,
			mimeType: row.mimeType,
			audioBytes: row.audioData.byteLength,
			durationSeconds: row.durationSeconds,
			transcriptionStatus: row.transcriptionStatus,
			transcript: row.transcript,
			transcriptLanguage: row.transcriptLanguage,
			transcriptionDurationSeconds: row.transcriptionDurationSeconds,
			transcriptionError: row.transcriptionError,
			processedAt: row.processedAt,
			notes: entry.notes,
			summary: entry.summary,
			location,
			tags: (tagsByEntryId.get(entry.id) ?? []).sort((left, right) =>
				left.name.localeCompare(right.name),
			),
		};
	});
}

async function resolveLocation(db: DiaryWriteDb, input: z.infer<typeof diaryLocationInputSchema>) {
	const nearbyLocation = getNearbyLocation(db, input);
	if (nearbyLocation?.name && nearbyLocation.city && nearbyLocation.country) {
		return nearbyLocation.id;
	}

	const geocodedLocation = await reverseGeocodeLocation(input);

	if (nearbyLocation) {
		db.update(locations).set(geocodedLocation).where(eq(locations.id, nearbyLocation.id)).run();
		return nearbyLocation.id;
	}

	return db
		.insert(locations)
		.values({
			...geocodedLocation,
			capturedAt: input.capturedAt,
			latitude: input.latitude,
			longitude: input.longitude,
			accuracy: nullableNumber(input.accuracy),
			altitude: nullableNumber(input.altitude),
			altitudeAccuracy: nullableNumber(input.altitudeAccuracy),
			heading: nullableNumber(input.heading),
			speed: nullableNumber(input.speed),
		})
		.returning({
			id: locations.id,
		})
		.get().id;
}

function getNearbyLocation(db: DiaryReadDb, input: z.infer<typeof diaryLocationInputSchema>) {
	const locationRows = db.select().from(locations).all();
	const nearestLocation = locationRows
		.map(location => ({
			location,
			distanceMeters: getDistanceMeters(
				input.latitude,
				input.longitude,
				location.latitude,
				location.longitude,
			),
		}))
		.filter(item => item.distanceMeters <= NEARBY_LOCATION_DISTANCE_METERS)
		.sort((left, right) => left.distanceMeters - right.distanceMeters)[0];

	return nearestLocation?.location ?? null;
}

async function reverseGeocodeLocation(input: z.infer<typeof diaryLocationInputSchema>) {
	const url = new URL(NOMINATIM_REVERSE_URL);
	url.searchParams.set('format', 'jsonv2');
	url.searchParams.set('addressdetails', '1');
	url.searchParams.set('lat', String(input.latitude));
	url.searchParams.set('lon', String(input.longitude));

	const response = await fetch(url, {
		headers: {
			Accept: 'application/json',
			'Accept-Language': 'en',
			'User-Agent': 'Vitals diary local app',
		},
	});

	if (!response.ok) {
		const body = await response.text();
		throw new Error(`Reverse geocoding failed (${response.status}): ${body}`);
	}

	const result = nominatimReverseResponseSchema.parse(await response.json());
	const city = normalizeOptionalText(
		result.address.city ??
			result.address.town ??
			result.address.village ??
			result.address.municipality ??
			result.address.hamlet ??
			result.address.county ??
			result.address.state,
	);
	const country = normalizeOptionalText(result.address.country);

	if (!city || !country) {
		throw new Error('Reverse geocoding did not return a city and country.');
	}

	return {
		name: `${city}, ${country}`,
		city,
		country,
		countryCode: normalizeOptionalText(result.address.country_code),
		geocodedAt: new Date().toISOString(),
	};
}

function getDistanceMeters(
	leftLatitude: number,
	leftLongitude: number,
	rightLatitude: number,
	rightLongitude: number,
) {
	const earthRadiusMeters = 6_371_000;
	const leftLatitudeRadians = degreesToRadians(leftLatitude);
	const rightLatitudeRadians = degreesToRadians(rightLatitude);
	const latitudeDelta = degreesToRadians(rightLatitude - leftLatitude);
	const longitudeDelta = degreesToRadians(rightLongitude - leftLongitude);
	const haversine =
		Math.sin(latitudeDelta / 2) ** 2 +
		Math.cos(leftLatitudeRadians) *
			Math.cos(rightLatitudeRadians) *
			Math.sin(longitudeDelta / 2) ** 2;

	return 2 * earthRadiusMeters * Math.atan2(Math.sqrt(haversine), Math.sqrt(1 - haversine));
}

function degreesToRadians(value: number) {
	return (value * Math.PI) / 180;
}

function insertEntryTags(db: DiaryWriteDb, entryId: number, tagNames: string[]) {
	const resolvedTags = ensureTagsByNames(db, tagNames);
	const existingTagIds = new Set(
		db
			.select({
				tagId: diaryEntryTags.tagId,
			})
			.from(diaryEntryTags)
			.where(eq(diaryEntryTags.entryId, entryId))
			.all()
			.map(row => row.tagId),
	);
	const missingTags = resolvedTags.filter(tag => !existingTagIds.has(tag.id));

	if (missingTags.length > 0) {
		db.insert(diaryEntryTags)
			.values(missingTags.map(tag => ({ entryId, tagId: tag.id })))
			.run();
	}
}

async function summarizeDiaryEntry(input: { notes: string; transcript?: string | null }) {
	const sections = [
		input.notes.trim() ? `Notes:\n${input.notes.trim()}` : null,
		input.transcript?.trim() ? `Voice transcript:\n${input.transcript.trim()}` : null,
	].filter(Boolean);

	if (sections.length === 0) {
		throw new Error('Cannot summarize an empty diary entry.');
	}

	const result = await generateText({
		model: models.smart_and_expensive,
		system:
			'You summarize one personal diary entry. Return concise markdown only. Do not invent details.',
		prompt: [
			'Create a useful summary of this diary entry.',
			'Capture concrete events, symptoms, mood, decisions, and follow-ups when present.',
			'Use either one short paragraph or a tight bullet list.',
			'Do not include a title.',
			'',
			sections.join('\n\n'),
		].join('\n'),
		temperature: 0,
		maxRetries: 2,
		maxOutputTokens: 1_000,
		timeout: { totalMs: 45_000 },
	});
	const summary = result.text.trim();

	if (!summary) {
		throw new Error('Summary model returned an empty response.');
	}

	return summary;
}

export async function listDiaryEntries(db: VitalsDatabase) {
	await ensureMissingLocationNames(db);
	return getDiaryRecords(db);
}

export function listPendingDiaryVoiceMemos(db: VitalsDatabase) {
	return getPendingVoiceMemoRecords(db);
}

export function listPendingDiaryVoiceMemoRecoveries(_db: VitalsDatabase) {
	return getDiaryRecoveryMetadataRecords()
		.filter(({ record }) => record.status !== 'completed' && !record.voiceMemoId)
		.sort((left, right) => right.record.createdAt.localeCompare(left.record.createdAt))
		.map(({ metadataPath, record }) => ({
			id: record.id,
			metadataPath,
			createdAt: record.createdAt,
			updatedAt: record.updatedAt,
			status: record.status,
			fileName: record.fileName,
			mimeType: record.mimeType,
			durationSeconds: record.durationSeconds,
			audioBytes: record.audioBytes,
			audioPath: record.audioPath,
			transcript: record.transcript,
			error: record.error,
			steps: record.steps,
		}));
}

async function ensureMissingLocationNames(db: VitalsDatabase) {
	const missingLocationRows = db
		.select()
		.from(locations)
		.where(or(isNull(locations.name), isNull(locations.city), isNull(locations.country)))
		.orderBy(asc(locations.id))
		.all();

	for (const location of missingLocationRows) {
		const geocodedLocation = await reverseGeocodeLocation({
			capturedAt: location.capturedAt,
			latitude: location.latitude,
			longitude: location.longitude,
			accuracy: location.accuracy,
			altitude: location.altitude,
			altitudeAccuracy: location.altitudeAccuracy,
			heading: location.heading,
			speed: location.speed,
		});

		db.update(locations).set(geocodedLocation).where(eq(locations.id, location.id)).run();
	}
}

export async function createDiaryEntry(
	db: VitalsDatabase,
	input: z.infer<typeof diaryCreateEntryInputSchema>,
) {
	const locationId = await resolveLocation(db, input.location);
	const entryId = db.transaction(tx => {
		const insertedEntry = tx
			.insert(diaryEntries)
			.values({
				createdAt: new Date().toISOString(),
				notes: input.notes.trim(),
				locationId,
			})
			.returning({
				id: diaryEntries.id,
			})
			.get();

		insertEntryTags(tx, insertedEntry.id, input.tagNames);
		return insertedEntry.id;
	});

	const summary = await summarizeDiaryEntry({ notes: input.notes });
	db.update(diaryEntries).set({ summary }).where(eq(diaryEntries.id, entryId)).run();

	const record = getDiaryRecord(db, entryId);
	if (!record) {
		throw new Error(`Diary entry ${entryId} was not found after creation.`);
	}

	return record;
}

export async function uploadDiaryVoiceMemo(
	db: VitalsDatabase,
	input: z.infer<typeof diaryUploadVoiceMemoInputSchema>,
) {
	const { voiceMemoId } = await saveDiaryVoiceMemo(db, input);
	await processDiaryVoiceMemo(db, voiceMemoId, input.transcript);

	const voiceMemo = db
		.select()
		.from(diaryVoiceMemos)
		.where(eq(diaryVoiceMemos.id, voiceMemoId))
		.get();

	if (!voiceMemo) {
		throw new Error(`Voice memo ${voiceMemoId} was not found after processing.`);
	}

	const record = getDiaryRecord(db, voiceMemo.entryId);
	if (!record) {
		throw new Error(`Diary entry ${voiceMemo.entryId} was not found after voice memo processing.`);
	}

	return record;
}

export async function saveDiaryVoiceMemo(
	db: VitalsDatabase,
	input: z.infer<typeof diaryUploadVoiceMemoInputSchema>,
) {
	const audioData = Buffer.from(input.dataBase64, 'base64');
	const recovery = createDiaryVoiceMemoRecovery(input, audioData);
	let recoveryRecord = recovery.record;

	try {
		recoveryRecord = updateDiaryVoiceMemoRecovery(
			recovery.metadataPath,
			recoveryRecord,
			'saving_to_database',
		);
		const locationId = await resolveLocation(db, input.location);
		const { entryId, voiceMemoId } = db.transaction(tx => {
			const insertedEntry = tx
				.insert(diaryEntries)
				.values({
					createdAt: new Date().toISOString(),
					notes: input.notes.trim(),
					locationId,
				})
				.returning({
					id: diaryEntries.id,
				})
				.get();

			insertEntryTags(tx, insertedEntry.id, input.tagNames);

			const insertedVoiceMemo = tx
				.insert(diaryVoiceMemos)
				.values({
					entryId: insertedEntry.id,
					createdAt: new Date().toISOString(),
					fileName: input.fileName.trim(),
					mimeType: input.mimeType.trim(),
					audioData,
					durationSeconds: nullableNumber(input.durationSeconds),
					transcriptionStatus: 'uploaded',
					transcript: normalizeOptionalText(input.transcript),
					transcriptLanguage: normalizeOptionalText(input.transcript) ? 'English' : null,
				})
				.returning({
					id: diaryVoiceMemos.id,
				})
				.get();

			return {
				entryId: insertedEntry.id,
				voiceMemoId: insertedVoiceMemo.id,
			};
		});

		recoveryRecord = updateDiaryVoiceMemoRecovery(
			recovery.metadataPath,
			recoveryRecord,
			'database_saved',
			{
				entryId,
				voiceMemoId,
			},
			{
				entryId,
				voiceMemoId,
			},
		);
		if (fs.existsSync(recovery.audioPath)) {
			recoveryRecord = deleteDiaryRecoveryAudio(
				recovery.metadataPath,
				recoveryRecord,
				recovery.audioPath,
			);
		}

		return {
			entryId,
			voiceMemoId,
		};
	} catch (error) {
		updateDiaryVoiceMemoRecovery(
			recovery.metadataPath,
			recoveryRecord,
			'failed',
			{
				error: getErrorMessage(error),
			},
			{
				error: getErrorMessage(error),
			},
		);
		throw error;
	}
}

export function startDiaryVoiceMemoDraft(
	_db: VitalsDatabase,
	input: z.infer<typeof diaryStartVoiceMemoDraftInputSchema>,
) {
	const recovery = createDiaryVoiceMemoDraftRecovery(input);
	return {
		recoveryId: recovery.record.id,
		metadataPath: recovery.metadataPath,
		audioPath: recovery.audioPath,
	};
}

export function appendDiaryVoiceMemoDraft(
	_db: VitalsDatabase,
	input: z.infer<typeof diaryAppendVoiceMemoDraftInputSchema>,
) {
	const recovery = findDiaryRecoveryById(input.recoveryId);
	const audioPath = recovery.record.audioPath;
	if (!audioPath) {
		throw new Error(`Diary recovery ${input.recoveryId} no longer has an audio path.`);
	}

	const chunk = Buffer.from(input.dataBase64, 'base64');
	fs.appendFileSync(audioPath, chunk);
	const audioBytes = recovery.record.audioBytes + chunk.byteLength;
	const record = updateDiaryVoiceMemoRecovery(
		recovery.metadataPath,
		recovery.record,
		'recording',
		{
			audioBytes,
		},
		{
			chunkBytes: chunk.byteLength,
			audioBytes,
		},
	);

	return {
		recoveryId: record.id,
		audioBytes: record.audioBytes,
	};
}

export async function finishDiaryVoiceMemoDraft(
	db: VitalsDatabase,
	input: z.infer<typeof diaryFinishVoiceMemoDraftInputSchema>,
) {
	const recovery = findDiaryRecoveryById(input.recoveryId);
	let recoveryRecord = recovery.record;
	try {
		if (!recoveryRecord.audioPath) {
			throw new Error(`Diary recovery ${input.recoveryId} no longer has an audio path.`);
		}

		const audioData = fs.readFileSync(recoveryRecord.audioPath);
		if (audioData.byteLength === 0) {
			throw new Error(`Diary recovery ${input.recoveryId} did not receive audio.`);
		}

		recoveryRecord = updateDiaryVoiceMemoRecovery(
			recovery.metadataPath,
			recoveryRecord,
			'saving_to_database',
			{
				audioBytes: audioData.byteLength,
				durationSeconds: nullableNumber(input.durationSeconds),
				transcript: normalizeOptionalText(input.transcript) ?? recoveryRecord.transcript,
			},
			{
				audioBytes: audioData.byteLength,
			},
		);
		const locationId = await resolveLocation(db, recoveryRecord.location);
		const inserted = db.transaction(tx => {
			const insertedEntry = tx
				.insert(diaryEntries)
				.values({
					createdAt: recoveryRecord.createdAt,
					notes: recoveryRecord.notes,
					locationId,
				})
				.returning({
					id: diaryEntries.id,
				})
				.get();

			insertEntryTags(tx, insertedEntry.id, recoveryRecord.tagNames);

			const insertedVoiceMemo = tx
				.insert(diaryVoiceMemos)
				.values({
					entryId: insertedEntry.id,
					createdAt: recoveryRecord.createdAt,
					fileName: recoveryRecord.fileName,
					mimeType: recoveryRecord.mimeType,
					audioData,
					durationSeconds: recoveryRecord.durationSeconds,
					transcriptionStatus: 'uploaded',
					transcript: recoveryRecord.transcript,
					transcriptLanguage: recoveryRecord.transcript ? 'English' : null,
				})
				.returning({
					id: diaryVoiceMemos.id,
				})
				.get();

			return {
				entryId: insertedEntry.id,
				voiceMemoId: insertedVoiceMemo.id,
			};
		});

		recoveryRecord = updateDiaryVoiceMemoRecovery(
			recovery.metadataPath,
			recoveryRecord,
			'database_saved',
			{
				entryId: inserted.entryId,
				voiceMemoId: inserted.voiceMemoId,
			},
			inserted,
		);
		const audioPath = recoveryRecord.audioPath;
		if (audioPath && fs.existsSync(audioPath)) {
			recoveryRecord = deleteDiaryRecoveryAudio(recovery.metadataPath, recoveryRecord, audioPath);
		}

		return {
			entryId: inserted.entryId,
			voiceMemoId: inserted.voiceMemoId,
		};
	} catch (error) {
		updateDiaryVoiceMemoRecovery(
			recovery.metadataPath,
			recoveryRecord,
			'failed',
			{
				error: getErrorMessage(error),
			},
			{
				error: getErrorMessage(error),
			},
		);
		throw error;
	}
}

export async function processSavedDiaryVoiceMemo(
	db: VitalsDatabase,
	input: z.infer<typeof diaryProcessVoiceMemoInputSchema>,
) {
	await processDiaryVoiceMemo(db, input.voiceMemoId, input.transcript);

	const voiceMemo = db
		.select()
		.from(diaryVoiceMemos)
		.where(eq(diaryVoiceMemos.id, input.voiceMemoId))
		.get();

	if (!voiceMemo) {
		throw new Error(`Voice memo ${input.voiceMemoId} was not found after processing.`);
	}

	const record = getDiaryRecord(db, voiceMemo.entryId);
	if (!record) {
		throw new Error(`Diary entry ${voiceMemo.entryId} was not found after voice memo processing.`);
	}

	return record;
}

export async function processDiaryVoiceMemoRecovery(
	db: VitalsDatabase,
	input: z.infer<typeof diaryProcessVoiceMemoRecoveryInputSchema>,
) {
	const recovery = findDiaryRecoveryById(input.recoveryId);
	const resolvedMetadataPath = recovery.metadataPath;
	let recoveryRecord = readDiaryRecoveryRecord(resolvedMetadataPath);
	let voiceMemoId = recoveryRecord.voiceMemoId;

	if (!voiceMemoId) {
		if (!recoveryRecord.audioPath) {
			throw new Error(`Diary recovery ${resolvedMetadataPath} has no audio path.`);
		}

		const audioData = fs.readFileSync(recoveryRecord.audioPath);
		recoveryRecord = updateDiaryVoiceMemoRecovery(
			resolvedMetadataPath,
			recoveryRecord,
			'saving_to_database',
			{
				audioBytes: audioData.byteLength,
			},
			{
				audioPath: recoveryRecord.audioPath,
				audioBytes: audioData.byteLength,
			},
		);
		const locationId = await resolveLocation(db, recoveryRecord.location);
		const inserted = db.transaction(tx => {
			const insertedEntry = tx
				.insert(diaryEntries)
				.values({
					createdAt: recoveryRecord.createdAt,
					notes: recoveryRecord.notes,
					locationId,
				})
				.returning({
					id: diaryEntries.id,
				})
				.get();

			insertEntryTags(tx, insertedEntry.id, recoveryRecord.tagNames);

			const insertedVoiceMemo = tx
				.insert(diaryVoiceMemos)
				.values({
					entryId: insertedEntry.id,
					createdAt: recoveryRecord.createdAt,
					fileName: recoveryRecord.fileName,
					mimeType: recoveryRecord.mimeType,
					audioData,
					durationSeconds: recoveryRecord.durationSeconds,
					transcriptionStatus: 'uploaded',
					transcript: recoveryRecord.transcript,
					transcriptLanguage: recoveryRecord.transcript ? 'English' : null,
				})
				.returning({
					id: diaryVoiceMemos.id,
				})
				.get();

			return {
				entryId: insertedEntry.id,
				voiceMemoId: insertedVoiceMemo.id,
			};
		});

		voiceMemoId = inserted.voiceMemoId;
		recoveryRecord = updateDiaryVoiceMemoRecovery(
			resolvedMetadataPath,
			recoveryRecord,
			'database_saved',
			{
				entryId: inserted.entryId,
				voiceMemoId,
			},
			inserted,
		);
		const audioPath = recoveryRecord.audioPath;
		if (audioPath && fs.existsSync(audioPath)) {
			recoveryRecord = deleteDiaryRecoveryAudio(resolvedMetadataPath, recoveryRecord, audioPath);
		}
	}

	await processDiaryVoiceMemo(db, voiceMemoId, recoveryRecord.transcript ?? undefined);
	const voiceMemo = db
		.select()
		.from(diaryVoiceMemos)
		.where(eq(diaryVoiceMemos.id, voiceMemoId))
		.get();

	if (!voiceMemo) {
		throw new Error(`Voice memo ${voiceMemoId} was not found after recovery processing.`);
	}

	const record = getDiaryRecord(db, voiceMemo.entryId);
	if (!record) {
		throw new Error(`Diary entry ${voiceMemo.entryId} was not found after recovery processing.`);
	}

	return record;
}

export function failDiaryVoiceMemo(
	db: VitalsDatabase,
	input: z.infer<typeof diaryFailVoiceMemoInputSchema>,
) {
	const voiceMemo = db
		.select()
		.from(diaryVoiceMemos)
		.where(eq(diaryVoiceMemos.id, input.voiceMemoId))
		.get();

	if (!voiceMemo) {
		throw new Error(`Voice memo ${input.voiceMemoId} does not exist.`);
	}

	db.update(diaryVoiceMemos)
		.set({
			transcriptionStatus: 'failed',
			transcriptionError: input.error,
			processedAt: new Date().toISOString(),
		})
		.where(eq(diaryVoiceMemos.id, input.voiceMemoId))
		.run();

	const recovery = findDiaryRecoveryByVoiceMemoId(input.voiceMemoId);
	if (recovery) {
		updateDiaryVoiceMemoRecovery(
			recovery.metadataPath,
			recovery.record,
			'failed',
			{
				error: input.error,
			},
			{
				error: input.error,
			},
		);
	}

	return getPendingVoiceMemoRecords(db);
}

export function deleteDiaryVoiceMemo(
	db: VitalsDatabase,
	input: z.infer<typeof diaryDeleteVoiceMemoInputSchema>,
) {
	const voiceMemo = db
		.select()
		.from(diaryVoiceMemos)
		.where(eq(diaryVoiceMemos.id, input.voiceMemoId))
		.get();

	if (!voiceMemo) {
		throw new Error(`Voice memo ${input.voiceMemoId} does not exist.`);
	}

	db.transaction(tx => {
		tx.delete(diaryVoiceMemos).where(eq(diaryVoiceMemos.id, input.voiceMemoId)).run();

		const entry = tx
			.select()
			.from(diaryEntries)
			.where(eq(diaryEntries.id, voiceMemo.entryId))
			.get();
		const remainingMemo = tx
			.select({ id: diaryVoiceMemos.id })
			.from(diaryVoiceMemos)
			.where(eq(diaryVoiceMemos.entryId, voiceMemo.entryId))
			.limit(1)
			.get();

		if (entry && !entry.notes.trim() && !remainingMemo) {
			tx.delete(diaryEntries).where(eq(diaryEntries.id, entry.id)).run();
		}
	});

	return getPendingVoiceMemoRecords(db);
}

export function addTagsToDiaryEntry(
	db: VitalsDatabase,
	input: z.infer<typeof diaryAddEntryTagsInputSchema>,
) {
	const entry = db.select().from(diaryEntries).where(eq(diaryEntries.id, input.entryId)).get();

	if (!entry) {
		throw new Error(`Diary entry ${input.entryId} does not exist.`);
	}

	insertEntryTags(db, entry.id, input.tagNames);

	const record = getDiaryRecord(db, entry.id);
	if (!record) {
		throw new Error(`Diary entry ${entry.id} was not found after tagging.`);
	}

	return record;
}

export function addTagsToDiaryVoiceMemo(
	db: VitalsDatabase,
	input: z.infer<typeof diaryAddVoiceMemoTagsInputSchema>,
) {
	const voiceMemo = db
		.select()
		.from(diaryVoiceMemos)
		.where(eq(diaryVoiceMemos.id, input.voiceMemoId))
		.get();

	if (!voiceMemo) {
		throw new Error(`Voice memo ${input.voiceMemoId} does not exist.`);
	}

	insertEntryTags(db, voiceMemo.entryId, input.tagNames);
	return getPendingVoiceMemoRecords(db);
}

async function processDiaryVoiceMemo(
	db: VitalsDatabase,
	voiceMemoId: number,
	streamingTranscript: string | undefined,
) {
	const recovery = findDiaryRecoveryByVoiceMemoId(voiceMemoId);
	let recoveryRecord = recovery?.record ?? null;
	const voiceMemo = db
		.select()
		.from(diaryVoiceMemos)
		.where(eq(diaryVoiceMemos.id, voiceMemoId))
		.get();

	if (!voiceMemo) {
		throw new Error(`Voice memo ${voiceMemoId} does not exist.`);
	}

	const entry = db.select().from(diaryEntries).where(eq(diaryEntries.id, voiceMemo.entryId)).get();

	if (!entry) {
		throw new Error(`Diary entry ${voiceMemo.entryId} does not exist.`);
	}

	try {
		db.update(diaryVoiceMemos)
			.set({
				transcriptionStatus: 'transcribing',
				transcriptionError: null,
			})
			.where(eq(diaryVoiceMemos.id, voiceMemoId))
			.run();
		if (recovery && recoveryRecord) {
			recoveryRecord = updateDiaryVoiceMemoRecovery(
				recovery.metadataPath,
				recoveryRecord,
				'transcribing',
				{
					error: null,
				},
				{
					hasStreamingTranscript: Boolean(normalizeOptionalText(streamingTranscript)),
				},
			);
		}

		const transcript =
			normalizeOptionalText(streamingTranscript) ??
			normalizeOptionalText(
				(
					await transcribeAudioWithElevenLabs({
						audioData: voiceMemo.audioData,
						fileName: voiceMemo.fileName,
						mimeType: voiceMemo.mimeType,
					})
				).text,
			) ??
			normalizeOptionalText(voiceMemo.transcript);

		if (!transcript) {
			throw new Error('Transcription did not return text.');
		}

		db.update(diaryVoiceMemos)
			.set({
				transcriptionStatus: 'summarizing',
				transcript,
				transcriptLanguage: 'English',
				transcriptionError: null,
			})
			.where(eq(diaryVoiceMemos.id, voiceMemoId))
			.run();
		if (recovery && recoveryRecord) {
			recoveryRecord = updateDiaryVoiceMemoRecovery(
				recovery.metadataPath,
				recoveryRecord,
				'summarizing',
				{
					transcript,
					error: null,
				},
				{
					transcriptChars: transcript.length,
				},
			);
		}

		const summary = await summarizeDiaryEntry({ notes: entry.notes, transcript });

		db.update(diaryEntries).set({ summary }).where(eq(diaryEntries.id, entry.id)).run();
		db.update(diaryVoiceMemos)
			.set({
				transcriptionStatus: 'completed',
				transcriptionError: null,
				processedAt: new Date().toISOString(),
			})
			.where(eq(diaryVoiceMemos.id, voiceMemoId))
			.run();
		if (recovery && recoveryRecord) {
			updateDiaryVoiceMemoRecovery(
				recovery.metadataPath,
				recoveryRecord,
				'completed',
				{
					summary,
					error: null,
				},
				{
					summaryChars: summary.length,
				},
			);
		}
	} catch (error) {
		db.update(diaryVoiceMemos)
			.set({
				transcriptionStatus: 'failed',
				transcriptionError: getErrorMessage(error),
				processedAt: new Date().toISOString(),
			})
			.where(eq(diaryVoiceMemos.id, voiceMemoId))
			.run();
		if (recovery && recoveryRecord) {
			updateDiaryVoiceMemoRecovery(
				recovery.metadataPath,
				recoveryRecord,
				'failed',
				{
					error: getErrorMessage(error),
				},
				{
					error: getErrorMessage(error),
				},
			);
		}
		throw error;
	}
}

export function getDiaryVoiceMemoAudio(db: VitalsDatabase, voiceMemoId: number) {
	return (
		db
			.select({
				id: diaryVoiceMemos.id,
				fileName: diaryVoiceMemos.fileName,
				mimeType: diaryVoiceMemos.mimeType,
				audioData: diaryVoiceMemos.audioData,
			})
			.from(diaryVoiceMemos)
			.where(eq(diaryVoiceMemos.id, voiceMemoId))
			.get() ?? null
	);
}
