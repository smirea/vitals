import { generateText } from 'ai';
import { and, asc, eq, inArray, like, or } from 'drizzle-orm';
import { z } from 'zod';

import type { VitalsDatabase } from 'server/db/client.ts';
import { ensureTagsByNames } from 'server/db/tags.ts';
import {
	type PillComponentRow,
	type PillPeriodRow,
	type PillPeriodTagRow,
	type PillRow,
	type PillTagRow,
	type TagRow,
	pillComponents,
	pillImages,
	pillPeriods,
	pillPeriodTags,
	pillTags,
	pills,
	tags,
} from 'server/db/schema.ts';
import { getStaticImageUrl } from 'server/utils/getStaticImageUrl.ts';
import models from 'server/utils/models';

const pillImageInputSchema = z.object({
	id: z.number().int().positive().optional(),
	fileName: z.string().trim().min(1),
	dataUrl: z.string().trim().min(1),
});

const pillComponentInputSchema = z.object({
	name: z.string().trim().optional().default(''),
	value: z.string().trim().optional().default(''),
	unit: z.string().trim().optional().default(''),
});

const pillTimingSchema = z.enum(['morning', 'afternoon', 'evening']);
const pillWeekdayValues = [
	'monday',
	'tuesday',
	'wednesday',
	'thursday',
	'friday',
	'saturday',
	'sunday',
] as const;
const pillWeekdaySchema = z.enum(pillWeekdayValues);

const pillPeriodInputSchema = z.object({
	id: z.number().int().positive().optional(),
	startDate: z.string().trim().optional().default(''),
	endDate: z.string().trim().optional().default(''),
	count: z.number().positive().optional().default(1),
	timing: pillTimingSchema.optional(),
	daysOfWeek: z.array(pillWeekdaySchema).default([]),
	tagNames: z.array(z.string().trim()).max(50).default([]),
});

export const pillUpsertInputSchema = z.object({
	id: z.number().int().positive().optional(),
	name: z.string().trim().min(1),
	value: z.string().trim().min(1),
	unit: z.string().trim().min(1),
	url: z.string().trim().optional().default(''),
	note: z.string().trim().optional().default(''),
	tagNames: z.array(z.string().trim().min(1)).max(50).optional().default([]),
	images: z.array(pillImageInputSchema).max(16),
	components: z.array(pillComponentInputSchema).max(200),
	periods: z.array(pillPeriodInputSchema).max(200),
});

export const pillSearchInputSchema = z.object({
	query: z.string().trim().max(200).default(''),
	limit: z.number().int().positive().max(50).default(12),
});

export const pillImageExtractionInputSchema = z.object({
	images: z.array(pillImageInputSchema).min(1).max(12),
});

const pillImageExtractionSchema = z.object({
	detected: z.boolean(),
	name: z
		.string()
		.nullable()
		.optional()
		.describe('Product name only. Do not include serving size or supplement facts text.'),
	value: z
		.string()
		.nullable()
		.optional()
		.describe('Serving amount only, such as "2" or "1 scoop". Do not include ingredient lists.'),
	unit: z
		.string()
		.nullable()
		.optional()
		.describe('Serving unit only, such as "capsules", "softgels", "tablet", or "scoop".'),
	note: z
		.string()
		.nullable()
		.optional()
		.describe('Short optional note with extra packaging context not captured elsewhere.'),
	components: z
		.array(
			z.object({
				name: z
					.string()
					.trim()
					.min(1)
					.describe('Supplement fact or active ingredient name exactly as shown.'),
				value: z
					.string()
					.trim()
					.min(1)
					.nullable()
					.optional()
					.describe('Amount for this component only, such as "50" or "2000 IU".'),
				unit: z
					.string()
					.trim()
					.min(1)
					.nullable()
					.optional()
					.describe('Unit for this component only, such as "mg", "mcg", or "IU".'),
			}),
		)
		.optional()
		.default([])
		.describe(
			'One row per supplement facts component. If a supplement facts panel is visible, include every listed component row.',
		),
	extractionNotes: z
		.string()
		.nullable()
		.optional()
		.describe('Short explanation if detection was uncertain or partial.'),
	confidence: z
		.number()
		.min(0)
		.max(1)
		.nullable()
		.optional()
		.describe('Confidence score from 0 to 1.'),
});

const PILL_IMAGE_EXTRACTION_TIMEOUT_MS = 45_000;

type PillRecord = ReturnType<typeof buildPillsPayload>[number];
type PillsReadDb = Pick<VitalsDatabase, 'select'>;
type PillsWriteDb = Pick<VitalsDatabase, 'delete' | 'insert' | 'select' | 'update'>;
type PillImageExtraction = z.infer<typeof pillImageExtractionSchema>;
type PillImageMetadataRow = {
	id: number;
	pillId: number;
	fileName: string;
};

function getTodayDateString() {
	const date = new Date();
	const year = String(date.getFullYear());
	const month = String(date.getMonth() + 1).padStart(2, '0');
	const day = String(date.getDate()).padStart(2, '0');
	return `${year}-${month}-${day}`;
}

function normalizeOptionalText(value: string | null | undefined) {
	const trimmed = value?.trim() ?? '';
	return trimmed.length > 0 ? trimmed : null;
}

function parseJsonFromText(text: string) {
	const trimmedText = text.trim();
	let jsonText: string;

	if (trimmedText.startsWith('```')) {
		const withoutOpeningFence = trimmedText.replace(/^```(?:json)?\s*/i, '');
		jsonText = withoutOpeningFence.replace(/\s*```$/, '');
	} else {
		const firstObjectCharacter = trimmedText.indexOf('{');
		const lastObjectCharacter = trimmedText.lastIndexOf('}');
		if (firstObjectCharacter < 0 || lastObjectCharacter <= firstObjectCharacter) {
			throw new Error('Model response did not contain valid JSON.');
		}
		jsonText = trimmedText.slice(firstObjectCharacter, lastObjectCharacter + 1);
	}

	try {
		return JSON.parse(jsonText);
	} catch (error) {
		throw new Error(`Model response was not valid JSON.\n\n${getDebugErrorMessage(error)}`);
	}
}

function sanitizePillComponents(input: z.infer<typeof pillUpsertInputSchema>['components']) {
	return input
		.map(component => ({
			name: component.name.trim(),
			value: normalizeOptionalText(component.value),
			unit: normalizeOptionalText(component.unit),
		}))
		.filter(component => component.name.length > 0);
}

function sanitizePillPeriods(args: { input: z.infer<typeof pillUpsertInputSchema>['periods'] }) {
	const { input } = args;

	return input
		.map(period => ({
			id: period.id,
			startDate: period.startDate.trim(),
			endDate: normalizeOptionalText(period.endDate),
			count: period.count,
			timing: period.timing ?? null,
			daysOfWeek: normalizePillWeekdays(period.daysOfWeek),
			tagNames: [
				...new Map(
					period.tagNames
						.map(tagName => tagName.trim())
						.filter(Boolean)
						.map(tagName => [tagName.toLocaleLowerCase(), tagName]),
				).values(),
			],
		}))
		.filter(
			period =>
				period.startDate.length > 0 ||
				period.endDate !== null ||
				period.count !== 1 ||
				period.timing !== null ||
				period.daysOfWeek.length > 0 ||
				period.tagNames.length > 0,
		)
		.map(period => {
			if (!period.startDate) {
				throw new Error('Each saved pill period must include a start date.');
			}
			if (!Number.isFinite(period.count) || period.count <= 0) {
				throw new Error('Each saved pill period must include a positive count.');
			}

			return period;
		});
}

function getActivePeriodCount(
	periods: Array<{
		endDate: string | null;
	}>,
) {
	const today = getTodayDateString();
	return periods.filter(period => !period.endDate || period.endDate > today).length;
}

function normalizePillWriteError(error: unknown, name: string) {
	if (error instanceof Error && error.message.includes('UNIQUE constraint failed: pills.name')) {
		return new Error(`A pill named '${name}' already exists.`);
	}

	return error;
}

function parseDataUrl(dataUrl: string) {
	const match = dataUrl.match(/^data:([^;]+);base64,(.+)$/);
	if (!match) {
		throw new Error('Image payload must be a base64 data URL.');
	}

	return {
		mimeType: match[1],
		data: match[2],
	};
}

function syncPillImages(args: {
	db: PillsWriteDb;
	pillId: number;
	images: z.infer<typeof pillImageInputSchema>[];
}) {
	const existingImages = args.db
		.select({
			id: pillImages.id,
		})
		.from(pillImages)
		.where(eq(pillImages.pillId, args.pillId))
		.all();
	const existingImageIds = new Set(existingImages.map(image => image.id));
	const retainedImageIds = new Set<number>();

	for (const [index, image] of args.images.entries()) {
		if (image.dataUrl.startsWith('data:')) {
			args.db
				.insert(pillImages)
				.values({
					pillId: args.pillId,
					sortOrder: index,
					fileName: image.fileName,
					dataUrl: image.dataUrl,
				})
				.run();
			continue;
		}

		if (!image.id) {
			throw new Error(`Image ${image.fileName} must include a base64 data URL.`);
		}
		if (!existingImageIds.has(image.id)) {
			throw new Error(`Pill image ${image.id} does not belong to pill ${args.pillId}.`);
		}

		retainedImageIds.add(image.id);
		args.db
			.update(pillImages)
			.set({
				sortOrder: index,
				fileName: image.fileName,
			})
			.where(and(eq(pillImages.id, image.id), eq(pillImages.pillId, args.pillId)))
			.run();
	}

	for (const imageId of existingImageIds) {
		if (!retainedImageIds.has(imageId)) {
			args.db.delete(pillImages).where(eq(pillImages.id, imageId)).run();
		}
	}
}

function isTimeoutError(error: unknown) {
	return (
		error instanceof Error &&
		(error.name === 'AbortError' || /timed?\s*out|timeout/i.test(error.message))
	);
}

function getDebugErrorMessage(error: unknown) {
	const message =
		error instanceof Error ? `${error.name}: ${error.message}` : `Error: ${String(error)}`;
	const details = safeJsonStringify(serializeError(error));
	return details && details !== '{}' ? `${message}\n\n${details}` : message;
}

function serializeError(error: unknown): unknown {
	if (typeof error === 'bigint') {
		return error.toString();
	}
	if (typeof error !== 'object' || error === null) {
		return error;
	}

	const output: Record<string, unknown> = {};
	for (const key of Reflect.ownKeys(error)) {
		output[String(key)] = (error as Record<PropertyKey, unknown>)[key];
	}
	if (error instanceof Error) {
		output.name = error.name;
		output.message = error.message;
		output.stack = error.stack;
		output.cause = serializeError(error.cause);
	}

	return output;
}

function safeJsonStringify(value: unknown) {
	try {
		return JSON.stringify(value, getCircularJsonReplacer(), 2);
	} catch {
		return String(value);
	}
}

function getCircularJsonReplacer() {
	const seen = new WeakSet<object>();
	return (_key: string, value: unknown) => {
		if (typeof value === 'bigint') {
			return value.toString();
		}
		if (typeof value !== 'object' || value === null) {
			return value;
		}
		if (seen.has(value)) {
			return '[Circular]';
		}
		seen.add(value);
		return value;
	};
}

function normalizeStoredPillTiming(
	value: string | null | undefined,
): z.infer<typeof pillTimingSchema> | null {
	return value === 'morning' || value === 'afternoon' || value === 'evening' ? value : null;
}

function normalizePillWeekdays(values: readonly z.infer<typeof pillWeekdaySchema>[]) {
	const selectedValues = new Set(values);
	const orderedValues = pillWeekdayValues.filter(value => selectedValues.has(value));
	return orderedValues.length === pillWeekdayValues.length ? [] : orderedValues;
}

function normalizeStoredPillWeekdays(value: unknown) {
	return normalizePillWeekdays(z.array(pillWeekdaySchema).parse(value));
}

function buildPillsPayload(args: {
	pillRows: PillRow[];
	componentRows: PillComponentRow[];
	imageRows: PillImageMetadataRow[];
	periodRows: PillPeriodRow[];
	tagRows: TagRow[];
	pillTagRows: PillTagRow[];
	periodTagRows: PillPeriodTagRow[];
}) {
	const componentMap = new Map<
		number,
		Array<{
			id: number;
			name: string;
			value: string | null;
			unit: string | null;
		}>
	>();
	const imageMap = new Map<
		number,
		Array<{
			id: number;
			fileName: string;
			dataUrl: string;
		}>
	>();
	const periodMap = new Map<
		number,
		Array<{
			id: number;
			startDate: string;
			endDate: string | null;
			count: number;
			timing: z.infer<typeof pillTimingSchema> | null;
			daysOfWeek: z.infer<typeof pillWeekdaySchema>[];
			tags: Array<{
				id: number;
				name: string;
				color: string;
				note: string | null;
				createdDate: string;
			}>;
		}>
	>();
	const tagsById = new Map(args.tagRows.map(row => [row.id, row]));
	const tagsByPeriodId = new Map<
		number,
		Array<{
			id: number;
			name: string;
			color: string;
			note: string | null;
			createdDate: string;
		}>
	>();

	for (const row of args.componentRows) {
		const list = componentMap.get(row.pillId) ?? [];
		list.push({
			id: row.id,
			name: row.name,
			value: row.value,
			unit: row.unit,
		});
		componentMap.set(row.pillId, list);
	}

	for (const row of args.imageRows) {
		const list = imageMap.get(row.pillId) ?? [];
		list.push({
			id: row.id,
			fileName: row.fileName,
			dataUrl: getStaticImageUrl('pill_images', row.id),
		});
		imageMap.set(row.pillId, list);
	}

	const tagsByPillId = new Map<number, typeof args.tagRows>();
	for (const row of args.pillTagRows) {
		const tag = tagsById.get(row.tagId);
		if (!tag) {
			continue;
		}
		const list = tagsByPillId.get(row.pillId) ?? [];
		list.push(tag);
		tagsByPillId.set(row.pillId, list);
	}

	for (const row of args.periodTagRows) {
		const tag = tagsById.get(row.tagId);
		if (!tag) {
			continue;
		}

		const list = tagsByPeriodId.get(row.pillPeriodId) ?? [];
		list.push({
			id: tag.id,
			name: tag.name,
			color: tag.color,
			note: tag.note,
			createdDate: tag.createdDate,
		});
		tagsByPeriodId.set(row.pillPeriodId, list);
	}

	for (const row of args.periodRows) {
		const list = periodMap.get(row.pillId) ?? [];
		const periodTags = tagsByPeriodId.get(row.id) ?? [];
		list.push({
			id: row.id,
			startDate: row.startDate,
			endDate: row.endDate,
			count: row.count,
			timing: normalizeStoredPillTiming(row.timing),
			daysOfWeek: normalizeStoredPillWeekdays(row.daysOfWeekJson),
			tags: periodTags.sort((left, right) => left.name.localeCompare(right.name)),
		});
		periodMap.set(row.pillId, list);
	}

	return args.pillRows.map(row => ({
		id: row.id,
		name: row.name,
		value: row.value,
		unit: row.unit,
		url: row.url,
		note: row.note,
		tags: (tagsByPillId.get(row.id) ?? []).sort((left, right) =>
			left.name.localeCompare(right.name),
		),
		components: componentMap.get(row.id) ?? [],
		images: imageMap.get(row.id) ?? [],
		periods: periodMap.get(row.id) ?? [],
	}));
}

function splitPillsByStatus(pillRecords: PillRecord[]) {
	const today = getTodayDateString();

	const activePills: PillRecord[] = [];
	const futurePills: PillRecord[] = [];
	const pastPills: PillRecord[] = [];

	for (const pill of pillRecords) {
		const hasActivePeriod = pill.periods.some(
			period => period.startDate <= today && (!period.endDate || period.endDate > today),
		);
		const hasFuturePeriod = pill.periods.some(
			period => period.startDate > today && (!period.endDate || period.endDate > today),
		);

		if (hasActivePeriod) {
			activePills.push(pill);
		} else if (hasFuturePeriod) {
			futurePills.push(pill);
		} else {
			pastPills.push(pill);
		}
	}

	return {
		activePills,
		futurePills,
		pastPills,
		totals: {
			all: pillRecords.length,
			active: activePills.length,
			future: futurePills.length,
			past: pastPills.length,
		},
	};
}

function getPillRecords(db: PillsReadDb, pillIds?: number[]) {
	const shouldFilterByIds = Array.isArray(pillIds);
	if (shouldFilterByIds && pillIds.length === 0) {
		return [];
	}

	const pillRowsQuery = db.select().from(pills).$dynamic();
	const pillRows = (
		shouldFilterByIds ? pillRowsQuery.where(inArray(pills.id, pillIds)) : pillRowsQuery
	)
		.orderBy(asc(pills.name), asc(pills.id))
		.all();
	const resolvedPillIds = pillRows.map(row => row.id);

	if (resolvedPillIds.length === 0) {
		return [];
	}

	const componentRows = db
		.select()
		.from(pillComponents)
		.where(inArray(pillComponents.pillId, resolvedPillIds))
		.orderBy(asc(pillComponents.pillId), asc(pillComponents.sortOrder), asc(pillComponents.id))
		.all();

	const imageRows = db
		.select({
			id: pillImages.id,
			pillId: pillImages.pillId,
			fileName: pillImages.fileName,
		})
		.from(pillImages)
		.where(inArray(pillImages.pillId, resolvedPillIds))
		.orderBy(asc(pillImages.pillId), asc(pillImages.sortOrder), asc(pillImages.id))
		.all();

	const pillTagRows = db
		.select()
		.from(pillTags)
		.where(inArray(pillTags.pillId, resolvedPillIds))
		.orderBy(asc(pillTags.pillId), asc(pillTags.tagId), asc(pillTags.id))
		.all();

	const periodRows = db
		.select()
		.from(pillPeriods)
		.where(inArray(pillPeriods.pillId, resolvedPillIds))
		.orderBy(asc(pillPeriods.pillId), asc(pillPeriods.startDate), asc(pillPeriods.id))
		.all();
	const periodIds = periodRows.map(row => row.id);
	const periodTagRows =
		periodIds.length === 0
			? []
			: db
					.select()
					.from(pillPeriodTags)
					.where(inArray(pillPeriodTags.pillPeriodId, periodIds))
					.orderBy(
						asc(pillPeriodTags.pillPeriodId),
						asc(pillPeriodTags.tagId),
						asc(pillPeriodTags.id),
					)
					.all();

	const allTagIds = [
		...new Set([...pillTagRows.map(row => row.tagId), ...periodTagRows.map(row => row.tagId)]),
	];
	const tagRows =
		allTagIds.length === 0
			? []
			: db
					.select()
					.from(tags)
					.where(inArray(tags.id, allTagIds))
					.orderBy(asc(tags.name), asc(tags.id))
					.all();

	return buildPillsPayload({
		pillRows,
		componentRows,
		imageRows,
		periodRows,
		tagRows,
		pillTagRows,
		periodTagRows,
	});
}

export function getPillsDashboard(db: VitalsDatabase) {
	const pillRecords = getPillRecords(db);

	return {
		pills: pillRecords,
		...splitPillsByStatus(pillRecords),
	};
}

export function searchPills(db: VitalsDatabase, input: z.infer<typeof pillSearchInputSchema>) {
	const query = input.query.trim();
	const whereClause =
		query.length === 0 ? null : or(like(pills.name, `%${query}%`), like(pills.name, `${query}%`));

	let queryBuilder = db
		.select({
			id: pills.id,
		})
		.from(pills)
		.$dynamic();

	if (whereClause) {
		queryBuilder = queryBuilder.where(whereClause);
	}

	const pillRows = queryBuilder.orderBy(asc(pills.name), asc(pills.id)).limit(input.limit).all();

	return getPillRecords(
		db,
		pillRows.map(row => row.id),
	);
}

export function upsertPill(db: VitalsDatabase, input: z.infer<typeof pillUpsertInputSchema>) {
	const name = input.name.trim();
	const normalizedValue = input.value.trim();
	const normalizedUnit = input.unit.trim();
	const components = sanitizePillComponents(input.components);
	const periods = sanitizePillPeriods({
		input: input.periods,
	});

	if (periods.length === 0) {
		// throw new Error('At least one pill date range is required.');
	}
	if (getActivePeriodCount(periods) > 1) {
		throw new Error('A pill can only have one active date range at a time.');
	}

	const normalizedUrl = normalizeOptionalText(input.url);
	const normalizedNote = normalizeOptionalText(input.note);

	try {
		return db.transaction(tx => {
			let pillId = input.id ?? null;
			const resolvedTags = ensureTagsByNames(tx, [
				...input.tagNames,
				...periods.flatMap(period => period.tagNames),
			]);
			const tagsByName = new Map(resolvedTags.map(tag => [tag.name.toLocaleLowerCase(), tag]));

			if (pillId !== null) {
				const existingPill = tx
					.select({
						id: pills.id,
					})
					.from(pills)
					.where(eq(pills.id, pillId))
					.get();

				if (!existingPill) {
					throw new Error(`Pill ${pillId} does not exist.`);
				}
			}

			if (pillId === null) {
				const insertedRow = tx
					.insert(pills)
					.values({
						name,
						value: normalizedValue,
						unit: normalizedUnit,
						url: normalizedUrl,
						note: normalizedNote,
					})
					.returning({
						id: pills.id,
					})
					.get();

				pillId = insertedRow.id;
			} else {
				tx.update(pills)
					.set({
						name,
						value: normalizedValue,
						unit: normalizedUnit,
						url: normalizedUrl,
						note: normalizedNote,
					})
					.where(eq(pills.id, pillId))
					.run();
			}

			tx.delete(pillComponents).where(eq(pillComponents.pillId, pillId)).run();
			tx.delete(pillTags).where(eq(pillTags.pillId, pillId)).run();

			const resolvedPillTags = input.tagNames
				.map(tagName => tagsByName.get(tagName.trim().toLocaleLowerCase()))
				.filter((tag): tag is TagRow => Boolean(tag));

			if (resolvedPillTags.length > 0) {
				tx.insert(pillTags)
					.values(resolvedPillTags.map(tag => ({ pillId, tagId: tag.id })))
					.run();
			}

			if (components.length > 0) {
				tx.insert(pillComponents)
					.values(
						components.map((component, index) => ({
							pillId,
							sortOrder: index,
							name: component.name,
							value: component.value,
							unit: component.unit,
						})),
					)
					.run();
			}

			syncPillImages({ db: tx, pillId, images: input.images });

			for (const period of periods) {
				let pillPeriodId = period.id ?? null;

				if (period.id) {
					const existingPeriod = tx
						.select({
							id: pillPeriods.id,
						})
						.from(pillPeriods)
						.where(and(eq(pillPeriods.id, period.id), eq(pillPeriods.pillId, pillId)))
						.get();

					if (!existingPeriod) {
						throw new Error(`Pill period ${period.id} does not belong to pill ${pillId}.`);
					}

					tx.update(pillPeriods)
						.set({
							startDate: period.startDate,
							endDate: period.endDate,
							count: period.count,
							timing: period.timing,
							daysOfWeekJson: period.daysOfWeek,
						})
						.where(eq(pillPeriods.id, period.id))
						.run();
					pillPeriodId = period.id;
				} else {
					const insertedPeriod = tx
						.insert(pillPeriods)
						.values({
							pillId,
							startDate: period.startDate,
							endDate: period.endDate,
							count: period.count,
							timing: period.timing,
							daysOfWeekJson: period.daysOfWeek,
						})
						.returning({
							id: pillPeriods.id,
						})
						.get();

					pillPeriodId = insertedPeriod.id;
				}

				if (pillPeriodId === null) {
					continue;
				}

				tx.delete(pillPeriodTags).where(eq(pillPeriodTags.pillPeriodId, pillPeriodId)).run();

				const resolvedPeriodTags = period.tagNames
					.map(tagName => tagsByName.get(tagName.toLocaleLowerCase()))
					.filter((tag): tag is TagRow => Boolean(tag));

				if (resolvedPeriodTags.length > 0) {
					tx.insert(pillPeriodTags)
						.values(
							resolvedPeriodTags.map(tag => ({
								pillPeriodId,
								tagId: tag.id,
							})),
						)
						.run();
				}
			}

			return getPillRecords(tx, [pillId])[0];
		});
	} catch (error) {
		throw normalizePillWriteError(error, name);
	}
}

export async function extractPillFromImages(input: z.infer<typeof pillImageExtractionInputSchema>) {
	const imageParts = input.images.map(image => {
		const parsedImage = parseDataUrl(image.dataUrl);
		return {
			type: 'image' as const,
			image: parsedImage.data,
			mediaType: parsedImage.mimeType,
		};
	});

	let modelText = '';

	try {
		const result = await generateText({
			model: models.smart_and_expensive,
			messages: [
				{
					role: 'user',
					content: [
						{
							type: 'text',
							text: [
								'Analyze all uploaded images together as one pill or supplement label extraction task.',
								'Return exactly one valid JSON object and nothing else. Do not wrap it in markdown.',
								'Schema:',
								'{',
								'  "detected": boolean,',
								'  "name": string | null,',
								'  "value": string | null,',
								'  "unit": string | null,',
								'  "note": string | null,',
								'  "components": [{ "name": string, "value": string | null, "unit": string | null }],',
								'  "extractionNotes": string | null,',
								'  "confidence": number | null',
								'}',
								'Rules:',
								'- If this is not clearly a supplement or medication label, set detected to false.',
								'- The top-level value and unit are serving size only.',
								'- Never put the full supplement panel or ingredient list into the top-level value or unit fields.',
								'- If a Supplement Facts or active ingredients panel is visible, components must contain one row per listed ingredient.',
								'- Keep values short and exact when possible.',
								'- If a label says something like "2 Veggie Capsules", return value="2" and unit="Veggie Capsules".',
								'- If a component row says something like "EPA 400 mg", return value="400" and unit="mg".',
								'- If the images show a supplement facts panel, capture every visible row.',
							].join('\n'),
						},
						...imageParts,
					],
				},
			],
			temperature: 0,
			maxRetries: 2,
			maxOutputTokens: 4_000,
			timeout: { totalMs: PILL_IMAGE_EXTRACTION_TIMEOUT_MS },
			system: 'You are a precise supplement label extraction engine.',
		});
		modelText = result.text;
	} catch (error) {
		if (isTimeoutError(error)) {
			throw new Error(
				`Image parsing timed out. Try again or upload fewer images.\n\n${getDebugErrorMessage(error)}`,
			);
		}

		throw new Error(`Image parsing failed.\n\n${getDebugErrorMessage(error)}`);
	}

	let parsedResponse: PillImageExtraction;
	try {
		parsedResponse = pillImageExtractionSchema.parse(parseJsonFromText(modelText));
	} catch (error) {
		throw new Error(
			`Image parsing failed.\n\n${getDebugErrorMessage(error)}\n\nModel response:\n${modelText}`,
		);
	}

	return {
		detected: parsedResponse.detected,
		name: normalizeOptionalText(parsedResponse.name),
		value: normalizeOptionalText(parsedResponse.value),
		unit: normalizeOptionalText(parsedResponse.unit),
		note: normalizeOptionalText(parsedResponse.note),
		components: parsedResponse.components
			.map(component => ({
				name: component.name?.trim() ?? '',
				value: normalizeOptionalText(component.value),
				unit: normalizeOptionalText(component.unit),
			}))
			.filter(component => component.name.length > 0),
		extractionNotes: normalizeOptionalText(parsedResponse.extractionNotes),
		confidence: parsedResponse.confidence ?? null,
		model: models.smart_and_expensive.modelId,
	};
}
