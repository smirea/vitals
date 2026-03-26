import path from 'path';
import { createHash } from 'crypto';

import { createOpenRouter } from '@openrouter/ai-sdk-provider';
import { generateText } from 'ai';
import { eq } from 'drizzle-orm';
import { PDFDocument } from 'pdf-lib';
import { z } from 'zod';

import { getDatabase, type VitalsDatabase } from 'server/db/client.ts';
import {
	labDocuments,
	labMeasurements,
	labResults,
	type LabDocumentRow,
	type LabMeasurementRow,
} from 'server/db/schema.ts';
import env from 'server/env.ts';
import { promiseParallel } from 'server/promise-parallel.ts';

const labUploadFileInputSchema = z.object({
	fileName: z.string().trim().min(1),
	mimeType: z.string().trim().min(1),
	dataBase64: z.string().trim().min(1),
});

export const labUploadDocumentsInputSchema = z.object({
	files: z.array(labUploadFileInputSchema).min(1).max(12),
});

export const labRetryDocumentInputSchema = z.object({
	documentId: z.number().int().positive(),
});

export const labReprocessDocumentInputSchema = z.object({
	documentId: z.number().int().positive(),
});

const nullableTextSchema = z.string().trim().min(1).nullable().optional();
const looseNullableTextSchema = z.union([z.string(), z.null()]).optional();
const looseNullableNumberSchema = z.union([z.number(), z.string(), z.null()]).optional();
const looseTextArraySchema = z
	.array(z.string().trim().min(1))
	.nullish()
	.transform(values => values ?? []);

const extractionMeasurementSchema = z.object({
	name: looseNullableTextSchema,
	sourceName: looseNullableTextSchema,
	measurementName: looseNullableTextSchema,
	analyteName: looseNullableTextSchema,
	canonicalName: looseNullableTextSchema,
	englishName: looseNullableTextSchema,
	standardizedName: looseNullableTextSchema,
	valueText: looseNullableTextSchema,
	value: looseNullableTextSchema,
	valueNumeric: looseNullableNumberSchema,
	unit: looseNullableTextSchema,
});

const extractionMetadataSchema = z.object({
	date: nullableTextSchema,
	collectionDate: nullableTextSchema,
	reportedDate: nullableTextSchema,
	receivedDate: nullableTextSchema,
	labName: nullableTextSchema,
	location: nullableTextSchema,
	language: nullableTextSchema,
	country: nullableTextSchema,
	notes: nullableTextSchema,
});

const extractionPageSchema = z.object({
	metadata: extractionMetadataSchema.optional(),
	measurements: z.array(extractionMeasurementSchema),
});

const normalizationResultSchema = z.object({
	canonicalName: looseNullableTextSchema,
	name: looseNullableTextSchema,
	sourceName: looseNullableTextSchema,
	originalName: looseNullableTextSchema,
	aliases: looseTextArraySchema,
	canonicalUnit: nullableTextSchema,
	knownUnits: looseTextArraySchema,
	canonicalRangeText: nullableTextSchema,
	canonicalRangeMin: looseNullableNumberSchema,
	canonicalRangeMax: looseNullableNumberSchema,
	rangeEvidence: looseTextArraySchema,
	valueText: nullableTextSchema,
	valueNumeric: looseNullableNumberSchema,
	unit: nullableTextSchema,
	sourcePage: z.union([z.number().int().positive(), z.string(), z.null()]).optional(),
});

const normalizationPassSchema = z.object({
	results: z.array(normalizationResultSchema),
});

type LabUploadDocumentsInput = z.infer<typeof labUploadDocumentsInputSchema>;
type ExtractionMeasurementOutput = {
	name: string;
	canonicalName: string;
	valueText: string | null;
	valueNumeric: number | null;
	unit: string | null;
	sourcePage: number;
};
type ExtractionPassOutput = {
	metadata?: z.infer<typeof extractionMetadataSchema>;
	measurements: ExtractionMeasurementOutput[];
};
type NormalizationResultOutput = {
	canonicalName: string;
	sourceName: string | null;
	aliases: string[];
	canonicalUnit: string | null;
	knownUnits: string[];
	canonicalRangeText: string | null;
	canonicalRangeMin: number | null;
	canonicalRangeMax: number | null;
	rangeEvidence: string[];
	valueText: string | null;
	valueNumeric: number | null;
	unit: string | null;
	sourcePage: number | null;
};
type NormalizationPassOutput = {
	results: NormalizationResultOutput[];
};

type MeasurementDraft = {
	key: string;
	name: string;
	category: string | null;
	aliases: string[];
	canonicalUnit: string | null;
	knownUnits: string[];
	canonicalRangeText: string | null;
	canonicalRangeMin: number | null;
	canonicalRangeMax: number | null;
	rangeEvidence: string[];
};

type NormalizedResultDraft = {
	measurementKey: string;
	originalName: string | null;
	originalValueText: string | null;
	originalValueNumeric: number | null;
	originalUnit: string | null;
	originalRangeText: string | null;
	originalRangeMin: number | null;
	originalRangeMax: number | null;
	valueText: string | null;
	valueNumeric: number | null;
	unit: string | null;
	note: string | null;
	confidence: number | null;
	sourcePage: number | null;
	evidence: string[];
};

type StructuredPassResult<T> = {
	output: T;
};

type LabPdfPage = {
	pageNumber: number;
	pageCount: number;
	fileName: string;
	pdfData: Uint8Array;
};

type PageNormalizationInput = {
	page: LabPdfPage;
	measurements: ExtractionPassOutput['measurements'];
};

type LooseExtractionMeasurement = z.infer<typeof extractionMeasurementSchema>;
type LooseNormalizationPassOutput = z.infer<typeof normalizationPassSchema>;
type LooseNormalizationResult = z.infer<typeof normalizationResultSchema>;

const NORMALIZATION_BATCH_SIZE = 10;

let processorPromise: Promise<void> | null = null;
let processorStarted = false;

export function getLabDashboard(db: VitalsDatabase) {
	syncLabMeasurementCategories(db);
	syncLabMeasurementUnitConversions(db);
	repairLabMeasurementRanges(db);

	const documents = db
		.select({
			id: labDocuments.id,
			date: labDocuments.date,
			group: labDocuments.group,
			queuedAt: labDocuments.queuedAt,
		})
		.from(labDocuments)
		.where(eq(labDocuments.status, 'completed'))
		.orderBy(labDocuments.date, labDocuments.id)
		.all()
		.reverse();

	const measurements = db
		.select()
		.from(labMeasurements)
		.orderBy(labMeasurements.name, labMeasurements.id)
		.all();

	const results = db
		.select()
		.from(labResults)
		.orderBy(labResults.documentId, labResults.sortOrder, labResults.id)
		.all();

	return { documents, measurements, results };
}

export function listLabDocuments(db: VitalsDatabase) {
	return db
		.select({
			id: labDocuments.id,
			fileName: labDocuments.fileName,
			status: labDocuments.status,
			statusText: labDocuments.statusText,
			group: labDocuments.group,
			date: labDocuments.date,
			labName: labDocuments.labName,
			queuedAt: labDocuments.queuedAt,
			lastError: labDocuments.lastError,
		})
		.from(labDocuments)
		.orderBy(labDocuments.id)
		.all()
		.reverse();
}

export function syncLabMeasurementCategories(db: VitalsDatabase) {
	const now = new Date().toISOString();
	const measurements = db.select().from(labMeasurements).all();

	for (const measurement of measurements) {
		const category = resolveCanonicalMeasurementCategory(measurement.name, measurement.category);
		if (category === measurement.category) {
			continue;
		}

		db.update(labMeasurements)
			.set({
				category,
				updatedAt: now,
			})
			.where(eq(labMeasurements.id, measurement.id))
			.run();

		if (category === OTHER_CATEGORY) {
			console.log(
				`[labs] measurement ${measurement.name} (${measurement.key}) fell back to ${OTHER_CATEGORY}`,
			);
		}
	}
}

export function syncLabMeasurementUnitConversions(db: VitalsDatabase) {
	const now = new Date().toISOString();
	const measurements = db.select().from(labMeasurements).all();

	for (const measurement of measurements) {
		const unitConversions = resolveMeasurementUnitConversions(measurement.name);
		if (JSON.stringify(unitConversions) === JSON.stringify(measurement.unitConversionsJson)) {
			continue;
		}

		db.update(labMeasurements)
			.set({
				unitConversionsJson: unitConversions,
				updatedAt: now,
			})
			.where(eq(labMeasurements.id, measurement.id))
			.run();
	}
}

function repairLabMeasurementRanges(db: VitalsDatabase) {
	const now = new Date().toISOString();
	const measurements = db.select().from(labMeasurements).all();

	for (const measurement of measurements) {
		const repairedRange = resolveCanonicalMeasurementRangeRepair(measurement);
		if (!repairedRange) {
			continue;
		}

		db.update(labMeasurements)
			.set({
				canonicalRangeMin: repairedRange.min,
				canonicalRangeMax: repairedRange.max,
				canonicalRangeText: repairedRange.text,
				updatedAt: now,
			})
			.where(eq(labMeasurements.id, measurement.id))
			.run();
	}
}

function resolveCanonicalMeasurementRangeRepair(measurement: LabMeasurementRow) {
	if (!hasClearlyBrokenCanonicalRange(measurement)) {
		return null;
	}

	const unit = canonicalizeUnitOrNull(measurement.canonicalUnit);
	if (!unit) {
		return null;
	}

	const rule = findMeasurementUnitStandardizationRule(measurement.name);
	const evidence = normalizeTextArray(measurement.rangeEvidenceJson);
	const candidates: Array<{
		min: number | null;
		max: number | null;
		text: string | null;
		score: number;
	}> = [];

	evidence.forEach((rawText, index) => {
		const parsed = parseReferenceRangeBoundsFromText(rawText);
		if (!parsed) {
			return;
		}

		const boundedScore = parsed.min !== undefined && parsed.max !== undefined ? 4 : 3;
		candidates.push({
			min: parsed.min ?? null,
			max: parsed.max ?? null,
			text: formatCanonicalRangeText(parsed.min ?? null, parsed.max ?? null),
			score: 100 - index * 2 + boundedScore,
		});

		if (!rule) {
			return;
		}

		Object.keys(rule.convertersByUnitKey).forEach(sourceUnitKey => {
			if (sourceUnitKey === normalizeUnitKey(unit)) {
				return;
			}

			const converted = standardizeMeasurementRangeToCanonicalUnit({
				measurementName: measurement.name,
				sourceUnit: sourceUnitKey,
				targetUnit: unit,
				rangeMin: parsed.min ?? null,
				rangeMax: parsed.max ?? null,
				rangeText: null,
			});

			candidates.push({
				min: converted.min,
				max: converted.max,
				text: converted.text,
				score: 100 - index * 2,
			});
		});
	});

	const repaired = candidates
		.filter(candidate => isPlausibleCanonicalRange(unit, candidate.min, candidate.max))
		.sort((left, right) => right.score - left.score)[0];

	if (!repaired) {
		return null;
	}

	if (
		repaired.min === measurement.canonicalRangeMin &&
		repaired.max === measurement.canonicalRangeMax &&
		repaired.text === measurement.canonicalRangeText
	) {
		return null;
	}

	return repaired;
}

function hasClearlyBrokenCanonicalRange(measurement: LabMeasurementRow) {
	const unit = canonicalizeUnitOrNull(measurement.canonicalUnit);
	if (!unit) {
		return false;
	}

	return !isPlausibleCanonicalRange(
		unit,
		measurement.canonicalRangeMin,
		measurement.canonicalRangeMax,
	);
}

function isPlausibleCanonicalRange(
	unit: string,
	min: number | null | undefined,
	max: number | null | undefined,
) {
	if (min !== null && min !== undefined && !Number.isFinite(min)) {
		return false;
	}
	if (max !== null && max !== undefined && !Number.isFinite(max)) {
		return false;
	}
	if (min !== null && min !== undefined && max !== null && max !== undefined && max < min) {
		return false;
	}

	const upperBound = getCanonicalRangeUpperBound(unit);
	if (min !== null && min !== undefined && Math.abs(min) > upperBound) {
		return false;
	}
	if (max !== null && max !== undefined && Math.abs(max) > upperBound) {
		return false;
	}

	return min !== null || max !== null;
}

function getCanonicalRangeUpperBound(unit: string) {
	if (unit === '%') {
		return 100;
	}
	if (unit === 'mg/dL') {
		return 2_000;
	}
	if (unit === 'g/dL') {
		return 100;
	}
	if (unit === 'mmol/L') {
		return 200;
	}

	return 10_000;
}

export async function uploadLabDocuments(db: VitalsDatabase, input: LabUploadDocumentsInput) {
	const parsed = labUploadDocumentsInputSchema.parse(input);
	const queued = enqueueLabDocuments(
		db,
		parsed.files.map(file => ({
			fileName: file.fileName,
			mimeType: file.mimeType,
			pdfData: Buffer.from(file.dataBase64, 'base64'),
		})),
	);

	scheduleLabProcessing();
	return {
		documents: queued,
	};
}

export async function retryLabDocument(
	db: VitalsDatabase,
	input: z.infer<typeof labRetryDocumentInputSchema>,
) {
	const parsed = labRetryDocumentInputSchema.parse(input);
	const document = db
		.select({
			id: labDocuments.id,
			fileName: labDocuments.fileName,
			status: labDocuments.status,
		})
		.from(labDocuments)
		.where(eq(labDocuments.id, parsed.documentId))
		.get();

	if (!document) {
		throw new Error('Document not found.');
	}
	if (document.status !== 'failed') {
		throw new Error('Only failed documents can be retried.');
	}

	db.update(labDocuments)
		.set({
			status: 'pending',
			statusText: 'Queued for retry',
			startedAt: null,
			completedAt: null,
			failedAt: null,
			lastError: null,
		})
		.where(eq(labDocuments.id, parsed.documentId))
		.run();

	logLabDocumentEvent(document, 'Queued for retry');
	scheduleLabProcessing();

	return {
		documentId: parsed.documentId,
	};
}

export async function reprocessLabDocument(
	db: VitalsDatabase,
	input: z.infer<typeof labReprocessDocumentInputSchema>,
) {
	const parsed = labReprocessDocumentInputSchema.parse(input);
	const document = db
		.select({
			id: labDocuments.id,
			fileName: labDocuments.fileName,
			status: labDocuments.status,
		})
		.from(labDocuments)
		.where(eq(labDocuments.id, parsed.documentId))
		.get();

	if (!document) {
		throw new Error('Document not found.');
	}
	if (document.status !== 'completed') {
		throw new Error('Only completed documents can be reprocessed.');
	}

	db.update(labDocuments)
		.set({
			status: 'pending',
			statusText: 'Queued for reprocess',
			startedAt: null,
			completedAt: null,
			failedAt: null,
			lastError: null,
		})
		.where(eq(labDocuments.id, parsed.documentId))
		.run();

	logLabDocumentEvent(document, 'Queued for reprocess');
	scheduleLabProcessing();

	return {
		documentId: parsed.documentId,
	};
}

export function getLabDocumentPdf(
	db: VitalsDatabase,
	documentId: number,
): Pick<LabDocumentRow, 'id' | 'fileName' | 'mimeType' | 'pdfData'> | null {
	return (
		db
			.select({
				id: labDocuments.id,
				fileName: labDocuments.fileName,
				mimeType: labDocuments.mimeType,
				pdfData: labDocuments.pdfData,
			})
			.from(labDocuments)
			.where(eq(labDocuments.id, documentId))
			.get() ?? null
	);
}

export function startLabProcessor() {
	if (processorStarted) {
		return;
	}
	processorStarted = true;
	resetStuckLabDocuments(getDatabase());
	scheduleLabProcessing();
}

function scheduleLabProcessing() {
	if (processorPromise) {
		return;
	}

	processorPromise = processTriggeredLabDocument()
		.catch(error => {
			console.error('[labs] processing trigger failed', error);
		})
		.finally(() => {
			processorPromise = null;

			const db = getDatabase();
			if (hasNextPendingLabDocument(db) && !hasActiveLabDocument(db)) {
				scheduleLabProcessing();
			}
		});
}

async function processTriggeredLabDocument(db = getDatabase()) {
	const outcome = await processNextPendingLabDocument(db);
	if (outcome === 'busy') {
		return;
	}
}

export async function processNextPendingLabDocument(db = getDatabase()) {
	const nextDocumentId = claimNextPendingLabDocument(db);
	if (nextDocumentId === 'busy' || nextDocumentId === null) {
		return nextDocumentId;
	}

	await processLabDocument(db, nextDocumentId);
	return 'processed' as const;
}

function hasNextPendingLabDocument(db: VitalsDatabase) {
	return db
		.select({
			id: labDocuments.id,
		})
		.from(labDocuments)
		.where(eq(labDocuments.status, 'pending'))
		.orderBy(labDocuments.id)
		.get();
}

function hasActiveLabDocument(db: VitalsDatabase) {
	return db
		.select({
			id: labDocuments.id,
		})
		.from(labDocuments)
		.where(eq(labDocuments.status, 'processing'))
		.orderBy(labDocuments.id)
		.get();
}

function claimNextPendingLabDocument(db: VitalsDatabase) {
	const client = db.$client;
	client.exec('BEGIN IMMEDIATE');

	try {
		const activeDocument = client
			.prepare("SELECT id FROM lab_documents WHERE status = 'processing' ORDER BY id LIMIT 1")
			.get() as { id: number } | null;
		if (activeDocument) {
			client.exec('COMMIT');
			return 'busy' as const;
		}

		const pendingDocument = client
			.prepare("SELECT id FROM lab_documents WHERE status = 'pending' ORDER BY id LIMIT 1")
			.get() as { id: number } | null;
		if (!pendingDocument) {
			client.exec('COMMIT');
			return null;
		}

		client
			.prepare(
				[
					'UPDATE lab_documents',
					"SET status = 'processing',",
					'started_at = ?,',
					'completed_at = NULL,',
					'failed_at = NULL,',
					'last_error = NULL',
					'WHERE id = ?',
				].join(' '),
			)
			.run(new Date().toISOString(), pendingDocument.id);

		client.exec('COMMIT');
		return pendingDocument.id;
	} catch (error) {
		client.exec('ROLLBACK');
		throw error;
	}
}

export function resetStuckLabDocuments(db = getDatabase()) {
	const interruptedDocuments = db
		.select({
			id: labDocuments.id,
			fileName: labDocuments.fileName,
		})
		.from(labDocuments)
		.where(eq(labDocuments.status, 'processing'))
		.all();

	if (interruptedDocuments.length === 0) {
		return;
	}

	db.update(labDocuments)
		.set({
			status: 'pending',
			statusText: 'Queued after interrupted processing',
			startedAt: null,
			lastError: 'Processing was interrupted and has been retried.',
		})
		.where(eq(labDocuments.status, 'processing'))
		.run();

	for (const document of interruptedDocuments) {
		logLabDocumentEvent(document, 'Queued after interrupted processing');
	}
}

export function enqueueLabDocuments(
	db: VitalsDatabase,
	files: Array<{
		fileName: string;
		mimeType: string;
		pdfData: Buffer;
	}>,
) {
	const now = new Date().toISOString();
	const queued: Array<{
		id: number;
		fileName: string;
		status: LabDocumentRow['status'];
		statusText: string;
		queuedAt: string;
		deduplicated: boolean;
	}> = [];

	for (const file of files) {
		const sha256 = createHash('sha256').update(file.pdfData).digest('hex');
		const existing = db
			.select({
				id: labDocuments.id,
				fileName: labDocuments.fileName,
				status: labDocuments.status,
				statusText: labDocuments.statusText,
				queuedAt: labDocuments.queuedAt,
			})
			.from(labDocuments)
			.where(eq(labDocuments.sha256, sha256))
			.get();

		if (existing) {
			logLabDocumentEvent(existing, 'Duplicate upload skipped');
			queued.push({
				...existing,
				deduplicated: true,
			});
			continue;
		}

		const inserted = db
			.insert(labDocuments)
			.values({
				fileName: file.fileName,
				mimeType: file.mimeType,
				pdfData: file.pdfData,
				sha256,
				status: 'pending',
				statusText: 'Queued for import',
				queuedAt: now,
			})
			.returning({
				id: labDocuments.id,
				fileName: labDocuments.fileName,
				status: labDocuments.status,
				statusText: labDocuments.statusText,
				queuedAt: labDocuments.queuedAt,
			})
			.get();

		queued.push({
			...inserted,
			deduplicated: false,
		});
		logLabDocumentEvent(inserted, inserted.statusText);
	}

	return queued;
}

function updateLabDocumentStatus(
	db: VitalsDatabase,
	document: Pick<LabDocumentRow, 'id' | 'fileName'>,
	statusText: string,
) {
	db.update(labDocuments)
		.set({
			statusText,
		})
		.where(eq(labDocuments.id, document.id))
		.run();

	logLabDocumentEvent(document, statusText);
}

function logLabDocumentEvent(document: Pick<LabDocumentRow, 'id' | 'fileName'>, message: string) {
	console.log(`[labs] #${document.id} ${document.fileName}: ${message}`);
}

function clearLabDocumentDerivedData(db: VitalsDatabase, documentId: number) {
	db.transaction(tx => {
		tx.delete(labResults).where(eq(labResults.documentId, documentId)).run();
		tx.update(labDocuments)
			.set({
				completedAt: null,
				failedAt: null,
				lastError: null,
				date: null,
				collectionDate: null,
				reportedDate: null,
				receivedDate: null,
				labName: null,
				location: null,
				language: null,
				country: null,
				notes: null,
			})
			.where(eq(labDocuments.id, documentId))
			.run();
	});
}

async function splitLabPdfIntoPages(document: LabDocumentRow) {
	const sourcePdf = await PDFDocument.load(document.pdfData);
	const pageCount = sourcePdf.getPageCount();
	const pages: LabPdfPage[] = [];
	const baseName = path.basename(document.fileName, path.extname(document.fileName));

	for (let pageIndex = 0; pageIndex < pageCount; pageIndex += 1) {
		const pagePdf = await PDFDocument.create();
		const [page] = await pagePdf.copyPages(sourcePdf, [pageIndex]);
		pagePdf.addPage(page);

		pages.push({
			pageNumber: pageIndex + 1,
			pageCount,
			fileName: `${baseName}_page_${pageIndex + 1}.pdf`,
			pdfData: await pagePdf.save(),
		});
	}

	return pages;
}

async function processLabDocument(db: VitalsDatabase, documentId: number) {
	const document = db.select().from(labDocuments).where(eq(labDocuments.id, documentId)).get();
	if (!document || document.status !== 'processing') {
		return;
	}

	try {
		updateLabDocumentStatus(db, document, 'Clearing previous imported data');
		clearLabDocumentDerivedData(db, documentId);
		const provider = createOpenRouter({ apiKey: env.OPENROUTER_API_KEY });
		const existingMeasurements = db.select().from(labMeasurements).all();
		updateLabDocumentStatus(db, document, 'Splitting document into pages');
		const pages = await splitLabPdfIntoPages(document);
		updateLabDocumentStatus(
			db,
			document,
			`Extracting measurements from ${pages.length} page${pages.length === 1 ? '' : 's'}`,
		);
		const extractionPass = await runExtractionPass({
			provider,
			modelId: env.BLOODWORK_OPENROUTER_MODEL,
			document,
			pages,
		});
		updateLabDocumentStatus(
			db,
			document,
			`Normalizing ${extractionPass.output.measurements.length} measurement${extractionPass.output.measurements.length === 1 ? '' : 's'}`,
		);
		const normalizationOutput = await resolveNormalizationOutput({
			db,
			document,
			provider,
			modelId: env.BLOODWORK_OPENROUTER_MODEL,
			existingMeasurements,
			extractionPass,
			pages,
		});
		const metadata = normalizeMetadataDraft(extractionPass.output.metadata, document.fileName);
		const { measurementDrafts, resultDrafts } = buildDraftsFromNormalization(normalizationOutput);
		const savedStatusText = `Saving ${resultDrafts.length} normalized result${resultDrafts.length === 1 ? '' : 's'}`;
		const completedStatusText = `Imported ${resultDrafts.length} result${resultDrafts.length === 1 ? '' : 's'}`;

		updateLabDocumentStatus(db, document, savedStatusText);

		db.transaction(tx => {
			const measurementIdByKey = upsertMeasurementDrafts(
				tx as unknown as VitalsDatabase,
				measurementDrafts,
			);

			if (resultDrafts.length > 0) {
				tx.insert(labResults)
					.values(
						resultDrafts.map((draft, index) => ({
							documentId,
							measurementId: measurementIdByKey.get(draft.measurementKey)!,
							sortOrder: index,
							originalName: draft.originalName,
							originalValueText: draft.originalValueText,
							originalValueNumeric: draft.originalValueNumeric,
							originalUnit: draft.originalUnit,
							originalRangeText: draft.originalRangeText,
							originalRangeMin: draft.originalRangeMin,
							originalRangeMax: draft.originalRangeMax,
							valueText: draft.valueText,
							valueNumeric: draft.valueNumeric,
							unit: draft.unit,
							note: draft.note,
							confidence: draft.confidence,
							sourcePage: draft.sourcePage,
							evidenceJson: draft.evidence,
						})),
					)
					.run();
			}

			tx.update(labDocuments)
				.set({
					status: 'completed',
					statusText: completedStatusText,
					completedAt: new Date().toISOString(),
					failedAt: null,
					lastError: null,
					date: metadata.date,
					collectionDate: metadata.collectionDate,
					reportedDate: metadata.reportedDate,
					receivedDate: metadata.receivedDate,
					labName: metadata.labName,
					location: metadata.location,
					language: metadata.language,
					country: metadata.country,
					notes: metadata.notes,
				})
				.where(eq(labDocuments.id, documentId))
				.run();
		});
		logLabDocumentEvent(document, completedStatusText);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		logLabDocumentEvent(document, `Import failed: ${message}`);
		db.update(labDocuments)
			.set({
				status: 'failed',
				statusText: 'Import failed',
				failedAt: new Date().toISOString(),
				lastError: message,
			})
			.where(eq(labDocuments.id, documentId))
			.run();
	}
}

async function runExtractionPass(args: {
	provider: ReturnType<typeof createOpenRouter>;
	modelId: string;
	document: LabDocumentRow;
	pages: LabPdfPage[];
}): Promise<StructuredPassResult<ExtractionPassOutput>> {
	const { provider, modelId, document, pages } = args;

	const pageOutputs = await promiseParallel(
		pages,
		async page => {
			const model = provider(modelId);
			const result = await generateStructuredOutput({
				model,
				schema: extractionPageSchema,
				messages: [
					{
						role: 'user' as const,
						content: [
							{
								type: 'text' as const,
								text: buildExtractionPrompt({
									fileName: document.fileName,
									pageNumber: page.pageNumber,
									pageCount: page.pageCount,
								}),
							},
							{
								type: 'file' as const,
								mediaType: 'application/pdf',
								filename: page.fileName,
								data: page.pdfData,
							},
						],
					},
				],
				maxOutputTokens: 3_072,
			});

			return {
				metadata: page.pageNumber === 1 ? result.output.metadata : undefined,
				measurements: result.output.measurements
					.map(measurement => normalizeExtractionMeasurement(measurement))
					.filter(isPresent)
					.map(measurement => ({
						...measurement,
						sourcePage: page.pageNumber,
					})),
			};
		},
		{
			concurrency: 5,
			retries: 1,
		},
	);

	const firstMetadata = pageOutputs.find(page => page.metadata)?.metadata;

	return {
		output: {
			metadata: firstMetadata,
			measurements: pageOutputs.flatMap(page => page.measurements),
		},
	};
}

async function runNormalizationPass(args: {
	provider: ReturnType<typeof createOpenRouter>;
	modelId: string;
	fileName: string;
	existingMeasurements: LabMeasurementRow[];
	input: PageNormalizationInput;
}): Promise<StructuredPassResult<LooseNormalizationPassOutput>> {
	const { provider, modelId, fileName, existingMeasurements, input } = args;
	const model = provider(modelId);

	return generateStructuredOutput({
		model,
		schema: normalizationPassSchema,
		messages: [
			{
				role: 'user' as const,
				content: [
					{
						type: 'text' as const,
						text: buildNormalizationPrompt({
							fileName,
							existingMeasurements,
							input,
						}),
					},
					{
						type: 'file' as const,
						mediaType: 'application/pdf',
						filename: input.page.fileName,
						data: input.page.pdfData,
					},
				],
			},
		],
		maxOutputTokens: 4_096,
	});
}

function buildExtractionPrompt(args: { fileName: string; pageNumber: number; pageCount: number }) {
	const { fileName, pageNumber, pageCount } = args;
	return [
		'Analyze this lab PDF page and return only structured JSON that matches the schema.',
		`This file contains original page ${pageNumber} of ${pageCount}.`,
		'Read the attached raw PDF page directly.',
		'Return only lab measurement rows that have a visible result value on this page.',
		'For each row, return the source name exactly as shown, a concise English canonical name, the value, and the unit.',
		'Do not return panel titles, section headers, explanations, or reference range rows by themselves.',
		'If the result is textual, keep it in valueText and leave valueNumeric null.',
		'If the result is numeric, set valueNumeric and also preserve the display form in valueText when useful.',
		'Do not invent measurements that are not visibly present on this page.',
		'If this is page 1 and report metadata is visible, include it. Otherwise omit metadata.',
		'Write the final JSON inside a <result_json>...</result_json> tag.',
		'Example:',
		[
			'<result_json>',
			'{',
			'  "metadata": {',
			'    "date": "2026-01-20",',
			'    "labName": "Quest Diagnostics"',
			'  },',
			'  "measurements": [',
			'    {',
			'      "name": "LDL Cholesterol",',
			'      "canonicalName": "LDL Cholesterol",',
			'      "valueText": "104",',
			'      "valueNumeric": 104,',
			'      "unit": "mg/dL"',
			'    }',
			'  ]',
			'}',
			'</result_json>',
		].join('\n'),
		`Source file: ${fileName}`,
	].join('\n');
}

function buildNormalizationPrompt(args: {
	fileName: string;
	existingMeasurements: LabMeasurementRow[];
	input: PageNormalizationInput;
}) {
	const { fileName, existingMeasurements, input } = args;
	const catalog = existingMeasurements
		.map(measurement => ({
			name: measurement.name,
			aliases: normalizeTextArray(measurement.aliasesJson),
			canonicalUnit: measurement.canonicalUnit,
			canonicalRangeMin: measurement.canonicalRangeMin,
			canonicalRangeMax: measurement.canonicalRangeMax,
			canonicalRangeText: measurement.canonicalRangeText,
		}))
		.slice(0, 600);

	return [
		'Normalize the extracted lab rows from this PDF page.',
		`This file contains original page ${input.page.pageNumber} of ${input.page.pageCount}.`,
		'Use the attached raw PDF page to verify values, units, and reference ranges for the listed rows.',
		'Return one final result per logical measurement from the extracted rows on this page.',
		'Use an existing canonical measurement name when it is clearly the same analyte; otherwise return a concise English canonical name.',
		'Do not repeat a nested measurement object. Return the flat fields only.',
		'Preserve the source row name in sourceName.',
		`Use sourcePage = ${input.page.pageNumber} for rows from this page unless a row clearly belongs to another page in the attached PDF.`,
		'Set canonical unit and canonical range only when they are clearly visible or already established.',
		'Keep aliases limited to names clearly matching this analyte.',
		'Keep rangeEvidence brief and only when a canonical range is returned.',
		'Write the final JSON inside a <result_json>...</result_json> tag.',
		'Example:',
		[
			'<result_json>',
			'{',
			'  "results": [',
			'    {',
			'      "canonicalName": "LDL Cholesterol",',
			'      "sourceName": "LDL-C",',
			'      "aliases": ["LDL-C"],',
			'      "canonicalUnit": "mg/dL",',
			'      "knownUnits": ["mg/dL"],',
			'      "canonicalRangeText": "<100",',
			'      "canonicalRangeMin": null,',
			'      "canonicalRangeMax": 100,',
			'      "rangeEvidence": ["reference range shown as <100 mg/dL"],',
			'      "valueText": "104",',
			'      "valueNumeric": 104,',
			'      "unit": "mg/dL",',
			'      "sourcePage": 1',
			'    }',
			'  ]',
			'}',
			'</result_json>',
		].join('\n'),
		`Source file: ${fileName}`,
		'Existing canonical measurements:',
		JSON.stringify(catalog, null, 2),
		'Extracted rows for this page:',
		JSON.stringify(
			input.measurements.map(measurement => ({
				name: measurement.name,
				canonicalName: measurement.canonicalName,
				valueText: measurement.valueText,
				valueNumeric: measurement.valueNumeric,
				unit: measurement.unit,
				sourcePage: measurement.sourcePage,
			})),
			null,
			2,
		),
	].join('\n');
}

function normalizeMetadataDraft(metadata: ExtractionPassOutput['metadata'] = {}, fileName: string) {
	const inferred = inferMetadataFromFileName(fileName);

	return {
		date: normalizeOptionalIsoDate(metadata.date) ?? inferred.date,
		collectionDate: normalizeOptionalIsoDate(metadata.collectionDate),
		reportedDate: normalizeOptionalIsoDate(metadata.reportedDate),
		receivedDate: normalizeOptionalIsoDate(metadata.receivedDate),
		labName: normalizeOptionalText(metadata.labName) ?? inferred.labName,
		location: normalizeOptionalText(metadata.location),
		language: normalizeOptionalText(metadata.language),
		country: normalizeOptionalText(metadata.country),
		notes: normalizeOptionalText(metadata.notes),
	};
}

function normalizeExtractionMeasurement(
	input: LooseExtractionMeasurement,
): Omit<ExtractionMeasurementOutput, 'sourcePage'> | null {
	const name = normalizeOptionalText(
		input.name ??
			input.sourceName ??
			input.measurementName ??
			input.analyteName ??
			input.canonicalName ??
			input.englishName ??
			input.standardizedName,
	);
	const canonicalName = normalizeOptionalText(
		input.canonicalName ?? input.englishName ?? input.standardizedName ?? input.name,
	);

	if (!name || !canonicalName) {
		return null;
	}

	return {
		name,
		canonicalName,
		valueText: normalizeOptionalText(input.valueText ?? input.value),
		valueNumeric: normalizeLooseNumber(input.valueNumeric),
		unit: normalizeOptionalText(input.unit),
	};
}

function normalizeNormalizationResult(
	input: LooseNormalizationResult,
): NormalizationResultOutput | null {
	const canonicalName = normalizeOptionalText(
		input.canonicalName ?? input.name ?? input.sourceName,
	);
	if (!canonicalName) {
		return null;
	}

	return {
		canonicalName,
		sourceName: normalizeOptionalText(input.sourceName ?? input.originalName ?? input.name),
		aliases: normalizeTextArray(input.aliases),
		canonicalUnit: normalizeOptionalText(input.canonicalUnit),
		knownUnits: normalizeTextArray(input.knownUnits),
		canonicalRangeText: normalizeOptionalText(input.canonicalRangeText),
		canonicalRangeMin: normalizeLooseNumber(input.canonicalRangeMin),
		canonicalRangeMax: normalizeLooseNumber(input.canonicalRangeMax),
		rangeEvidence: normalizeTextArray(input.rangeEvidence),
		valueText: normalizeOptionalText(input.valueText),
		valueNumeric: normalizeLooseNumber(input.valueNumeric),
		unit: normalizeOptionalText(input.unit),
		sourcePage: normalizePositiveInteger(input.sourcePage),
	};
}

function buildDraftsFromNormalization(output: NormalizationPassOutput) {
	const measurementDraftMap = new Map<string, MeasurementDraft>();
	const resultDrafts: NormalizedResultDraft[] = [];

	for (const row of output.results) {
		const cleanedMeasurement = cleanMeasurementDraft(row);
		const measurementKey = buildMeasurementKey(cleanedMeasurement.name);
		if (!measurementKey) {
			continue;
		}

		const existingMeasurement = measurementDraftMap.get(measurementKey);
		if (existingMeasurement) {
			existingMeasurement.aliases = unionText(
				existingMeasurement.aliases,
				cleanedMeasurement.aliases,
			);
			existingMeasurement.knownUnits = unionText(
				existingMeasurement.knownUnits,
				cleanedMeasurement.knownUnits,
			);
			existingMeasurement.rangeEvidence = unionText(
				existingMeasurement.rangeEvidence,
				cleanedMeasurement.rangeEvidence,
			);
			existingMeasurement.category =
				existingMeasurement.category ?? cleanedMeasurement.category ?? null;
			existingMeasurement.canonicalUnit =
				existingMeasurement.canonicalUnit ?? cleanedMeasurement.canonicalUnit ?? null;
			existingMeasurement.canonicalRangeText =
				existingMeasurement.canonicalRangeText ?? cleanedMeasurement.canonicalRangeText ?? null;
			existingMeasurement.canonicalRangeMin =
				existingMeasurement.canonicalRangeMin ?? cleanedMeasurement.canonicalRangeMin ?? null;
			existingMeasurement.canonicalRangeMax =
				existingMeasurement.canonicalRangeMax ?? cleanedMeasurement.canonicalRangeMax ?? null;
		} else {
			measurementDraftMap.set(measurementKey, {
				key: measurementKey,
				...cleanedMeasurement,
			});
		}

		const standardizedResult = standardizeResultDraft({
			measurement: measurementDraftMap.get(measurementKey)!,
			row,
		});

		resultDrafts.push({
			measurementKey,
			...standardizedResult,
		});
	}

	const dedupedResults = dedupeResultDrafts(resultDrafts);
	return {
		measurementDrafts: Array.from(measurementDraftMap.values()).sort((left, right) =>
			left.name.localeCompare(right.name),
		),
		resultDrafts: dedupedResults,
	};
}

async function resolveNormalizationOutput(args: {
	db: VitalsDatabase;
	document: LabDocumentRow;
	provider: ReturnType<typeof createOpenRouter>;
	modelId: string;
	existingMeasurements: LabMeasurementRow[];
	extractionPass: StructuredPassResult<ExtractionPassOutput>;
	pages: LabPdfPage[];
}) {
	const { db, document, provider, modelId, existingMeasurements, extractionPass, pages } = args;
	const measurementsByPage = new Map<number, ExtractionPassOutput['measurements']>();

	for (const measurement of extractionPass.output.measurements) {
		const pageMeasurements = measurementsByPage.get(measurement.sourcePage) ?? [];
		pageMeasurements.push(measurement);
		measurementsByPage.set(measurement.sourcePage, pageMeasurements);
	}

	const pageInputs = pages
		.flatMap(page => {
			const measurements = measurementsByPage.get(page.pageNumber) ?? [];
			const batches = chunkItems(measurements, NORMALIZATION_BATCH_SIZE);

			return batches.map((batch, batchIndex) => ({
				page,
				measurements: batch,
				batchIndex,
				batchCount: batches.length,
			}));
		})
		.filter(input => input.measurements.length > 0);

	if (pageInputs.length === 0) {
		return {
			results: [],
		};
	}

	const normalizationOutputs = await promiseParallel(
		pageInputs,
		async input => {
			try {
				const normalizationPass = await runNormalizationPass({
					provider,
					modelId,
					fileName: document.fileName,
					existingMeasurements,
					input,
				});
				return {
					results: normalizationPass.output.results
						.map(result => normalizeNormalizationResult(result))
						.filter(isPresent)
						.map(result => ({
							...result,
							sourcePage: result.sourcePage ?? input.page.pageNumber,
						})),
				};
			} catch (error) {
				if (!isRetryableNormalizationFormatError(error)) {
					throw error;
				}

				updateLabDocumentStatus(
					db,
					document,
					`Retrying normalization for page ${formatNormalizationBatchLabel(input)}`,
				);
				logLabDocumentEvent(
					document,
					`Normalization retry reason on page ${formatNormalizationBatchLabel(input)}: ${formatLabError(error)}`,
				);

				const retryPass = await runNormalizationPass({
					provider,
					modelId,
					fileName: document.fileName,
					existingMeasurements,
					input,
				});
				return {
					results: retryPass.output.results
						.map(result => normalizeNormalizationResult(result))
						.filter(isPresent)
						.map(result => ({
							...result,
							sourcePage: result.sourcePage ?? input.page.pageNumber,
						})),
				};
			}
		},
		{
			concurrency: 5,
			retries: 0,
		},
	);

	return {
		results: normalizationOutputs.flatMap(output => output.results),
	};
}

function formatNormalizationBatchLabel(input: {
	page: Pick<LabPdfPage, 'pageNumber'>;
	batchIndex: number;
	batchCount: number;
}) {
	if (input.batchCount <= 1) {
		return String(input.page.pageNumber);
	}

	return `${input.page.pageNumber} (${input.batchIndex + 1}/${input.batchCount})`;
}

function chunkItems<T>(items: T[], chunkSize: number) {
	if (chunkSize <= 0) {
		throw new Error('Chunk size must be positive.');
	}

	const chunks: T[][] = [];
	for (let index = 0; index < items.length; index += chunkSize) {
		chunks.push(items.slice(index, index + chunkSize));
	}

	return chunks;
}

function formatLabError(error: unknown) {
	return error instanceof Error ? error.message : String(error);
}

function isRetryableNormalizationFormatError(error: unknown) {
	const message = formatLabError(error).toLowerCase();

	return (
		message.includes('invalid input') ||
		message.includes('invalid_type') ||
		message.includes('expected') ||
		message.includes('json')
	);
}

function cleanMeasurementDraft(
	input: NormalizationPassOutput['results'][number],
): Omit<MeasurementDraft, 'key'> {
	const preferredCanonicalUnit = resolvePreferredCanonicalUnit(
		input.canonicalName,
		input.canonicalUnit,
	);
	const canonicalRange = resolveRangeDraft({
		rangeText: input.canonicalRangeText,
		rangeMin: input.canonicalRangeMin,
		rangeMax: input.canonicalRangeMax,
	});
	const standardizedCanonicalRange = standardizeMeasurementRangeToCanonicalUnit({
		measurementName: input.canonicalName,
		sourceUnit: input.canonicalUnit,
		targetUnit: preferredCanonicalUnit,
		rangeMin: canonicalRange.min,
		rangeMax: canonicalRange.max,
		rangeText: canonicalRange.text,
	});

	return {
		name: normalizeMeasurementName(input.canonicalName),
		category: resolveCanonicalMeasurementCategory(input.canonicalName, null),
		aliases: unionText(normalizeTextArray(input.aliases), []),
		canonicalUnit: preferredCanonicalUnit,
		knownUnits: normalizeTextArray(input.knownUnits).map(canonicalizeUnitLabel),
		canonicalRangeText: standardizedCanonicalRange.text,
		canonicalRangeMin: standardizedCanonicalRange.min,
		canonicalRangeMax: standardizedCanonicalRange.max,
		rangeEvidence: normalizeTextArray(input.rangeEvidence),
	};
}

function standardizeResultDraft(args: {
	measurement: MeasurementDraft;
	row: NormalizationPassOutput['results'][number];
}) {
	const { measurement, row } = args;
	const resultRange = resolveRangeDraft({
		rangeText: null,
		rangeMin: measurement.canonicalRangeMin,
		rangeMax: measurement.canonicalRangeMax,
	});
	const normalized = standardizeMeasurementResultUnit({
		measurementName: measurement.name,
		targetUnit: measurement.canonicalUnit,
		valueNumeric: row.valueNumeric ?? null,
		valueText: normalizeOptionalText(row.valueText),
		unit: canonicalizeUnitOrNull(row.unit),
		originalValueNumeric: row.valueNumeric ?? null,
		originalValueText: normalizeOptionalText(row.valueText),
		originalUnit: canonicalizeUnitOrNull(row.unit),
	});

	return {
		originalName: normalizeOptionalText(row.sourceName),
		originalValueText: normalized.originalValueText,
		originalValueNumeric: normalized.originalValueNumeric,
		originalUnit: normalized.originalUnit,
		originalRangeText: null,
		originalRangeMin: null,
		originalRangeMax: null,
		valueText: normalizeNormalizedValueText(normalized.valueNumeric, normalized.valueText),
		valueNumeric: normalized.valueNumeric,
		unit: normalized.unit ?? measurement.canonicalUnit ?? null,
		note: null,
		confidence: null,
		sourcePage: row.sourcePage ?? null,
		evidence: unionText(
			normalizeTextArray(row.rangeEvidence),
			resultRange.text ? [resultRange.text] : [],
		),
	};
}

function dedupeResultDrafts(rows: NormalizedResultDraft[]) {
	const byMeasurementKey = new Map<string, NormalizedResultDraft>();

	for (const row of rows) {
		const existing = byMeasurementKey.get(row.measurementKey);
		if (!existing) {
			byMeasurementKey.set(row.measurementKey, row);
			continue;
		}

		const existingScore = scoreResultDraft(existing);
		const rowScore = scoreResultDraft(row);
		if (rowScore > existingScore) {
			byMeasurementKey.set(row.measurementKey, row);
		}
	}

	return Array.from(byMeasurementKey.values()).sort((left, right) =>
		left.measurementKey.localeCompare(right.measurementKey),
	);
}

function scoreResultDraft(row: NormalizedResultDraft) {
	let score = 0;
	if (row.valueNumeric !== null) score += 3;
	if (row.unit) score += 2;
	if (row.originalRangeMin !== null || row.originalRangeMax !== null || row.originalRangeText)
		score += 2;
	if (row.sourcePage !== null) score += 1;
	if (row.confidence !== null) score += row.confidence;
	return score;
}

function upsertMeasurementDrafts(db: VitalsDatabase, drafts: MeasurementDraft[]) {
	const idByKey = new Map<string, number>();
	const now = new Date().toISOString();

	for (const draft of drafts) {
		const existing = db
			.select()
			.from(labMeasurements)
			.where(eq(labMeasurements.key, draft.key))
			.get();

		if (!existing) {
			const inserted = db
				.insert(labMeasurements)
				.values({
					key: draft.key,
					name: draft.name,
					category: draft.category,
					aliasesJson: draft.aliases.filter(alias => alias !== draft.name),
					canonicalUnit: draft.canonicalUnit,
					knownUnitsJson: draft.knownUnits,
					unitConversionsJson: resolveMeasurementUnitConversions(draft.name),
					canonicalRangeMin: draft.canonicalRangeMin,
					canonicalRangeMax: draft.canonicalRangeMax,
					canonicalRangeText: draft.canonicalRangeText,
					rangeEvidenceJson: draft.rangeEvidence,
					createdAt: now,
					updatedAt: now,
				})
				.returning({
					id: labMeasurements.id,
				})
				.get();
			idByKey.set(draft.key, inserted.id);
			if (draft.category === OTHER_CATEGORY) {
				console.log(
					`[labs] new measurement ${draft.name} (${draft.key}) assigned to ${OTHER_CATEGORY}`,
				);
			}
			continue;
		}

		const aliases = unionText(normalizeTextArray(existing.aliasesJson), draft.aliases).filter(
			alias => alias !== draft.name,
		);
		const knownUnits = unionText(normalizeTextArray(existing.knownUnitsJson), draft.knownUnits);
		const rangeEvidence = unionText(
			normalizeTextArray(existing.rangeEvidenceJson),
			draft.rangeEvidence,
		);

		db.update(labMeasurements)
			.set({
				name: draft.name,
				category: draft.category,
				aliasesJson: aliases,
				canonicalUnit: draft.canonicalUnit ?? existing.canonicalUnit,
				knownUnitsJson: knownUnits,
				unitConversionsJson: resolveMeasurementUnitConversions(draft.name),
				canonicalRangeMin: draft.canonicalRangeMin ?? existing.canonicalRangeMin,
				canonicalRangeMax: draft.canonicalRangeMax ?? existing.canonicalRangeMax,
				canonicalRangeText: draft.canonicalRangeText ?? existing.canonicalRangeText,
				rangeEvidenceJson: rangeEvidence,
				updatedAt: now,
			})
			.where(eq(labMeasurements.id, existing.id))
			.run();

		if (draft.category === OTHER_CATEGORY && existing.category !== OTHER_CATEGORY) {
			console.log(
				`[labs] measurement ${draft.name} (${draft.key}) reassigned to ${OTHER_CATEGORY}`,
			);
		}

		idByKey.set(draft.key, existing.id);
	}

	return idByKey;
}

async function generateStructuredOutput<T>(args: {
	model: any;
	schema: z.ZodType<T>;
	messages: any[];
	maxOutputTokens?: number;
}): Promise<StructuredPassResult<T>> {
	let lastError: unknown = null;
	const messages = injectResultJsonInstruction(args.messages);

	for (let attempt = 0; attempt < 2; attempt += 1) {
		try {
			const result = await generateText({
				model: args.model,
				temperature: 0,
				maxRetries: 1,
				messages,
				maxOutputTokens: args.maxOutputTokens,
			});

			return {
				output: args.schema.parse(parseJsonObjectFromTaggedText(result.text)),
			};
		} catch (error) {
			lastError = error;
			if (!isRetryableNormalizationFormatError(error) || attempt === 1) {
				throw error;
			}
		}
	}

	throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

function injectResultJsonInstruction(messages: any[]) {
	if (messages.length === 0) {
		return [
			{
				role: 'assistant' as const,
				content: [{ type: 'text' as const, text: '<result_json>' }],
			},
		];
	}

	const [firstMessage, ...restMessages] = messages;
	if (!firstMessage || !Array.isArray(firstMessage.content)) {
		return messages;
	}

	return [
		{
			...firstMessage,
			content: [
				{
					type: 'text' as const,
					text: [
						'You may think and reason freely.',
						'Start your final answer with <result_json> and end it with </result_json>.',
						'Output exactly one <result_json>...</result_json> block containing the final JSON object.',
						'Do not put JSON outside the <result_json> tags.',
					].join('\n'),
				},
				...firstMessage.content,
			],
		},
		...restMessages,
		{
			role: 'assistant' as const,
			content: [{ type: 'text' as const, text: '<result_json>' }],
		},
	];
}

function parseJsonObjectFromTaggedText(text: string) {
	const trimmed = text.trim();
	if (!trimmed) {
		throw new Error('No output generated.');
	}

	const startTag = '<result_json>';
	const endTag = '</result_json>';
	const startIndex = trimmed.indexOf(startTag);
	const endIndex = trimmed.lastIndexOf(endTag);

	if (startIndex < 0 || endIndex < 0 || endIndex <= startIndex) {
		return parseJsonObjectFromText(trimmed);
	}

	const candidate = trimmed.slice(startIndex + startTag.length, endIndex).trim();

	if (!candidate) {
		throw new Error('Empty <result_json> block.');
	}

	return parseJsonObject(candidate);
}

function parseJsonObjectFromText(text: string) {
	const objectStart = text.indexOf('{');
	const objectEnd = text.lastIndexOf('}');
	if (objectStart < 0 || objectEnd <= objectStart) {
		throw new Error('Missing <result_json> block.');
	}

	return parseJsonObject(text.slice(objectStart, objectEnd + 1));
}

function parseJsonObject(text: string) {
	try {
		return JSON.parse(text);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		const trimmed = text.trimEnd();
		const likelyTruncated =
			message.includes("Expected ']'") ||
			message.includes("Expected '}'") ||
			(!trimmed.endsWith('}') && !trimmed.endsWith(']'));

		throw new Error(
			`JSON parse error: ${message}${likelyTruncated ? ' (output appears truncated)' : ''}`,
		);
	}
}

function normalizeOptionalText(value: string | null | undefined) {
	if (typeof value !== 'string') {
		return null;
	}

	const trimmed = value.trim();
	return trimmed ? trimmed : null;
}

function normalizeOptionalNumber(value: number | null | undefined) {
	return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function normalizeLooseNumber(value: number | string | null | undefined) {
	if (typeof value === 'number' && Number.isFinite(value)) {
		return value;
	}

	if (typeof value !== 'string') {
		return null;
	}

	const normalized = value
		.replace(',', '.')
		.replace(/[^0-9.+-]/g, '')
		.trim();
	if (!normalized) {
		return null;
	}

	const parsed = Number.parseFloat(normalized);
	return Number.isFinite(parsed) ? parsed : null;
}

function normalizePositiveInteger(value: number | string | null | undefined) {
	if (typeof value === 'number' && Number.isInteger(value) && value > 0) {
		return value;
	}

	if (typeof value !== 'string') {
		return null;
	}

	const parsed = Number.parseInt(value.trim(), 10);
	return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function isPresent<T>(value: T | null | undefined): value is T {
	return value !== null && value !== undefined;
}

function normalizeTextArray(values: string[] | null | undefined) {
	const normalized = new Set<string>();
	for (const value of values ?? []) {
		const next = normalizeOptionalText(value);
		if (next) {
			normalized.add(next);
		}
	}
	return Array.from(normalized);
}

function unionText(left: string[], right: string[]) {
	return Array.from(new Set([...left, ...right].map(value => value.trim()).filter(Boolean)));
}

function normalizeOptionalIsoDate(value: string | null | undefined) {
	const text = normalizeOptionalText(value);
	if (!text) {
		return null;
	}

	try {
		return normalizeIsoDate(text);
	} catch {
		return null;
	}
}

function normalizeMeasurementName(value: string) {
	return value.replace(/\s+/g, ' ').trim();
}

function canonicalizeUnitOrNull(value: string | null | undefined) {
	const text = normalizeOptionalText(value);
	return text ? canonicalizeUnitLabel(text) : null;
}

function resolveRangeDraft(args: {
	rangeText: string | null | undefined;
	rangeMin: number | null | undefined;
	rangeMax: number | null | undefined;
}) {
	const rangeText = normalizeOptionalText(args.rangeText);
	const directMin = normalizeOptionalNumber(args.rangeMin);
	const directMax = normalizeOptionalNumber(args.rangeMax);
	if (directMin !== null || directMax !== null) {
		return {
			text: rangeText,
			min: directMin,
			max: directMax,
		};
	}

	const parsed = rangeText ? parseReferenceRangeBoundsFromText(rangeText) : undefined;
	return {
		text: rangeText,
		min: parsed?.min ?? null,
		max: parsed?.max ?? null,
	};
}

function inferMetadataFromFileName(fileName: string) {
	const baseName = path.basename(fileName, path.extname(fileName));
	const dateMatch = baseName.match(/\d{4}-\d{2}-\d{2}/);
	const inferredDate = dateMatch ? dateMatch[0] : null;
	const normalizedName = baseName.toLowerCase();
	let labName = 'Unknown Lab';
	if (normalizedName.includes('quest')) {
		labName = 'Quest Diagnostics';
	} else if (normalizedName.includes('labcorp')) {
		labName = 'LabCorp';
	} else if (
		normalizedName.includes('physicians-lab') ||
		normalizedName.includes('physicians_lab')
	) {
		labName = 'Physicians Lab';
	} else if (normalizedName.includes('limbach') || normalizedName.includes('mdi')) {
		labName = 'MDI Limbach Berlin GmbH';
	}

	return {
		date: inferredDate,
		labName,
	};
}

function buildMeasurementKey(name: string) {
	return name
		.normalize('NFKD')
		.replace(/[\u0300-\u036f]/g, '')
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, ' ')
		.trim();
}

const HEMATOLOGY_CATEGORY = 'Hematology';
const LIPIDS_CATEGORY = 'Lipids';
const METABOLISM_CATEGORY = 'Metabolism';
const KIDNEY_ELECTROLYTES_CATEGORY = 'Kidney & Electrolytes';
const LIVER_BILIARY_CATEGORY = 'Liver & Biliary';
const THYROID_CATEGORY = 'Thyroid';
const HORMONES_CATEGORY = 'Hormones';
const IRON_CATEGORY = 'Iron';
const VITAMINS_MINERALS_CATEGORY = 'Vitamins & Minerals';
const IMMUNE_INFLAMMATION_CATEGORY = 'Immune & Inflammation';
const INFECTIOUS_DISEASE_CATEGORY = 'Infectious Disease';
const URINALYSIS_CATEGORY = 'Urinalysis';
const GUT_HEALTH_CATEGORY = 'Gut Health';
const AMINO_ACIDS_CATEGORY = 'Amino Acids';
const FATTY_ACIDS_CATEGORY = 'Fatty Acids';
const PHYSICAL_MEASURES_CATEGORY = 'Physical Measures';
const OTHER_CATEGORY = 'Other';

const CANONICAL_BLOODWORK_CATEGORIES = new Set([
	HEMATOLOGY_CATEGORY,
	LIPIDS_CATEGORY,
	METABOLISM_CATEGORY,
	KIDNEY_ELECTROLYTES_CATEGORY,
	LIVER_BILIARY_CATEGORY,
	THYROID_CATEGORY,
	HORMONES_CATEGORY,
	IRON_CATEGORY,
	VITAMINS_MINERALS_CATEGORY,
	IMMUNE_INFLAMMATION_CATEGORY,
	INFECTIOUS_DISEASE_CATEGORY,
	URINALYSIS_CATEGORY,
	GUT_HEALTH_CATEGORY,
	AMINO_ACIDS_CATEGORY,
	FATTY_ACIDS_CATEGORY,
	PHYSICAL_MEASURES_CATEGORY,
	OTHER_CATEGORY,
]);

const BLOODWORK_CATEGORY_BY_KEY: Record<string, string> = {
	'aalp apo a1': LIPIDS_CATEGORY,
	'aalp apo c1': LIPIDS_CATEGORY,
	'aalp apo c2': LIPIDS_CATEGORY,
	'aalp apo c3': LIPIDS_CATEGORY,
	'aalp apo c4': LIPIDS_CATEGORY,
	'alpha pregnanediol': HORMONES_CATEGORY,
	adiponectin: METABOLISM_CATEGORY,
	androstenedione: HORMONES_CATEGORY,
	'apoe genotype': LIPIDS_CATEGORY,
	'apolipoprotein b a1 ratio': LIPIDS_CATEGORY,
	'apolipoprotein c1': LIPIDS_CATEGORY,
	'apolipoprotein c2': LIPIDS_CATEGORY,
	'apolipoprotein c3': LIPIDS_CATEGORY,
	'apolipoprotein c4': LIPIDS_CATEGORY,
	'arachidonic acid': FATTY_ACIDS_CATEGORY,
	'arachidonic acid epa ratio': FATTY_ACIDS_CATEGORY,
	'adma sdma': KIDNEY_ELECTROLYTES_CATEGORY,
	'blood pressure': PHYSICAL_MEASURES_CATEGORY,
	'body mass index': PHYSICAL_MEASURES_CATEGORY,
	bilirubin: URINALYSIS_CATEGORY,
	'c peptide': METABOLISM_CATEGORY,
	'cholesterol hdl ratio': LIPIDS_CATEGORY,
	'creatine kinase': METABOLISM_CATEGORY,
	'cystatin c': KIDNEY_ELECTROLYTES_CATEGORY,
	dha: FATTY_ACIDS_CATEGORY,
	dihydrotestosterone: HORMONES_CATEGORY,
	dpa: FATTY_ACIDS_CATEGORY,
	egfr: KIDNEY_ELECTROLYTES_CATEGORY,
	epa: FATTY_ACIDS_CATEGORY,
	'erythrocyte sedimentation rate': IMMUNE_INFLAMMATION_CATEGORY,
	'f2 isoprostanes creatinine': IMMUNE_INFLAMMATION_CATEGORY,
	fasting: METABOLISM_CATEGORY,
	'fibrinogen antigen': HEMATOLOGY_CATEGORY,
	'health quotient score': PHYSICAL_MEASURES_CATEGORY,
	height: PHYSICAL_MEASURES_CATEGORY,
	'height feet': PHYSICAL_MEASURES_CATEGORY,
	'height inches': PHYSICAL_MEASURES_CATEGORY,
	'hdl efflux capacity': LIPIDS_CATEGORY,
	'hdl efflux pcad score': LIPIDS_CATEGORY,
	'hdl pcad score': LIPIDS_CATEGORY,
	'hdlfx pcad score': LIPIDS_CATEGORY,
	'hdlfx pcec': LIPIDS_CATEGORY,
	'igf 1': HORMONES_CATEGORY,
	'igf 1 z score': HORMONES_CATEGORY,
	'insulin intact': METABOLISM_CATEGORY,
	'insulin resistance score': METABOLISM_CATEGORY,
	'intact parathyroid hormone': HORMONES_CATEGORY,
	'ionized calcium': VITAMINS_MINERALS_CATEGORY,
	'large hdl': LIPIDS_CATEGORY,
	'ldl particle number': LIPIDS_CATEGORY,
	'ldl pattern': LIPIDS_CATEGORY,
	'ldl peak size': LIPIDS_CATEGORY,
	ldh: METABOLISM_CATEGORY,
	'linoleic acid': FATTY_ACIDS_CATEGORY,
	'lipoprotein associated phospholipase a2 activity': LIPIDS_CATEGORY,
	'lp pla2 activity': LIPIDS_CATEGORY,
	'medium ldl': LIPIDS_CATEGORY,
	mpv: HEMATOLOGY_CATEGORY,
	myeloperoxidase: IMMUNE_INFLAMMATION_CATEGORY,
	'nasem recommended summation': FATTY_ACIDS_CATEGORY,
	'omega 6 total': FATTY_ACIDS_CATEGORY,
	'omega 6 omega 3 ratio': FATTY_ACIDS_CATEGORY,
	omegacheck: FATTY_ACIDS_CATEGORY,
	'oxidized ldl': LIPIDS_CATEGORY,
	'plasminogen activator inhibitor pai 1 ag': HEMATOLOGY_CATEGORY,
	'plasminogen activator inhibitor 1 antigen': HEMATOLOGY_CATEGORY,
	protein: URINALYSIS_CATEGORY,
	'prostate specific antigen': OTHER_CATEGORY,
	'serum igg': IMMUNE_INFLAMMATION_CATEGORY,
	'serum osmolality': KIDNEY_ELECTROLYTES_CATEGORY,
	'small ldl': LIPIDS_CATEGORY,
	'thyroid stimulating immunoglobulin': THYROID_CATEGORY,
	'thyroxine binding globulin': THYROID_CATEGORY,
	tmao: METABOLISM_CATEGORY,
	'total omega 3': FATTY_ACIDS_CATEGORY,
	trab: THYROID_CATEGORY,
	weight: PHYSICAL_MEASURES_CATEGORY,
	'waist circumference': PHYSICAL_MEASURES_CATEGORY,
};

const BLOODWORK_CATEGORY_BY_SOURCE_CATEGORY: Record<string, string> = {
	Autoimmune: IMMUNE_INFLAMMATION_CATEGORY,
	'Actin (Smooth Muscle) Antibody': IMMUNE_INFLAMMATION_CATEGORY,
	'B-Vitamin/Methylation Cofactor Assessment': VITAMINS_MINERALS_CATEGORY,
	'Bone Health': VITAMINS_MINERALS_CATEGORY,
	CBC: HEMATOLOGY_CATEGORY,
	'CBC With Differential/Platelet': HEMATOLOGY_CATEGORY,
	'C-Reactive Protein, Quant': IMMUNE_INFLAMMATION_CATEGORY,
	'Cellular Energy Production': METABOLISM_CATEGORY,
	'Comp. Metabolic Panel (14)': KIDNEY_ELECTROLYTES_CATEGORY,
	'Cortisol - AM': HORMONES_CATEGORY,
	'Creatine Kinase,Total': METABOLISM_CATEGORY,
	'Diabetes Risk': METABOLISM_CATEGORY,
	Electrolytes: KIDNEY_ELECTROLYTES_CATEGORY,
	Enzymes: METABOLISM_CATEGORY,
	'Essential Amino Acids': AMINO_ACIDS_CATEGORY,
	'Fatty Acids': FATTY_ACIDS_CATEGORY,
	'Folate (Folic Acid), Serum': VITAMINS_MINERALS_CATEGORY,
	'General Health': KIDNEY_ELECTROLYTES_CATEGORY,
	'Glucose (2 Spec, WHO) Toler,S': METABOLISM_CATEGORY,
	'Gut Assessment': GUT_HEALTH_CATEGORY,
	'HBsAg Screen': INFECTIOUS_DISEASE_CATEGORY,
	'HCV Antibody reflex to NAA': INFECTIOUS_DISEASE_CATEGORY,
	'HEMOGLOBIN A1C W/CALC MPG': METABOLISM_CATEGORY,
	'Hep A Ab, IgM': INFECTIOUS_DISEASE_CATEGORY,
	'Hepatic Function Panel (7)': LIVER_BILIARY_CATEGORY,
	'Hepatitis B Core Ab W/Reflex': INFECTIOUS_DISEASE_CATEGORY,
	Hematology: HEMATOLOGY_CATEGORY,
	Hormones: HORMONES_CATEGORY,
	Immunology: IMMUNE_INFLAMMATION_CATEGORY,
	'Infectious Disease': INFECTIOUS_DISEASE_CATEGORY,
	Inflammation: IMMUNE_INFLAMMATION_CATEGORY,
	'Inflammation and Oxidative Stress': IMMUNE_INFLAMMATION_CATEGORY,
	'Iron and TIBC': IRON_CATEGORY,
	'Iron Status': IRON_CATEGORY,
	'Kidney Function': KIDNEY_ELECTROLYTES_CATEGORY,
	'Kidney Health': KIDNEY_ELECTROLYTES_CATEGORY,
	'Lipid Panel': LIPIDS_CATEGORY,
	Lipids: LIPIDS_CATEGORY,
	'Liver Fibrosis': LIVER_BILIARY_CATEGORY,
	'Liver Function': LIVER_BILIARY_CATEGORY,
	'Liver Health': LIVER_BILIARY_CATEGORY,
	Magnesium: VITAMINS_MINERALS_CATEGORY,
	Metabolism: METABOLISM_CATEGORY,
	Microbiology: INFECTIOUS_DISEASE_CATEGORY,
	Minerals: VITAMINS_MINERALS_CATEGORY,
	'Muscle Assessment': AMINO_ACIDS_CATEGORY,
	'NASH FibroSure': LIVER_BILIARY_CATEGORY,
	'Non-Essential Amino Acids': AMINO_ACIDS_CATEGORY,
	'Physical Measures': PHYSICAL_MEASURES_CATEGORY,
	Proteins: LIVER_BILIARY_CATEGORY,
	'Prostate Health': OTHER_CATEGORY,
	TSH: THYROID_CATEGORY,
	'Thyroid Antibodies': THYROID_CATEGORY,
	'Urinalysis, Complete': URINALYSIS_CATEGORY,
	Vitamins: VITAMINS_MINERALS_CATEGORY,
};

const BLOODWORK_CATEGORY_RULES: Array<{ category: string; pattern: RegExp }> = [
	{
		category: URINALYSIS_CATEGORY,
		pattern:
			/^(?:appearance|bacteria|casts?|color \(urine\)|epithelial cells \(non renal\)|glucose \(urine\)|hyaline cast|ketones|leukocyte esterase|nitrite(?:, urine)?|occult blood|ph|specific gravity|squamous epithelial cells|urine color|urobilinogen|wbc esterase)$/i,
	},
	{
		category: HEMATOLOGY_CATEGORY,
		pattern:
			/^(?:absolute .+|basophils|eosinophils|hematocrit|hemoglobin|immature granulocytes|lymphocytes|mch|mchc|mcv|monocytes|mpv|neutrophils|nucleated red blood cells|platelet count|platelets|rdw|red blood cells|white blood cells)$/i,
	},
	{
		category: LIPIDS_CATEGORY,
		pattern:
			/^(?:apolipoprotein a[- ]?1|apolipoprotein b(?:\/a[- ]?1 ratio)?|apolipoprotein c[1-4]|cholesterol\/hdl ratio|hdl cholesterol|hdl efflux (?:capacity|pcad score)|hdlfx (?:pcad score|pcec)|large hdl|ldl cholesterol|ldl particle number|ldl pattern|ldl peak size|lipoprotein \(a\)|lipoprotein-associated phospholipase a2 activity|lp-pla2 activity|medium ldl|non-hdl cholesterol|oxidized ldl|small ldl|total cholesterol|triglycerides|vldl cholesterol)$/i,
	},
	{
		category: METABOLISM_CATEGORY,
		pattern:
			/^(?:3-hydroxy-3-methylglutaric acid|3-hydroxybutyric acid|adipic acid|adiponectin|amylase|c-peptide|cis-aconitic acid|derived mean glucose|estimated average glucose|glucose|glucose, fasting|glucose, 2 hour|hba1c \(hplc\)|hba1c \(ifcc\)|hemoglobin a1c|homa-ir|insulin resistance score|insulin intact|lactic acid|lipase|mean plasma glucose|pyruvic acid|suberic acid|succinic acid|tmao|uric acid)$/i,
	},
	{
		category: KIDNEY_ELECTROLYTES_CATEGORY,
		pattern:
			/^(?:bun|bun\/creatinine ratio|carbon dioxide|chloride|creatinine|cystatin c|egfr|egfr \(ckd-epi\)|egfr african american|egfr non-african american|estimated glomerular filtration rate|potassium|serum osmolality|sodium)$/i,
	},
	{
		category: LIVER_BILIARY_CATEGORY,
		pattern:
			/^(?:alanine aminotransferase|alt \(gpt\)|albumin|albumin\/globulin ratio|alkaline phosphatase|alpha [12] globulin|alpha-2-macroglobulin|apolipoprotein a-1|aspartate aminotransferase|ast \(got\)|beta [12] globulin|direct bilirubin|fibrosis interpretation|fibrosis score|fibrosis stage|gamma globulin|gamma-gt|gamma-glutamyl transferase|haptoglobin|indirect bilirubin|nash grade|nash score|necroinflammatory activity grade|necroinflammatory activity score|necroinflammatory interpretation|protein electrophoresis interpretation|serum protein electrophoresis interpretation|steatosis grade|steatosis score|total bilirubin|total globulin|total protein)$/i,
	},
	{
		category: THYROID_CATEGORY,
		pattern:
			/^(?:free t3|free t4|thyroglobulin antibody|thyroid peroxidase antibody|thyroid stimulating immunoglobulin|thyroxine binding globulin|trab|tsh|tsh \(basal\))$/i,
	},
	{
		category: HORMONES_CATEGORY,
		pattern:
			/^(?:alpha-pregnanediol|androstenedione|bioavailable testosterone|cortisol - am|dhea-s|dihydrotestosterone|estradiol|estradiol \(e2\)|follicle stimulating hormone \(fsh\)|free testosterone|free testosterone index|igf-1|igf-1 z score|insulin|intact parathyroid hormone|luteinizing hormone \(lh\)|prolactin|sex hormone binding globulin|testosterone)$/i,
	},
	{
		category: IRON_CATEGORY,
		pattern:
			/^(?:ferritin|iron|iron saturation|total iron binding capacity|transferrin|transferrin saturation|unsaturated iron binding capacity)$/i,
	},
	{
		category: VITAMINS_MINERALS_CATEGORY,
		pattern:
			/^(?:albumin-corrected calcium|calcium|ceruloplasmin|folate|folic acid|holotranscobalamin \(holotc\)|ionized calcium|magnesium|magnesium in erythrocytes|selenium|vitamin b12|vitamin b2|vitamin b6|vitamin d, 25-hydroxy|vitamin d3 \(25-oh\)|zinc)$/i,
	},
	{
		category: IMMUNE_INFLAMMATION_CATEGORY,
		pattern:
			/^(?:actin \(smooth muscle\) antibody|alpha-1-antitrypsin|ana screen|anti-dsdna antibody ifa|b2 glycoprotein i \(iga\)ab|beta-2 glycoprotein i ig[agm](?: antibody)?|c-reactive protein|cardiolipin (?:ab|antibody) ?(?:\((?:iga|igg|igm)\)|ig[agm])|centromere b antibody|chromatin(?: \(nucleosomal\))? antibody|complement c[34]|complement component c3c|complement component c4[ac]|cyclic citrullinated peptide \(ccp\) antibody \(igg\)|deamidated gliadin peptide (?:antibody )?ig[ag]|dna ab \(ds\) crithidia,ifa|erythrocyte sedimentation rate|(?:almond|brazil nut|cacao|casein|cashew nut|clam|codfish|coffee|cow's milk|crab|egg white|hazelnut|lobster|macadamia nut|maize\/corn|peanut|salmon|scallop|sesame seed|shrimp|soybean|tomato|tuna|walnut|wheat|yeast)(?: \([^)]+\))? ig[eg](?: class)?|f2-isoprostanes\/creatinine|high-sensitivity c-reactive protein|igg|igg subclass [1-4]|immunoglobulin a|immunoglobulin g|immunoglobulin g subclass [1-4]|immunoglobulin m|jo-1 antibody|mmp9|mutated citrullinated vimentin \(mcv\) antibody|myeloperoxidase|rheumatoid factor ig[agm]|rnp antibody|scl-70 antibody|serum igg|sjogren's antibody \(ss-[ab]\)|sm(?:\/rnp)? antibody|smith antibody|smooth muscle antibody screen|smooth muscle antibody titer|tissue transglutaminase (?:ab, )?igg|tissue transglutaminase antibody ig[ag]|tissue transglutaminase iga|wheat \(f4\) igg)$/i,
	},
	{
		category: INFECTIOUS_DISEASE_CATEGORY,
		pattern:
			/^(?:beta hemolytic streptococcus, group c|chlamydia trachomatis rna|hcv index|hepatitis a antibody igm|hepatitis a antibody total|hepatitis b core antibody total|hepatitis b surface antigen screen|hepatitis c antibody|hiv (?:ag\/ab screen|antigen\/antibody screen|final interpretation)|hsv [12] igg(?: inhibition| type specific antibody)?|neisseria gonorrhoeae rna|rapid plasma reagin|rpr|trichomonas vaginalis rna|upper respiratory culture)$/i,
	},
	{
		category: GUT_HEALTH_CATEGORY,
		pattern: /^(?:allantoin|benzoic acid|glutamine|hippuric acid|histidine|xanthurenic acid)$/i,
	},
	{
		category: AMINO_ACIDS_CATEGORY,
		pattern:
			/^(?:1-methyl-histidine|2-aminobutyric acid|3-methyl-histidine|alanine|asparagine|beta-alanine|citrulline|gamma-aminobutyric acid|glycine|hydroxyproline|isoleucine|leucine|methionine|phenylalanine|proline|serine|taurine|threonine|tryptophan|tyrosine|valine)$/i,
	},
	{
		category: FATTY_ACIDS_CATEGORY,
		pattern:
			/^(?:arachidonic acid(?:\/epa ratio)?|dha|docosahexaenoic acid \(dha\)|dpa|eicosapentaenoic acid \(epa\)|epa|linoleic acid|omega-3 index|omega-6(?:\/omega-3 ratio| total)?|omegacheck|total fatty acids|total omega-3)$/i,
	},
	{
		category: PHYSICAL_MEASURES_CATEGORY,
		pattern:
			/^(?:blood pressure|body mass index|height|height \(feet\)|height \(inches\)|waist circumference|weight)$/i,
	},
];

function resolveCanonicalMeasurementCategory(name: string, sourceCategory?: string | null) {
	const key = buildMeasurementKey(name);
	const mappedByKey = BLOODWORK_CATEGORY_BY_KEY[key];
	if (mappedByKey) {
		return mappedByKey;
	}

	for (const rule of BLOODWORK_CATEGORY_RULES) {
		if (rule.pattern.test(name)) {
			return rule.category;
		}
	}

	const normalizedSourceCategory = normalizeOptionalText(sourceCategory);
	if (normalizedSourceCategory) {
		if (CANONICAL_BLOODWORK_CATEGORIES.has(normalizedSourceCategory)) {
			return normalizedSourceCategory;
		}

		const remappedSourceCategory = BLOODWORK_CATEGORY_BY_SOURCE_CATEGORY[normalizedSourceCategory];
		if (remappedSourceCategory) {
			return remappedSourceCategory;
		}
	}

	return OTHER_CATEGORY;
}

const ISO_DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;

function toInteger(value: string, fieldName: string) {
	const parsed = Number.parseInt(value, 10);
	if (Number.isNaN(parsed)) {
		throw new Error(`Invalid ${fieldName}: ${value}`);
	}
	return parsed;
}

function isValidCalendarDate(year: number, month: number, day: number) {
	if (month < 1 || month > 12) {
		return false;
	}
	if (day < 1 || day > 31) {
		return false;
	}
	const date = new Date(Date.UTC(year, month - 1, day));
	return (
		date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day
	);
}

function normalizeIsoDate(rawDate: string) {
	const value = rawDate.trim();
	if (!value) {
		throw new Error('Date is empty');
	}

	if (ISO_DATE_REGEX.test(value)) {
		const [year, month, day] = value.split('-').map(part => toInteger(part, 'date part'));
		if (!isValidCalendarDate(year, month, day)) {
			throw new Error(`Invalid ISO date: ${value}`);
		}
		return value;
	}

	const yearFirst = value.match(/^(\d{4})[./-](\d{1,2})[./-](\d{1,2})$/);
	if (yearFirst) {
		const year = toInteger(yearFirst[1], 'year');
		const month = toInteger(yearFirst[2], 'month');
		const day = toInteger(yearFirst[3], 'day');
		if (!isValidCalendarDate(year, month, day)) {
			throw new Error(`Invalid date: ${value}`);
		}
		return `${year.toString().padStart(4, '0')}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
	}

	const dayFirst = value.match(/^(\d{1,2})[./-](\d{1,2})[./-](\d{4})$/);
	if (dayFirst) {
		const day = toInteger(dayFirst[1], 'day');
		const month = toInteger(dayFirst[2], 'month');
		const year = toInteger(dayFirst[3], 'year');
		if (!isValidCalendarDate(year, month, day)) {
			throw new Error(`Invalid date: ${value}`);
		}
		return `${year.toString().padStart(4, '0')}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
	}

	const parsedTimestamp = Date.parse(value);
	if (!Number.isNaN(parsedTimestamp)) {
		return new Date(parsedTimestamp).toISOString().slice(0, 10);
	}

	throw new Error(`Could not parse date: ${value}`);
}

function parseRangeNumber(raw: string) {
	const parsed = Number.parseFloat(raw.replace(/[<>]/g, '').replace(',', '.').trim());
	return Number.isFinite(parsed) ? parsed : undefined;
}

function parseReferenceRangeBoundsFromText(text: string) {
	const trimmed = text.trim();
	if (!trimmed) {
		return undefined;
	}

	const pair = trimmed.match(
		/([<>]?\s*-?\d+(?:[.,]\d+)?)\s*(?:-|–|—|to)\s*([<>]?\s*-?\d+(?:[.,]\d+)?)/i,
	);
	if (pair) {
		const min = parseRangeNumber(pair[1]!);
		const max = parseRangeNumber(pair[2]!);
		if (min === undefined && max === undefined) {
			return undefined;
		}
		return { min, max };
	}

	const comparator = trimmed.match(/([<>]=?)\s*(-?\d+(?:[.,]\d+)?)/);
	if (!comparator) {
		return undefined;
	}

	const value = parseRangeNumber(comparator[2]!);
	if (value === undefined) {
		return undefined;
	}

	if (comparator[1]!.includes('<')) {
		return { max: value };
	}

	return { min: value };
}

type UnitStandardizationConverter = {
	convert: (value: number) => number;
	factor: number;
	offset: number;
};

type MeasurementUnitStandardizationRule = {
	namePattern: RegExp;
	canonicalUnit: string;
	convertersByUnitKey: Record<string, UnitStandardizationConverter>;
};

const IDENTITY_UNIT_CONVERTER: UnitStandardizationConverter = {
	convert: value => value,
	factor: 1,
	offset: 0,
};

function scaledUnitConverter(factor: number): UnitStandardizationConverter {
	return {
		convert: value => value * factor,
		factor,
		offset: 0,
	};
}

function shiftedScaledUnitConverter(args: {
	factor: number;
	offset: number;
}): UnitStandardizationConverter {
	return {
		convert: value => value * args.factor + args.offset,
		factor: args.factor,
		offset: args.offset,
	};
}

function normalizeUnitKey(unit: string) {
	return unit
		.trim()
		.replace(/\u03bc/g, 'µ')
		.replace(/μ/g, 'µ')
		.replace(/\((?:calc|calculated)\)/gi, '')
		.replace(/\./g, '')
		.replace(/\s+/g, '')
		.toLowerCase();
}

function createUnitConverterMap(
	entries: Array<{
		unit: string;
		converter: UnitStandardizationConverter;
	}>,
) {
	const map: Record<string, UnitStandardizationConverter> = {};
	for (const entry of entries) {
		map[normalizeUnitKey(entry.unit)] = entry.converter;
	}
	return map;
}

const UNIT_CANONICAL_LABELS_BY_KEY: Record<string, string> = {
	'%': '%',
	'%oftotalhgb': '%',
	'mg/dl': 'mg/dL',
	'mg/l': 'mg/L',
	'mmol/l': 'mmol/L',
	'mmol/mol': 'mmol/mol',
	'g/dl': 'g/dL',
	'g/l': 'g/L',
	'µmol/l': 'µmol/L',
	'umol/l': 'µmol/L',
	'µg/dl': 'µg/dL',
	'ug/dl': 'µg/dL',
	'mcg/dl': 'µg/dL',
	'µg/l': 'µg/L',
	'ug/l': 'µg/L',
	'ng/ml': 'ng/mL',
	'pg/ml': 'pg/mL',
	'ng/l': 'ng/L',
	'iu/l': 'IU/L',
	'u/l': 'U/L',
	'mui/l': 'mUI/L',
	'µkat/l': 'µkat/L',
	'ukat/l': 'µkat/L',
	'uiu/ml': 'uIU/mL',
	'miu/l': 'mIU/L',
	'mu/l': 'mU/L',
	'k/µl': 'K/µL',
	'k/ul': 'K/µL',
	'x10e3/ul': 'K/µL',
	'gpt/l': 'K/µL',
	'thous/mcl': 'K/µL',
	'm/µl': 'M/µL',
	'm/ul': 'M/µL',
	'x10e6/ul': 'M/µL',
	'mill/mcl': 'M/µL',
	'l/l': 'L/L',
	'ml/min/1.73m2': 'mL/min/1.73m2',
};

const MEASUREMENT_UNIT_STANDARDIZATION_RULES: MeasurementUnitStandardizationRule[] = [
	{
		namePattern: /^(?:glucose|estimated average glucose|mean glucose)$/i,
		canonicalUnit: 'mg/dL',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'mg/dL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'mmol/L', converter: scaledUnitConverter(18.0182) },
		]),
	},
	{
		namePattern:
			/^(?:cholesterol|cholesterol, total|total cholesterol|hdl cholesterol|ldl cholesterol(?: \(calculated\))?|non-hdl cholesterol|vldl cholesterol)$/i,
		canonicalUnit: 'mg/dL',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'mg/dL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'mmol/L', converter: scaledUnitConverter(38.67) },
		]),
	},
	{
		namePattern: /^triglycerides?$/i,
		canonicalUnit: 'mg/dL',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'mg/dL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'mmol/L', converter: scaledUnitConverter(88.57) },
		]),
	},
	{
		namePattern: /^creatinine$/i,
		canonicalUnit: 'mg/dL',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'mg/dL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'µmol/L', converter: scaledUnitConverter(1 / 88.4) },
		]),
	},
	{
		namePattern: /^bilirubin(?:, (?:total|direct|indirect))?$/i,
		canonicalUnit: 'mg/dL',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'mg/dL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'µmol/L', converter: scaledUnitConverter(1 / 17.104) },
		]),
	},
	{
		namePattern: /^(?:albumin|globulin(?:, total)?|protein, total|total protein)$/i,
		canonicalUnit: 'g/dL',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'g/dL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'g/L', converter: scaledUnitConverter(0.1) },
		]),
	},
	{
		namePattern: /^hematocrit$/i,
		canonicalUnit: '%',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: '%', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'L/L', converter: scaledUnitConverter(100) },
		]),
	},
	{
		namePattern: /^hemoglobin a1c$/i,
		canonicalUnit: '%',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: '%', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: '% of total Hgb', converter: IDENTITY_UNIT_CONVERTER },
			{
				unit: 'mmol/mol',
				converter: shiftedScaledUnitConverter({ factor: 0.09148, offset: 2.152 }),
			},
		]),
	},
	{
		namePattern: /^apolipoprotein (?:a1|b)$/i,
		canonicalUnit: 'mg/dL',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'mg/dL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'g/L', converter: scaledUnitConverter(100) },
		]),
	},
	{
		namePattern: /^iron$/i,
		canonicalUnit: 'µg/dL',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'µg/dL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'µmol/L', converter: scaledUnitConverter(5.585) },
		]),
	},
	{
		namePattern: /^c-reactive protein$/i,
		canonicalUnit: 'mg/L',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'mg/L', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'mg/dL', converter: scaledUnitConverter(10) },
		]),
	},
	{
		namePattern: /^tsh$/i,
		canonicalUnit: 'uIU/mL',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'uIU/mL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'mU/L', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'mIU/L', converter: IDENTITY_UNIT_CONVERTER },
		]),
	},
	{
		namePattern: /^(?:white blood cells?|wbc|platelet count|platelets|absolute .+)$/i,
		canonicalUnit: 'K/µL',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'K/µL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'K/uL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'x10E3/uL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'Gpt/L', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'Thous/mcL', converter: IDENTITY_UNIT_CONVERTER },
		]),
	},
	{
		namePattern: /^(?:red blood cells?|rbc)$/i,
		canonicalUnit: 'M/µL',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'M/µL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'M/uL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'x10E6/uL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'Mill/mcL', converter: IDENTITY_UNIT_CONVERTER },
		]),
	},
];

function resolveMeasurementUnitConversions(measurementName: string) {
	const rule = findMeasurementUnitStandardizationRule(measurementName);
	if (!rule) {
		return [];
	}

	return Object.entries(rule.convertersByUnitKey).map(([unitKey, converter]) => ({
		unit: canonicalizeUnitLabel(unitKey),
		factor: converter.factor,
		...(converter.offset ? { offset: converter.offset } : {}),
	}));
}

function canonicalizeUnitLabel(unit: string) {
	const normalizedKey = normalizeUnitKey(unit);
	return UNIT_CANONICAL_LABELS_BY_KEY[normalizedKey] || unit.replace(/\s+/g, ' ').trim();
}

function roundStandardizedNumber(value: number) {
	const rounded = Number.parseFloat(value.toFixed(6));
	return Object.is(rounded, -0) ? 0 : rounded;
}

function findMeasurementUnitStandardizationRule(measurementName: string) {
	for (const rule of MEASUREMENT_UNIT_STANDARDIZATION_RULES) {
		if (rule.namePattern.test(measurementName)) {
			return rule;
		}
	}
	return null;
}

function resolvePreferredCanonicalUnit(measurementName: string, unit: string | null | undefined) {
	const rule = findMeasurementUnitStandardizationRule(measurementName);
	if (rule) {
		return rule.canonicalUnit;
	}

	return canonicalizeUnitOrNull(unit);
}

function standardizeMeasurementRangeToCanonicalUnit(args: {
	measurementName: string;
	sourceUnit: string | null | undefined;
	targetUnit: string | null | undefined;
	rangeMin: number | null;
	rangeMax: number | null;
	rangeText: string | null;
}) {
	const sourceUnit = canonicalizeUnitOrNull(args.sourceUnit);
	const targetUnit = canonicalizeUnitOrNull(args.targetUnit);
	const baseRange = {
		text: args.rangeText,
		min: args.rangeMin,
		max: args.rangeMax,
	};

	if (!sourceUnit || !targetUnit || sourceUnit === targetUnit) {
		return baseRange;
	}

	const rule = findMeasurementUnitStandardizationRule(args.measurementName);
	if (!rule || targetUnit !== rule.canonicalUnit) {
		return baseRange;
	}

	const converter = rule.convertersByUnitKey[normalizeUnitKey(sourceUnit)];
	if (!converter) {
		return baseRange;
	}

	const min =
		args.rangeMin === null ? null : roundStandardizedNumber(converter.convert(args.rangeMin));
	const max =
		args.rangeMax === null ? null : roundStandardizedNumber(converter.convert(args.rangeMax));

	return {
		text: formatCanonicalRangeText(min, max),
		min,
		max,
	};
}

function formatCanonicalRangeText(min: number | null, max: number | null) {
	if (min !== null && max !== null) {
		return `${min}-${max}`;
	}
	if (min !== null) {
		return `>=${min}`;
	}
	if (max !== null) {
		return `<=${max}`;
	}
	return null;
}

function parseComparableNumericValue(valueNumeric: number | null, valueText: string | null) {
	if (valueNumeric !== null) {
		return {
			comparator: '',
			numericValue: valueNumeric,
			hasComparator: false,
		};
	}

	if (!valueText) {
		return null;
	}

	const match = valueText.trim().match(/^(<=|>=|<|>)?\s*(-?\d+(?:[.,]\d+)?)$/);
	if (!match) {
		return null;
	}

	const numericValue = Number.parseFloat(match[2]!.replace(',', '.'));
	if (!Number.isFinite(numericValue)) {
		return null;
	}

	return {
		comparator: match[1] || '',
		numericValue,
		hasComparator: Boolean(match[1]),
	};
}

function normalizeNormalizedValueText(valueNumeric: number | null, valueText: string | null) {
	const normalizedText = normalizeOptionalText(valueText);
	const comparable = parseComparableNumericValue(valueNumeric, normalizedText);

	if (!comparable) {
		if (valueNumeric !== null && (!normalizedText || normalizedText.length > 96)) {
			return String(roundStandardizedNumber(valueNumeric));
		}

		return normalizedText;
	}

	const normalizedNumber = String(roundStandardizedNumber(comparable.numericValue));
	return comparable.hasComparator
		? `${comparable.comparator}${normalizedNumber}`
		: normalizedNumber;
}

function standardizeMeasurementResultUnit(args: {
	measurementName: string;
	targetUnit: string | null;
	valueNumeric: number | null;
	valueText: string | null;
	unit: string | null;
	originalValueNumeric: number | null;
	originalValueText: string | null;
	originalUnit: string | null;
}) {
	const currentUnit = args.unit ?? args.targetUnit ?? null;
	if (!currentUnit) {
		return {
			valueNumeric: args.valueNumeric,
			valueText: args.valueText,
			unit: currentUnit,
			originalValueNumeric: args.originalValueNumeric,
			originalValueText: args.originalValueText,
			originalUnit: args.originalUnit,
		};
	}

	const rule = findMeasurementUnitStandardizationRule(args.measurementName);
	if (!rule) {
		return {
			valueNumeric: args.valueNumeric,
			valueText: args.valueText,
			unit: currentUnit,
			originalValueNumeric: args.originalValueNumeric,
			originalValueText: args.originalValueText,
			originalUnit: args.originalUnit,
		};
	}

	const sourceUnitKey = normalizeUnitKey(currentUnit);
	const targetUnit = args.targetUnit ? canonicalizeUnitLabel(args.targetUnit) : rule.canonicalUnit;
	const targetUnitKey = normalizeUnitKey(targetUnit);
	const converter = rule.convertersByUnitKey[sourceUnitKey];
	if (!converter) {
		return {
			valueNumeric: args.valueNumeric,
			valueText: args.valueText,
			unit: currentUnit,
			originalValueNumeric: args.originalValueNumeric,
			originalValueText: args.originalValueText,
			originalUnit: args.originalUnit,
		};
	}

	if (sourceUnitKey === targetUnitKey) {
		return {
			valueNumeric: args.valueNumeric,
			valueText: args.valueText,
			unit: targetUnit,
			originalValueNumeric: args.originalValueNumeric,
			originalValueText: args.originalValueText,
			originalUnit: args.originalUnit,
		};
	}

	const comparable = parseComparableNumericValue(args.valueNumeric, args.valueText);
	if (!comparable) {
		return {
			valueNumeric: args.valueNumeric,
			valueText: args.valueText,
			unit: currentUnit,
			originalValueNumeric: args.originalValueNumeric,
			originalValueText: args.originalValueText,
			originalUnit: args.originalUnit,
		};
	}

	const convertedNumeric = roundStandardizedNumber(converter.convert(comparable.numericValue));
	const convertedValueText = comparable.hasComparator
		? `${comparable.comparator}${convertedNumeric}`
		: String(convertedNumeric);

	return {
		valueNumeric: convertedNumeric,
		valueText: convertedValueText,
		unit: targetUnit,
		originalValueNumeric: args.originalValueNumeric,
		originalValueText: args.originalValueText,
		originalUnit: args.originalUnit ?? currentUnit,
	};
}
