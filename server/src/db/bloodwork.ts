import fs from 'fs';
import os from 'os';
import path from 'path';
import { spawnSync } from 'child_process';
import { createHash } from 'crypto';

import { createOpenRouter } from '@openrouter/ai-sdk-provider';
import { generateText, Output } from 'ai';
import { eq } from 'drizzle-orm';
import { getDocument } from 'pdfjs-dist/legacy/build/pdf.mjs';
import { z } from 'zod';

import { getDatabase, type VitalsDatabase } from 'server/db/client.ts';
import {
	bloodworkDocuments,
	bloodworkMeasurements,
	bloodworkResults,
	type BloodworkDocumentRow,
	type BloodworkMeasurementRow,
} from 'server/db/schema.ts';
import env from 'server/env.ts';

const LOCAL_TEXT_MIN_CHARACTERS_PER_PAGE = 320;

const bloodworkUploadFileInputSchema = z.object({
	fileName: z.string().trim().min(1),
	mimeType: z.string().trim().min(1),
	dataBase64: z.string().trim().min(1),
});

export const bloodworkUploadDocumentsInputSchema = z.object({
	files: z.array(bloodworkUploadFileInputSchema).min(1).max(12),
});

export const bloodworkRetryDocumentInputSchema = z.object({
	documentId: z.number().int().positive(),
});

const nullableTextSchema = z.string().trim().min(1).nullable().optional();
const nullableNumberSchema = z.number().finite().nullable().optional();

const extractionMeasurementSchema = z.object({
	name: z.string().trim().min(1),
	category: nullableTextSchema,
	originalName: nullableTextSchema,
	valueText: nullableTextSchema,
	valueNumeric: nullableNumberSchema,
	unit: nullableTextSchema,
	rangeText: nullableTextSchema,
	rangeMin: nullableNumberSchema,
	rangeMax: nullableNumberSchema,
	note: nullableTextSchema,
	sourcePage: z.number().int().positive().nullable().optional(),
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

const extractionPassSchema = z.object({
	metadata: extractionMetadataSchema,
	measurements: z.array(extractionMeasurementSchema),
});

const normalizationMeasurementSchema = z.object({
	name: z.string().trim().min(1),
	category: nullableTextSchema,
	aliases: z.array(z.string().trim().min(1)).optional().default([]),
	canonicalUnit: nullableTextSchema,
	knownUnits: z.array(z.string().trim().min(1)).optional().default([]),
	canonicalRangeText: nullableTextSchema,
	canonicalRangeMin: nullableNumberSchema,
	canonicalRangeMax: nullableNumberSchema,
	rangeEvidence: z.array(z.string().trim().min(1)).optional().default([]),
});

const normalizationResultSchema = z.object({
	measurement: normalizationMeasurementSchema,
	originalName: nullableTextSchema,
	originalValueText: nullableTextSchema,
	originalValueNumeric: nullableNumberSchema,
	originalUnit: nullableTextSchema,
	originalRangeText: nullableTextSchema,
	originalRangeMin: nullableNumberSchema,
	originalRangeMax: nullableNumberSchema,
	valueText: nullableTextSchema,
	valueNumeric: nullableNumberSchema,
	unit: nullableTextSchema,
	note: nullableTextSchema,
	confidence: z.number().finite().min(0).max(1).nullable().optional(),
	sourcePage: z.number().int().positive().nullable().optional(),
	evidence: z.array(z.string().trim().min(1)).optional().default([]),
});

const normalizationPassSchema = z.object({
	results: z.array(normalizationResultSchema),
});

type BloodworkUploadDocumentsInput = z.infer<typeof bloodworkUploadDocumentsInputSchema>;
type ExtractionPassOutput = z.infer<typeof extractionPassSchema>;
type NormalizationPassOutput = z.infer<typeof normalizationPassSchema>;

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

type PdfProbe = {
	extractedText: string;
	extractedTextStatus: string;
	pageCount: number;
	parserEngine: 'pdf-text' | 'mistral-ocr';
};

type StructuredPassResult<T> = {
	output: T;
};

let processorPromise: Promise<void> | null = null;
let processorStarted = false;

export function getBloodworkDashboard(db: VitalsDatabase) {
	const documents = db
		.select({
			id: bloodworkDocuments.id,
			date: bloodworkDocuments.date,
			group: bloodworkDocuments.group,
			queuedAt: bloodworkDocuments.queuedAt,
		})
		.from(bloodworkDocuments)
		.where(eq(bloodworkDocuments.status, 'completed'))
		.orderBy(bloodworkDocuments.date, bloodworkDocuments.id)
		.all()
		.reverse();

	const measurements = db
		.select()
		.from(bloodworkMeasurements)
		.orderBy(bloodworkMeasurements.name, bloodworkMeasurements.id)
		.all();

	const results = db
		.select()
		.from(bloodworkResults)
		.orderBy(bloodworkResults.documentId, bloodworkResults.sortOrder, bloodworkResults.id)
		.all();

	return { documents, measurements, results };
}

export function listBloodworkDocuments(db: VitalsDatabase) {
	return db
		.select({
			id: bloodworkDocuments.id,
			fileName: bloodworkDocuments.fileName,
			status: bloodworkDocuments.status,
			statusText: bloodworkDocuments.statusText,
			group: bloodworkDocuments.group,
			date: bloodworkDocuments.date,
			labName: bloodworkDocuments.labName,
			queuedAt: bloodworkDocuments.queuedAt,
			lastError: bloodworkDocuments.lastError,
		})
		.from(bloodworkDocuments)
		.orderBy(bloodworkDocuments.id)
		.all()
		.reverse();
}

export async function uploadBloodworkDocuments(
	db: VitalsDatabase,
	input: BloodworkUploadDocumentsInput,
) {
	const parsed = bloodworkUploadDocumentsInputSchema.parse(input);
	const queued = enqueueBloodworkDocuments(
		db,
		parsed.files.map(file => ({
			fileName: file.fileName,
			mimeType: file.mimeType,
			pdfData: Buffer.from(file.dataBase64, 'base64'),
		})),
	);

	scheduleBloodworkProcessing();
	return {
		documents: queued,
	};
}

export async function retryBloodworkDocument(
	db: VitalsDatabase,
	input: z.infer<typeof bloodworkRetryDocumentInputSchema>,
) {
	const parsed = bloodworkRetryDocumentInputSchema.parse(input);
	const document = db
		.select({
			id: bloodworkDocuments.id,
			fileName: bloodworkDocuments.fileName,
			status: bloodworkDocuments.status,
		})
		.from(bloodworkDocuments)
		.where(eq(bloodworkDocuments.id, parsed.documentId))
		.get();

	if (!document) {
		throw new Error('Document not found.');
	}
	if (document.status !== 'failed') {
		throw new Error('Only failed documents can be retried.');
	}

	db.update(bloodworkDocuments)
		.set({
			status: 'pending',
			statusText: 'Queued for retry',
			startedAt: null,
			completedAt: null,
			failedAt: null,
			lastError: null,
		})
		.where(eq(bloodworkDocuments.id, parsed.documentId))
		.run();

	logBloodworkDocumentEvent(document, 'Queued for retry');
	scheduleBloodworkProcessing();

	return {
		documentId: parsed.documentId,
	};
}

export function getBloodworkDocumentPdf(
	db: VitalsDatabase,
	documentId: number,
): Pick<BloodworkDocumentRow, 'id' | 'fileName' | 'mimeType' | 'pdfData'> | null {
	return (
		db
			.select({
				id: bloodworkDocuments.id,
				fileName: bloodworkDocuments.fileName,
				mimeType: bloodworkDocuments.mimeType,
				pdfData: bloodworkDocuments.pdfData,
			})
			.from(bloodworkDocuments)
			.where(eq(bloodworkDocuments.id, documentId))
			.get() ?? null
	);
}

export function startBloodworkProcessor() {
	if (processorStarted) {
		return;
	}
	processorStarted = true;
	resetStuckBloodworkDocuments(getDatabase());
	scheduleBloodworkProcessing();
}

function scheduleBloodworkProcessing() {
	if (processorPromise) {
		return;
	}

	processorPromise = processTriggeredBloodworkDocument()
		.catch(error => {
			console.error('[bloodwork] processing trigger failed', error);
		})
		.finally(() => {
			processorPromise = null;

			const db = getDatabase();
			if (hasNextPendingBloodworkDocument(db) && !hasActiveBloodworkDocument(db)) {
				scheduleBloodworkProcessing();
			}
		});
}

async function processTriggeredBloodworkDocument(db = getDatabase()) {
	const outcome = await processNextPendingBloodworkDocument(db);
	if (outcome === 'busy') {
		return;
	}
}

export async function processNextPendingBloodworkDocument(db = getDatabase()) {
	const nextDocumentId = claimNextPendingBloodworkDocument(db);
	if (nextDocumentId === 'busy' || nextDocumentId === null) {
		return nextDocumentId;
	}

	await processBloodworkDocument(db, nextDocumentId);
	return 'processed' as const;
}

function hasNextPendingBloodworkDocument(db: VitalsDatabase) {
	return db
		.select({
			id: bloodworkDocuments.id,
		})
		.from(bloodworkDocuments)
		.where(eq(bloodworkDocuments.status, 'pending'))
		.orderBy(bloodworkDocuments.id)
		.get();
}

function hasActiveBloodworkDocument(db: VitalsDatabase) {
	return db
		.select({
			id: bloodworkDocuments.id,
		})
		.from(bloodworkDocuments)
		.where(eq(bloodworkDocuments.status, 'processing'))
		.orderBy(bloodworkDocuments.id)
		.get();
}

function claimNextPendingBloodworkDocument(db: VitalsDatabase) {
	const client = db.$client;
	client.exec('BEGIN IMMEDIATE');

	try {
		const activeDocument = client
			.prepare("SELECT id FROM bloodwork_documents WHERE status = 'processing' ORDER BY id LIMIT 1")
			.get() as { id: number } | null;
		if (activeDocument) {
			client.exec('COMMIT');
			return 'busy' as const;
		}

		const pendingDocument = client
			.prepare("SELECT id FROM bloodwork_documents WHERE status = 'pending' ORDER BY id LIMIT 1")
			.get() as { id: number } | null;
		if (!pendingDocument) {
			client.exec('COMMIT');
			return null;
		}

		client
			.prepare(
				[
					'UPDATE bloodwork_documents',
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

export function resetStuckBloodworkDocuments(db = getDatabase()) {
	const interruptedDocuments = db
		.select({
			id: bloodworkDocuments.id,
			fileName: bloodworkDocuments.fileName,
		})
		.from(bloodworkDocuments)
		.where(eq(bloodworkDocuments.status, 'processing'))
		.all();

	if (interruptedDocuments.length === 0) {
		return;
	}

	db.update(bloodworkDocuments)
		.set({
			status: 'pending',
			statusText: 'Queued after interrupted processing',
			startedAt: null,
			lastError: 'Processing was interrupted and has been retried.',
		})
		.where(eq(bloodworkDocuments.status, 'processing'))
		.run();

	for (const document of interruptedDocuments) {
		logBloodworkDocumentEvent(document, 'Queued after interrupted processing');
	}
}

export function enqueueBloodworkDocuments(
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
		status: BloodworkDocumentRow['status'];
		statusText: string;
		queuedAt: string;
		deduplicated: boolean;
	}> = [];

	for (const file of files) {
		const sha256 = createHash('sha256').update(file.pdfData).digest('hex');
		const existing = db
			.select({
				id: bloodworkDocuments.id,
				fileName: bloodworkDocuments.fileName,
				status: bloodworkDocuments.status,
				statusText: bloodworkDocuments.statusText,
				queuedAt: bloodworkDocuments.queuedAt,
			})
			.from(bloodworkDocuments)
			.where(eq(bloodworkDocuments.sha256, sha256))
			.get();

		if (existing) {
			logBloodworkDocumentEvent(existing, 'Duplicate upload skipped');
			queued.push({
				...existing,
				deduplicated: true,
			});
			continue;
		}

		const inserted = db
			.insert(bloodworkDocuments)
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
				id: bloodworkDocuments.id,
				fileName: bloodworkDocuments.fileName,
				status: bloodworkDocuments.status,
				statusText: bloodworkDocuments.statusText,
				queuedAt: bloodworkDocuments.queuedAt,
			})
			.get();

		queued.push({
			...inserted,
			deduplicated: false,
		});
		logBloodworkDocumentEvent(inserted, inserted.statusText);
	}

	return queued;
}

function updateBloodworkDocumentStatus(
	db: VitalsDatabase,
	document: Pick<BloodworkDocumentRow, 'id' | 'fileName'>,
	statusText: string,
) {
	db.update(bloodworkDocuments)
		.set({
			statusText,
		})
		.where(eq(bloodworkDocuments.id, document.id))
		.run();

	logBloodworkDocumentEvent(document, statusText);
}

function logBloodworkDocumentEvent(
	document: Pick<BloodworkDocumentRow, 'id' | 'fileName'>,
	message: string,
) {
	console.log(`[bloodwork] #${document.id} ${document.fileName}: ${message}`);
}

async function processBloodworkDocument(db: VitalsDatabase, documentId: number) {
	const document = db
		.select()
		.from(bloodworkDocuments)
		.where(eq(bloodworkDocuments.id, documentId))
		.get();
	if (!document || document.status !== 'processing') {
		return;
	}

	try {
		updateBloodworkDocumentStatus(db, document, 'Extracting text from PDF');
		const probe = await probePdf(document);
		const provider = getOpenRouterProvider();
		const modelId = getBloodworkModelId();
		const existingMeasurements = db.select().from(bloodworkMeasurements).all();
		updateBloodworkDocumentStatus(db, document, 'Extracting measurements from document');
		const extractionPass = await runExtractionPass({
			provider,
			modelId,
			document,
			probe,
		});
		updateBloodworkDocumentStatus(db, document, 'Normalizing measurements');
		const normalizationOutput = await resolveNormalizationOutput({
			db,
			document,
			provider,
			modelId,
			probe,
			existingMeasurements,
			extractionPass,
		});
		const metadata = normalizeMetadataDraft(
			extractionPass.output.metadata,
			document.fileName,
			probe.extractedText,
		);
		const { measurementDrafts, resultDrafts } = buildDraftsFromNormalization(normalizationOutput);
		const savedStatusText = `Saving ${resultDrafts.length} normalized result${resultDrafts.length === 1 ? '' : 's'}`;
		const completedStatusText = `Imported ${resultDrafts.length} result${resultDrafts.length === 1 ? '' : 's'}`;

		updateBloodworkDocumentStatus(db, document, savedStatusText);

		db.transaction(tx => {
			tx.delete(bloodworkResults).where(eq(bloodworkResults.documentId, documentId)).run();

			const measurementIdByKey = upsertMeasurementDrafts(
				tx as unknown as VitalsDatabase,
				measurementDrafts,
			);

			if (resultDrafts.length > 0) {
				tx.insert(bloodworkResults)
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
							evidenceJson: JSON.stringify(draft.evidence),
						})),
					)
					.run();
			}

			tx.update(bloodworkDocuments)
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
				.where(eq(bloodworkDocuments.id, documentId))
				.run();
		});
		logBloodworkDocumentEvent(document, completedStatusText);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		logBloodworkDocumentEvent(document, `Import failed: ${message}`);
		db.update(bloodworkDocuments)
			.set({
				status: 'failed',
				statusText: 'Import failed',
				failedAt: new Date().toISOString(),
				lastError: message,
			})
			.where(eq(bloodworkDocuments.id, documentId))
			.run();
	}
}

async function probePdf(document: BloodworkDocumentRow): Promise<PdfProbe> {
	const pageCount = await getPdfPageCount(document.pdfData);
	const extractedWithPdftotext = extractPdfTextWithPdftotext(document.pdfData, document.fileName);
	if (extractedWithPdftotext) {
		return {
			extractedText: extractedWithPdftotext,
			extractedTextStatus: 'pdftotext',
			pageCount,
			parserEngine: choosePdfParserEngine(extractedWithPdftotext, pageCount),
		};
	}

	const extractedWithPdfjs = await extractPdfTextWithPdfjs(document.pdfData);
	return {
		extractedText: extractedWithPdfjs,
		extractedTextStatus: 'pdfjs',
		pageCount,
		parserEngine: choosePdfParserEngine(extractedWithPdfjs, pageCount),
	};
}

async function runExtractionPass(args: {
	provider: ReturnType<typeof createOpenRouter>;
	modelId: string;
	document: BloodworkDocumentRow;
	probe: PdfProbe;
}): Promise<StructuredPassResult<ExtractionPassOutput>> {
	const { provider, modelId, document, probe } = args;
	const fileModel = provider(modelId, {
		plugins: [
			{
				id: 'file-parser',
				pdf: {
					engine: probe.parserEngine,
				},
			},
			{
				id: 'response-healing',
			},
		],
	});

	const fileMessages = [
		{
			role: 'user' as const,
			content: [
				{
					type: 'text' as const,
					text: buildExtractionPrompt({
						fileName: document.fileName,
						probe,
					}),
				},
				{
					type: 'file' as const,
					mediaType: 'application/pdf',
					filename: document.fileName,
					data: new Uint8Array(document.pdfData),
				},
			],
		},
	];

	try {
		const fileResult = await generateStructuredOutput({
			model: fileModel,
			schema: extractionPassSchema,
			messages: fileMessages,
			maxOutputTokens: 16_384,
		});

		if (fileResult.output.measurements.length > 0 || !probe.extractedText.trim()) {
			return fileResult;
		}
	} catch (error) {
		if (!probe.extractedText.trim()) {
			throw error;
		}
	}

	const textModel = provider(modelId, {
		plugins: [
			{
				id: 'response-healing',
			},
		],
	});

	return generateStructuredOutput({
		model: textModel,
		schema: extractionPassSchema,
		messages: [
			{
				role: 'user',
				content: [
					{
						type: 'text',
						text: buildTextOnlyExtractionPrompt({
							fileName: document.fileName,
							probe,
						}),
					},
				],
			},
		],
		maxOutputTokens: 16_384,
	});
}

async function runNormalizationPass(args: {
	provider: ReturnType<typeof createOpenRouter>;
	modelId: string;
	document: BloodworkDocumentRow;
	probe: PdfProbe;
	existingMeasurements: BloodworkMeasurementRow[];
	extractionPass: StructuredPassResult<ExtractionPassOutput>;
}): Promise<StructuredPassResult<NormalizationPassOutput>> {
	const { provider, modelId, document, probe, existingMeasurements, extractionPass } = args;
	const model = provider(modelId, {
		plugins: [
			{
				id: 'response-healing',
			},
		],
	});

	const normalizationUserMessage = {
		role: 'user' as const,
		content: [
			{
				type: 'text' as const,
				text: buildNormalizationPrompt({
					fileName: document.fileName,
					probe,
					existingMeasurements,
					extractionOutput: extractionPass.output,
				}),
			},
		],
	};

	return generateStructuredOutput({
		model,
		schema: normalizationPassSchema,
		messages: [normalizationUserMessage],
		maxOutputTokens: 12_288,
	});
}

function buildExtractionPrompt(args: { fileName: string; probe: PdfProbe }) {
	const { fileName, probe } = args;
	return [
		'Analyze this bloodwork PDF as one complete document.',
		'Return only structured JSON that matches the schema.',
		'Extract report-level metadata and raw analyte rows.',
		'Translate analyte names to concise English when needed.',
		'Preserve the original/raw name separately whenever the source wording differs.',
		'Keep one item per visible row even if rows appear duplicated elsewhere in the PDF.',
		'If a value is qualitative, keep it in valueText and leave valueNumeric null.',
		'If a range is textual like "<1" or "3.5 - 5.2", preserve rangeText and also set numeric bounds when possible.',
		'Use page numbers when possible.',
		`Source file: ${fileName}`,
		`Local text probe status: ${probe.extractedTextStatus}`,
		`Local text probe length: ${probe.extractedText.length} characters across ${probe.pageCount} pages.`,
	].join('\n');
}

function buildTextOnlyExtractionPrompt(args: { fileName: string; probe: PdfProbe }) {
	const { fileName, probe } = args;

	return [
		buildExtractionPrompt({
			fileName,
			probe,
		}),
		'The full-PDF extraction path failed or returned no analytes.',
		'Use the following locally extracted document text as the source of truth for this extraction pass.',
		'Document text:',
		probe.extractedText,
	].join('\n\n');
}

function buildNormalizationPrompt(args: {
	fileName: string;
	probe: PdfProbe;
	existingMeasurements: BloodworkMeasurementRow[];
	extractionOutput: ExtractionPassOutput;
}) {
	const { fileName, probe, existingMeasurements, extractionOutput } = args;
	const catalog = existingMeasurements
		.map(measurement => ({
			name: measurement.name,
			category: measurement.category,
			aliases: parseJsonArray<string>(measurement.aliasesJson),
			canonicalUnit: measurement.canonicalUnit,
			canonicalRangeMin: measurement.canonicalRangeMin,
			canonicalRangeMax: measurement.canonicalRangeMax,
			canonicalRangeText: measurement.canonicalRangeText,
		}))
		.slice(0, 600);

	return [
		'Normalize the previously parsed bloodwork document into one final result per logical measurement.',
		'Use existing canonical measurement names whenever they are clearly the same analyte.',
		'Create new canonical measurements when no existing entry is a clean match.',
		'Return English canonical measurement names only.',
		'Deduplicate same-document duplicates and keep the best final value/range/unit combination.',
		'Normalize result units to the canonical measurement unit.',
		'Choose exactly one canonical range per canonical measurement.',
		'Preserve original/raw fields on each result row.',
		'If the source has conflicting ranges for the same analyte, choose one canonical range and explain the evidence briefly.',
		'Do not output more than one result row for the same logical measurement in this document.',
		`Source file: ${fileName}`,
		`Parser engine: ${probe.parserEngine}`,
		'Existing canonical measurements:',
		JSON.stringify(catalog, null, 2),
		'First-pass extraction output:',
		JSON.stringify(extractionOutput, null, 2),
	].join('\n');
}

function normalizeMetadataDraft(
	metadata: ExtractionPassOutput['metadata'],
	fileName: string,
	extractedText: string,
) {
	const inferred = inferMetadataFromFileName(fileName, extractedText);

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

function buildDraftsFromNormalization(output: NormalizationPassOutput) {
	const measurementDraftMap = new Map<string, MeasurementDraft>();
	const resultDrafts: NormalizedResultDraft[] = [];

	for (const row of output.results) {
		const cleanedMeasurement = cleanMeasurementDraft(row.measurement);
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
	document: BloodworkDocumentRow;
	provider: ReturnType<typeof createOpenRouter>;
	modelId: string;
	probe: PdfProbe;
	existingMeasurements: BloodworkMeasurementRow[];
	extractionPass: StructuredPassResult<ExtractionPassOutput>;
}) {
	const { db, document, provider, modelId, probe, existingMeasurements, extractionPass } = args;

	let lastError: unknown = null;

	for (let attempt = 1; attempt <= 2; attempt += 1) {
		if (attempt === 2 && lastError) {
			updateBloodworkDocumentStatus(db, document, 'Retrying normalization after invalid response');
			logBloodworkDocumentEvent(
				document,
				`Normalization retry reason: ${formatBloodworkError(lastError)}`,
			);
		}

		try {
			const normalizationPass = await runNormalizationPass({
				provider,
				modelId,
				document,
				probe,
				existingMeasurements,
				extractionPass,
			});
			return normalizationPass.output;
		} catch (error) {
			if (
				extractionPass.output.measurements.length === 0 ||
				!isRetryableNormalizationFormatError(error)
			) {
				throw error;
			}

			lastError = error;
			if (attempt === 1) {
				continue;
			}

			updateBloodworkDocumentStatus(
				db,
				document,
				'Normalization failed, using extracted rows fallback',
			);
			logBloodworkDocumentEvent(
				document,
				`Normalization fallback reason: ${formatBloodworkError(error)}`,
			);
			return buildFallbackNormalizationOutput({
				existingMeasurements,
				extractionOutput: extractionPass.output,
			});
		}
	}

	throw new Error('Normalization retry flow ended without a result.');
}

function formatBloodworkError(error: unknown) {
	return error instanceof Error ? error.message : String(error);
}

function isRetryableNormalizationFormatError(error: unknown) {
	const message = formatBloodworkError(error).toLowerCase();

	return (
		message.includes('invalid input') ||
		message.includes('invalid_type') ||
		message.includes('expected') ||
		message.includes('json')
	);
}

function buildFallbackNormalizationOutput(args: {
	existingMeasurements: BloodworkMeasurementRow[];
	extractionOutput: ExtractionPassOutput;
}): NormalizationPassOutput {
	const lookup = buildMeasurementLookup(args.existingMeasurements);

	return {
		results: args.extractionOutput.measurements.map(row => {
			const matchedMeasurement = findExistingMeasurementForRow(lookup, row);
			const normalizedUnit = canonicalizeUnitOrNull(row.unit);
			const range = resolveRangeDraft({
				rangeText: row.rangeText,
				rangeMin: row.rangeMin,
				rangeMax: row.rangeMax,
			});
			const existingAliases = matchedMeasurement
				? parseJsonArray<string>(matchedMeasurement.aliasesJson)
				: [];
			const existingKnownUnits = matchedMeasurement
				? parseJsonArray<string>(matchedMeasurement.knownUnitsJson)
				: [];
			const existingRangeEvidence = matchedMeasurement
				? parseJsonArray<string>(matchedMeasurement.rangeEvidenceJson)
				: [];
			const preferredName = matchedMeasurement?.name ?? normalizeMeasurementName(row.name);
			const preferredCanonicalUnit = resolvePreferredCanonicalUnit(
				preferredName,
				matchedMeasurement?.canonicalUnit ?? normalizedUnit,
			);
			const canonicalRange = standardizeMeasurementRangeToCanonicalUnit({
				measurementName: preferredName,
				sourceUnit: normalizedUnit,
				targetUnit: preferredCanonicalUnit,
				rangeMin: matchedMeasurement?.canonicalRangeMin ?? range.min,
				rangeMax: matchedMeasurement?.canonicalRangeMax ?? range.max,
				rangeText: matchedMeasurement?.canonicalRangeText ?? range.text,
			});

			return {
				measurement: {
					name: preferredName,
					category: normalizeOptionalText(row.category) ?? matchedMeasurement?.category ?? null,
					aliases: unionText(
						existingAliases,
						[normalizeOptionalText(row.name), normalizeOptionalText(row.originalName)].filter(
							(value): value is string => Boolean(value && value !== preferredName),
						),
					),
					canonicalUnit: preferredCanonicalUnit,
					knownUnits: unionText(existingKnownUnits, normalizedUnit ? [normalizedUnit] : []),
					canonicalRangeText: canonicalRange.text,
					canonicalRangeMin: canonicalRange.min,
					canonicalRangeMax: canonicalRange.max,
					rangeEvidence: unionText(existingRangeEvidence, range.text ? [range.text] : []),
				},
				originalName: normalizeOptionalText(row.originalName) ?? normalizeOptionalText(row.name),
				originalValueText: normalizeOptionalText(row.valueText),
				originalValueNumeric: normalizeOptionalNumber(row.valueNumeric),
				originalUnit: normalizedUnit,
				originalRangeText: range.text,
				originalRangeMin: range.min,
				originalRangeMax: range.max,
				valueText: normalizeOptionalText(row.valueText),
				valueNumeric: normalizeOptionalNumber(row.valueNumeric),
				unit: normalizedUnit,
				note: normalizeOptionalText(row.note),
				confidence: null,
				sourcePage: row.sourcePage ?? null,
				evidence: range.text ? [range.text] : [],
			};
		}),
	};
}

function buildMeasurementLookup(existingMeasurements: BloodworkMeasurementRow[]) {
	const lookup = new Map<string, BloodworkMeasurementRow>();

	for (const measurement of existingMeasurements) {
		lookup.set(measurement.key, measurement);

		for (const alias of parseJsonArray<string>(measurement.aliasesJson)) {
			const aliasKey = buildMeasurementKey(alias);
			if (aliasKey && !lookup.has(aliasKey)) {
				lookup.set(aliasKey, measurement);
			}
		}
	}

	return lookup;
}

function findExistingMeasurementForRow(
	lookup: Map<string, BloodworkMeasurementRow>,
	row: ExtractionPassOutput['measurements'][number],
) {
	const candidateKeys = [
		buildMeasurementKey(row.name),
		buildMeasurementKey(normalizeOptionalText(row.originalName) ?? ''),
	].filter(Boolean);

	for (const key of candidateKeys) {
		const matched = lookup.get(key);
		if (matched) {
			return matched;
		}
	}

	return null;
}

function cleanMeasurementDraft(
	input: NormalizationPassOutput['results'][number]['measurement'],
): Omit<MeasurementDraft, 'key'> {
	const preferredCanonicalUnit = resolvePreferredCanonicalUnit(input.name, input.canonicalUnit);
	const canonicalRange = resolveRangeDraft({
		rangeText: input.canonicalRangeText,
		rangeMin: input.canonicalRangeMin,
		rangeMax: input.canonicalRangeMax,
	});
	const standardizedCanonicalRange = standardizeMeasurementRangeToCanonicalUnit({
		measurementName: input.name,
		sourceUnit: input.canonicalUnit,
		targetUnit: preferredCanonicalUnit,
		rangeMin: canonicalRange.min,
		rangeMax: canonicalRange.max,
		rangeText: canonicalRange.text,
	});

	return {
		name: normalizeMeasurementName(input.name),
		category: normalizeOptionalText(input.category),
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
	const originalRange = resolveRangeDraft({
		rangeText: row.originalRangeText,
		rangeMin: row.originalRangeMin,
		rangeMax: row.originalRangeMax,
	});
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
		originalValueNumeric: row.originalValueNumeric ?? row.valueNumeric ?? null,
		originalValueText:
			normalizeOptionalText(row.originalValueText) ?? normalizeOptionalText(row.valueText),
		originalUnit: canonicalizeUnitOrNull(row.originalUnit) ?? canonicalizeUnitOrNull(row.unit),
	});

	return {
		originalName: normalizeOptionalText(row.originalName),
		originalValueText: normalized.originalValueText,
		originalValueNumeric: normalized.originalValueNumeric,
		originalUnit: normalized.originalUnit,
		originalRangeText: originalRange.text,
		originalRangeMin: originalRange.min,
		originalRangeMax: originalRange.max,
		valueText: normalizeNormalizedValueText(normalized.valueNumeric, normalized.valueText),
		valueNumeric: normalized.valueNumeric,
		unit: normalized.unit ?? measurement.canonicalUnit ?? null,
		note: normalizeOptionalText(row.note),
		confidence: normalizeOptionalNumber(row.confidence),
		sourcePage: row.sourcePage ?? null,
		evidence: unionText(
			normalizeTextArray(row.evidence),
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
			.from(bloodworkMeasurements)
			.where(eq(bloodworkMeasurements.key, draft.key))
			.get();

		if (!existing) {
			const inserted = db
				.insert(bloodworkMeasurements)
				.values({
					key: draft.key,
					name: draft.name,
					category: draft.category,
					aliasesJson: JSON.stringify(draft.aliases.filter(alias => alias !== draft.name)),
					canonicalUnit: draft.canonicalUnit,
					knownUnitsJson: JSON.stringify(draft.knownUnits),
					canonicalRangeMin: draft.canonicalRangeMin,
					canonicalRangeMax: draft.canonicalRangeMax,
					canonicalRangeText: draft.canonicalRangeText,
					rangeEvidenceJson: JSON.stringify(draft.rangeEvidence),
					createdAt: now,
					updatedAt: now,
				})
				.returning({
					id: bloodworkMeasurements.id,
				})
				.get();
			idByKey.set(draft.key, inserted.id);
			continue;
		}

		const aliases = unionText(parseJsonArray<string>(existing.aliasesJson), draft.aliases).filter(
			alias => alias !== draft.name,
		);
		const knownUnits = unionText(parseJsonArray<string>(existing.knownUnitsJson), draft.knownUnits);
		const rangeEvidence = unionText(
			parseJsonArray<string>(existing.rangeEvidenceJson),
			draft.rangeEvidence,
		);

		db.update(bloodworkMeasurements)
			.set({
				name: draft.name,
				category: draft.category ?? existing.category,
				aliasesJson: JSON.stringify(aliases),
				canonicalUnit: draft.canonicalUnit ?? existing.canonicalUnit,
				knownUnitsJson: JSON.stringify(knownUnits),
				canonicalRangeMin: draft.canonicalRangeMin ?? existing.canonicalRangeMin,
				canonicalRangeMax: draft.canonicalRangeMax ?? existing.canonicalRangeMax,
				canonicalRangeText: draft.canonicalRangeText ?? existing.canonicalRangeText,
				rangeEvidenceJson: JSON.stringify(rangeEvidence),
				updatedAt: now,
			})
			.where(eq(bloodworkMeasurements.id, existing.id))
			.run();

		idByKey.set(draft.key, existing.id);
	}

	return idByKey;
}

async function generateStructuredOutput<T>(args: {
	model: ReturnType<ReturnType<typeof createOpenRouter>>;
	schema: z.ZodType<T>;
	messages: any[];
	maxOutputTokens?: number;
}): Promise<StructuredPassResult<T>> {
	try {
		const result = await generateText({
			model: args.model,
			output: Output.object({
				schema: args.schema,
			}),
			temperature: 0.25,
			maxRetries: 2,
			messages: args.messages,
			maxOutputTokens: args.maxOutputTokens,
		});

		return {
			output: args.schema.parse(result.output),
		};
	} catch {
		const textResult = await generateText({
			model: args.model,
			temperature: 0.75,
			maxRetries: 1,
			messages: args.messages,
			maxOutputTokens: args.maxOutputTokens,
		});

		const parsed = args.schema.parse(parseJsonObjectFromText(textResult.text));
		return {
			output: parsed,
		};
	}
}

function parseJsonObjectFromText(text: string) {
	const trimmed = text.trim();
	if (!trimmed) {
		throw new Error('No output generated.');
	}

	const fencedMatch = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/i);
	const candidate =
		fencedMatch?.[1]?.trim() ??
		(() => {
			const objectStart = trimmed.indexOf('{');
			const objectEnd = trimmed.lastIndexOf('}');
			if (objectStart >= 0 && objectEnd > objectStart) {
				return trimmed.slice(objectStart, objectEnd + 1);
			}
			return trimmed;
		})();

	return JSON.parse(candidate);
}

function getOpenRouterProvider() {
	return createOpenRouter({ apiKey: env.OPENROUTER_API_KEY });
}

function getBloodworkModelId() {
	return env.BLOODWORK_OPENROUTER_MODEL;
}

async function getPdfPageCount(pdfData: Buffer) {
	const document = await getDocument({ data: new Uint8Array(pdfData) }).promise;
	return document.numPages;
}

function extractPdfTextWithPdftotext(pdfData: Buffer, fileName: string) {
	const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'vitals-bloodwork-'));
	const pdfPath = path.join(tempDir, fileName);

	try {
		fs.writeFileSync(pdfPath, pdfData);
		const result = spawnSync('pdftotext', ['-layout', '-enc', 'UTF-8', pdfPath, '-'], {
			encoding: 'utf8',
			maxBuffer: 20 * 1024 * 1024,
		});

		if (result.status !== 0) {
			return null;
		}

		const text = (result.stdout || '').trim();
		return text || null;
	} catch {
		return null;
	} finally {
		fs.rmSync(tempDir, { recursive: true, force: true });
	}
}

async function extractPdfTextWithPdfjs(pdfData: Buffer) {
	const document = await getDocument({ data: new Uint8Array(pdfData) }).promise;
	const pageTexts: string[] = [];

	for (let pageIndex = 1; pageIndex <= document.numPages; pageIndex += 1) {
		const page = await document.getPage(pageIndex);
		const textContent = await page.getTextContent();
		const lines = new Map<number, Array<{ x: number; token: string }>>();

		for (const item of textContent.items) {
			if (!('str' in item)) continue;
			const token = item.str.trim();
			if (!token) continue;

			const transform =
				'transform' in item && Array.isArray(item.transform) ? item.transform : null;
			const x = transform ? Number(transform[4]) : Number.NaN;
			const y = transform ? Number(transform[5]) : Number.NaN;
			if (!Number.isFinite(x) || !Number.isFinite(y)) continue;

			const existingY = Array.from(lines.keys()).find(lineY => Math.abs(lineY - y) <= 1.8);
			const lineY = existingY ?? y;
			const line = lines.get(lineY) ?? [];
			line.push({ x, token });
			lines.set(lineY, line);
		}

		const pageBody = Array.from(lines.entries())
			.sort((left, right) => right[0] - left[0])
			.map(([, tokens]) =>
				tokens
					.sort((left, right) => left.x - right.x)
					.map(item => item.token)
					.join(' '),
			)
			.map(line => line.replace(/\s+/g, ' ').trim())
			.filter(Boolean)
			.join('\n')
			.trim();

		if (pageBody) {
			pageTexts.push(pageBody);
		}
	}

	return pageTexts.join('\n\n');
}

function choosePdfParserEngine(extractedText: string, pageCount: number) {
	const normalizedText = extractedText.trim();
	if (!normalizedText) {
		return 'mistral-ocr' as const;
	}

	const charactersPerPage = normalizedText.length / Math.max(pageCount, 1);
	return charactersPerPage >= LOCAL_TEXT_MIN_CHARACTERS_PER_PAGE ? 'pdf-text' : 'mistral-ocr';
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

function parseJsonArray<T>(raw: string | null | undefined): T[] {
	if (!raw) {
		return [];
	}

	try {
		const parsed = JSON.parse(raw);
		return Array.isArray(parsed) ? (parsed as T[]) : [];
	} catch {
		return [];
	}
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

function inferMetadataFromFileName(fileName: string, extractedText: string) {
	const baseName = path.basename(fileName, path.extname(fileName));
	const dateMatch = baseName.match(/\d{4}-\d{2}-\d{2}/);
	const inferredDate = dateMatch ? dateMatch[0] : null;
	const normalizedText = extractedText.toLowerCase();
	let labName = 'Unknown Lab';
	if (normalizedText.includes('quest diagnostics')) {
		labName = 'Quest Diagnostics';
	} else if (
		normalizedText.includes('labcorp') ||
		normalizedText.includes('laboratory corporation of america')
	) {
		labName = 'LabCorp';
	} else if (normalizedText.includes('physicians lab')) {
		labName = 'Physicians Lab';
	} else if (normalizedText.includes('mdi limbach')) {
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
};

type MeasurementUnitStandardizationRule = {
	namePattern: RegExp;
	canonicalUnit: string;
	convertersByUnitKey: Record<string, UnitStandardizationConverter>;
};

const IDENTITY_UNIT_CONVERTER: UnitStandardizationConverter = {
	convert: value => value,
};

function scaledUnitConverter(factor: number): UnitStandardizationConverter {
	return {
		convert: value => value * factor,
	};
}

function shiftedScaledUnitConverter(args: {
	factor: number;
	offset: number;
}): UnitStandardizationConverter {
	return {
		convert: value => value * args.factor + args.offset,
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
	'x10e3/ul': 'x10E3/uL',
	'x10e6/ul': 'x10E6/uL',
	'gpt/l': 'Gpt/L',
	'thous/mcl': 'Thous/mcL',
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
		namePattern: /^(?:platelet count|platelets)$/i,
		canonicalUnit: 'x10E3/uL',
		convertersByUnitKey: createUnitConverterMap([
			{ unit: 'x10E3/uL', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'Gpt/L', converter: IDENTITY_UNIT_CONVERTER },
			{ unit: 'Thous/mcL', converter: IDENTITY_UNIT_CONVERTER },
		]),
	},
];

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
