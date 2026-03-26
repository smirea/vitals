import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';

import { eq } from 'drizzle-orm';
import { z } from 'zod';

import { getDatabase } from 'server/db/client.ts';
import {
	labDocuments,
	labMeasurements,
	labResults,
	type LabDocumentRow,
} from 'server/db/schema.ts';
import { createRouter, publicProcedure } from 'server/trpc/shared.ts';

const tmpDir = path.join('/tmp', 'vitals');
fs.mkdirSync(tmpDir, { recursive: true });

// biome-ignore lint: simple ansi strip
const stripAnsi = (s: string) => s.replace(/\x1b\[[0-9;]*m/g, '');

const labDocumentIdInputSchema = z.object({
	documentId: z.number().int().positive(),
});

const labUploadDocumentsInputSchema = z.object({
	files: z
		.array(
			z.object({
				fileName: z.string().trim().min(1),
				mimeType: z.string().trim().min(1),
				dataBase64: z.string().trim().min(1),
			}),
		)
		.min(1)
		.max(12),
});

export const labsRouter = createRouter({
	getDashboard: publicProcedure.query(({ ctx }) => {
		const documents = ctx.db
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

		const measurements = ctx.db
			.select()
			.from(labMeasurements)
			.orderBy(labMeasurements.name, labMeasurements.id)
			.all();

		const results = ctx.db
			.select()
			.from(labResults)
			.orderBy(labResults.documentId, labResults.sortOrder, labResults.id)
			.all();

		return { documents, measurements, results };
	}),

	listDocuments: publicProcedure.query(({ ctx }) => {
		return ctx.db
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
	}),

	uploadDocuments: publicProcedure
		.input(labUploadDocumentsInputSchema)
		.mutation(({ ctx, input }) => {
			const now = new Date().toISOString();
			const queued: Array<{
				id: number;
				fileName: string;
				status: LabDocumentRow['status'];
				statusText: string;
				queuedAt: string;
				deduplicated: boolean;
			}> = [];

			for (const file of input.files) {
				const pdfData = Buffer.from(file.dataBase64, 'base64');
				const sha256 = createHash('sha256').update(pdfData).digest('hex');

				const existing = ctx.db
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
					console.log(`[labs] #${existing.id} ${existing.fileName}: duplicate upload skipped`);
					queued.push({ ...existing, deduplicated: true });
					continue;
				}

				const inserted = ctx.db
					.insert(labDocuments)
					.values({
						fileName: file.fileName,
						mimeType: file.mimeType,
						pdfData,
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

				queued.push({ ...inserted, deduplicated: false });
				console.log(`[labs] #${inserted.id} ${inserted.fileName}: queued for import`);
				processNextImport();
			}

			return { documents: queued };
		}),

	retryDocument: publicProcedure
		.input(labDocumentIdInputSchema)
		.mutation(({ ctx, input }) => requeueDocument(ctx.db, input.documentId, 'failed', 'retry')),

	reprocessDocument: publicProcedure
		.input(labDocumentIdInputSchema)
		.mutation(({ ctx, input }) =>
			requeueDocument(ctx.db, input.documentId, 'completed', 'reprocess'),
		),
});

function requeueDocument(
	db: ReturnType<typeof getDatabase>,
	documentId: number,
	requiredStatus: LabDocumentRow['status'],
	action: string,
) {
	const document = db
		.select({ id: labDocuments.id, fileName: labDocuments.fileName, status: labDocuments.status })
		.from(labDocuments)
		.where(eq(labDocuments.id, documentId))
		.get();

	if (!document) throw new Error('Document not found.');
	if (document.status !== requiredStatus)
		throw new Error(`Only ${requiredStatus} documents can be ${action}ed.`);

	db.update(labDocuments)
		.set({
			status: 'pending',
			statusText: `Queued for ${action}`,
			startedAt: null,
			completedAt: null,
			failedAt: null,
			lastError: null,
		})
		.where(eq(labDocuments.id, documentId))
		.run();

	console.log(`[labs] #${document.id} ${document.fileName}: queued for ${action}`);
	processNextImport();
	return { documentId };
}

export function getLabDocumentPdf(
	db: ReturnType<typeof getDatabase>,
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
	const db = getDatabase();

	db.update(labDocuments)
		.set({
			status: 'pending',
			statusText: 'Queued after interrupted processing',
			startedAt: null,
			lastError: null,
		})
		.where(eq(labDocuments.status, 'processing'))
		.run();

	processNextImport();
}

function processNextImport() {
	const db = getDatabase();
	const busy = db
		.select({ id: labDocuments.id })
		.from(labDocuments)
		.where(eq(labDocuments.status, 'processing'))
		.get();
	if (busy) return;

	const next = db
		.select({ id: labDocuments.id, fileName: labDocuments.fileName, pdfData: labDocuments.pdfData })
		.from(labDocuments)
		.where(eq(labDocuments.status, 'pending'))
		.get();
	if (!next) return;

	const tmpPath = path.join(tmpDir, `doc_${next.id}_${next.fileName}`);
	fs.writeFileSync(tmpPath, new Uint8Array(next.pdfData));

	const scriptPath = path.resolve(
		import.meta.dir,
		'..',
		'..',
		'..',
		'..',
		'scripts',
		'lab-import.ts',
	);
	const proc = Bun.spawn(['bun', 'run', scriptPath, tmpPath], {
		stdout: 'inherit',
		stderr: 'pipe',
	});

	proc.exited.then(async code => {
		if (code !== 0) {
			const raw = await new Response(proc.stderr).text();
			const message = stripAnsi(raw).trim() || `Process exited with code ${code}`;
			console.error(`[labs] #${next.id}: import failed — ${message}`);
			db.update(labDocuments)
				.set({
					status: 'failed',
					statusText: 'Import failed',
					failedAt: new Date().toISOString(),
					lastError: message,
				})
				.where(eq(labDocuments.id, next.id))
				.run();
		}
		processNextImport();
	});
}
