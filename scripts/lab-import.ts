#!/usr/bin/env bun
import { createScript, style } from './createScript';
import fs from 'fs';
import path from 'path';
import { generateText, Output } from 'ai';
import env from 'server/env';
import { getDatabase } from 'server/db/client';
import { labDocuments, labMeasurements, labResults } from 'server/db/schema';
import { textBlock } from 'shared/textBlock';
import z from 'zod';
import { createHash } from 'crypto';
import { eq } from 'drizzle-orm';
import convert, { getMeasureKind } from 'convert';
import typedUnits from './units.json';
import models from 'server/utils/models';

const tmpDir = path.join('/tmp', 'vitals');
fs.mkdirSync(tmpDir, { recursive: true });
const unitsJson = typedUnits as Record<string, string | { count: number; unit: string }>;
const db = getDatabase();

function updateDocStatus(documentId: number, fields: Record<string, unknown>) {
	db.update(labDocuments)
		.set({ ...fields, statusUpdatedAt: new Date().toISOString() })
		.where(eq(labDocuments.id, documentId))
		.run();
}

const extractedDataSchema = z.object({
	date: z.string().optional(),
	labName: z.string().optional(),
	location: z.string().optional(),
	measurements: z.array(
		z.object({
			name: z.string(),
			sourceName: z.string(),
			valueText: z.string(),
			unit: z.string(),
			valueNumeric: z.number().optional(),
			referenceText: z.string().optional(),
			referenceMin: z.number().optional(),
			referenceMax: z.number().optional(),
			flag: z.string().optional(),
		}),
	),
});

void createScript(async () => {
	const filePath = process.argv[2];
	if (!filePath) throw new Error('pass an absolute file path as an arg');
	if (!path.isAbsolute(filePath)) throw new Error('path must be absolute');
	if (!fs.existsSync(filePath)) throw new Error(`file not found: ${filePath}`);

	const pdfData = fs.readFileSync(filePath);
	const sha256 = createHash('sha256').update(pdfData).digest('hex');
	const fileName = path.basename(filePath);

	let documentId: number;
	const existing = db
		.select({ id: labDocuments.id })
		.from(labDocuments)
		.where(eq(labDocuments.sha256, sha256))
		.get();

	if (existing) {
		console.log(style.label('existing document', `#${existing.id}, will overwrite results`));
		documentId = existing.id;
	} else {
		const inserted = db
			.insert(labDocuments)
			.values({
				fileName,
				mimeType: 'application/pdf',
				pdfData,
				sha256,
				status: 'pending',
				statusText: 'Queued for import',
				queuedAt: new Date().toISOString(),
			})
			.returning({ id: labDocuments.id })
			.get();
		documentId = inserted.id;
		console.log(style.label('created document', `#${documentId}`));
	}

	try {
		updateDocStatus(documentId, {
			status: 'processing',
			statusText: 'Converting PDF to markdown',
			startedAt: new Date().toISOString(),
		});

		const markdown = extractMarkdown(filePath);
		updateDocStatus(documentId, { rawMarkdown: markdown, statusText: 'Extracting measurements' });

		const data = await extractData(markdown);

		const resultCount = data.measurements.length;

		console.table(
			data.measurements.map((m, i) => ({
				'#': i + 1,
				name: m.name,
				value: m.valueText,
				unit: m.unit ?? '',
				original_unit: m.originalUnit ?? '',
				ref: m.referenceText ?? '',
				flag: m.flag ?? '',
			})),
		);
		console.log(style.label('total measurements', String(resultCount)));

		const tmpJsonPath = path.join(tmpDir, `extracted_${documentId}.json`);
		fs.writeFileSync(tmpJsonPath, JSON.stringify(data.measurements, null, 2));
		console.log(style.label('saved to', tmpJsonPath));

		updateDocStatus(documentId, { statusText: `Saving ${resultCount} results` });

		saveResults(documentId, data);

		updateDocStatus(documentId, {
			status: 'completed',
			statusText: `Imported ${resultCount} results`,
			completedAt: new Date().toISOString(),
			lastError: null,
			date: data.date ?? null,
			labName: data.labName ?? null,
			location: data.location ?? null,
		});

		console.log(style.label('done', `imported ${resultCount} results for document #${documentId}`));
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		updateDocStatus(documentId, {
			status: 'failed',
			statusText: 'Import failed',
			failedAt: new Date().toISOString(),
			lastError: message,
		});
		throw error;
	}
});

function saveResults(documentId: number, data: Awaited<ReturnType<typeof extractData>>) {
	db.transaction(tx => {
		tx.delete(labResults).where(eq(labResults.documentId, documentId)).run();

		const now = new Date().toISOString();
		const seen = new Map<string, number>();

		for (let i = 0; i < data.measurements.length; i++) {
			const key = data.measurements[i].name.toLowerCase().trim();
			seen.set(key, i);
		}

		for (const [key, index] of seen) {
			const m = data.measurements[index];

			let measurement = tx.select().from(labMeasurements).where(eq(labMeasurements.key, key)).get();

			if (!measurement) {
				measurement = tx
					.insert(labMeasurements)
					.values({
						key,
						name: m.name,
						aliasesJson: m.sourceName !== m.name ? [m.sourceName] : [],
						unit: m.unit ?? null,
						range: m.referenceText ?? null,
						rangeMin: m.referenceMin ?? null,
						rangeMax: m.referenceMax ?? null,
						createdAt: now,
						updatedAt: now,
					})
					.returning()
					.get();
			} else {
				const aliases = measurement.aliasesJson ?? [];
				if (m.sourceName && m.sourceName !== m.name && !aliases.includes(m.sourceName)) {
					tx.update(labMeasurements)
						.set({ aliasesJson: [...aliases, m.sourceName], updatedAt: now })
						.where(eq(labMeasurements.id, measurement.id))
						.run();
				}
			}

			tx.insert(labResults)
				.values({
					documentId,
					measurementId: measurement.id,
					sortOrder: index,
					originalName: m.sourceName,
					originalValueText: m.originalValueText,
					originalValueNumeric: m.originalValueNumeric ?? null,
					originalUnit: m.originalUnit ?? null,
					originalRangeText: m.referenceText ?? null,
					originalRangeMin: m.referenceMin ?? null,
					originalRangeMax: m.referenceMax ?? null,
					valueText: m.valueText,
					valueNumeric: m.valueNumeric ?? null,
					unit: m.unit ?? null,
					note: m.flag ?? null,
				})
				.run();
		}
	});
}

const CHUNK_TARGET_LINES = 100;

async function extractData(rawMarkdown: string) {
	const measurementsDb = compileMeasurementDatabase();
	const model = models.smart_and_expensive;
	const markdown = compactMarkdown(rawMarkdown);
	const chunks = chunkMarkdown(markdown, CHUNK_TARGET_LINES);
	const savedChars = rawMarkdown.length - markdown.length;

	console.log(style.header(`parsing ${chunks.length} chunk(s) with ${model.modelId}`));
	if (savedChars > 0)
		console.log(
			style.label('compacted', `${rawMarkdown.length} → ${markdown.length} chars (-${savedChars})`),
		);

	const databaseBlock = Object.entries(measurementsDb)
		.map(([name, { aliases }]) =>
			aliases.length ? `- '${name}', known aliases: ${aliases.join('; ')}` : `- '${name}'`,
		)
		.join('\n');

	type Measurement = z.infer<typeof extractedDataSchema>['measurements'][number];
	const allMeasurements: Measurement[] = [];
	let date: string | undefined;
	let labName: string | undefined;
	let location: string | undefined;
	let lastTableHeaders: string | null = null;

	for (let i = 0; i < chunks.length; i++) {
		const chunk = chunks[i];
		console.log(
			style.label(`chunk ${i + 1}/${chunks.length}`, `${chunk.split('\n').length} lines`),
		);

		const continuationHint = lastTableHeaders
			? `\n\nIMPORTANT: The previous chunk ended with a table whose columns were: ${lastTableHeaders}\nIf this chunk starts with table rows that have no header, those rows belong to that table. Use those column headers to interpret the data.`
			: '';

		for (let attempt = 0; attempt < 2; attempt++) {
			try {
				const result = await generateText({
					model,
					temperature: 0.1,
					maxRetries: 1,
					maxOutputTokens: 8e3,
					output: Output.object({ schema: extractedDataSchema }),
					system: textBlock`
					You are extracting structured lab results from parsed PDF markdown.
					You must explicitly extract values visible in the <markdown /> and do not infer any data not provided.
					You must translate all text to english if it's not already in english.
					The <markdown /> is a chunk of a larger document extracted from a multi-page PDF of lab results. The parsing is pretty good but not 100% reliable so there might be areas to fix - a usual pitfall is tables that are missing headers because they spanned across multiple pages and the parser did not detect that.
					You are provided a <database /> of the existing canonical names, it is not exhaustive as we are using this process to build it. If you find matches in the <database /> then use those as the canonical names, otherwise take your best guess given the context.

					<database>${databaseBlock}</database>

					type output_json_schema = {
						date?: string; // ISO date the lab was made, if provided
						labName?: string; // the name of the company that processed this
						location?: string; // full address including city, country, if available
						measurements: Array<{
							name: string; // canonical name according to the <database /> if possible. If there is no high certainty match in the <database /> then translate and sanitize the source name
							sourceName: string; // the literal name from the source, unaltered
							valueText: string; // the literal value from the source, unaltered and without the unit
							valueNumeric?: number; // the number value if this is a numeric value
							unit: 'n/a' | string; // the unit for this measurement
							referenceText?: string; // it's very common that every measurement has its own reference range indicated next to it
							referenceMin?: number; // parse min reference value if provided
							referenceMax?: number; // parsed max reference value if provided
							flag?: string; // if there is any flag for this specific measurement or notes
						}>
					}

					<example_response>${JSON.stringify({
						date: '2017-11-02',
						labName: 'Quest Diagnostics',
						location: 'One Malcolm Avenue, Teterboro, NJ 07608, USA',
						measurements: [
							{
								name: 'MPV',
								sourceName: 'MPV',
								valueText: '8.7',
								unit: 'fl',
								valueNumeric: 8.7,
								referenceText: '7.5-12.5',
								referenceMin: 7.5,
								referenceMax: 12.5,
							},
							{
								name: 'Hemoglobin A1c',
								sourceName: 'HEMOGLOBIN A1C (calc)',
								valueText: '5.0',
								unit: '% of total hgb',
								valueNumeric: 5,
								referenceText: '<5.7',
								referenceMax: 5.7,
							},
						],
					})}</example_response>
					${continuationHint}
				`,
					prompt: `<markdown>${chunk}</markdown>`,
				});
				const output = result.output;

				if (!date && output.date) date = output.date;
				if (!labName && output.labName) labName = output.labName;
				if (!location && output.location) location = output.location;
				allMeasurements.push(...output.measurements);
				break;
			} catch (error: any) {
				const retryable =
					error?.name === 'AI_NoOutputGeneratedError' || error?.name === 'AI_JSONParseError';
				if (retryable) {
					if (attempt === 0) {
						console.log(style.label(`chunk ${i + 1}/${chunks.length}`, `${error.name}, retrying`));
						continue;
					}
					console.log(
						style.label(`chunk ${i + 1}/${chunks.length}`, `${error.name} after retry, skipping`),
					);
				} else {
					throw error;
				}
			}
		}

		lastTableHeaders = getLastTableHeaders(chunk);
	}

	// 🏁 unit normalization across all accumulated measurements

	const unitsToFigureOut: { measurement: string; unit: string }[] = [];

	const updateMeasurement = (item: Measurement, count: number | ((v: number) => number)) => {
		const fn = typeof count === 'number' ? (x: number) => x * count : count;
		if (item.valueNumeric) item.valueNumeric = fn(item.valueNumeric);
		if (item.referenceMin) item.referenceMin = fn(item.referenceMin);
		if (item.referenceMax) item.referenceMax = fn(item.referenceMax);
	};

	const originals = allMeasurements.map(m => ({
		valueText: m.valueText,
		valueNumeric: m.valueNumeric,
		unit: m.unit,
	}));

	for (const item of allMeasurements) {
		const matchedDb = measurementsDb[item.name.toLocaleLowerCase().trim()];
		if (item.valueNumeric == null) {
			if (/^\d+(\.\d+)?$/.test(item.valueText)) item.valueNumeric = parseFloat(item.valueText);
		}
		if (!item.unit) continue;
		item.unit = item.unit.toLocaleLowerCase().trim();
		if (item.unit === 'n/a') {
			delete (item as any).unit;
			continue;
		}

		if (!matchedDb) continue;

		if (unitsJson[item.unit]) {
			const m = unitsJson[item.unit];
			if (typeof m === 'string') item.unit = m;
			else {
				item.unit = m.unit;
				updateMeasurement(item, m.count);
			}
		}

		if (item.unit === matchedDb.unit) continue;

		if (matchedDb.unit) {
			const conversionKey = `${item.unit}→${matchedDb.unit}` as const;
			if (getMeasureKind(item.unit)) {
				updateMeasurement(
					item,
					value => convert(value, item.unit as any).to(matchedDb.unit as any).quantity,
				);
			} else if (unitsJson[conversionKey]) {
				const m = unitsJson[conversionKey];
				if (typeof m !== 'string') updateMeasurement(item, m.count);
			} else {
				unitsToFigureOut.push({ measurement: item.name, unit: item.unit });
			}
		}
	}

	let jsonUpdated = false;
	if (unitsToFigureOut.length) {
		console.log('- asking an LLM to figure out unit mappings');
		const { output } = await generateText({
			model: models.smart_and_expensive,
			temperature: 0.5,
			maxOutputTokens: 5e3,
			output: Output.array({
				element: z.object({
					measurement: z.string(),
					canonicalUnit: z.string(),
					multiplier: z.number(),
				}),
			}),
			system: textBlock`
				Sanitize and find the canonical units of measurement for the given list of <measurements />.
				These values were extracted from various lab bloodwork via a combination of OCR and LLM parsing, so they were created from potentially different standards and also have some parsing errors. Map the units and provide the conversion multiplier to go from 1 of the source unit to 1 of the target unit.
				It could also be that the unit is actually correct for the measurement, in which case just clean it up if needed and return it back as { multiplier: 1 }
				Reply with 1 object for each with the appropriate mapping:
				type output_object = {
					measurement: string; // the EXACT name given to you in the input <measurements /> (it will be used on my end to map your work)
					canonicalUnit: string; // the canonical unit for this measurement
					multiplier: number; // the multiplier to apply to the numeric value when converting from source to target (e.g. if original is "grams" and the correct is "mg", then the multiplier is 1000)
				}

				Example, for input:
				- measurement "glucose" with unit "g/dL"
				- measurement "non hdl cholesterol" with unit "mg/dL (calc)"
				- measurement "some random name that is probably a parsing error" with unit "grams"

				Example output:
				[
					{"measurement":"glucose","canonicalUnit":"mg/dL","multiplier":1000},
					{"measurement":"non hdl cholesterol","canonicalUnit":"mg/dL","multiplier":1}
				]
			`,
			prompt: `<measurements>${unitsToFigureOut.map(x => `measurement "${x.measurement}" with unit "${x.unit}"`).join('\n')}</measurements>`,
		});

		const mapping = Object.fromEntries(output.map(x => [x.measurement, x]));

		for (const { measurement } of unitsToFigureOut) {
			const m = mapping[measurement];
			const item = allMeasurements.find(x => x.name === measurement);
			if (!m || !item) continue;
			unitsJson[`${item.unit}→${m.canonicalUnit}`] = { unit: m.canonicalUnit, count: m.multiplier };
			jsonUpdated = true;
			item.unit = m.canonicalUnit;
			updateMeasurement(item, m.multiplier);
		}
	}

	if (jsonUpdated)
		fs.writeFileSync(path.join(__dirname, 'units.json'), JSON.stringify(unitsJson, null, 4));

	return {
		date,
		labName,
		location,
		measurements: allMeasurements.map((m, i) => ({
			...m,
			originalValueText: originals[i].valueText,
			originalValueNumeric: originals[i].valueNumeric,
			originalUnit: originals[i].unit,
		})),
	};
}

function compactMarkdown(markdown: string): string {
	return markdown
		.replace(/\|[-:\s|]+\|/g, match => match.replace(/-{2,}/g, '-'))
		.replace(/\| +/g, '|')
		.replace(/ +\|/g, '|');
}

function chunkMarkdown(markdown: string, targetLines: number): string[] {
	const lines = markdown.split('\n');
	if (lines.length <= targetLines) return [markdown];

	const isTableLine = (line: string) => line.trimStart().startsWith('|');

	const chunks: string[] = [];
	let start = 0;

	while (start < lines.length) {
		let end = Math.min(start + targetLines, lines.length);

		if (end < lines.length) {
			// don't split inside a table — scan for the nearest non-table boundary
			if (isTableLine(lines[end])) {
				let before = end - 1;
				while (before > start && isTableLine(lines[before])) before--;

				let after = end;
				while (after < lines.length && isTableLine(lines[after])) after++;

				// pick whichever boundary is closer to the target
				end = end - before <= after - end ? before + 1 : after;
			}
		}

		chunks.push(lines.slice(start, end).join('\n'));
		start = end;
	}

	return chunks;
}

function getLastTableHeaders(markdown: string): string | null {
	const lines = markdown.split('\n');

	for (let i = lines.length - 1; i >= 0; i--) {
		if (!lines[i].trimStart().startsWith('|')) continue;

		// found a table line, walk back to find the header row
		let tableStart = i;
		while (tableStart > 0 && lines[tableStart - 1].trimStart().startsWith('|')) {
			tableStart--;
		}

		const headerLine = lines[tableStart];
		const separator = lines[tableStart + 1];
		if (headerLine && separator && /^\s*\|[\s\-:|]+\|/.test(separator)) {
			return headerLine
				.split('|')
				.map(c => c.trim())
				.filter(Boolean)
				.join(' | ');
		}

		return null;
	}

	return null;
}

function compileMeasurementDatabase() {
	const result: Record<string, { aliases: string[]; unit: string | null }> = {};
	const measurements = db
		.select()
		.from(labMeasurements)
		.orderBy(labMeasurements.name, labMeasurements.id)
		.all();

	for (const measurement of measurements) {
		const aliases = measurement.aliasesJson.filter(alias => alias && alias !== measurement.name);
		result[measurement.name] = { aliases, unit: measurement.unit };
	}

	return result;
}

function extractMarkdown(file: string) {
	console.log(style.header('convert to markdown via marker-pdf'));
	const hash = createHash('sha256').update(fs.readFileSync(file).toString()).digest('hex');
	const targetDir = path.join(tmpDir, 'bloodwork_' + hash);
	let outDir: string | undefined = undefined;

	if (fs.existsSync(targetDir)) {
		outDir = fs
			.readdirSync(targetDir)
			.map(x => path.join(targetDir, x))
			.find(x => fs.statSync(x).isDirectory());

		if (outDir) console.log(style.label('using cached markdown', outDir));
	}

	if (!outDir) {
		fs.mkdirSync(targetDir, { recursive: true });
		const { stderr, stdout, exitCode } = Bun.spawnSync({
			cmd: [
				'marker_single',
				'--use_llm',
				'--table_image_expansion_ratio',
				'0.2',
				'--disable_image_extraction',
				'--table_rewriting_prompt',
				"You are correcting markdown for a clinical lab-results. If you detect tables whose header row is omitted because it's split across multiple pages, add the implied header row and preserve the full column structure.",
				'--output_dir',
				targetDir,
				file,
			],
			env: {
				...process.env,
				GOOGLE_API_KEY: env.MARKER_PDF_GEMINI_KEY,
			},
		});
		const markerOutput = stdout.toString() + stderr.toString();

		if (exitCode !== 0) {
			console.error(stderr.toString());
			throw new Error(markerOutput || `marker_single failed with exit code ${exitCode}`);
		}

		const markerOutDir = markerOutput.match(/Saved markdown to (.+)$/m);
		if (!markerOutDir?.[1]) {
			console.error(markerOutput);
			throw new Error('Could not find save dir path');
		}
		outDir = markerOutDir[1];
	}

	const mdFile = fs.readdirSync(outDir).find(x => x.endsWith('.md'));

	if (!mdFile) {
		throw new Error(
			'Could not find markdown file in: ' +
				outDir +
				'\nfound:\n' +
				fs.readdirSync(outDir).join('\n'),
		);
	}

	return fs.readFileSync(path.join(outDir, mdFile)).toString();
}
