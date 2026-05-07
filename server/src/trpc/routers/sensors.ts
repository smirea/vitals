import path from 'path';

import { and, asc, desc, eq, gte } from 'drizzle-orm';
import { z } from 'zod';

import type { VitalsDatabase } from 'server/db/client.ts';
import { getPillsDashboard } from 'server/db/pills.ts';
import {
	diaryEntries,
	diaryEntryTags,
	diaryVoiceMemos,
	labDocuments,
	labMeasurements,
	labResults,
	locations,
	tags,
} from 'server/db/schema.ts';
import { createRouter, publicProcedure } from 'server/trpc/shared.ts';

const sensorKeys = ['labs', 'pills', 'voiceMemos', 'macrofactor', 'whoop', 'workouts'] as const;
const sensorOutputModes = ['json', 'text', 'csv'] as const;
const dateSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);

const labsConfigSchema = z.object({
	textFilter: z.string().optional().default(''),
	categories: z.array(z.string().trim().min(1)).optional().default([]),
	startDate: dateSchema.nullable().optional(),
	onlyLatest: z.boolean().optional().default(false),
});
const voiceMemosConfigSchema = z.object({
	content: z.enum(['raw', 'summary', 'both']).optional().default('raw'),
});
const macrofactorConfigSchema = z.object({
	recipeDetails: z.boolean().optional().default(false),
});

const sensorRunInputSchema = z.object({
	key: z.enum(sensorKeys),
	outputMode: z.enum(sensorOutputModes),
	startDate: dateSchema,
	labs: labsConfigSchema.optional().default({ textFilter: '', categories: [], onlyLatest: false }),
	voiceMemos: voiceMemosConfigSchema.optional().default({ content: 'raw' }),
	macrofactor: macrofactorConfigSchema.optional().default({ recipeDetails: false }),
});

type SensorKey = (typeof sensorKeys)[number];
type SensorOutputMode = (typeof sensorOutputModes)[number];
type SensorRunInput = z.infer<typeof sensorRunInputSchema>;
type SensorRunResult = {
	key: SensorKey;
	label: string;
	outputMode: SensorOutputMode;
	completedAt: string;
	json: unknown | null;
	text: string | null;
	csvFiles: Array<{
		fileName: string;
		content: string;
	}>;
};

type CsvValue = string | number | null | undefined;

const labels = {
	labs: 'Labs',
	pills: 'Pills',
	voiceMemos: "Captain's Log Voice Memos",
	macrofactor: 'MacroFactor',
	whoop: 'WHOOP',
	workouts: 'Workouts',
} satisfies Record<SensorKey, string>;

const scriptsRoot = path.join(process.env.HOME ?? '/Users/stefan', 'code', 'scripts');
const scriptPaths: Partial<Record<SensorKey, string>> = {
	macrofactor: path.join(scriptsRoot, 'src', 'macrofactor.ts'),
	whoop: path.join(scriptsRoot, 'src', 'whoop.ts'),
	workouts: path.join(scriptsRoot, 'src', 'workouts.ts'),
};

export const sensorsRouter = createRouter({
	getConfig: publicProcedure.query(({ ctx }) => {
		const labDates = ctx.db
			.select({
				date: labDocuments.date,
			})
			.from(labDocuments)
			.where(eq(labDocuments.status, 'completed'))
			.orderBy(desc(labDocuments.date), desc(labDocuments.id))
			.all()
			.map(row => row.date)
			.filter((date): date is string => !!date);

		const labCategoryRows = ctx.db
			.select({
				category: labMeasurements.category,
			})
			.from(labResults)
			.innerJoin(labDocuments, eq(labResults.documentId, labDocuments.id))
			.innerJoin(labMeasurements, eq(labResults.measurementId, labMeasurements.id))
			.where(eq(labDocuments.status, 'completed'))
			.orderBy(asc(labMeasurements.category), asc(labMeasurements.name))
			.all()
			.map(row => row.category?.trim() ?? '')
			.filter(Boolean);
		const labCategoryCounts = new Map<string, number>();
		for (const category of labCategoryRows) {
			labCategoryCounts.set(category, (labCategoryCounts.get(category) ?? 0) + 1);
		}

		return {
			labDates: [...new Set(labDates)],
			labCategories: [...labCategoryCounts.entries()].map(([category, count]) => ({
				category,
				count,
			})),
		};
	}),

	runExtractor: publicProcedure.input(sensorRunInputSchema).mutation(async ({ ctx, input }) => {
		switch (input.key) {
			case 'labs':
				return runLabsSensor(ctx.db, input);
			case 'pills':
				return runPillsSensor(ctx.db, input);
			case 'voiceMemos':
				return runVoiceMemosSensor(ctx.db, input);
			case 'macrofactor':
			case 'whoop':
			case 'workouts':
				return runScriptSensor(input);
			default:
				input.key satisfies never;
				throw new Error('Unknown sensor key.');
		}
	}),
});

function createSensorResult(
	input: SensorRunInput,
	data: {
		json: unknown | null;
		text: string | null;
		csvFiles: SensorRunResult['csvFiles'];
	},
): SensorRunResult {
	return {
		key: input.key,
		label: labels[input.key],
		outputMode: input.outputMode,
		completedAt: new Date().toISOString(),
		...data,
	};
}

function runLabsSensor(db: VitalsDatabase, input: SensorRunInput): SensorRunResult {
	const config = labsConfigSchema.parse(input.labs);
	const filters = splitTextFilter(config.textFilter);
	const categories = new Set(config.categories.map(category => category.toLocaleLowerCase()));
	const rows = db
		.select({
			documentId: labDocuments.id,
			date: labDocuments.date,
			labName: labDocuments.labName,
			group: labDocuments.group,
			location: labDocuments.location,
			measurementKey: labMeasurements.key,
			lab: labMeasurements.name,
			category: labMeasurements.category,
			aliases: labMeasurements.aliasesJson,
			valueText: labResults.valueText,
			valueNumeric: labResults.valueNumeric,
			unit: labResults.unit,
			range: labMeasurements.range,
			rangeMin: labMeasurements.rangeMin,
			rangeMax: labMeasurements.rangeMax,
			flags: labResults.note,
			sourcePage: labResults.sourcePage,
		})
		.from(labResults)
		.innerJoin(labDocuments, eq(labResults.documentId, labDocuments.id))
		.innerJoin(labMeasurements, eq(labResults.measurementId, labMeasurements.id))
		.where(
			config.startDate
				? and(eq(labDocuments.status, 'completed'), gte(labDocuments.date, config.startDate))
				: eq(labDocuments.status, 'completed'),
		)
		.orderBy(
			desc(labDocuments.date),
			desc(labDocuments.id),
			asc(labResults.sortOrder),
			asc(labResults.id),
		)
		.all()
		.filter(row => {
			if (categories.size > 0 && !categories.has((row.category ?? '').toLocaleLowerCase())) {
				return false;
			}
			if (filters.length === 0) {
				return true;
			}
			const haystack = [
				row.measurementKey,
				row.lab,
				row.category,
				row.labName,
				row.group,
				row.location,
				...(row.aliases ?? []),
			]
				.filter(Boolean)
				.join(' ')
				.toLocaleLowerCase();
			return filters.some(filter => haystack.includes(filter));
		})
		.map(row => ({
			measurementKey: row.measurementKey,
			date: row.date,
			lab: row.lab,
			category: row.category,
			value: formatValue(row.valueText, row.valueNumeric),
			unit: row.unit,
			range: row.range ?? formatRange(row.rangeMin, row.rangeMax),
			flags: row.flags,
			source: {
				documentId: row.documentId,
				labName: row.labName,
				group: row.group,
				location: row.location,
				page: row.sourcePage,
			},
		}));
	const outputRows = (config.onlyLatest ? filterLatestLabRows(rows) : rows).map(
		({ measurementKey: _measurementKey, ...row }) => row,
	);

	return formatStructuredSensorResult(input, outputRows, {
		fileName: 'labs.csv',
		columns: ['date', 'lab', 'category', 'value', 'unit', 'range', 'flags', 'documentId', 'page'],
		toCsvRow: row => ({
			date: row.date,
			lab: row.lab,
			category: row.category,
			value: row.value,
			unit: row.unit,
			range: row.range,
			flags: row.flags,
			documentId: row.source.documentId,
			page: row.source.page,
		}),
	});
}

function filterLatestLabRows<
	Row extends {
		measurementKey: string;
	},
>(rows: Row[]) {
	const rowsByMeasurementKey = new Map<string, Row>();
	for (const row of rows) {
		const key = row.measurementKey.trim().toLocaleLowerCase();
		if (!rowsByMeasurementKey.has(key)) {
			rowsByMeasurementKey.set(key, row);
		}
	}
	return [...rowsByMeasurementKey.values()];
}

function runPillsSensor(db: VitalsDatabase, input: SensorRunInput): SensorRunResult {
	const dashboard = getPillsDashboard(db);
	const rows = dashboard.pills
		.map(pill => {
			const periods = pill.periods.filter(
				period => !period.endDate || period.endDate >= input.startDate,
			);
			if (pill.periods.length > 0 && periods.length === 0) {
				return null;
			}

			return {
				id: pill.id,
				name: pill.name,
				serving: formatServing(pill.value, pill.unit),
				url: pill.url,
				note: pill.note,
				tags: pill.tags.map(tag => tag.name),
				components: pill.components.map(component => ({
					name: component.name,
					serving: formatServing(component.value, component.unit),
				})),
				periods: periods.map(period => ({
					startDate: period.startDate,
					endDate: period.endDate,
					count: period.count,
					timing: period.timing,
					daysOfWeek: period.daysOfWeek,
					tags: period.tags.map(tag => tag.name),
				})),
			};
		})
		.filter((row): row is NonNullable<typeof row> => row !== null);

	return formatStructuredSensorResult(input, rows, {
		fileName: 'pills.csv',
		columns: ['name', 'serving', 'periods', 'components', 'tags', 'note', 'url'],
		toCsvRow: row => ({
			name: row.name,
			serving: row.serving,
			periods: row.periods.map(formatPillPeriod).join('\n'),
			components: row.components
				.map(component => `${component.name} ${component.serving}`.trim())
				.join('\n'),
			tags: row.tags.join('\n'),
			note: row.note,
			url: row.url,
		}),
	});
}

function runVoiceMemosSensor(db: VitalsDatabase, input: SensorRunInput): SensorRunResult {
	const config = voiceMemosConfigSchema.parse(input.voiceMemos);
	const entryRows = db
		.select({
			entryId: diaryEntries.id,
			entryCreatedAt: diaryEntries.createdAt,
			notes: diaryEntries.notes,
			summary: diaryEntries.summary,
			locationName: locations.name,
			city: locations.city,
			country: locations.country,
			voiceMemoId: diaryVoiceMemos.id,
			voiceMemoCreatedAt: diaryVoiceMemos.createdAt,
			fileName: diaryVoiceMemos.fileName,
			durationSeconds: diaryVoiceMemos.durationSeconds,
			transcriptionStatus: diaryVoiceMemos.transcriptionStatus,
			transcript: diaryVoiceMemos.transcript,
			transcriptionError: diaryVoiceMemos.transcriptionError,
			processedAt: diaryVoiceMemos.processedAt,
		})
		.from(diaryVoiceMemos)
		.innerJoin(diaryEntries, eq(diaryVoiceMemos.entryId, diaryEntries.id))
		.innerJoin(locations, eq(diaryEntries.locationId, locations.id))
		.where(gte(diaryVoiceMemos.createdAt, `${input.startDate}T00:00:00`))
		.orderBy(desc(diaryVoiceMemos.createdAt), desc(diaryVoiceMemos.id))
		.all();

	const tagRows = db
		.select({
			entryId: diaryEntryTags.entryId,
			name: tags.name,
		})
		.from(diaryEntryTags)
		.innerJoin(tags, eq(diaryEntryTags.tagId, tags.id))
		.orderBy(asc(tags.name), asc(tags.id))
		.all();
	const tagsByEntryId = new Map<number, string[]>();
	for (const row of tagRows) {
		const names = tagsByEntryId.get(row.entryId) ?? [];
		names.push(row.name);
		tagsByEntryId.set(row.entryId, names);
	}

	const rows = entryRows.map(row => {
		const content = getVoiceMemoContent(config.content, {
			transcript: row.transcript,
			summary: row.summary,
		});

		return {
			id: row.voiceMemoId,
			createdAt: row.voiceMemoCreatedAt,
			fileName: row.fileName,
			durationSeconds: row.durationSeconds,
			transcriptionStatus: row.transcriptionStatus,
			transcriptionError: row.transcriptionError,
			processedAt: row.processedAt,
			...content,
			entry: {
				id: row.entryId,
				createdAt: row.entryCreatedAt,
				notes: row.notes,
				location: [row.locationName, row.city, row.country].filter(Boolean).join(', '),
				tags: tagsByEntryId.get(row.entryId) ?? [],
			},
		};
	});
	const contentColumns =
		config.content === 'both'
			? ['transcript', 'summary']
			: [config.content === 'raw' ? 'transcript' : 'summary'];

	return formatStructuredSensorResult(input, rows, {
		fileName: 'captains-log-voice-memos.csv',
		columns: [
			'createdAt',
			'fileName',
			'durationSeconds',
			'status',
			...contentColumns,
			'entryNotes',
			'location',
			'tags',
			'error',
		],
		toCsvRow: row => ({
			createdAt: row.createdAt,
			fileName: row.fileName,
			durationSeconds: row.durationSeconds,
			status: row.transcriptionStatus,
			transcript: 'transcript' in row ? row.transcript : null,
			summary: 'summary' in row ? row.summary : null,
			entryNotes: row.entry.notes,
			location: row.entry.location,
			tags: row.entry.tags.join('\n'),
			error: row.transcriptionError,
		}),
	});
}

function getVoiceMemoContent(
	content: z.infer<typeof voiceMemosConfigSchema>['content'],
	row: {
		transcript: string | null;
		summary: string | null;
	},
) {
	switch (content) {
		case 'raw':
			return { transcript: row.transcript };
		case 'summary':
			return { summary: row.summary };
		case 'both':
			return { transcript: row.transcript, summary: row.summary };
		default:
			content satisfies never;
			throw new Error('Unknown voice memo content mode.');
	}
}

async function runScriptSensor(input: SensorRunInput): Promise<SensorRunResult> {
	const scriptPath = scriptPaths[input.key];
	if (!scriptPath) {
		throw new Error(`No script configured for ${input.key}.`);
	}

	const scriptFormat = getScriptFormat(input);
	const stdout = await runBunScript(scriptPath, [
		'--start',
		input.startDate,
		'--format',
		scriptFormat,
	]);

	if (input.outputMode === 'json') {
		const json = JSON.parse(stdout);
		return createSensorResult(input, {
			json:
				input.key === 'macrofactor'
					? normalizeMacrofactorJson(json, macrofactorConfigSchema.parse(input.macrofactor))
					: json,
			text: null,
			csvFiles: [],
		});
	}

	if (input.outputMode === 'csv') {
		return createSensorResult(input, {
			json: null,
			text: null,
			csvFiles:
				input.key === 'macrofactor' && scriptFormat === 'csv:full'
					? splitNamedCsvSections(stdout, 'macrofactor')
					: [{ fileName: `${input.key}.csv`, content: stdout }],
		});
	}

	return createSensorResult(input, {
		json: null,
		text: stdout.trimEnd(),
		csvFiles: [],
	});
}

function normalizeMacrofactorJson(json: unknown, config: z.infer<typeof macrofactorConfigSchema>) {
	if (config.recipeDetails || typeof json !== 'object' || json === null || Array.isArray(json)) {
		return json;
	}

	const { recipeBreakdown: _recipeBreakdown, ...withoutRecipeDetails } = json as Record<
		string,
		unknown
	>;
	return withoutRecipeDetails;
}

function getScriptFormat(input: SensorRunInput) {
	if (input.outputMode === 'json') {
		return 'json';
	}
	if (input.key === 'macrofactor') {
		return macrofactorConfigSchema.parse(input.macrofactor).recipeDetails ? 'csv:full' : 'csv';
	}
	if (input.key === 'workouts' && input.outputMode === 'csv') {
		return 'csv:full';
	}
	return 'csv';
}

async function runBunScript(scriptPath: string, args: string[]) {
	const proc = Bun.spawn({
		cmd: [
			'bun',
			'run',
			'--env-file',
			path.join(scriptsRoot, '.env'),
			'--env-file',
			path.join(scriptsRoot, '.env.local'),
			scriptPath,
			...args,
		],
		cwd: scriptsRoot,
		stdout: 'pipe',
		stderr: 'pipe',
	});
	const [stdout, stderr, exitCode] = await Promise.all([
		new Response(proc.stdout).text(),
		new Response(proc.stderr).text(),
		proc.exited,
	]);

	if (exitCode !== 0) {
		throw new Error(
			[
				`${path.basename(scriptPath)} failed with exit code ${exitCode}.`,
				stderr.trim() ? `stderr:\n${stderr.trim()}` : '',
				stdout.trim() ? `stdout:\n${stdout.trim()}` : '',
			]
				.filter(Boolean)
				.join('\n\n'),
		);
	}

	return stdout;
}

function formatStructuredSensorResult<Row>(
	input: SensorRunInput,
	rows: Row[],
	csv: {
		fileName: string;
		columns: string[];
		toCsvRow: (row: Row) => Record<string, CsvValue>;
	},
): SensorRunResult {
	if (input.outputMode === 'json') {
		return createSensorResult(input, {
			json: rows,
			text: null,
			csvFiles: [],
		});
	}

	const csvText = renderCsv(csv.columns, rows.map(csv.toCsvRow));
	if (input.outputMode === 'csv') {
		return createSensorResult(input, {
			json: null,
			text: null,
			csvFiles: [{ fileName: csv.fileName, content: csvText }],
		});
	}

	return createSensorResult(input, {
		json: null,
		text: csvText.trimEnd(),
		csvFiles: [],
	});
}

function splitTextFilter(value: string) {
	return value
		.split(',')
		.map(part => part.trim().toLocaleLowerCase())
		.filter(Boolean);
}

function formatValue(valueText: string | null, valueNumeric: number | null) {
	const text = valueText?.trim() ?? '';
	if (text) {
		return text;
	}
	if (valueNumeric === null) {
		return '';
	}
	return Number.isInteger(valueNumeric)
		? String(valueNumeric)
		: Number(valueNumeric.toFixed(4)).toString();
}

function formatRange(rangeMin: number | null, rangeMax: number | null) {
	if (rangeMin !== null && rangeMax !== null) {
		return `${formatNumber(rangeMin)} - ${formatNumber(rangeMax)}`;
	}
	if (rangeMin !== null) {
		return `>= ${formatNumber(rangeMin)}`;
	}
	if (rangeMax !== null) {
		return `<= ${formatNumber(rangeMax)}`;
	}
	return '';
}

function formatNumber(value: number) {
	return Number.isInteger(value) ? String(value) : Number(value.toFixed(4)).toString();
}

function formatServing(value?: string | null, unit?: string | null) {
	return [value?.trim(), unit?.trim()].filter(Boolean).join(' ');
}

function formatPillPeriod(period: {
	startDate: string;
	endDate: string | null;
	count: number;
	timing: string | null;
	daysOfWeek: string[];
	tags: string[];
}) {
	return [
		`${period.startDate}${period.endDate ? ` to ${period.endDate}` : ''}`,
		period.count === 1 ? '' : `${period.count}x`,
		period.timing,
		period.daysOfWeek.length > 0 ? period.daysOfWeek.join('/') : '',
		period.tags.length > 0 ? `tags: ${period.tags.join(', ')}` : '',
	]
		.filter(Boolean)
		.join(' ');
}

function renderCsv(columns: string[], rows: Record<string, CsvValue>[]) {
	return `${[
		columns.join(','),
		...rows.map(row => columns.map(column => escapeCsvValue(row[column])).join(',')),
	].join('\n')}\n`;
}

function escapeCsvValue(value: CsvValue) {
	if (value == null) {
		return '';
	}
	const stringValue = String(value);
	if (!/[",\n\r]/.test(stringValue)) {
		return stringValue;
	}
	return `"${stringValue.replaceAll('"', '""')}"`;
}

function splitNamedCsvSections(text: string, prefix: string) {
	const matches = [...text.matchAll(/\n?==== ([^=]+) ===\n/g)];
	if (matches.length === 0) {
		return [{ fileName: `${prefix}.csv`, content: text }];
	}

	return matches.map((match, index) => {
		const name = match[1]?.trim() ?? `${prefix}-${index + 1}`;
		const contentStart = (match.index ?? 0) + match[0].length;
		const contentEnd = matches[index + 1]?.index ?? text.length;
		return {
			fileName: `${prefix}-${slugifyFileName(name)}.csv`,
			content: `${text.slice(contentStart, contentEnd).trim()}\n`,
		};
	});
}

function slugifyFileName(value: string) {
	return value
		.toLocaleLowerCase()
		.replace(/[^a-z0-9]+/g, '-')
		.replace(/^-|-$/g, '');
}
