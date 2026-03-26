#!/usr/bin/env bun
import { createScript, style } from './createScript';
import fs from 'fs';
import path from 'path';
import { generateText, Output } from 'ai';
import { createOpenRouter } from '@openrouter/ai-sdk-provider';
import env from 'server/env';
import { getDatabase } from 'server/db/client';
import { labMeasurements } from 'server/db/schema';
import { textBlock } from 'shared/textBlock';
import z from 'zod';
import { createHash } from 'crypto';
import convert, { getMeasureKind } from 'convert';
import typedUnits from './units.json';

const tmpDir = path.join('/tmp', 'vitals');
fs.mkdirSync(tmpDir, { recursive: true });
const unitsJson = typedUnits as Record<string, string | { count: number; unit: string }>;

const openrouter = createOpenRouter({ apiKey: env.OPENROUTER_API_KEY });

void createScript(async () => {
	const [, , fileMaybeRelative] = process.argv;
	if (!fs.existsSync(fileMaybeRelative)) throw new Error('pass a file as an arg');
	const file = path.resolve(fileMaybeRelative);

	// console.log(style.header('convert to images'));
	// execSync(
	// 	`magick -density 150 '${path.resolve(file)}' -quality 95 -background white -alpha remove -alpha off '${path.join(tmpDir, `parse__${path.basename(file)}__%03d.png`)}'`,
	// 	{ stdio: 'inherit' },
	// );

	const markdown = extractMarkdown(file);

	const measurementsDb = await compileMeasurementDatabase();

	const result = await generateText({
		model: openrouter('google/gemini-2.5-flash'),
		temperature: 0.1,
		maxRetries: 1,
		maxOutputTokens: 20e3,
		output: Output.object({
			schema: z.object({
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
						range: z
							.object({
								text: z.string(),
								min: z.number().optional(),
								max: z.number().optional(),
							})
							.optional(),
						flag: z.string().optional(),
					}),
				),
			}),
		}),
		system: textBlock`
			You must explicitly extract values visible in the <markdown /> and do not infer any data not provided.
			You must translate all text to english if it's not already in english.
			The <markdown /> was extracted by parsing a multi-page PDF of lab results. The parsing is pretty good but not 100% reliable so there might be areas to fix - a usual pitfall is tables that are missing headers because they spanned across multiple pages and the parser did not detect that.
			You are provided a <database /> of the existing canonical names, it is not exhaustive as we are using this process to build it. If you find matches in the <database /> then use those as the canonical names, otherwise take your best guess given the context.

			<database>${Object.entries(measurementsDb)
				.map(
					([name, { aliases }]) =>
						`- '${name}' (canonical name) ${aliases.length ? ', known aliases: ' + aliases.join('; ') : ': no other known aliases'}`,
				)
				.join('\n')}</database>

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
					range?: {
						text: string; // the literal reference range if provided
						min?: number; // parse min reference value if provided
						max?: number; // parsed max reference value if provided
					}
					flag?: string; // if there is any flag for this specific measurement or notes
				}>
			}
		`,
		prompt: `<markdown>${markdown}</markdown>`,
	});

	// 🏁 assume LLM has correctly matched names to the ones in the database and proceed with unit mapping

	const canonicalUnits = Object.fromEntries(
		Object.entries(measurementsDb).map(x => [x[0].toLocaleLowerCase(), x[1].unit]),
	);

	const toFigureOut: { measurement: string; unit: string }[] = [];

	for (const item of result.output.measurements) {
		const matchedDb = measurementsDb[item.name.toLocaleLowerCase().trim()];
		if (item.valueNumeric == null) {
			if (/^\d+$/.test(item.valueText)) item.valueNumeric = parseInt(item.valueText, 10);
			else if (/^\d+(\.\d+)?$/.test(item.valueText)) item.valueNumeric = parseFloat(item.valueText);
		}
		if (!item.unit) continue;
		item.unit = item.unit.toLocaleLowerCase().trim();
		if (item.unit === 'n/a') {
			delete (item as any).unit;
			continue;
		}

		if (item.unit === matchedDb?.unit) continue;

		let multiplier = 1;
		if (unitsJson[item.unit]) {
			const m = unitsJson[item.unit];
			if (typeof m === 'string') item.unit = m;
			else {
				item.unit = m.unit;
				multiplier = m.count;
			}
		}
		if (item.valueNumeric) item.valueNumeric *= multiplier;
		if (item.range?.min) item.range.min *= multiplier;
		if (item.range?.max) item.range.max *= multiplier;

		if (matchedDb?.unit) {
			const map = (unit: string, value: number) =>
				convert(value, unit as any).to(matchedDb.unit as any).quantity;
			// matched AND both have units AND units are different AND we know how to convert
			if (getMeasureKind(item.unit)) {
				if (item.valueNumeric) item.valueNumeric = map(item.valueText, item.valueNumeric);
			} else {
			}
		}
	}

	console.dir(result.output, { depth: null });
	console.log({ unitsToFigureOut: toFigureOut });

	const unitsMapped = await generateText({
		model: openrouter('google/gemini-3.1-pro'),
		temperature: 0.5,
		maxOutputTokens: 5e3,
		output: Output.array({
			element: z.object({
				measurement: z.string(),
				canonicalUnit: z.string(),
				canonicalName: z.string(),
				multiplier: z.number(),
			}),
		}),
		system: textBlock`
			Sanitize and find the canonical units of measurement for the given list of <measurements />.
			These values were extracted from various lab bloodwork via a combination of OCR and LLM parsing, so they were created from potentially different standards and also have some parsing errors. Map the units and provide the conversion multiplier to go from 1 of the source unit to 1 of the target unit.
			It could also be that the unit is actually correct for the measurement, in which case just clean it up if needed and return it back as { multiplier: 1 }
			Reply with 1 object for each with the appropriate mapping:
			type output_object = {
				measurement: string; // the EXACT name given to you in the input <measurements />
				canonicalName: string; // the canonical sanitized name for this measurement
				canonicalUnit: string; // the canonical unit for this measurement
				multiplier: number; // the conversion multiplier to convert 1 count of the original unit to the new canonicalUnit (e.g. if original is "grams" and the correct is "mg", then the multiplier is 1000)
			}

			Example, for input:
			- measurement "GLUCOSE" with unit "g/dL"
			- measurement "NON HDL CHOLESTEROL" with unit "mg/dL (calc)"

			Example output
			[
				{"measurement":"GLUCOSE","canonicalName":"glucose","canonicalUnit":"mg/dL","multiplier":1000},
				{"measurement":"NON HDL CHOLESTEROL (calculated)","canonical name": "Non HDL Cholesterol","canonicalUnit":"mg/dL","multiplier":1}
			]
		`,
		prompt: `<measurements>${toFigureOut.map(x => `measurement "${x.measurement}" with unit "${x.unit}"`).join('\n')}</measurements>`,
	});
});

async function compileMeasurementDatabase() {
	const result: Record<string, { aliases: string[]; unit: string | null }> = {};
	const db = getDatabase();
	const measurements = db
		.select()
		.from(labMeasurements)
		.orderBy(labMeasurements.name, labMeasurements.id)
		.all();

	for (const measurement of measurements) {
		const aliases = measurement.aliasesJson.filter(alias => alias && alias !== measurement.name);

		result[measurement.name] = {
			aliases,
			unit: measurement.unit,
		};
	}

	return result;
}

function extractMarkdown(file: string) {
	console.log(style.header('convert to markdown via marker-pdf'));
	const hash = createHash('sha256').update(fs.readFileSync(file).toString()).digest('hex');
	const targetDir = path.join(tmpDir, 'lab_' + hash);
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
