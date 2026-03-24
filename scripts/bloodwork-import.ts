#!/usr/bin/env bun
import { createScript, style } from './createScript';
import fs from 'fs';
import path from 'path';
import { generateText, Output } from 'ai';
import { createOpenRouter } from '@openrouter/ai-sdk-provider';
import env from 'server/env';
import { getDatabase } from 'server/db/client';
import { bloodworkMeasurements } from 'server/db/schema';
import { textBlock } from 'shared/textBlock';
import z from 'zod';
import { createHash } from 'crypto';

const tmpDir = path.join('/tmp', 'vitals');
fs.mkdirSync(tmpDir, { recursive: true });

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

	/*
	Parse the <markdown /> extracted from a pdf of a lab result according to the following <schema />.
	<result_json>${JSON.stringify({
		date: '2026-01-20',
		labName: 'Quest Diagnostics',
		measurements: [
			{
				name: 'LDL Cholesterol',
				canonicalName: 'LDL Cholesterol',
				valueText: '104',
				valueNumeric: 104,
				unit: 'mg/dL',
				reference: '<= 50',
			},
		],
	})}</result_json>
	*/

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
						valueNumeric: z.number().optional(),
						unit: z.string().optional(),
						rangeText: z.string().optional(),
						rangeMin: z.number().optional(),
						rangeMax: z.number().optional(),
						flag: z.string().optional(),
					}),
				),
			}),
		}),
		system: textBlock`
			You must explicitly extract values visible in the <markdown /> and do not infer any data not provided.
			You must translate all text to english if it's not already in english.
			Do not invent measurements that are not visibly present on this page.
			The <markdown /> was extracted by parsing a multi-page PDF of lab results. The parsing is pretty good but not 100% reliable so there might be areas to fix - a usual pitfall is tables that are missing headers because they spanned across multiple pages and the parser did not detect that.
			You are provided a <database /> of the existing canonical names, it is not exhaustive as we are using this process to build it. If you find matches in the <database /> then use those as the canonical names, otherwise take your best guess given the context.

			<database>${JSON.stringify(await extractMeasurementDatabase())}</database>

			type output_json_schema = {
				date?: string; // the date the lab was made, if provided
				labName?: string; // the name of the company that processed this
				location?: string; // full address including city, country, if available
				measurements: Array<{
					name: string; // canonical name according to the <database /> if possible, otherwise the translated source name
					sourceName: string; // the literal name from the source, unaltered
					valueText: string; // the literal value from the source, unaltered and without the unit
					valueNumeric?: number; // the number value if this is a numeric value
					unit?: string; // only if present in the file, do not create one if not in the source
					rangeText?: string; // the literal reference range if provided
					rangeMin?: number; // parse min reference value if provided
					rangeMax?: number; // parsed max reference value if provided
					flag?: string; // if there is any flag for this specific measurement
				}>
			}
		`,
		prompt: `<markdown>${markdown}</markdown>`,
	});

	for (const item of result.output.measurements) {
		if (item.valueNumeric != null) continue;
		if (/^\d+?$/.test(item.valueText)) item.valueNumeric = parseInt(item.valueText, 10);
		else if (/^\d+(\.\d+)?$/.test(item.valueText)) item.valueNumeric = parseFloat(item.valueText);
	}

	console.dir(result.output, { depth: null });
});

async function extractMeasurementDatabase() {
	const result: Record<string, { aliases: string[]; unit?: string; range?: string }> = {};
	const db = getDatabase();
	const measurements = db
		.select()
		.from(bloodworkMeasurements)
		.orderBy(bloodworkMeasurements.name, bloodworkMeasurements.id)
		.all();

	for (const measurement of measurements) {
		const aliases = measurement.aliasesJson.filter(alias => alias && alias !== measurement.name);
		const range =
			measurement.canonicalRangeText ??
			formatRange(measurement.canonicalRangeMin, measurement.canonicalRangeMax) ??
			undefined;

		result[measurement.name] = {
			aliases,
			...(measurement.canonicalUnit ? { unit: measurement.canonicalUnit } : {}),
			...(range ? { range } : {}),
		};
	}

	return result;
}

function formatRange(min: number | null, max: number | null) {
	if (min != null && max != null) return `${min}-${max}`;
	if (min != null) return `>= ${min}`;
	if (max != null) return `<= ${max}`;
	return null;
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
