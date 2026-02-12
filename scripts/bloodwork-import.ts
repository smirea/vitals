import fs from 'fs';
import path from 'path';

import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { createOpenRouter } from '@openrouter/ai-sdk-provider';
import { generateObject, generateText } from 'ai';
import { getDocument } from 'pdfjs-dist/legacy/build/pdf.mjs';
import { z } from 'zod';

import {
    bloodworkLabSchema,
    bloodworkMeasurementSchema,
    buildBloodworkFileName,
    normalizeIsoDate,
    slugifyForPath,
    type BloodworkMeasurement,
    type BloodworkLab,
} from './bloodwork-schema.ts';
import { createScript } from './createScript.ts';

const DEFAULT_S3_BUCKET = 'stefan-life';
const DEFAULT_S3_PREFIX = 'vitals';
const DEFAULT_MODEL_IDS = ['google/gemini-3-flash-preview'];
const DEFAULT_TO_IMPORT_DIRECTORY = path.resolve(process.cwd(), 'data/to-import');
const DEFAULT_OUTPUT_DIRECTORY = path.resolve(process.cwd(), 'data');
const EXTRACTED_TEXT_LIMIT = 45_000;
const MODEL_MAX_OUTPUT_TOKENS = 350;
const METADATA_MAX_OUTPUT_TOKENS = 280;
const MEASUREMENT_BATCH_SIZE = 6;
const MAX_MEASUREMENT_PASSES_PER_PAGE = 8;
const EXCLUDED_MEASUREMENT_NAMES_LIMIT = 30;

type CliOptions = {
    importAll: boolean;
    inputPdfPath: string | null;
    continueOnError: boolean;
    skipUpload: boolean;
    modelIds: string[];
};

type ImportResult = {
    outputPath: string;
    s3Key: string | null;
    modelId: string;
};

type ExtractedPdfText = {
    fullText: string;
    pageTexts: string[];
};

const bloodworkMetadataSchema = z.object({
    date: z.string().trim().min(1).transform(normalizeIsoDate),
    labName: z.string().trim().min(1),
    location: z.string().trim().min(1).optional(),
    importLocation: z.string().trim().min(1).optional(),
    importLocationIsInferred: z.boolean().optional(),
    weightKg: z.number().positive().finite().optional(),
    notes: z.string().trim().min(1).optional(),
});

const measurementBatchSchema = z.object({
    measurements: z.array(bloodworkMeasurementSchema).max(MEASUREMENT_BATCH_SIZE),
});

const HELP_TEXT = [
    'Usage:',
    '  bun scripts/bloodwork-import.ts <path-to-pdf> [--skip-upload] [--model <openrouter-model-id>]',
    '  bun scripts/bloodwork-import.ts --all [--continue-on-error] [--skip-upload] [--model <openrouter-model-id>]',
    '',
    'Flags:',
    '  --all                 Import every .pdf file from data/to-import',
    '  --continue-on-error   Continue processing other files when --all is used',
    '  --skip-upload         Skip S3 upload (useful for local validation)',
    '  --model <id>          Override model id (can be repeated)',
].join('\n');

function parseCliOptions(argv: string[]): CliOptions {
    let importAll = false;
    let continueOnError = false;
    let skipUpload = false;
    const modelIds: string[] = [];
    const positional: string[] = [];

    for (let index = 0; index < argv.length; index++) {
        const token = argv[index];
        if (token === '--all') {
            importAll = true;
            continue;
        }
        if (token === '--continue-on-error') {
            continueOnError = true;
            continue;
        }
        if (token === '--skip-upload') {
            skipUpload = true;
            continue;
        }
        if (token === '--model') {
            const modelId = argv[index + 1];
            if (!modelId || modelId.startsWith('--')) {
                throw new Error(`Missing model id after --model\n\n${HELP_TEXT}`);
            }
            modelIds.push(modelId);
            index++;
            continue;
        }
        if (token.startsWith('--model=')) {
            const modelId = token.slice('--model='.length).trim();
            if (!modelId) {
                throw new Error(`Missing model id in ${token}\n\n${HELP_TEXT}`);
            }
            modelIds.push(modelId);
            continue;
        }
        if (token.startsWith('--')) {
            throw new Error(`Unknown flag: ${token}\n\n${HELP_TEXT}`);
        }
        positional.push(token);
    }

    if (importAll && positional.length > 0) {
        throw new Error(`Do not pass a file path when using --all\n\n${HELP_TEXT}`);
    }

    if (!importAll && positional.length !== 1) {
        throw new Error(`Expected exactly one PDF path or --all\n\n${HELP_TEXT}`);
    }

    if (!importAll && continueOnError) {
        throw new Error(`--continue-on-error can only be used together with --all\n\n${HELP_TEXT}`);
    }

    return {
        importAll,
        inputPdfPath: importAll ? null : positional[0],
        continueOnError,
        skipUpload,
        modelIds,
    };
}

function requireEnv(name: string): string {
    const value = process.env[name]?.trim();
    if (!value) {
        throw new Error(`Missing required environment variable: ${name}`);
    }
    return value;
}

function resolveModelIds(cliModelIds: string[]): string[] {
    if (cliModelIds.length > 0) {
        return cliModelIds;
    }
    return DEFAULT_MODEL_IDS;
}

function resolveInputFiles(options: CliOptions): string[] {
    if (options.importAll) {
        if (!fs.existsSync(DEFAULT_TO_IMPORT_DIRECTORY)) {
            throw new Error(`Import directory does not exist: ${DEFAULT_TO_IMPORT_DIRECTORY}`);
        }
        return fs
            .readdirSync(DEFAULT_TO_IMPORT_DIRECTORY, { withFileTypes: true })
            .filter(entry => entry.isFile() && entry.name.toLowerCase().endsWith('.pdf'))
            .map(entry => path.join(DEFAULT_TO_IMPORT_DIRECTORY, entry.name))
            .sort((left, right) => left.localeCompare(right));
    }

    if (!options.inputPdfPath) {
        throw new Error(`Missing input PDF path\n\n${HELP_TEXT}`);
    }

    const resolvedPath = path.resolve(process.cwd(), options.inputPdfPath);
    if (!fs.existsSync(resolvedPath)) {
        throw new Error(`Input file does not exist: ${resolvedPath}`);
    }
    if (!resolvedPath.toLowerCase().endsWith('.pdf')) {
        throw new Error(`Input file must be a .pdf file: ${resolvedPath}`);
    }
    if (!fs.statSync(resolvedPath).isFile()) {
        throw new Error(`Input path is not a file: ${resolvedPath}`);
    }
    return [resolvedPath];
}

function assertPdfSignature(bytes: Uint8Array, filePath: string): void {
    if (bytes.length < 4) {
        throw new Error(`Invalid PDF file (too short): ${filePath}`);
    }
    const isPdf =
        bytes[0] === 0x25 &&
        bytes[1] === 0x50 &&
        bytes[2] === 0x44 &&
        bytes[3] === 0x46;
    if (!isPdf) {
        throw new Error(`Invalid PDF signature: ${filePath}`);
    }
}

async function extractPdfText(bytes: Uint8Array): Promise<ExtractedPdfText> {
    const document = await getDocument({ data: bytes }).promise;
    const pageTexts: string[] = [];

    for (let pageIndex = 1; pageIndex <= document.numPages; pageIndex++) {
        const page = await document.getPage(pageIndex);
        const textContent = await page.getTextContent();
        const lines: Array<{ y: number; tokens: string[] }> = [];

        for (const item of textContent.items) {
            if (!('str' in item)) continue;
            const token = item.str.trim();
            if (!token) continue;

            const transform = 'transform' in item && Array.isArray(item.transform) ? item.transform : null;
            const y = transform ? Number(transform[5]) : Number.NaN;
            const previousLine = lines.at(-1);
            if (!previousLine || !Number.isFinite(y) || Math.abs(previousLine.y - y) > 2) {
                lines.push({ y, tokens: [token] });
            } else {
                previousLine.tokens.push(token);
            }
        }

        const pageBody = lines
            .map(line => line.tokens.join(' ').replace(/\s+/g, ' ').trim())
            .filter(Boolean)
            .join('\n')
            .trim();

        if (pageBody) {
            pageTexts.push(`Page ${pageIndex}\n${pageBody}`);
        }
    }

    return {
        fullText: pageTexts.join('\n'),
        pageTexts,
    };
}

function cleanUnknown(value: unknown): unknown {
    if (typeof value === 'string') {
        const trimmed = value.trim();
        return trimmed ? trimmed : undefined;
    }

    if (Array.isArray(value)) {
        return value
            .map(item => cleanUnknown(item))
            .filter(item => item !== undefined);
    }

    if (value && typeof value === 'object') {
        const next: Record<string, unknown> = {};
        for (const [key, child] of Object.entries(value)) {
            const cleanedChild = cleanUnknown(child);
            if (cleanedChild !== undefined) {
                next[key] = cleanedChild;
            }
        }
        return next;
    }

    return value;
}

function normalizeModelOutput(raw: unknown, sourcePath: string): BloodworkLab {
    const cleaned = cleanUnknown(raw);
    if (!cleaned || typeof cleaned !== 'object' || Array.isArray(cleaned)) {
        throw new Error('Model output must be an object');
    }

    const output = { ...cleaned } as Record<string, unknown>;
    if (!output.importLocation) {
        output.importLocation = sourcePath;
        output.importLocationIsInferred = true;
    }
    return bloodworkLabSchema.parse(output);
}

function buildMetadataPrompt(sourcePath: string, extractedText: string): string {
    const extractedSegment = extractedText
        ? extractedText.slice(0, EXTRACTED_TEXT_LIMIT)
        : 'No machine-readable text was extracted from the PDF.';

    return [
        `Source file: ${sourcePath}`,
        '',
        'Extract only report-level metadata from this bloodwork report.',
        'Return date (YYYY-MM-DD), labName, optional location, optional weightKg, optional notes.',
        'Do not include measurements in this step.',
        '',
        'Extracted text (may be partial):',
        extractedSegment,
    ].join('\n');
}

function buildMeasurementPrompt({
    sourcePath,
    pageText,
    excludedNames,
}: {
    sourcePath: string;
    pageText: string;
    excludedNames: string[];
}): string {
    const excluded = excludedNames.length > 0 ? excludedNames.join(', ') : 'none';
    return [
        `Source file: ${sourcePath}`,
        '',
        `Extract up to ${MEASUREMENT_BATCH_SIZE} bloodwork measurements from this page text.`,
        'Do not include any item from the excluded names list.',
        'Use standardized English measurement names and keep source values/ranges/units accurate.',
        'If there are no new measurements, return an empty measurements array.',
        '',
        `Excluded names: ${excluded}`,
        '',
        'Page text:',
        pageText,
    ].join('\n');
}

function buildMeasurementKey(measurement: BloodworkMeasurement): string {
    const rawValue = measurement.value;
    const valuePart =
        rawValue === undefined || rawValue === null
            ? ''
            : typeof rawValue === 'number'
                ? rawValue.toString()
                : rawValue.trim().toLowerCase();
    const rangePart = measurement.referenceRange
        ? [
            measurement.referenceRange.lower?.toString() ?? '',
            measurement.referenceRange.upper?.toString() ?? '',
            measurement.referenceRange.text?.trim().toLowerCase() ?? '',
        ].join('|')
        : '';

    return [
        measurement.name.trim().toLowerCase(),
        measurement.unit?.trim().toLowerCase() ?? '',
        valuePart,
        rangePart,
    ].join('|');
}

function mergeUniqueMeasurements(measurements: BloodworkMeasurement[]): BloodworkMeasurement[] {
    const unique = new Map<string, BloodworkMeasurement>();
    for (const measurement of measurements) {
        const key = buildMeasurementKey(measurement);
        if (!unique.has(key)) {
            unique.set(key, measurement);
        }
    }
    return Array.from(unique.values());
}

function titleCase(value: string): string {
    return value
        .toLowerCase()
        .split(' ')
        .filter(Boolean)
        .map(token => token[0].toUpperCase() + token.slice(1))
        .join(' ');
}

function heuristicExtractMeasurements(pageTexts: string[]): BloodworkMeasurement[] {
    const rejectedNamePattern =
        /\b(patient|account|address|phone|date|time|specimen|control|provider|labcorp|quest|reported|entered|collected|birth|sex|ss#|number)\b/i;
    const linePattern =
        /^([A-Za-zÄÖÜäöüß][A-Za-zÄÖÜäöüß0-9 ,/%().+:'-]{2,80}?)\s+([<>]?\d+(?:[.,]\d+)?)\s*([A-Za-zµμ/%][A-Za-z0-9µμ/%.-]{0,20})?(?:\s+([<>]?\d+(?:[.,]\d+)?)\s*-\s*([<>]?\d+(?:[.,]\d+)?))?/;

    const measurements: BloodworkMeasurement[] = [];
    const seen = new Set<string>();

    for (const pageText of pageTexts) {
        const lines = pageText.split('\n').map(line => line.trim()).filter(Boolean);
        for (const line of lines) {
            const match = line.match(linePattern);
            if (!match) continue;

            const rawName = match[1].replace(/\s+/g, ' ').trim();
            if (!rawName || rejectedNamePattern.test(rawName)) continue;

            const standardizedName = titleCase(rawName);
            const value = Number.parseFloat(match[2].replace(',', '.'));
            const unit = match[3]?.trim();
            const lower = match[4] ? Number.parseFloat(match[4].replace(',', '.')) : undefined;
            const upper = match[5] ? Number.parseFloat(match[5].replace(',', '.')) : undefined;

            const measurement: BloodworkMeasurement = {
                name: standardizedName,
                originalName: rawName,
                value: Number.isFinite(value) ? value : match[2],
                unit: unit || undefined,
                referenceRange:
                    lower !== undefined || upper !== undefined
                        ? {
                            lower: Number.isFinite(lower) ? lower : undefined,
                            upper: Number.isFinite(upper) ? upper : undefined,
                        }
                        : undefined,
            };

            const key = buildMeasurementKey(measurement);
            if (!seen.has(key)) {
                seen.add(key);
                measurements.push(measurement);
            }
        }
    }

    return measurements;
}

function normalizeMetadataOutput(
    raw: unknown,
    sourcePath: string,
): z.infer<typeof bloodworkMetadataSchema> {
    const cleaned = cleanUnknown(raw);
    if (!cleaned || typeof cleaned !== 'object' || Array.isArray(cleaned)) {
        throw new Error('Model metadata output must be an object');
    }

    const output = { ...cleaned } as Record<string, unknown>;
    if (!output.importLocation) {
        output.importLocation = sourcePath;
        output.importLocationIsInferred = true;
    }

    return bloodworkMetadataSchema.parse(output);
}

function inferLabNameFromText(text: string): string | null {
    const normalized = text.toLowerCase();
    if (normalized.includes('quest diagnostics')) return 'Quest Diagnostics';
    if (normalized.includes('labcorp') || normalized.includes('laboratory corporation of america')) {
        return 'LabCorp';
    }
    if (normalized.includes('physicians lab')) return 'Physicians Lab';
    if (normalized.includes('mdi limbach')) return 'MDI Limbach Berlin GmbH';
    return null;
}

function inferMetadataFromPath({
    sourcePath,
    extractedText,
}: {
    sourcePath: string;
    extractedText: string;
}): z.infer<typeof bloodworkMetadataSchema> {
    const fileName = path.basename(sourcePath, path.extname(sourcePath));
    const dateMatch = fileName.match(/\d{4}-\d{2}-\d{2}/);
    if (!dateMatch) {
        throw new Error(`Could not infer date from filename: ${fileName}`);
    }

    const rawLabToken = fileName
        .replace(dateMatch[0], '')
        .replace(/[_-]+/g, ' ')
        .replace(/\b(lab|results|result|hormones|metabolic|metabloic|de)\b/gi, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    const inferredFromText = inferLabNameFromText(extractedText);
    const weakLabTokens = new Set([
        'lab',
        'labs',
        'result',
        'results',
        'testosterone',
        'hormones',
        'metabolic',
        'metabloic',
    ]);
    const normalizedRawLabToken = rawLabToken.toLowerCase();
    const shouldIgnoreRawToken =
        !rawLabToken ||
        /^\d+$/.test(rawLabToken) ||
        weakLabTokens.has(normalizedRawLabToken);
    const inferredLabName = shouldIgnoreRawToken
        ? inferredFromText || 'Unknown Lab'
        : rawLabToken;

    return bloodworkMetadataSchema.parse({
        date: dateMatch[0],
        labName: inferredLabName,
        importLocation: sourcePath,
        importLocationIsInferred: true,
    });
}

function parseJsonFromText(text: string): unknown {
    const trimmed = text.trim();
    if (!trimmed) {
        throw new Error('Model returned empty text');
    }

    const fenced = trimmed.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
    const candidates = fenced
        ? [fenced[1], trimmed]
        : [trimmed];

    for (const candidate of candidates) {
        try {
            return JSON.parse(candidate);
        } catch {
            // keep trying
        }

        const objectStart = candidate.indexOf('{');
        const objectEnd = candidate.lastIndexOf('}');
        if (objectStart >= 0 && objectEnd > objectStart) {
            try {
                return JSON.parse(candidate.slice(objectStart, objectEnd + 1));
            } catch {
                // keep trying
            }
        }

        const arrayStart = candidate.indexOf('[');
        const arrayEnd = candidate.lastIndexOf(']');
        if (arrayStart >= 0 && arrayEnd > arrayStart) {
            try {
                return JSON.parse(candidate.slice(arrayStart, arrayEnd + 1));
            } catch {
                // keep trying
            }
        }
    }

    throw new Error(`Could not parse JSON from model output: ${trimmed.slice(0, 300)}`);
}

async function generateObjectWithModelFallback<Schema extends z.ZodTypeAny>({
    provider,
    modelIds,
    schema,
    prompt,
    maxOutputTokens,
    contextLabel,
}: {
    provider: ReturnType<typeof createOpenRouter>;
    modelIds: string[];
    schema: Schema;
    prompt: string;
    maxOutputTokens: number;
    contextLabel: string;
}): Promise<{ object: z.infer<Schema>; modelId: string }> {
    let lastError: unknown = null;

    for (const modelId of modelIds) {
        try {
            const result = await generateObject({
                model: provider(modelId, {
                    plugins: [{ id: 'response-healing' }],
                }),
                schema,
                prompt,
                temperature: 0,
                maxRetries: 2,
                maxOutputTokens,
                system: 'You are a precise medical lab data extraction engine.',
            });

            return {
                object: result.object as z.infer<Schema>,
                modelId,
            };
        } catch (error) {
            try {
                const textResult = await generateText({
                    model: provider(modelId),
                    prompt: [
                        prompt,
                        '',
                        'Return only valid JSON.',
                        'Do not wrap JSON in markdown.',
                    ].join('\n'),
                    temperature: 0,
                    maxRetries: 1,
                    maxOutputTokens,
                    system: 'You are a precise medical lab data extraction engine.',
                });
                const parsed = schema.parse(parseJsonFromText(textResult.text)) as z.infer<Schema>;
                return {
                    object: parsed,
                    modelId,
                };
            } catch (textFallbackError) {
                lastError = `${String(error)} | Text fallback failed: ${String(textFallbackError)}`;
            }
        }
    }

    throw new Error(
        `All model attempts failed for ${contextLabel}. Attempted: ${modelIds.join(', ')}. Last error: ${String(lastError)}`,
    );
}

async function extractMeasurementsFromPages({
    provider,
    modelIds,
    sourcePath,
    pageTexts,
}: {
    provider: ReturnType<typeof createOpenRouter>;
    modelIds: string[];
    sourcePath: string;
    pageTexts: string[];
}): Promise<BloodworkMeasurement[]> {
    const uniqueMeasurements = new Map<string, BloodworkMeasurement>();

    for (const pageText of pageTexts) {
        for (let pass = 0; pass < MAX_MEASUREMENT_PASSES_PER_PAGE; pass++) {
            const excludedNames = Array.from(uniqueMeasurements.values())
                .map(item => item.originalName?.trim() || item.name)
                .filter(Boolean)
                .slice(-EXCLUDED_MEASUREMENT_NAMES_LIMIT);

            const prompt = buildMeasurementPrompt({
                sourcePath,
                pageText,
                excludedNames,
            });

            let batch: z.infer<typeof measurementBatchSchema>;
            try {
                const result = await generateObjectWithModelFallback({
                    provider,
                    modelIds,
                    schema: measurementBatchSchema,
                    prompt,
                    maxOutputTokens: MODEL_MAX_OUTPUT_TOKENS,
                    contextLabel: `${sourcePath} (measurement pass ${pass + 1})`,
                });
                batch = result.object;
            } catch {
                break;
            }

            if (batch.measurements.length === 0) {
                break;
            }

            let added = 0;
            for (const measurement of batch.measurements) {
                const key = buildMeasurementKey(measurement);
                if (!uniqueMeasurements.has(key)) {
                    uniqueMeasurements.set(key, measurement);
                    added++;
                }
            }

            if (added === 0) {
                break;
            }
        }
    }

    return Array.from(uniqueMeasurements.values());
}

async function generateLabObject({
    openRouterApiKey,
    modelIds,
    pdfPath,
    extractedText,
    pageTexts,
}: {
    openRouterApiKey: string;
    modelIds: string[];
    pdfPath: string;
    extractedText: string;
    pageTexts: string[];
}): Promise<{ lab: BloodworkLab; modelId: string }> {
    const provider = createOpenRouter({ apiKey: openRouterApiKey });
    let metadataModelId: string | null = null;
    let metadata: z.infer<typeof bloodworkMetadataSchema>;
    try {
        const metadataPrompt = buildMetadataPrompt(pdfPath, extractedText);
        const metadataResult = await generateObjectWithModelFallback({
            provider,
            modelIds,
            schema: bloodworkMetadataSchema,
            prompt: metadataPrompt,
            maxOutputTokens: METADATA_MAX_OUTPUT_TOKENS,
            contextLabel: `${pdfPath} (metadata)`,
        });
        metadata = normalizeMetadataOutput(metadataResult.object, pdfPath);
        metadataModelId = metadataResult.modelId;
    } catch {
        metadata = inferMetadataFromPath({
            sourcePath: pdfPath,
            extractedText,
        });
    }
    const inferredMetadata = inferMetadataFromPath({
        sourcePath: pdfPath,
        extractedText,
    });
    metadata = {
        ...metadata,
        date: inferredMetadata.date,
    };

    let measurements: BloodworkMeasurement[] = [];
    try {
        const extractedMeasurements = await extractMeasurementsFromPages({
            provider,
            modelIds,
            sourcePath: pdfPath,
            pageTexts,
        });
        measurements = mergeUniqueMeasurements(extractedMeasurements);
    } catch {
        measurements = [];
    }

    if (measurements.length === 0) {
        measurements = heuristicExtractMeasurements(pageTexts);
    }
    if (measurements.length === 0) {
        measurements = [{
            name: 'Unparsed Result',
            notes: 'Automatic extraction returned no structured measurements for this report.',
        }];
    }

    return {
        lab: bloodworkLabSchema.parse({
            ...metadata,
            measurements,
        }),
        modelId: metadataModelId ?? modelIds[0],
    };
}

function resolveOutputFileName({
    lab,
    sourcePath,
}: {
    lab: BloodworkLab;
    sourcePath: string;
}): string {
    const baseFileName = buildBloodworkFileName(lab);
    const baseOutputPath = path.join(DEFAULT_OUTPUT_DIRECTORY, baseFileName);
    if (!fs.existsSync(baseOutputPath)) {
        return baseFileName;
    }

    try {
        const existing = JSON.parse(fs.readFileSync(baseOutputPath, 'utf8')) as Record<string, unknown>;
        if (existing.importLocation === sourcePath) {
            return baseFileName;
        }
    } catch {
        // if existing file is malformed, keep it untouched and write a distinct filename
    }

    const sourceSlug = slugifyForPath(path.basename(sourcePath, path.extname(sourcePath)));
    return baseFileName.replace(/\.json$/i, `_${sourceSlug}.json`);
}

function buildS3KeyFromFileName(fileName: string, prefix: string): string {
    const normalizedPrefix = prefix.replace(/^\/+|\/+$/g, '');
    return normalizedPrefix ? `${normalizedPrefix}/${fileName}` : fileName;
}

async function maybeUploadToS3({
    s3Client,
    s3Bucket,
    s3Prefix,
    fileName,
    jsonPayload,
}: {
    s3Client: S3Client | null;
    s3Bucket: string;
    s3Prefix: string;
    fileName: string;
    jsonPayload: string;
}): Promise<string | null> {
    if (!s3Client) {
        return null;
    }

    const key = buildS3KeyFromFileName(fileName, s3Prefix);
    await s3Client.send(
        new PutObjectCommand({
            Bucket: s3Bucket,
            Key: key,
            Body: jsonPayload,
            ContentType: 'application/json; charset=utf-8',
        }),
    );

    return key;
}

async function importSingleFile({
    pdfPath,
    openRouterApiKey,
    modelIds,
    s3Client,
    s3Bucket,
    s3Prefix,
}: {
    pdfPath: string;
    openRouterApiKey: string;
    modelIds: string[];
    s3Client: S3Client | null;
    s3Bucket: string;
    s3Prefix: string;
}): Promise<ImportResult> {
    const pdfBytes = new Uint8Array(await Bun.file(pdfPath).arrayBuffer());
    assertPdfSignature(pdfBytes, pdfPath);

    const extracted = await extractPdfText(pdfBytes);
    const { lab, modelId } = await generateLabObject({
        openRouterApiKey,
        modelIds,
        pdfPath,
        extractedText: extracted.fullText,
        pageTexts: extracted.pageTexts,
    });

    fs.mkdirSync(DEFAULT_OUTPUT_DIRECTORY, { recursive: true });
    const outputFileName = resolveOutputFileName({
        lab,
        sourcePath: pdfPath,
    });
    const outputPath = path.join(DEFAULT_OUTPUT_DIRECTORY, outputFileName);
    const jsonPayload = JSON.stringify(lab, null, 4);
    await Bun.write(outputPath, jsonPayload);

    const s3Key = await maybeUploadToS3({
        s3Client,
        s3Bucket,
        s3Prefix,
        fileName: outputFileName,
        jsonPayload,
    });

    return { outputPath, s3Key, modelId };
}

function createS3ClientIfNeeded(options: {
    skipUpload: boolean;
}): { s3Client: S3Client | null; s3Bucket: string; s3Prefix: string } {
    const s3Bucket = process.env.VITALS_S3_BUCKET?.trim() || DEFAULT_S3_BUCKET;
    const s3Prefix = process.env.VITALS_S3_PREFIX?.trim() || DEFAULT_S3_PREFIX;

    if (options.skipUpload) {
        return { s3Client: null, s3Bucket, s3Prefix };
    }

    const region = process.env.AWS_REGION?.trim() || process.env.AWS_DEFAULT_REGION?.trim();
    if (!region) {
        throw new Error('Missing required environment variable: AWS_REGION (or AWS_DEFAULT_REGION)');
    }

    const accessKeyId = requireEnv('AWS_ACCESS_KEY_ID');
    const secretAccessKey = requireEnv('AWS_SECRET_ACCESS_KEY');
    const sessionToken = process.env.AWS_SESSION_TOKEN?.trim();

    return {
        s3Client: new S3Client({
            region,
            credentials: {
                accessKeyId,
                secretAccessKey,
                sessionToken: sessionToken || undefined,
            },
        }),
        s3Bucket,
        s3Prefix,
    };
}

async function runBloodworkImporter(argv: string[] = process.argv.slice(2)): Promise<void> {
    const options = parseCliOptions(argv);
    const openRouterApiKey = requireEnv('OPENROUTER_API_KEY');
    const modelIds = resolveModelIds(options.modelIds);
    const files = resolveInputFiles(options);
    const { s3Client, s3Bucket, s3Prefix } = createS3ClientIfNeeded({
        skipUpload: options.skipUpload,
    });

    console.info(`Importing ${files.length} file(s)`);
    console.info(`Model candidates: ${modelIds.join(', ')}`);
    if (options.skipUpload) {
        console.info('S3 upload is disabled for this run (--skip-upload)');
    } else {
        console.info(`S3 destination: s3://${s3Bucket}/${s3Prefix}`);
    }

    const failures: Array<{ file: string; error: unknown }> = [];
    let successCount = 0;

    for (const filePath of files) {
        console.info(`\nProcessing ${filePath}`);
        try {
            const result = await importSingleFile({
                pdfPath: filePath,
                openRouterApiKey,
                modelIds,
                s3Client,
                s3Bucket,
                s3Prefix,
            });

            successCount += 1;
            console.info(`Wrote ${result.outputPath}`);
            if (result.s3Key) {
                console.info(`Uploaded s3://${s3Bucket}/${result.s3Key}`);
            }
            console.info(`Parsed with ${result.modelId}`);
        } catch (error) {
            failures.push({ file: filePath, error });
            if (options.continueOnError) {
                console.error(`Failed ${filePath}:`, error);
            }
            if (!options.continueOnError) {
                throw error;
            }
        }
    }

    console.info(`\nCompleted with ${successCount} success(es), ${failures.length} failure(s)`);
    if (failures.length > 0) {
        const failedFiles = failures.map(item => item.file).join('\n');
        throw new Error(`Import failures:\n${failedFiles}`);
    }
}

export {
    parseCliOptions,
    resolveInputFiles,
    assertPdfSignature,
    normalizeModelOutput,
    resolveModelIds,
    runBloodworkImporter,
};

if (import.meta.main) {
    createScript(async () => {
        await runBloodworkImporter();
    });
}
