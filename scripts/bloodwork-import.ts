import fs from 'fs';
import path from 'path';

import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { createOpenRouter } from '@openrouter/ai-sdk-provider';
import { generateObject } from 'ai';
import { getDocument } from 'pdfjs-dist/legacy/build/pdf.mjs';

import {
    bloodworkLabSchema,
    buildBloodworkFileName,
    buildBloodworkS3Key,
    type BloodworkLab,
} from './bloodwork-schema.ts';
import { createScript } from './createScript.ts';

const DEFAULT_S3_BUCKET = 'stefan-life';
const DEFAULT_S3_PREFIX = 'vitals';
const DEFAULT_MODEL_IDS = ['google/gemini-3-flash', 'google/gemini-2.5-flash'];
const DEFAULT_TO_IMPORT_DIRECTORY = path.resolve(process.cwd(), 'data/to-import');
const DEFAULT_OUTPUT_DIRECTORY = path.resolve(process.cwd(), 'data');
const EXTRACTED_TEXT_LIMIT = 45_000;

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
    const envModelList = process.env.OPENROUTER_MODEL
        ?.split(',')
        .map(item => item.trim())
        .filter(Boolean);
    if (envModelList && envModelList.length > 0) {
        return envModelList;
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

async function extractPdfText(bytes: Uint8Array): Promise<string> {
    const document = await getDocument({ data: bytes }).promise;
    const chunks: string[] = [];

    for (let pageIndex = 1; pageIndex <= document.numPages; pageIndex++) {
        const page = await document.getPage(pageIndex);
        const textContent = await page.getTextContent();
        const pageText = textContent.items
            .map(item => ('str' in item ? item.str : ''))
            .join(' ')
            .replace(/\s+/g, ' ')
            .trim();
        if (pageText) {
            chunks.push(`Page ${pageIndex}: ${pageText}`);
        }
    }

    return chunks.join('\n');
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

function buildPrompt(sourcePath: string, extractedText: string): string {
    const extractedSegment = extractedText
        ? extractedText.slice(0, EXTRACTED_TEXT_LIMIT)
        : 'No machine-readable text was extracted from the PDF. Use the PDF file itself as the source.';

    return [
        `Source file: ${sourcePath}`,
        '',
        'Convert this bloodwork report into the provided schema.',
        'Requirements:',
        '- Keep all text in English.',
        '- Standardize measurement names in English.',
        '- Use ISO date format (YYYY-MM-DD).',
        '- Include weightKg only when explicitly present.',
        '- Include location and importLocation if present.',
        '- For each measurement include: name, originalName, value, unit, referenceRange, flag, notes when available.',
        '- Do not fabricate values. Omit unknown fields.',
        '',
        'Extracted text (may be partial):',
        extractedSegment,
    ].join('\n');
}

async function generateLabObject({
    openRouterApiKey,
    modelIds,
    pdfPath,
    pdfBytes,
    extractedText,
}: {
    openRouterApiKey: string;
    modelIds: string[];
    pdfPath: string;
    pdfBytes: Uint8Array;
    extractedText: string;
}): Promise<{ lab: BloodworkLab; modelId: string }> {
    const provider = createOpenRouter({ apiKey: openRouterApiKey });
    const prompt = buildPrompt(pdfPath, extractedText);

    let lastError: unknown = null;
    for (const modelId of modelIds) {
        try {
            const result = await generateObject({
                model: provider(modelId),
                schema: bloodworkLabSchema,
                temperature: 0,
                maxRetries: 2,
                system: 'You are a precise medical lab data extraction engine.',
                messages: [{
                    role: 'user',
                    content: [
                        { type: 'text', text: prompt },
                        {
                            type: 'file',
                            data: pdfBytes,
                            mediaType: 'application/pdf',
                            filename: path.basename(pdfPath),
                        },
                    ],
                }],
            });

            return {
                lab: normalizeModelOutput(result.object, pdfPath),
                modelId,
            };
        } catch (err) {
            lastError = err;
        }
    }

    throw new Error(
        `All model attempts failed for ${pdfPath}. Attempted: ${modelIds.join(', ')}. Last error: ${String(lastError)}`,
    );
}

async function maybeUploadToS3({
    s3Client,
    s3Bucket,
    s3Prefix,
    lab,
    jsonPayload,
}: {
    s3Client: S3Client | null;
    s3Bucket: string;
    s3Prefix: string;
    lab: BloodworkLab;
    jsonPayload: string;
}): Promise<string | null> {
    if (!s3Client) {
        return null;
    }

    const key = buildBloodworkS3Key(lab, s3Prefix);
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

    const extractedText = await extractPdfText(pdfBytes);
    const { lab, modelId } = await generateLabObject({
        openRouterApiKey,
        modelIds,
        pdfPath,
        pdfBytes,
        extractedText,
    });

    fs.mkdirSync(DEFAULT_OUTPUT_DIRECTORY, { recursive: true });
    const outputPath = path.join(DEFAULT_OUTPUT_DIRECTORY, buildBloodworkFileName(lab));
    const jsonPayload = JSON.stringify(lab, null, 4);
    await Bun.write(outputPath, jsonPayload);

    const s3Key = await maybeUploadToS3({
        s3Client,
        s3Bucket,
        s3Prefix,
        lab,
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

    requireEnv('AWS_ACCESS_KEY_ID');
    requireEnv('AWS_SECRET_ACCESS_KEY');

    return {
        s3Client: new S3Client({ region }),
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
