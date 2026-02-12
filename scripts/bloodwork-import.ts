import fs from 'fs';
import { spawnSync } from 'child_process';
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
const DEFAULT_GLOSSARY_VALIDATOR_MODEL_IDS = ['google/gemini-3-flash-preview'];
const DEFAULT_TO_IMPORT_DIRECTORY = path.resolve(process.cwd(), 'data/to-import');
const DEFAULT_OUTPUT_DIRECTORY = path.resolve(process.cwd(), 'data');
const DEFAULT_GLOSSARY_PATH = path.resolve(process.cwd(), 'server/src/bloodwork-glossary.json');
const EXTRACTED_TEXT_LIMIT = 45_000;
const MODEL_MAX_OUTPUT_TOKENS = 1_400;
const METADATA_MAX_OUTPUT_TOKENS = 280;
const NORMALIZATION_MAX_OUTPUT_TOKENS = 6_000;
const GLOSSARY_VALIDATION_MAX_OUTPUT_TOKENS = 1_600;
const MODEL_REQUEST_TIMEOUT_MS = 30_000;
const MAX_MEASUREMENTS_PER_PAGE = 120;
const MAX_NORMALIZATION_CANDIDATES = 320;
const MAX_GLOSSARY_DECISIONS = 64;

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

const NON_MEASUREMENT_NAME_EXACT = new Set([
    'page',
    'seite',
    'result',
    'results',
    'resultat',
    'tests ordered',
    'desired range',
    'interpretation',
    'dob',
    'dob:',
    'height',
    'height:',
    'weight',
    'weight:',
    'received on',
    'received on:',
    'date/time',
    'date/time collected',
    'date/time reported',
    'reference interval',
    'reference-/zielbereich',
    'wertelage',
    'tests',
    'analyse',
    'code',
    'note',
    'for',
    'lower',
    'total',
    'ny,',
]);

const NON_MEASUREMENT_NAME_PATTERNS: RegExp[] = [
    /\b(?:patient|geburtsdatum|geschlecht|barcode|auftrag|fallnummer|tagesnummer|ext\.?nr)\b/i,
    /\b(?:address|account|physician|specimen|control number|npi|lab report|labcorp raritan)\b/i,
    /\b(?:tel|fax|phone|e-?mail|www\.|http|@|street|avenue|berlin|new york|zionskirchstr|aroser allee)\b/i,
    /\b(?:collected|reported|entered|date entered|date\/time)\b/i,
    /\b(?:durch die dakks|akkreditiert|leitlinie|diagnostik|therapie|dyslipid|legende|quelle)\b/i,
    /\b(?:for inquiries|overall report status|final|negative not infected)\b/i,
    /\b(?:comment|comments|canceled|cancelled|immature cells|hematology comments)\b/i,
    /\b(?:zielwert|risiko|wahrscheinlichkeit|schlaganfallen|myokardinfarkten)\b/i,
    /\b(?:zielbereich|befund freigegeben)\b/i,
    /\b(?:miami|federal law|document in error|customer service|comparative hepatol|gastroenterology|circulation|bmc)\b/i,
    /\b(?:guideline|guidelines|source|according|consider retesting|see note|see comments)\b/i,
    /\b(?:sensitivity|specificity|study of|patients where|is above|had a sensitivity|had a specificity)\b/i,
    /\b(?:desirable range|homa-?ir von)\b/i,
    /\(mz\d+[a-z]?\)/i,
    /^(?:for|consider|according|source|reference|note|monday|tuesday|wednesday|thursday|friday)\b/i,
];

const NON_MEASUREMENT_UNIT_EXACT = new Set([
    'von',
    'a',
    'ext',
    'seite',
]);

const NON_MEASUREMENT_VALUE_EXACT = new Set([
    'canceled',
    'cancelled',
    'pending',
    'n/a',
    'na',
    'see note',
    'not reported',
    'not available',
]);

const GERMAN_NAME_REPLACEMENTS: Array<{ pattern: RegExp; replacement: string }> = [
    { pattern: /\bLeukozyten\b/gi, replacement: 'Leukocytes' },
    { pattern: /\bErythrozyten\b/gi, replacement: 'Erythrocytes' },
    { pattern: /\bHämoglobin\b/gi, replacement: 'Hemoglobin' },
    { pattern: /\bHamatokrit\b/gi, replacement: 'Hematocrit' },
    { pattern: /\bHämatokrit\b/gi, replacement: 'Hematocrit' },
    { pattern: /\bThrombozyten\b/gi, replacement: 'Platelets' },
    { pattern: /\bNeutrophile Granulozyten\b/gi, replacement: 'Neutrophils' },
    { pattern: /\bLymphozyten\b/gi, replacement: 'Lymphocytes' },
    { pattern: /\bMonozyten\b/gi, replacement: 'Monocytes' },
    { pattern: /\bEosinophile\b/gi, replacement: 'Eosinophils' },
    { pattern: /\bBasophile\b/gi, replacement: 'Basophils' },
    { pattern: /\bGlukose\b/gi, replacement: 'Glucose' },
    { pattern: /\bNatrium\b/gi, replacement: 'Sodium' },
    { pattern: /\bKalium\b/gi, replacement: 'Potassium' },
    { pattern: /\bHarnsäure\b/gi, replacement: 'Uric Acid' },
    { pattern: /\bGesamteiweiß\b/gi, replacement: 'Total Protein' },
    { pattern: /\bTransferrinsättigung\b/gi, replacement: 'Transferrin Saturation' },
    { pattern: /\bLuteinisierendes Hormon\b/gi, replacement: 'Luteinizing Hormone' },
    { pattern: /\bFollikelstim\. Hormon\b/gi, replacement: 'Follicle Stimulating Hormone' },
    { pattern: /\bÖstradiol\b/gi, replacement: 'Estradiol' },
    { pattern: /\bFolsäure\b/gi, replacement: 'Folate' },
    { pattern: /\bFreies Testosteron\b/gi, replacement: 'Free Testosterone' },
    { pattern: /\bFreies-Testosteron-Index\b/gi, replacement: 'Free Testosterone Index' },
    { pattern: /\bSexualhormonbindendes Globulin\b/gi, replacement: 'Sex Hormone Binding Globulin' },
    { pattern: /\bDHEA-Sulfat\b/gi, replacement: 'DHEA Sulfate' },
    { pattern: /\babgeleitete mittlere Glucose\b/gi, replacement: 'Estimated Average Glucose' },
    { pattern: /\bHba1c \(ifcc\/neue Std\.\)\b/gi, replacement: 'HbA1c (IFCC)' },
];

const MEASUREMENT_NAME_TRANSLATION_REPLACEMENTS: Array<{ pattern: RegExp; replacement: string }> = [
    { pattern: /\bgesamt\b/gi, replacement: 'Total' },
    { pattern: /\bkorrigiertes?\b/gi, replacement: 'Corrected' },
    { pattern: /\bcholesterin\b/gi, replacement: 'Cholesterol' },
    { pattern: /\breaktives?\b/gi, replacement: 'Reactive' },
    { pattern: /\bultrasensitiv(?:e|es|em|en)?\b/gi, replacement: 'High Sensitivity' },
    { pattern: /\bfreies?\b/gi, replacement: 'Free' },
    { pattern: /\btrijodthyronin\b/gi, replacement: 'Triiodothyronine' },
    { pattern: /\bthyroxin\b/gi, replacement: 'Thyroxine' },
    { pattern: /\bund\b/gi, replacement: 'and' },
    { pattern: /\brestandardisiert(?:e|es|em|en)?\b/gi, replacement: 'Restandardized' },
    { pattern: /\bsulfat\b/gi, replacement: 'Sulfate' },
    { pattern: /\bbasal\b/gi, replacement: 'Basal' },
    { pattern: /\bi\.\s*s\./gi, replacement: 'Serum' },
    { pattern: /\bi\.s\./gi, replacement: 'Serum' },
];

const GLOSSARY_TRAILING_QUALIFIER_KEYS = new Set([
    'se',
    'eb',
    'si',
    'glex',
    'hplc',
    'ifcc',
    'eclia',
    'serum',
    'plasma',
    'wholeblood',
    'gen2',
    'gen3',
    'gen4',
    'ii',
    'iii',
    'iv',
    'v',
]);

const GLOSSARY_CANONICAL_NAME_RULES: Array<{ pattern: RegExp; canonicalName: string }> = [
    { pattern: /^albumin(?: serum)?$/i, canonicalName: 'Albumin' },
    { pattern: /\balbumin corrected calcium\b/i, canonicalName: 'Albumin-Corrected Calcium' },
    { pattern: /\balbumin globulin ratio\b/i, canonicalName: 'Albumin/Globulin Ratio' },
    { pattern: /^alkaline phosphatase(?: phosphatase)?$/i, canonicalName: 'Alkaline Phosphatase' },
    { pattern: /\balanine aminotransferase|^alt(?: sgpt)?(?: p5p)?$/i, canonicalName: 'ALT (SGPT)' },
    { pattern: /\baspartate aminotransferase|^ast(?: sgot)?(?: p5p)?$/i, canonicalName: 'AST (SGOT)' },
    { pattern: /\bbilirubin total|^total bilirubin$|bilirubin gesamt/i, canonicalName: 'Bilirubin, Total' },
    { pattern: /\bbilirubin direct\b/i, canonicalName: 'Bilirubin, Direct' },
    { pattern: /\bbilirubin indirect\b/i, canonicalName: 'Bilirubin, Indirect' },
    { pattern: /\bc reactive protein|^crp$/i, canonicalName: 'C-Reactive Protein' },
    { pattern: /\bestimated average glucose\b/i, canonicalName: 'Estimated Average Glucose' },
    { pattern: /^glucose(?: si)?$/i, canonicalName: 'Glucose' },
    { pattern: /\bfree testosterone index\b/i, canonicalName: 'Free Testosterone Index' },
    { pattern: /\bfree testosterone\b/i, canonicalName: 'Free Testosterone' },
    { pattern: /\bft3\b/i, canonicalName: 'Free T3' },
    { pattern: /\bft4\b/i, canonicalName: 'Free T4' },
    { pattern: /\bhba1c|hemoglobin a1c\b/i, canonicalName: 'Hemoglobin A1c' },
    { pattern: /^(?:non hdl cholesterol|non hdl)$/i, canonicalName: 'Non-HDL Cholesterol' },
    { pattern: /^(?:hdl cholesterol|hdl chol(?:esterol)?)$/i, canonicalName: 'HDL Cholesterol' },
    { pattern: /^(?:ldl cholesterol|ldl chol(?:esterol)?|ldl)$/i, canonicalName: 'LDL Cholesterol' },
    { pattern: /\btriglyceride\b/i, canonicalName: 'Triglycerides' },
    { pattern: /\bhoma ir\b/i, canonicalName: 'HOMA-IR' },
    { pattern: /\bsex hormone binding globulin\b/i, canonicalName: 'Sex Hormone Binding Globulin' },
    { pattern: /\bdhea sulfate|^dhea s$/i, canonicalName: 'DHEA-S' },
    { pattern: /\btransferrin saturation\b/i, canonicalName: 'Transferrin Saturation' },
    { pattern: /\bluteinizing hormone\b/i, canonicalName: 'Luteinizing Hormone' },
    { pattern: /\bfollicle stimulating hormone\b/i, canonicalName: 'Follicle Stimulating Hormone' },
    { pattern: /^insulin(?: serum)?$/i, canonicalName: 'Insulin' },
    { pattern: /^igg(?: serum)?$/i, canonicalName: 'IgG' },
    { pattern: /^tsh(?: basal)?$/i, canonicalName: 'TSH' },
    { pattern: /^globulin$/i, canonicalName: 'Globulin' },
    { pattern: /^creatinine$/i, canonicalName: 'Creatinine' },
    { pattern: /^egfr non$/i, canonicalName: 'eGFR' },
    { pattern: /^cortisol(?: a m| am)$/i, canonicalName: 'Cortisol, AM' },
    { pattern: /^cortisol corti?one 11b hsd(?: ii)?$/i, canonicalName: 'Cortisol/Cortisone 11B-HSD' },
    { pattern: /^(?:ol|chol) hdl$/i, canonicalName: 'Cholesterol/HDL Ratio' },
    { pattern: /^hdl$/i, canonicalName: 'HDL Cholesterol' },
    { pattern: /^ldl chol calc(?: nih)?$/i, canonicalName: 'LDL Cholesterol' },
    { pattern: /^vldl cholesterol cal$/i, canonicalName: 'VLDL Cholesterol' },
    { pattern: /^free dhea(?: 2)?$/i, canonicalName: 'Free DHEA' },
    { pattern: /^glucose serum$/i, canonicalName: 'Glucose' },
    { pattern: /\bvitamin d3 25 oh|vitamin d 25 oh\b/i, canonicalName: 'Vitamin D, 25-OH' },
    { pattern: /\bvitamin b12\b/i, canonicalName: 'Vitamin B12' },
    { pattern: /\bvitamin b2\b/i, canonicalName: 'Vitamin B2' },
    { pattern: /\bvitamin b6\b/i, canonicalName: 'Vitamin B6' },
    { pattern: /\bmagnesium in erythrocytes\b/i, canonicalName: 'Magnesium, RBC' },
    { pattern: /\bapolipoprotein a 1|^apolipoprotein a1$/i, canonicalName: 'Apolipoprotein A1' },
    { pattern: /\bapolipoprotein b\b/i, canonicalName: 'Apolipoprotein B' },
    { pattern: /\bprolactin\b/i, canonicalName: 'Prolactin' },
    { pattern: /\btestosterone total ms\b/i, canonicalName: 'Testosterone, Total' },
    { pattern: /\btestosterone free and bioavailable\b/i, canonicalName: 'Testosterone, Free and Bioavailable' },
    { pattern: /\btestosterone bioavailable\b/i, canonicalName: 'Testosterone, Bioavailable' },
    { pattern: /\bfibrosis scoring|^fibrosis score$/i, canonicalName: 'Fibrosis Score' },
    { pattern: /\bfibrosis stage(?: f\d+)?$/i, canonicalName: 'Fibrosis Stage' },
    { pattern: /\bsteatosis grading|^steatosis grade$/i, canonicalName: 'Steatosis Grade' },
    { pattern: /\bsteatosis score$/i, canonicalName: 'Steatosis Score' },
    { pattern: /\bnash scoring|^nash score$/i, canonicalName: 'NASH Score' },
    { pattern: /\bnash grade$/i, canonicalName: 'NASH Grade' },
    { pattern: /^egfr(?: if(?: nonafricn| africn(?: am)?)?)?$/i, canonicalName: 'eGFR' },
    { pattern: /^tibc$/i, canonicalName: 'TIBC' },
    { pattern: /^platelet$/i, canonicalName: 'Platelets' },
];

const QUALITATIVE_RESULT_PATTERN =
    /\b(?:negative|positive|detected|not detected|nonreactive|reactive|indeterminate|trace|abnormal|normal)\b/i;
const VALUE_TOKEN_PATTERN =
    /(?:^|[^\w])(?:[<>]=?|=)?\d+(?:[.,]\d+)?(?:\s*-\s*[<>]?\d+(?:[.,]\d+)?)?(?:%|[A-Za-z/]+)?|(?:^|\s)(?:negative|positive|detected|not detected|nonreactive|reactive|indeterminate|abnormal|normal)(?:\s|$)/i;
const RANGE_TOKEN_PATTERN =
    /[<>]?\d+(?:[.,]\d+)?\s*-\s*[<>]?\d+(?:[.,]\d+)?|(?:^|\s)[<>]\s*\d+(?:[.,]\d+)?/i;
const UNIT_TOKEN_PATTERN =
    /\b(?:mg\/d?l|g\/d?l|ng\/m?l|pg\/m?l|iu\/l|m?u\/l|u\/l|mmol\/l|mmol\/mol|µ?mol\/l|x10e\d+\/u?l|f?mol\/l|fl|pg|ratio|%|nmol\/l|pmol\/l|s\/co\s*ratio|gpt\/l|tpt\/l|\/µ?l|µ?kat\/l|ng\/dl|ml\/m)\b/i;
const LIKELY_ANALYTE_NAME_PATTERN =
    /\b(?:wbc|rbc|hemoglobin|hematocrit|platelet|neutrophil|lymph|monocyte|eos|baso|glucose|hba1c|creatinine|bun|egfr|bilirubin|albumin|globulin|protein|ast|alt|ggt|alkaline phosphatase|ldh|amylase|lipase|cholesterol|cholester|triglyceride|ldl|hdl|ferritin|iron|uibc|tibc|transferrin|insulin|tsh|ft3|ft4|lh|fsh|estradiol|prolactin|testosterone|dhea|vitamin|folate|magnesium|homa|apolipoprotein|fibrosis|steatosis|nash|cortisol|pregnanediol|estrone|estriol|androsterone|etiocholanolone|igg|omega|epa|dha|crp|hcv|hep b|hbsag|culture|streptococcus)\b/i;
const NON_ENGLISH_NAME_PATTERN = /[äöüßéèàáìíòóùúñç]/i;
const NON_ENGLISH_GLOSSARY_TOKENS = [
    'leukozyten',
    'erythrozyten',
    'haemoglobin',
    'hämoglobin',
    'haematokrit',
    'hämatokrit',
    'thrombozyten',
    'lymphozyten',
    'monozyten',
    'eosinophile',
    'basophile',
    'glukose',
    'harnsaeure',
    'harnsäure',
    'gesamteiweiss',
    'gesamteiweiß',
    'transferrinsaettigung',
    'transferrinsättigung',
    'luteinisierendes',
    'follikelstim',
    'folsaeure',
    'folsäure',
    'korrigiertes',
    'gesamt',
    'cholesterin',
    'reaktives',
    'ultrasensitiv',
    'freies',
    'trijodthyronin',
    'thyroxin',
    'restandardisiert',
    'sulfat',
    'und',
    'hormon',
    'bestimmt als',
    'flavinadenindinucleotid',
];
const GLOSSARY_FALLBACK_REJECT_PATTERN =
    /\b(?:page|result|desired|desirable|reference|range|guideline|comment|note|customer|service|therapy|study|specificity|sensitivity|mirea|patient|account|code|source|marker|axis|cbc)\b/i;

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
    measurements: z.array(bloodworkMeasurementSchema).max(MAX_MEASUREMENTS_PER_PAGE),
});

const measurementNormalizationSchema = z.object({
    measurements: z.array(bloodworkMeasurementSchema).max(MAX_NORMALIZATION_CANDIDATES),
});

const glossaryRangeSchema = z.object({
    min: z.number().finite().optional(),
    max: z.number().finite().optional(),
    unit: z.string().trim().min(1).optional(),
});

const bloodworkGlossaryEntrySchema = z.object({
    canonicalName: z.string().trim().min(1),
    aliases: z.array(z.string().trim().min(1)).default([]),
    knownRanges: z.array(glossaryRangeSchema).default([]),
    unitHints: z.array(z.string().trim().min(1)).default([]),
    createdAt: z.string().trim().min(1),
    updatedAt: z.string().trim().min(1),
});

const bloodworkGlossarySchema = z.object({
    version: z.literal(1),
    updatedAt: z.string().trim().min(1),
    entries: z.array(bloodworkGlossaryEntrySchema),
});

const glossaryValidationDecisionSchema = z.object({
    index: z.number().int().nonnegative(),
    action: z.string().trim().min(1),
    targetCanonicalName: z.string().trim().min(1).optional(),
    canonicalName: z.string().trim().min(1).optional(),
    aliases: z.array(z.string().trim().min(1)).max(6).optional(),
    reason: z.string().trim().min(1).optional(),
});

const glossaryValidationBatchSchema = z.object({
    decisions: z.array(glossaryValidationDecisionSchema).max(MAX_GLOSSARY_DECISIONS),
});

type BloodworkGlossary = z.infer<typeof bloodworkGlossarySchema>;
type BloodworkGlossaryEntry = z.infer<typeof bloodworkGlossaryEntrySchema>;

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

function splitExtractedTextIntoPages(text: string): string[] {
    return text
        .split('\f')
        .map(page => page.replace(/\r/g, '').trim())
        .filter(Boolean);
}

function commandExists(binary: string): boolean {
    const result = spawnSync('which', [binary], {
        stdio: 'ignore',
    });
    return result.status === 0;
}

function extractPdfTextWithPdftotext(pdfPath: string): ExtractedPdfText | null {
    if (!commandExists('pdftotext')) {
        return null;
    }

    const command = spawnSync(
        'pdftotext',
        ['-layout', '-enc', 'UTF-8', pdfPath, '-'],
        {
            encoding: 'utf8',
            maxBuffer: 20 * 1024 * 1024,
        },
    );

    if (command.status !== 0) {
        return null;
    }

    const pageTexts = splitExtractedTextIntoPages(command.stdout || '');
    if (pageTexts.length === 0) {
        return null;
    }

    return {
        fullText: pageTexts.join('\n\n'),
        pageTexts,
    };
}

async function extractPdfTextWithPdfjs(bytes: Uint8Array): Promise<ExtractedPdfText> {
    const document = await getDocument({ data: bytes }).promise;
    const pageTexts: string[] = [];

    for (let pageIndex = 1; pageIndex <= document.numPages; pageIndex++) {
        const page = await document.getPage(pageIndex);
        const textContent = await page.getTextContent();
        const lines = new Map<number, Array<{ x: number; token: string }>>();

        for (const item of textContent.items) {
            if (!('str' in item)) continue;
            const token = item.str.trim();
            if (!token) continue;

            const transform = 'transform' in item && Array.isArray(item.transform) ? item.transform : null;
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
            .map(([, tokens]) => tokens.sort((left, right) => left.x - right.x).map(item => item.token).join(' '))
            .map(line => line.replace(/\s+/g, ' ').trim())
            .filter(Boolean)
            .join('\n')
            .trim();

        if (pageBody) {
            pageTexts.push(pageBody);
        }
    }

    return {
        fullText: pageTexts.join('\n\n'),
        pageTexts,
    };
}

async function extractPdfText(pdfPath: string, bytes: Uint8Array): Promise<ExtractedPdfText> {
    const extractedWithPdftotext = extractPdfTextWithPdftotext(pdfPath);
    if (extractedWithPdftotext) {
        return extractedWithPdftotext;
    }
    return extractPdfTextWithPdfjs(bytes);
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

function normalizeTextForMatch(value: string): string {
    return value
        .normalize('NFKD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .trim();
}

function isTrailingGlossaryQualifierToken(value: string): boolean {
    const normalized = value
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '');
    if (!normalized) {
        return false;
    }
    if (GLOSSARY_TRAILING_QUALIFIER_KEYS.has(normalized)) {
        return true;
    }
    if (/^gen\d+$/.test(normalized)) {
        return true;
    }
    if (/^[ivx]{2,4}$/.test(normalized)) {
        return true;
    }
    return false;
}

function stripTrailingGlossaryQualifierParentheses(name: string): string {
    let next = name.trim();
    while (true) {
        const match = next.match(/\(([^()]+)\)\s*$/);
        if (!match) {
            return next;
        }
        const token = match[1]?.trim();
        if (!token || !isTrailingGlossaryQualifierToken(token)) {
            return next;
        }
        next = next.slice(0, match.index).trim();
    }
}

function normalizeMeasurementNameForGlossary(name: string): string {
    let next = name
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    if (!next) {
        return '';
    }

    for (const replacement of GERMAN_NAME_REPLACEMENTS) {
        next = next.replace(replacement.pattern, replacement.replacement);
    }
    for (const replacement of MEASUREMENT_NAME_TRANSLATION_REPLACEMENTS) {
        next = next.replace(replacement.pattern, replacement.replacement);
    }

    next = next
        .replace(/\b(?:Androgen|Estrogen and Progesterone|HPA\s*-\s*Axis)\s+Markers?\b/gi, ' ')
        .replace(/\bGen\.?\s*\d+\b/gi, ' ')
        .replace(/\b(?:II|III|IV|V)\b(?=\s*(?:\(|$))/g, ' ')
        .replace(/\(([^()]+)\)\s*\(\1\)/gi, '($1)')
        .replace(/\s+/g, ' ')
        .replace(/\s*-\s*/g, '-')
        .replace(/\s*\/\s*/g, '/')
        .replace(/[,:;]+$/g, '')
        .trim();

    next = stripTrailingGlossaryQualifierParentheses(next);

    next = next
        .replace(/\(\s*\)/g, '')
        .replace(/\s+/g, ' ')
        .trim();

    const normalizedKey = normalizeGlossaryNameKey(next);
    if (!normalizedKey) {
        return '';
    }
    if (/^cbc(?:\b| )/.test(normalizedKey)) {
        return '';
    }
    if (normalizedKey === 'cortisol estriol') {
        return '';
    }

    for (const rule of GLOSSARY_CANONICAL_NAME_RULES) {
        if (rule.pattern.test(normalizedKey)) {
            return rule.canonicalName;
        }
    }

    return next;
}

function normalizeMeasurementForGlossary(measurement: BloodworkMeasurement): BloodworkMeasurement | null {
    const sourceName = measurement.name.replace(/\s+/g, ' ').trim();
    const canonicalName = normalizeMeasurementNameForGlossary(sourceName);
    if (!canonicalName || !isEnglishGlossaryName(canonicalName)) {
        return null;
    }

    const sourceOriginalName = measurement.originalName?.replace(/\s+/g, ' ').trim() || sourceName;
    const nextOriginalName =
        sourceOriginalName &&
        normalizeGlossaryNameKey(sourceOriginalName) !== normalizeGlossaryNameKey(canonicalName)
            ? sourceOriginalName
            : undefined;

    return {
        ...measurement,
        name: canonicalName,
        originalName: nextOriginalName,
    };
}

function nowIsoTimestamp(): string {
    return new Date().toISOString();
}

function isEnglishGlossaryName(value: string): boolean {
    const trimmed = value.trim();
    if (!trimmed) {
        return false;
    }
    if (NON_ENGLISH_NAME_PATTERN.test(trimmed)) {
        return false;
    }
    if (!/^[A-Za-z0-9 ,./()+'%-]+$/.test(trimmed)) {
        return false;
    }
    if (!/[A-Za-z]/.test(trimmed)) {
        return false;
    }
    const normalized = normalizeTextForMatch(trimmed);
    for (const token of NON_ENGLISH_GLOSSARY_TOKENS) {
        if (normalized.includes(token)) {
            return false;
        }
    }
    return true;
}

function isHighConfidenceGlossaryFallbackName(name: string): boolean {
    const trimmed = normalizeMeasurementNameForGlossary(name);
    if (!isEnglishGlossaryName(trimmed)) {
        return false;
    }
    if (trimmed.length > 55) {
        return false;
    }
    if (GLOSSARY_FALLBACK_REJECT_PATTERN.test(trimmed)) {
        return false;
    }
    if (/\b(?:mg\/d?l|iu\/l|mmol\/l|ng\/m?l|pg\/m?l|µ?mol\/l|x10e\d+\/u?l)\b/i.test(trimmed)) {
        return false;
    }

    const normalized = normalizeTextForMatch(trimmed);
    if (!LIKELY_ANALYTE_NAME_PATTERN.test(normalized)) {
        return false;
    }
    if (/(?:\b\d{2,}\b)|(?:\b0\d\b)/.test(trimmed) && !/\b(?:b12|b6|d3|ft3|ft4|a1c|e2|omega-3)\b/i.test(trimmed)) {
        return false;
    }
    if (normalized.split(' ').length > 8) {
        return false;
    }
    return true;
}

function createEmptyBloodworkGlossary(): BloodworkGlossary {
    return {
        version: 1,
        updatedAt: nowIsoTimestamp(),
        entries: [],
    };
}

function normalizeGlossaryNameKey(name: string): string {
    return normalizeTextForMatch(name).replace(/[^a-z0-9]+/g, ' ').trim();
}

function sortUniqueStrings(values: string[]): string[] {
    return Array.from(new Set(values.map(item => item.trim()).filter(Boolean))).sort((left, right) => left.localeCompare(right));
}

function buildGlossaryRangeFingerprint(range: z.infer<typeof glossaryRangeSchema>): string {
    return [
        range.min?.toString() ?? '',
        range.max?.toString() ?? '',
        normalizeTextForMatch(range.unit ?? ''),
    ].join('|');
}

function normalizeGlossaryEntry(entry: BloodworkGlossaryEntry): BloodworkGlossaryEntry | null {
    const canonicalName = normalizeMeasurementNameForGlossary(entry.canonicalName.trim());
    if (!canonicalName || !isEnglishGlossaryName(canonicalName)) {
        return null;
    }
    const canonicalKey = normalizeGlossaryNameKey(canonicalName);
    const aliases = sortUniqueStrings(
        entry.aliases
            .map(alias => normalizeMeasurementNameForGlossary(alias))
            .filter(alias => isEnglishGlossaryName(alias))
            .filter(alias => normalizeGlossaryNameKey(alias) !== canonicalKey),
    );
    const unitHints = sortUniqueStrings(entry.unitHints);
    const rangesByKey = new Map<string, z.infer<typeof glossaryRangeSchema>>();

    for (const range of entry.knownRanges) {
        const normalizedRange: z.infer<typeof glossaryRangeSchema> = {
            min: range.min,
            max: range.max,
            unit: range.unit?.trim() || undefined,
        };
        const hasAnyValue =
            normalizedRange.min !== undefined ||
            normalizedRange.max !== undefined;
        if (!hasAnyValue) {
            continue;
        }
        rangesByKey.set(buildGlossaryRangeFingerprint(normalizedRange), normalizedRange);
    }

    return {
        canonicalName,
        aliases,
        knownRanges: Array.from(rangesByKey.values()),
        unitHints,
        createdAt: entry.createdAt || nowIsoTimestamp(),
        updatedAt: entry.updatedAt || nowIsoTimestamp(),
    };
}

function mergeGlossaryEntries(target: BloodworkGlossaryEntry, source: BloodworkGlossaryEntry): void {
    for (const alias of source.aliases) {
        upsertAliasIntoGlossaryEntry(target, alias);
    }

    for (const unitHint of source.unitHints) {
        const normalizedHint = unitHint.trim();
        if (!normalizedHint) {
            continue;
        }
        if (!target.unitHints.some(existing => normalizeTextForMatch(existing) === normalizeTextForMatch(normalizedHint))) {
            target.unitHints = sortUniqueStrings([...target.unitHints, normalizedHint]);
        }
    }

    const rangesByKey = new Map<string, z.infer<typeof glossaryRangeSchema>>();
    for (const range of target.knownRanges) {
        rangesByKey.set(buildGlossaryRangeFingerprint(range), range);
    }
    for (const range of source.knownRanges) {
        rangesByKey.set(buildGlossaryRangeFingerprint(range), range);
    }
    target.knownRanges = Array.from(rangesByKey.values());
    target.createdAt = target.createdAt < source.createdAt ? target.createdAt : source.createdAt;
    target.updatedAt = target.updatedAt > source.updatedAt ? target.updatedAt : source.updatedAt;
}

function normalizeGlossaryState(glossary: BloodworkGlossary): BloodworkGlossary {
    const now = nowIsoTimestamp();
    const mergedByCanonicalKey = new Map<string, BloodworkGlossaryEntry>();

    for (const entry of glossary.entries) {
        const normalized = normalizeGlossaryEntry(entry);
        if (!normalized) {
            continue;
        }
        const key = normalizeGlossaryNameKey(normalized.canonicalName);
        if (!key) {
            continue;
        }
        const existing = mergedByCanonicalKey.get(key);
        if (existing) {
            mergeGlossaryEntries(existing, normalized);
            continue;
        }
        mergedByCanonicalKey.set(key, normalized);
    }

    const normalizedEntries = Array.from(mergedByCanonicalKey.values())
        .sort((left, right) => left.canonicalName.localeCompare(right.canonicalName));

    return {
        version: 1,
        updatedAt: glossary.updatedAt || now,
        entries: normalizedEntries,
    };
}

function loadBloodworkGlossary(glossaryPath: string): BloodworkGlossary {
    if (!fs.existsSync(glossaryPath)) {
        return createEmptyBloodworkGlossary();
    }

    let raw: unknown;
    try {
        raw = JSON.parse(fs.readFileSync(glossaryPath, 'utf8'));
    } catch {
        return createEmptyBloodworkGlossary();
    }

    try {
        const parsed = bloodworkGlossarySchema.parse(raw);
        return normalizeGlossaryState(parsed);
    } catch {
        return createEmptyBloodworkGlossary();
    }
}

function saveBloodworkGlossary(glossaryPath: string, glossary: BloodworkGlossary): void {
    const normalizedGlossary = normalizeGlossaryState({
        ...glossary,
        updatedAt: nowIsoTimestamp(),
    });
    fs.mkdirSync(path.dirname(glossaryPath), { recursive: true });
    fs.writeFileSync(glossaryPath, `${JSON.stringify(normalizedGlossary, null, 4)}\n`, 'utf8');
}

function resolveGlossaryValidatorModelIds(primaryModelIds: string[]): string[] {
    const envModels = process.env.VITALS_GLOSSARY_VALIDATOR_MODEL_IDS
        ?.split(',')
        .map(modelId => modelId.trim())
        .filter(Boolean);
    if (envModels && envModels.length > 0) {
        return envModels;
    }

    if (DEFAULT_GLOSSARY_VALIDATOR_MODEL_IDS.length > 0) {
        return DEFAULT_GLOSSARY_VALIDATOR_MODEL_IDS;
    }

    return primaryModelIds;
}

function buildGlossaryLookup(glossary: BloodworkGlossary): Map<string, BloodworkGlossaryEntry> {
    const lookup = new Map<string, BloodworkGlossaryEntry>();
    for (const entry of glossary.entries) {
        const keys = [entry.canonicalName, ...entry.aliases];
        for (const key of keys) {
            lookup.set(normalizeGlossaryNameKey(key), entry);
        }
    }
    return lookup;
}

function upsertAliasIntoGlossaryEntry(entry: BloodworkGlossaryEntry, alias: string): void {
    const trimmedAlias = normalizeMeasurementNameForGlossary(alias);
    if (!trimmedAlias || !isEnglishGlossaryName(trimmedAlias)) {
        return;
    }
    if (normalizeGlossaryNameKey(trimmedAlias) === normalizeGlossaryNameKey(entry.canonicalName)) {
        return;
    }
    if (!entry.aliases.some(existing => normalizeGlossaryNameKey(existing) === normalizeGlossaryNameKey(trimmedAlias))) {
        entry.aliases = sortUniqueStrings([...entry.aliases, trimmedAlias]);
    }
}

function upsertUnitHintIntoGlossaryEntry(entry: BloodworkGlossaryEntry, unit: string | undefined): void {
    if (!unit) {
        return;
    }
    const trimmedUnit = unit.trim();
    if (!trimmedUnit) {
        return;
    }
    if (!entry.unitHints.some(existing => normalizeTextForMatch(existing) === normalizeTextForMatch(trimmedUnit))) {
        entry.unitHints = sortUniqueStrings([...entry.unitHints, trimmedUnit]);
    }
}

function upsertRangeIntoGlossaryEntry(entry: BloodworkGlossaryEntry, measurement: BloodworkMeasurement): void {
    if (!measurement.referenceRange) {
        return;
    }
    const nextRange: z.infer<typeof glossaryRangeSchema> = {
        min: measurement.referenceRange.min,
        max: measurement.referenceRange.max,
        unit: measurement.unit?.trim() || undefined,
    };
    const hasRangeValue =
        nextRange.min !== undefined ||
        nextRange.max !== undefined;
    if (!hasRangeValue) {
        return;
    }
    const fingerprint = buildGlossaryRangeFingerprint(nextRange);
    const existingFingerprints = new Set(entry.knownRanges.map(range => buildGlossaryRangeFingerprint(range)));
    if (!existingFingerprints.has(fingerprint)) {
        entry.knownRanges = [...entry.knownRanges, nextRange];
    }
}

function touchGlossaryEntry(entry: BloodworkGlossaryEntry): void {
    entry.updatedAt = nowIsoTimestamp();
}

function createGlossaryEntryFromMeasurement({
    canonicalName,
    measurement,
}: {
    canonicalName: string;
    measurement: BloodworkMeasurement;
}): BloodworkGlossaryEntry {
    const timestamp = nowIsoTimestamp();
    const entry: BloodworkGlossaryEntry = {
        canonicalName,
        aliases: [],
        knownRanges: [],
        unitHints: [],
        createdAt: timestamp,
        updatedAt: timestamp,
    };
    upsertAliasIntoGlossaryEntry(entry, canonicalName);
    if (measurement.name) {
        upsertAliasIntoGlossaryEntry(entry, measurement.name);
    }
    if (measurement.originalName) {
        upsertAliasIntoGlossaryEntry(entry, measurement.originalName);
    }
    upsertUnitHintIntoGlossaryEntry(entry, measurement.unit);
    upsertRangeIntoGlossaryEntry(entry, measurement);
    return entry;
}

function cleanMeasurementCandidate(measurement: BloodworkMeasurement): BloodworkMeasurement {
    const { notes: legacyNotes, ...measurementWithoutLegacyNotes } = measurement;
    const name = measurement.name.replace(/\s+/g, ' ').trim();
    const originalName = measurement.originalName?.replace(/\s+/g, ' ').trim() || undefined;
    let unit = measurement.unit?.replace(/\s+/g, ' ').trim() || undefined;
    const note = measurement.note?.replace(/\s+/g, ' ').trim() || legacyNotes?.replace(/\s+/g, ' ').trim() || undefined;
    const category = measurement.category?.replace(/\s+/g, ' ').trim() || undefined;
    let normalizedName = name;
    let flag = measurement.flag;

    const indicator = normalizedName.match(/\s([+-])$/);
    if (indicator) {
        normalizedName = normalizedName.slice(0, -2).trim();
        if (!flag) {
            flag = indicator[1] === '+' ? 'high' : 'low';
        }
    }
    normalizedName = normalizedName.replace(/[,:;]+$/g, '').replace(/\s+/g, ' ').trim();

    if (unit && /^(high|low|abnormal|normal|critical)$/i.test(unit)) {
        if (!flag) {
            const normalizedUnit = normalizeTextForMatch(unit);
            if (
                normalizedUnit === 'high' ||
                normalizedUnit === 'abnormal' ||
                normalizedUnit === 'critical'
            ) {
                flag = normalizedUnit === 'high' ? 'high' : 'abnormal';
            } else if (normalizedUnit === 'low') {
                flag = 'low';
            } else if (normalizedUnit === 'normal') {
                flag = 'normal';
            }
        }
        unit = undefined;
    }

    return {
        ...measurementWithoutLegacyNotes,
        name: normalizedName,
        originalName,
        unit,
        note,
        category,
        flag,
    };
}

function hasUsableMeasurementValue(measurement: BloodworkMeasurement): boolean {
    const value = measurement.value;
    if (value === undefined || value === null) {
        return false;
    }
    if (typeof value === 'number') {
        return Number.isFinite(value);
    }
    const normalized = normalizeTextForMatch(value);
    if (!normalized) {
        return false;
    }
    return !NON_MEASUREMENT_VALUE_EXACT.has(normalized);
}

function isLikelyMeasurementCandidate(measurement: BloodworkMeasurement): boolean {
    const normalizedName = normalizeTextForMatch(measurement.name);
    if (!normalizedName) {
        return false;
    }
    if (NON_MEASUREMENT_NAME_EXACT.has(normalizedName)) {
        return false;
    }
    if (normalizedName.endsWith(':')) {
        return false;
    }
    if (!/[a-z]/i.test(normalizedName)) {
        return false;
    }
    if (/^\d+(?:[.,]\d+)?$/.test(normalizedName)) {
        return false;
    }
    if (NON_MEASUREMENT_NAME_PATTERNS.some(pattern => pattern.test(normalizedName))) {
        return false;
    }
    if (/^(?:for|is|be|this|that|these|those|and|with|without|under|over)\b/i.test(normalizedName)) {
        return false;
    }
    if (normalizedName.split(' ').length > 10) {
        return false;
    }
    if (/[.;]/.test(measurement.name) && !measurement.unit) {
        return false;
    }
    if (
        /^[a-z]+,\s*[a-z]+(?:\s+[a-z])?$/.test(normalizedName) &&
        !measurement.unit &&
        !measurement.referenceRange
    ) {
        return false;
    }
    if (
        /^[a-z]+(?:,\s*)?[a-z]+$/.test(normalizedName) &&
        !LIKELY_ANALYTE_NAME_PATTERN.test(normalizedName)
    ) {
        return false;
    }
    if (
        normalizedName.includes(',') &&
        !measurement.unit &&
        !measurement.referenceRange &&
        typeof measurement.value === 'number' &&
        measurement.value >= 1 &&
        measurement.value <= 31
    ) {
        return false;
    }

    const unit = measurement.unit ? normalizeTextForMatch(measurement.unit) : '';
    if (unit && NON_MEASUREMENT_UNIT_EXACT.has(unit)) {
        return false;
    }
    if (unit && NON_MEASUREMENT_NAME_PATTERNS.some(pattern => pattern.test(unit))) {
        return false;
    }
    const hasReferenceRange = Boolean(measurement.referenceRange);
    const hasValidUnit = Boolean(measurement.unit && UNIT_TOKEN_PATTERN.test(measurement.unit));
    const hasQualitativeValue =
        typeof measurement.value === 'string' && QUALITATIVE_RESULT_PATTERN.test(measurement.value);
    const nameLooksAnalyte = LIKELY_ANALYTE_NAME_PATTERN.test(normalizedName);
    const hasStructuredEvidence = hasValidUnit || hasReferenceRange || hasQualitativeValue;
    if (!nameLooksAnalyte && !hasStructuredEvidence) {
        return false;
    }
    if (
        !nameLooksAnalyte &&
        typeof measurement.value === 'number' &&
        !hasValidUnit &&
        !hasReferenceRange
    ) {
        return false;
    }

    return hasUsableMeasurementValue(measurement);
}

function applyGermanNameFallback(measurement: BloodworkMeasurement): BloodworkMeasurement {
    let nextName = measurement.name;
    for (const replacement of GERMAN_NAME_REPLACEMENTS) {
        nextName = nextName.replace(replacement.pattern, replacement.replacement);
    }
    nextName = nextName
        .replace(/\bHemoglobi\b/gi, 'Hemoglobin')
        .replace(/\bCholester\b/gi, 'Cholesterol')
        .replace(/\bAlkaline\b/gi, 'Alkaline Phosphatase')
        .replace(/\bCarbon Dioxide,\b/gi, 'Carbon Dioxide');
    nextName = nextName.replace(/\s+/g, ' ').trim();
    if (nextName === measurement.name) {
        return measurement;
    }
    return {
        ...measurement,
        originalName: measurement.originalName || measurement.name,
        name: nextName,
    };
}

function filterLikelyMeasurements(measurements: BloodworkMeasurement[]): BloodworkMeasurement[] {
    return mergeUniqueMeasurements(
        measurements
            .map(cleanMeasurementCandidate)
            .filter(isLikelyMeasurementCandidate)
            .map(applyGermanNameFallback),
    );
}

function extractTableLikeLines(pageText: string): string[] {
    const rawLines = pageText
        .split('\n')
        .map(line => line.replace(/\t/g, ' ').replace(/\u00a0/g, ' '))
        .map(line => line.replace(/\s+$/g, ''));

    const selected = new Set<string>();

    for (let index = 0; index < rawLines.length; index++) {
        const rawLine = rawLines[index]?.trim();
        if (!rawLine || rawLine.length > 220) {
            continue;
        }

        const normalizedWholeLine = normalizeTextForMatch(rawLine);
        if (!normalizedWholeLine) {
            continue;
        }
        if (NON_MEASUREMENT_NAME_EXACT.has(normalizedWholeLine)) {
            continue;
        }
        if (NON_MEASUREMENT_NAME_PATTERNS.some(pattern => pattern.test(normalizedWholeLine))) {
            continue;
        }

        const parts = rawLine
            .split(/\s{2,}/)
            .map(part => part.trim())
            .filter(Boolean);
        if (parts.length < 2) {
            continue;
        }

        const namePart = parts[0]!;
        if (namePart.length < 2 || namePart.length > 100) {
            continue;
        }
        if (NON_MEASUREMENT_NAME_PATTERNS.some(pattern => pattern.test(namePart))) {
            continue;
        }

        const tail = parts.slice(1).join(' ');
        const hasValue = VALUE_TOKEN_PATTERN.test(tail);
        if (!hasValue) {
            continue;
        }

        const hasUnitOrRangeOrQualitative =
            UNIT_TOKEN_PATTERN.test(tail) ||
            RANGE_TOKEN_PATTERN.test(tail) ||
            QUALITATIVE_RESULT_PATTERN.test(tail);

        if (!hasUnitOrRangeOrQualitative && !LIKELY_ANALYTE_NAME_PATTERN.test(namePart)) {
            continue;
        }

        let mergedName = namePart;
        if (index > 0) {
            const previousRawLine = rawLines[index - 1]?.trim() || '';
            if (
                previousRawLine &&
                previousRawLine.length < 60 &&
                previousRawLine.split(/\s{2,}/).length === 1 &&
                !/\d/.test(previousRawLine) &&
                !NON_MEASUREMENT_NAME_PATTERNS.some(pattern => pattern.test(previousRawLine))
            ) {
                mergedName = `${previousRawLine} ${namePart}`.replace(/\s+/g, ' ');
            }
        }

        selected.add([mergedName, ...parts.slice(1)].join(' | '));
    }

    return Array.from(selected);
}

function parseNumericValueToken(raw: string): number | string {
    const trimmed = raw.trim();
    if (!trimmed) {
        return trimmed;
    }
    if (/^[<>]/.test(trimmed)) {
        return trimmed;
    }
    const parsed = Number.parseFloat(trimmed.replace(',', '.'));
    if (Number.isFinite(parsed)) {
        return parsed;
    }
    return trimmed;
}

function parseReferenceRangeFromText(text: string): BloodworkMeasurement['referenceRange'] | undefined {
    const trimmed = text.trim();
    if (!trimmed) {
        return undefined;
    }
    const pair = trimmed.match(/([<>]?\d+(?:[.,]\d+)?)\s*-\s*([<>]?\d+(?:[.,]\d+)?)/);
    if (pair) {
        const min = Number.parseFloat(pair[1]!.replace(/[<>]/g, '').replace(',', '.'));
        const max = Number.parseFloat(pair[2]!.replace(/[<>]/g, '').replace(',', '.'));
        return {
            min: Number.isFinite(min) ? min : undefined,
            max: Number.isFinite(max) ? max : undefined,
        };
    }

    const comparator = trimmed.match(/([<>]=?)\s*(-?\d+(?:[.,]\d+)?)/);
    if (!comparator) {
        return undefined;
    }

    const bound = Number.parseFloat(comparator[2]!.replace(',', '.'));
    if (!Number.isFinite(bound)) {
        return undefined;
    }

    if (comparator[1]!.includes('<')) {
        return { max: bound };
    }

    return { min: bound };
}

function parseMeasurementFromTableLine(line: string): BloodworkMeasurement | null {
    const parts = line
        .split('|')
        .map(part => part.trim())
        .filter(Boolean);
    if (parts.length < 2) {
        return null;
    }

    const name = parts[0]!;
    const otherParts = parts.slice(1);
    let value: BloodworkMeasurement['value'];
    let unit: string | undefined;
    let referenceRange: BloodworkMeasurement['referenceRange'] | undefined;
    let flag: BloodworkMeasurement['flag'] | undefined;

    for (const part of otherParts) {
        if (!part) {
            continue;
        }

        if (!flag && /^(high|low|abnormal|normal|critical)$/i.test(part)) {
            const normalizedPart = normalizeTextForMatch(part);
            if (normalizedPart === 'high') flag = 'high';
            if (normalizedPart === 'low') flag = 'low';
            if (normalizedPart === 'abnormal') flag = 'abnormal';
            if (normalizedPart === 'normal') flag = 'normal';
            if (normalizedPart === 'critical') flag = 'critical';
            continue;
        }

        if (!referenceRange) {
            const parsedRange = parseReferenceRangeFromText(part);
            if (parsedRange) {
                referenceRange = parsedRange;
                continue;
            }
        }

        if (!unit && UNIT_TOKEN_PATTERN.test(part)) {
            unit = part;
            continue;
        }

        if (value === undefined) {
            if (QUALITATIVE_RESULT_PATTERN.test(part)) {
                value = part;
                continue;
            }
            const exactValue = part.match(/^[<>]?\d+(?:[.,]\d+)?$/);
            if (exactValue) {
                value = parseNumericValueToken(exactValue[0]);
                continue;
            }
            const inlineValue = part.match(/[<>]?\d+(?:[.,]\d+)?/);
            if (inlineValue && part.length <= 18) {
                value = parseNumericValueToken(inlineValue[0]);
                continue;
            }
        }
    }

    if (value === undefined) {
        return null;
    }

    return {
        name,
        originalName: name,
        value,
        unit,
        referenceRange,
        flag,
    };
}

function parseMeasurementsFromTableLikeLines(lines: string[]): BloodworkMeasurement[] {
    const parsed: BloodworkMeasurement[] = [];
    for (const line of lines) {
        const measurement = parseMeasurementFromTableLine(line);
        if (measurement) {
            parsed.push(measurement);
        }
    }
    return parsed;
}

function extractCardStyleMeasurements(pageTexts: string[]): BloodworkMeasurement[] {
    const measurements: BloodworkMeasurement[] = [];
    const blockedHeadingPattern =
        /\b(?:final|next steps|key:|ordering physician|performing lab|report status|panel|stress hormone test)\b/i;
    const blockedValueLinePattern =
        /\b(?:no historical data|for additional information|patients being treated|quest diagnostics|contact the|monday - friday)\b/i;

    for (const pageText of pageTexts) {
        const lines = pageText
            .split('\n')
            .map(line => line.replace(/\s+/g, ' ').trim())
            .filter(Boolean);

        for (let lineIndex = 0; lineIndex < lines.length; lineIndex++) {
            const heading = lines[lineIndex]!;
            if (!/^[A-Z0-9 ,().\-/'"]{3,}$/.test(heading)) {
                continue;
            }
            if (blockedHeadingPattern.test(heading)) {
                continue;
            }
            if (NON_MEASUREMENT_NAME_PATTERNS.some(pattern => pattern.test(heading))) {
                continue;
            }
            if (!LIKELY_ANALYTE_NAME_PATTERN.test(heading)) {
                continue;
            }

            let unit: string | undefined;
            let referenceRange: BloodworkMeasurement['referenceRange'] | undefined;
            let value: BloodworkMeasurement['value'];
            let metadataLineIndex = lineIndex;

            for (let lookahead = lineIndex + 1; lookahead <= lineIndex + 4 && lookahead < lines.length; lookahead++) {
                const nextLine = lines[lookahead]!;
                const desiredRange = nextLine.match(/^Desired Range:\s*(.+)$/i);
                if (desiredRange) {
                    referenceRange = parseReferenceRangeFromText(desiredRange[1]!);
                    const unitTokens = desiredRange[1]!.match(/[A-Za-zµμ%/][A-Za-z0-9µμ%/.()-]*/g);
                    if (!unit && unitTokens && unitTokens.length > 0) {
                        unit = unitTokens.at(-1);
                    }
                    metadataLineIndex = lookahead;
                    continue;
                }
                const unitMeasure = nextLine.match(/^Unit of Measure:\s*(.+)$/i);
                if (unitMeasure) {
                    unit = unitMeasure[1]!.trim();
                    metadataLineIndex = lookahead;
                }
            }

            for (
                let lookahead = metadataLineIndex + 1;
                lookahead <= metadataLineIndex + 12 && lookahead < lines.length;
                lookahead++
            ) {
                const nextLine = lines[lookahead]!;
                if (/^Desired Range:/i.test(nextLine) || /^Unit of Measure:/i.test(nextLine)) {
                    continue;
                }
                if (
                    lookahead > metadataLineIndex + 1 &&
                    /^[A-Z0-9 ,().\-/'"]{3,}$/.test(nextLine) &&
                    LIKELY_ANALYTE_NAME_PATTERN.test(nextLine)
                ) {
                    break;
                }

                const resultLine = nextLine.match(/^Result:\s*(.+)$/i);
                if (resultLine && resultLine[1]!.trim()) {
                    const resultValue = resultLine[1]!.trim();
                    value = parseNumericValueToken(resultValue);
                    break;
                }

                const directValue = nextLine.match(/^[<>]?\d+(?:[.,]\d+)?$/);
                if (directValue) {
                    value = parseNumericValueToken(directValue[0]);
                    break;
                }

                const valueWithBar = nextLine.match(/^([<>]?\d+(?:[.,]\d+)?)\s*\|/);
                if (valueWithBar) {
                    value = parseNumericValueToken(valueWithBar[1]!);
                    break;
                }

                if (blockedValueLinePattern.test(nextLine)) {
                    continue;
                }

                if (!referenceRange) {
                    const parsedRange = parseReferenceRangeFromText(nextLine);
                    if (parsedRange) {
                        referenceRange = parsedRange;
                        continue;
                    }
                }

                const numericMatch = nextLine.match(/[<>]?\d+(?:[.,]\d+)?/);
                if (numericMatch) {
                    value = parseNumericValueToken(numericMatch[0]);
                    break;
                }
            }

            if (value === undefined) {
                continue;
            }

            const normalizedHeading =
                heading === heading.toUpperCase()
                    ? titleCase(heading.toLowerCase())
                    : heading;

            measurements.push({
                name: normalizedHeading,
                originalName: heading,
                value,
                unit,
                referenceRange,
            });
        }
    }

    return mergeUniqueMeasurements(measurements);
}

function buildMeasurementExtractionPrompt({
    sourcePath,
    pageText,
    pageNumber,
}: {
    sourcePath: string;
    pageText: string;
    pageNumber: number;
}): string {
    return [
        `Source file: ${sourcePath}`,
        `Page number: ${pageNumber}`,
        '',
        'Extract bloodwork analyte rows from this report page.',
        'Only include rows that are true lab analytes or derived biomarker results with a reported value.',
        'Do not include demographics, addresses, contact details, page headers/footers, IDs, notes, interpretations, guideline paragraphs, or section labels.',
        'Use concise standardized English names in `name`.',
        'If source text is non-English, preserve the raw source label in `originalName` and translate `name` to English.',
        'Use `referenceRange` only as `{ "min"?: number, "max"?: number }`.',
        'For comparator ranges, map `<N` to `{ "max": N }` and `>N` to `{ "min": N }`.',
        'If there is a measurement-specific comment, put it in optional `note`.',
        'Keep values, units, and numeric range bounds accurate to source.',
        'If no analytes are present on this page, return an empty measurements array.',
        '',
        'Page text (layout-preserved):',
        pageText.slice(0, EXTRACTED_TEXT_LIMIT),
    ].join('\n');
}

function buildMeasurementNormalizationPrompt({
    sourcePath,
    extractedText,
    candidates,
}: {
    sourcePath: string;
    extractedText: string;
    candidates: BloodworkMeasurement[];
}): string {
    return [
        `Source file: ${sourcePath}`,
        '',
        'Clean and normalize these candidate bloodwork measurements.',
        'Keep only real blood analytes and derived biomarkers.',
        'Remove any candidate that is administrative text, page metadata, patient identity, addresses, phone/email, comments, interpretations, guideline citations, or narrative explanations.',
        'Drop fragmentary/incomplete labels (for example words cut from sentence fragments) unless you can confidently normalize them into a standard analyte.',
        'The final `name` must be in English. Keep `originalName` as source-language label when translation occurs.',
        'Use `referenceRange` only as `{ "min"?: number, "max"?: number }`; never emit `lower`, `upper`, or `text`.',
        'For comparator ranges, map `<N` to `{ "max": N }` and `>N` to `{ "min": N }`.',
        'Preserve numeric/string values, units, range bounds, flags, and optional `note` exactly unless clearly malformed.',
        'Return deduplicated measurements only.',
        '',
        'Extracted report text (for context):',
        extractedText.slice(0, EXTRACTED_TEXT_LIMIT),
        '',
        'Candidate measurements JSON:',
        JSON.stringify(candidates.slice(0, MAX_NORMALIZATION_CANDIDATES), null, 2),
    ].join('\n');
}

function buildGlossaryValidationPrompt({
    sourcePath,
    existingGlossaryEntries,
    unknownMeasurements,
}: {
    sourcePath: string;
    existingGlossaryEntries: Array<{
        canonicalName: string;
        aliases: string[];
    }>;
    unknownMeasurements: Array<{ index: number; measurement: BloodworkMeasurement }>;
}): string {
    const candidates = unknownMeasurements.map(item => ({
        index: item.index,
        name: item.measurement.name,
        originalName: item.measurement.originalName,
        value: item.measurement.value,
        unit: item.measurement.unit,
        referenceRange: item.measurement.referenceRange,
        note: item.measurement.note ?? item.measurement.notes,
    }));

    return [
        `Source file: ${sourcePath}`,
        '',
        'You are the second-pass bloodwork glossary validator.',
        'Classify each candidate as one of: alias, new_valid, invalid.',
        'alias: candidate refers to an existing canonical analyte in the provided glossary names.',
        'new_valid: candidate is a real analyte but not yet in glossary.',
        'invalid: candidate is parsing noise, narrative text, metadata, or uncertain.',
        'Strict rule: canonicalName and aliases must be English-only terms using ASCII letters/digits/punctuation.',
        'If the candidate is non-English or uncertain, choose invalid.',
        'For alias, set targetCanonicalName exactly to one of the existing canonical names.',
        'For new_valid, set canonicalName in English and include optional English aliases.',
        'Never return non-English canonical names or aliases.',
        '',
        'Existing glossary entries (canonical names with aliases):',
        JSON.stringify(existingGlossaryEntries, null, 2),
        '',
        'Candidates:',
        JSON.stringify(candidates, null, 2),
    ].join('\n');
}

async function validateUnknownMeasurementsWithGlossaryModel({
    provider,
    modelIds,
    sourcePath,
    glossary,
    unknownMeasurements,
}: {
    provider: ReturnType<typeof createOpenRouter>;
    modelIds: string[];
    sourcePath: string;
    glossary: BloodworkGlossary;
    unknownMeasurements: Array<{ index: number; measurement: BloodworkMeasurement }>;
}): Promise<Map<number, z.infer<typeof glossaryValidationDecisionSchema>>> {
    if (unknownMeasurements.length === 0) {
        return new Map();
    }

    const existingGlossaryEntries = glossary.entries
        .map(entry => ({
            canonicalName: entry.canonicalName,
            aliases: entry.aliases,
        }))
        .sort((left, right) => left.canonicalName.localeCompare(right.canonicalName));
    const prompt = buildGlossaryValidationPrompt({
        sourcePath,
        existingGlossaryEntries,
        unknownMeasurements,
    });

    let result: z.infer<typeof glossaryValidationBatchSchema>;
    try {
        const response = await generateObjectWithModelFallback({
            provider,
            modelIds,
            schema: glossaryValidationBatchSchema,
            prompt,
            maxOutputTokens: GLOSSARY_VALIDATION_MAX_OUTPUT_TOKENS,
            contextLabel: `${sourcePath} (glossary validation)`,
            textFallbackArrayKey: 'decisions',
        });
        result = response.object;
    } catch (error) {
        if (process.env.VITALS_IMPORT_DEBUG === 'true') {
            console.error(`[debug] glossary validation failed for ${sourcePath}:`, error);
        }
        return new Map();
    }

    const decisions = new Map<number, z.infer<typeof glossaryValidationDecisionSchema>>();
    for (const decision of result.decisions) {
        if (!unknownMeasurements.some(item => item.index === decision.index)) {
            continue;
        }
        decisions.set(decision.index, decision);
    }
    if (decisions.size === 0 && process.env.VITALS_IMPORT_DEBUG === 'true') {
        console.error(`[debug] glossary validation returned no decisions for ${sourcePath}`);
    }
    return decisions;
}

function findGlossaryEntryByName({
    glossary,
    lookup,
    name,
}: {
    glossary: BloodworkGlossary;
    lookup: Map<string, BloodworkGlossaryEntry>;
    name: string;
}): BloodworkGlossaryEntry | null {
    const key = normalizeGlossaryNameKey(name);
    if (!key) {
        return null;
    }
    const existing = lookup.get(key);
    if (existing) {
        return existing;
    }

    const found = glossary.entries.find(entry => normalizeGlossaryNameKey(entry.canonicalName) === key);
    if (!found) {
        return null;
    }
    lookup.set(key, found);
    return found;
}

function appendGlossaryEntry({
    glossary,
    lookup,
    entry,
}: {
    glossary: BloodworkGlossary;
    lookup: Map<string, BloodworkGlossaryEntry>;
    entry: BloodworkGlossaryEntry;
}): void {
    glossary.entries.push(entry);
    const keys = [entry.canonicalName, ...entry.aliases];
    for (const key of keys) {
        lookup.set(normalizeGlossaryNameKey(key), entry);
    }
}

function createFallbackGlossaryDecision(
    measurement: BloodworkMeasurement,
): z.infer<typeof glossaryValidationDecisionSchema> | null {
    const canonicalName = normalizeMeasurementNameForGlossary(measurement.name);
    if (!canonicalName || !isHighConfidenceGlossaryFallbackName(canonicalName)) {
        return null;
    }
    return {
        index: 0,
        action: 'new_valid',
        canonicalName,
        aliases: measurement.originalName && isEnglishGlossaryName(measurement.originalName)
            ? [measurement.originalName.trim()]
            : [],
        reason: 'fallback-new-valid',
    };
}

function applyGlossaryDecision({
    glossary,
    lookup,
    measurement,
    decision,
    acceptedMeasurements,
}: {
    glossary: BloodworkGlossary;
    lookup: Map<string, BloodworkGlossaryEntry>;
    measurement: BloodworkMeasurement;
    decision: z.infer<typeof glossaryValidationDecisionSchema>;
    acceptedMeasurements: BloodworkMeasurement[];
}): void {
    const normalizedAction = normalizeTextForMatch(decision.action).replace(/[^a-z0-9]+/g, '');
    const action =
        normalizedAction === 'alias' || normalizedAction === 'existingalias'
            ? 'alias'
            : normalizedAction === 'newvalid' ||
                normalizedAction === 'new' ||
                normalizedAction === 'newentry' ||
                normalizedAction === 'validnew'
                ? 'new_valid'
                : 'invalid';

    if (action === 'invalid') {
        return;
    }

    if (action === 'alias') {
        const targetCanonicalName = decision.targetCanonicalName?.trim();
        if (!targetCanonicalName || !isEnglishGlossaryName(targetCanonicalName)) {
            return;
        }
        const existingEntry = findGlossaryEntryByName({
            glossary,
            lookup,
            name: targetCanonicalName,
        });
        if (!existingEntry) {
            return;
        }

        const aliasedMeasurement: BloodworkMeasurement = {
            ...measurement,
            name: existingEntry.canonicalName,
        };
        upsertAliasIntoGlossaryEntry(existingEntry, aliasedMeasurement.name);
        if (aliasedMeasurement.originalName) {
            upsertAliasIntoGlossaryEntry(existingEntry, aliasedMeasurement.originalName);
        }
        if (decision.aliases) {
            for (const alias of decision.aliases) {
                upsertAliasIntoGlossaryEntry(existingEntry, alias);
            }
        }
        upsertUnitHintIntoGlossaryEntry(existingEntry, aliasedMeasurement.unit);
        upsertRangeIntoGlossaryEntry(existingEntry, aliasedMeasurement);
        touchGlossaryEntry(existingEntry);
        acceptedMeasurements.push(aliasedMeasurement);
        return;
    }

    const canonicalName = decision.canonicalName?.trim();
    if (!canonicalName || !isEnglishGlossaryName(canonicalName)) {
        return;
    }

    const existingEntry = findGlossaryEntryByName({
        glossary,
        lookup,
        name: canonicalName,
    });
    const measurementWithCanonicalName: BloodworkMeasurement = {
        ...measurement,
        name: canonicalName,
    };

    if (existingEntry) {
        upsertAliasIntoGlossaryEntry(existingEntry, canonicalName);
        if (measurementWithCanonicalName.originalName) {
            upsertAliasIntoGlossaryEntry(existingEntry, measurementWithCanonicalName.originalName);
        }
        if (decision.aliases) {
            for (const alias of decision.aliases) {
                upsertAliasIntoGlossaryEntry(existingEntry, alias);
            }
        }
        upsertUnitHintIntoGlossaryEntry(existingEntry, measurementWithCanonicalName.unit);
        upsertRangeIntoGlossaryEntry(existingEntry, measurementWithCanonicalName);
        touchGlossaryEntry(existingEntry);
    } else {
        const entry = createGlossaryEntryFromMeasurement({
            canonicalName,
            measurement: measurementWithCanonicalName,
        });
        if (decision.aliases) {
            for (const alias of decision.aliases) {
                upsertAliasIntoGlossaryEntry(entry, alias);
            }
        }
        appendGlossaryEntry({
            glossary,
            lookup,
            entry,
        });
    }
    acceptedMeasurements.push(measurementWithCanonicalName);
}

async function applyGlossaryValidationToMeasurements({
    provider,
    glossary,
    sourcePath,
    primaryModelIds,
    measurements,
}: {
    provider: ReturnType<typeof createOpenRouter>;
    glossary: BloodworkGlossary;
    sourcePath: string;
    primaryModelIds: string[];
    measurements: BloodworkMeasurement[];
}): Promise<BloodworkMeasurement[]> {
    if (measurements.length === 0) {
        return measurements;
    }

    const lookup = buildGlossaryLookup(glossary);
    const accepted: BloodworkMeasurement[] = [];
    const unknown: Array<{ index: number; measurement: BloodworkMeasurement }> = [];

    for (const measurement of measurements) {
        const normalizedMeasurement = normalizeMeasurementForGlossary(measurement);
        if (!normalizedMeasurement) {
            continue;
        }

        const knownEntryFromName = findGlossaryEntryByName({
            glossary,
            lookup,
            name: normalizedMeasurement.name,
        });
        const knownEntryFromOriginal =
            normalizedMeasurement.originalName && isEnglishGlossaryName(normalizedMeasurement.originalName)
                ? findGlossaryEntryByName({
                    glossary,
                    lookup,
                    name: normalizedMeasurement.originalName,
                })
                : null;
        const knownEntry = knownEntryFromName ?? knownEntryFromOriginal;

        if (knownEntry) {
            const resolvedMeasurement = {
                ...normalizedMeasurement,
                name: knownEntry.canonicalName,
            };
            upsertAliasIntoGlossaryEntry(knownEntry, resolvedMeasurement.name);
            if (resolvedMeasurement.originalName) {
                upsertAliasIntoGlossaryEntry(knownEntry, resolvedMeasurement.originalName);
            }
            upsertUnitHintIntoGlossaryEntry(knownEntry, resolvedMeasurement.unit);
            upsertRangeIntoGlossaryEntry(knownEntry, resolvedMeasurement);
            touchGlossaryEntry(knownEntry);
            accepted.push(resolvedMeasurement);
            continue;
        }

        unknown.push({
            index: unknown.length,
            measurement: normalizedMeasurement,
        });
    }

    if (unknown.length > 0) {
        const validatorModelIds = resolveGlossaryValidatorModelIds(primaryModelIds);
        for (let offset = 0; offset < unknown.length; offset += MAX_GLOSSARY_DECISIONS) {
            const unknownChunk = unknown.slice(offset, offset + MAX_GLOSSARY_DECISIONS);
            const decisions = await validateUnknownMeasurementsWithGlossaryModel({
                provider,
                modelIds: validatorModelIds,
                sourcePath,
                glossary,
                unknownMeasurements: unknownChunk,
            });

            for (const item of unknownChunk) {
                let decision = decisions.get(item.index);
                if (!decision) {
                    const fallback = createFallbackGlossaryDecision(item.measurement);
                    if (fallback) {
                        decision = {
                            ...fallback,
                            index: item.index,
                        };
                    }
                }
                if (!decision) {
                    continue;
                }

                applyGlossaryDecision({
                    glossary,
                    lookup,
                    measurement: item.measurement,
                    decision,
                    acceptedMeasurements: accepted,
                });
            }
        }
    }

    glossary.updatedAt = nowIsoTimestamp();
    return mergeUniqueMeasurements(accepted);
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
            measurement.referenceRange.min?.toString() ?? '',
            measurement.referenceRange.max?.toString() ?? '',
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

function buildMeasurementNameKey(name: string): string {
    return normalizeTextForMatch(name).replace(/[^a-z0-9]+/g, ' ').trim();
}

function mergeMeasurementsByPreferredName(
    preferred: BloodworkMeasurement[],
    fallback: BloodworkMeasurement[],
): BloodworkMeasurement[] {
    const merged = new Map<string, BloodworkMeasurement>();
    for (const measurement of fallback) {
        merged.set(buildMeasurementNameKey(measurement.name), measurement);
    }
    for (const measurement of preferred) {
        merged.set(buildMeasurementNameKey(measurement.name), measurement);
    }
    return mergeUniqueMeasurements(Array.from(merged.values()));
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
            const min = match[4] ? Number.parseFloat(match[4].replace(',', '.')) : undefined;
            const max = match[5] ? Number.parseFloat(match[5].replace(',', '.')) : undefined;

            const measurement: BloodworkMeasurement = {
                name: standardizedName,
                originalName: rawName,
                value: Number.isFinite(value) ? value : match[2],
                unit: unit || undefined,
                referenceRange:
                    min !== undefined || max !== undefined
                        ? {
                            min: Number.isFinite(min) ? min : undefined,
                            max: Number.isFinite(max) ? max : undefined,
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

function extractNarrativeResultMeasurements(pageTexts: string[]): BloodworkMeasurement[] {
    const measurements: BloodworkMeasurement[] = [];
    const combined = pageTexts.join('\n');
    const qualitativeValueMatch = combined.match(
        /\b(Abnormal|Positive|Negative|Reactive|Nonreactive|Detected|Not detected)\b/i,
    );

    if (/upper respiratory culture/i.test(combined) && qualitativeValueMatch) {
        const rawValue = qualitativeValueMatch[1]!;
        measurements.push({
            name: 'Upper Respiratory Culture',
            originalName: 'Upper Respiratory Culture',
            value: rawValue[0]!.toUpperCase() + rawValue.slice(1).toLowerCase(),
        });
    }

    const qualitativeLinePattern =
        /^([A-Za-z][A-Za-z0-9 ,()/.-]{2,90})\s+(Abnormal|Positive|Negative|Reactive|Nonreactive|Detected|Not detected)\b/i;
    for (const pageText of pageTexts) {
        const lines = pageText.split('\n').map(line => line.replace(/\s+/g, ' ').trim()).filter(Boolean);
        for (const line of lines) {
            const match = line.match(qualitativeLinePattern);
            if (!match) {
                continue;
            }

            const rawName = match[1]!.trim();
            const value = match[2]!.trim();
            if (
                NON_MEASUREMENT_NAME_PATTERNS.some(pattern => pattern.test(rawName)) ||
                normalizeTextForMatch(rawName).startsWith('result ')
            ) {
                continue;
            }

            measurements.push({
                name: rawName,
                originalName: rawName,
                value,
            });
        }
    }

    return mergeUniqueMeasurements(measurements);
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

async function withModelRequestTimeout<T>(
    execute: (abortSignal: AbortSignal) => Promise<T>,
): Promise<T> {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;

    const timeoutPromise = new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
            controller.abort();
            reject(new Error(`Model request timed out after ${MODEL_REQUEST_TIMEOUT_MS}ms`));
        }, MODEL_REQUEST_TIMEOUT_MS);
    });

    try {
        return await Promise.race([
            execute(controller.signal),
            timeoutPromise,
        ]);
    } finally {
        if (timer) {
            clearTimeout(timer);
        }
    }
}

async function generateObjectWithModelFallback<Schema extends z.ZodTypeAny>({
    provider,
    modelIds,
    schema,
    prompt,
    maxOutputTokens,
    contextLabel,
    textFallbackArrayKey,
}: {
    provider: ReturnType<typeof createOpenRouter>;
    modelIds: string[];
    schema: Schema;
    prompt: string;
    maxOutputTokens: number;
    contextLabel: string;
    textFallbackArrayKey?: string;
}): Promise<{ object: z.infer<Schema>; modelId: string }> {
    let lastError: unknown = null;
    const debug = process.env.VITALS_IMPORT_DEBUG === 'true';

    for (const modelId of modelIds) {
        try {
            if (debug) {
                console.info(`[debug] start object call: ${contextLabel} (${modelId})`);
            }
            const result = await withModelRequestTimeout(abortSignal => generateObject({
                model: provider(modelId, {
                    plugins: [{ id: 'response-healing' }],
                }),
                schema,
                prompt,
                temperature: 0,
                maxRetries: 2,
                maxOutputTokens,
                system: 'You are a precise medical lab data extraction engine.',
                abortSignal,
            }));
            if (debug) {
                console.info(`[debug] success object call: ${contextLabel} (${modelId})`);
            }

            return {
                object: result.object as z.infer<Schema>,
                modelId,
            };
        } catch (error) {
            if (debug) {
                console.error(`[debug] failed object call: ${contextLabel} (${modelId})`, error);
            }
            if (String(error).includes('timed out')) {
                lastError = error;
                continue;
            }
            try {
                if (debug) {
                    console.info(`[debug] start text fallback: ${contextLabel} (${modelId})`);
                }
                const textResult = await withModelRequestTimeout(abortSignal => generateText({
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
                    abortSignal,
                }));
                if (debug) {
                    console.info(`[debug] success text fallback: ${contextLabel} (${modelId})`);
                }
                const parsedJson = parseJsonFromText(textResult.text);
                const normalizedParsedJson =
                    Array.isArray(parsedJson) && textFallbackArrayKey
                        ? {
                            [textFallbackArrayKey]: parsedJson,
                        }
                        : parsedJson;
                const parsed = schema.parse(normalizedParsedJson) as z.infer<Schema>;
                return {
                    object: parsed,
                    modelId,
                };
            } catch (textFallbackError) {
                if (debug) {
                    console.error(`[debug] failed text fallback: ${contextLabel} (${modelId})`, textFallbackError);
                }
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
    const extractedMeasurements: BloodworkMeasurement[] = [];

    for (let pageIndex = 0; pageIndex < pageTexts.length; pageIndex++) {
        const pageText = pageTexts[pageIndex];
        if (!/\d/.test(pageText)) {
            continue;
        }

        const tableLikeLines = extractTableLikeLines(pageText);
        if (tableLikeLines.length === 0) {
            continue;
        }

        extractedMeasurements.push(...parseMeasurementsFromTableLikeLines(tableLikeLines));

        const prompt = buildMeasurementExtractionPrompt({
            sourcePath,
            pageText: tableLikeLines.join('\n'),
            pageNumber: pageIndex + 1,
        });

        try {
            const result = await generateObjectWithModelFallback({
                provider,
                modelIds,
                schema: measurementBatchSchema,
                prompt,
                maxOutputTokens: MODEL_MAX_OUTPUT_TOKENS,
                contextLabel: `${sourcePath} (measurements page ${pageIndex + 1})`,
                textFallbackArrayKey: 'measurements',
            });
            extractedMeasurements.push(...result.object.measurements);
        } catch {
            continue;
        }
    }

    return mergeUniqueMeasurements(extractedMeasurements);
}

async function normalizeMeasurementsWithModel({
    provider,
    modelIds,
    sourcePath,
    extractedText,
    candidates,
}: {
    provider: ReturnType<typeof createOpenRouter>;
    modelIds: string[];
    sourcePath: string;
    extractedText: string;
    candidates: BloodworkMeasurement[];
}): Promise<BloodworkMeasurement[]> {
    const filteredCandidates = filterLikelyMeasurements(candidates).slice(0, MAX_NORMALIZATION_CANDIDATES);
    if (filteredCandidates.length === 0) {
        return [];
    }

    const prompt = buildMeasurementNormalizationPrompt({
        sourcePath,
        extractedText,
        candidates: filteredCandidates,
    });

    try {
        const result = await generateObjectWithModelFallback({
            provider,
            modelIds,
            schema: measurementNormalizationSchema,
            prompt,
            maxOutputTokens: NORMALIZATION_MAX_OUTPUT_TOKENS,
            contextLabel: `${sourcePath} (normalization)`,
            textFallbackArrayKey: 'measurements',
        });
        return filterLikelyMeasurements(result.object.measurements);
    } catch {
        return filteredCandidates;
    }
}

async function generateLabObject({
    openRouterApiKey,
    modelIds,
    pdfPath,
    extractedText,
    pageTexts,
    glossary,
}: {
    openRouterApiKey: string;
    modelIds: string[];
    pdfPath: string;
    extractedText: string;
    pageTexts: string[];
    glossary: BloodworkGlossary;
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

    const cardStyleMeasurements = filterLikelyMeasurements(extractCardStyleMeasurements(pageTexts));
    let measurements: BloodworkMeasurement[] = [];
    try {
        const extractedMeasurements = await extractMeasurementsFromPages({
            provider,
            modelIds,
            sourcePath: pdfPath,
            pageTexts,
        });
        measurements = await normalizeMeasurementsWithModel({
            provider,
            modelIds,
            sourcePath: pdfPath,
            extractedText,
            candidates: extractedMeasurements,
        });
    } catch {
        measurements = [];
    }

    if (cardStyleMeasurements.length >= 3) {
        measurements = mergeMeasurementsByPreferredName(cardStyleMeasurements, measurements);
    }

    if (measurements.length === 0) {
        const heuristicFallback = heuristicExtractMeasurements(pageTexts);
        const narrativeFallback = extractNarrativeResultMeasurements(pageTexts);
        measurements = filterLikelyMeasurements([
            ...heuristicFallback,
            ...cardStyleMeasurements,
            ...narrativeFallback,
        ]);
    }

    measurements = await applyGlossaryValidationToMeasurements({
        provider,
        glossary,
        sourcePath: pdfPath,
        primaryModelIds: modelIds,
        measurements,
    });

    if (measurements.length === 0) {
        measurements = [{
            name: 'Unparsed Result',
            note: 'Automatic extraction returned no structured measurements for this report.',
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
    glossary,
    glossaryPath,
}: {
    pdfPath: string;
    openRouterApiKey: string;
    modelIds: string[];
    s3Client: S3Client | null;
    s3Bucket: string;
    s3Prefix: string;
    glossary: BloodworkGlossary;
    glossaryPath: string;
}): Promise<ImportResult> {
    const pdfBytes = new Uint8Array(await Bun.file(pdfPath).arrayBuffer());
    assertPdfSignature(pdfBytes, pdfPath);

    const extracted = await extractPdfText(pdfPath, pdfBytes);
    const { lab, modelId } = await generateLabObject({
        openRouterApiKey,
        modelIds,
        pdfPath,
        extractedText: extracted.fullText,
        pageTexts: extracted.pageTexts,
        glossary,
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

    saveBloodworkGlossary(glossaryPath, glossary);

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
    const glossaryPath = process.env.VITALS_BLOODWORK_GLOSSARY_PATH?.trim() || DEFAULT_GLOSSARY_PATH;
    const glossary = loadBloodworkGlossary(glossaryPath);
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
                glossary,
                glossaryPath,
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
    filterLikelyMeasurements,
    isEnglishGlossaryName,
    resolveModelIds,
    runBloodworkImporter,
};

if (import.meta.main) {
    createScript(async () => {
        await runBloodworkImporter();
    });
}
