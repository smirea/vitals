import fs from 'fs';
import { spawnSync } from 'child_process';
import path from 'path';

import { AnalyzeDocumentCommand, TextractClient } from '@aws-sdk/client-textract';
import { DeleteObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { createOpenRouter } from '@openrouter/ai-sdk-provider';
import { generateObject, generateText } from 'ai';
import { getDocument } from 'pdfjs-dist/legacy/build/pdf.mjs';
import { z } from 'zod';

import {
    bloodworkLabSchema,
    bloodworkMeasurementSchema,
    buildBloodworkFileName,
    normalizeIsoDate,
    parseReferenceRangeBoundsFromText,
    slugifyForPath,
    type BloodworkMeasurement,
    type BloodworkLab,
    type BloodworkMeasurementDuplicateValue,
    type BloodworkMergedSource,
} from './bloodwork-schema.ts';
import { createScript } from './createScript.ts';
import {
    PROJECT_DATA_DIR,
    PROJECT_GLOSSARY_PATH,
    PROJECT_TO_IMPORT_DIR,
} from './project-paths.ts';

const DEFAULT_S3_BUCKET = 'stefan-life';
const DEFAULT_S3_PREFIX = 'vitals';
const DEFAULT_MODEL_IDS = ['google/gemini-3-flash-preview'];
const DEFAULT_GLOSSARY_VALIDATOR_MODEL_IDS = ['google/gemini-3-flash-preview'];
const DEFAULT_TO_IMPORT_DIRECTORY = PROJECT_TO_IMPORT_DIR;
const DEFAULT_OUTPUT_DIRECTORY = PROJECT_DATA_DIR;
const DEFAULT_GLOSSARY_PATH = PROJECT_GLOSSARY_PATH;
const DEFAULT_REVIEW_REPORT_DIRECTORY = path.join(PROJECT_DATA_DIR, 'review');
const MERGE_WINDOW_DAYS = 7;
const EXTRACTED_TEXT_LIMIT = 45_000;
const MODEL_MAX_OUTPUT_TOKENS = 1_400;
const METADATA_MAX_OUTPUT_TOKENS = 280;
const NORMALIZATION_MAX_OUTPUT_TOKENS = 6_000;
const GLOSSARY_VALIDATION_MAX_OUTPUT_TOKENS = 1_600;
const MODEL_REQUEST_TIMEOUT_MS = 30_000;
const MAX_MEASUREMENTS_PER_PAGE = 120;
const MAX_NORMALIZATION_CANDIDATES = 320;
const MAX_GLOSSARY_DECISIONS = 64;
const RESOLUTION_MIN_CONFIDENCE = 0.85;
const RESOLUTION_MIN_MARGIN = 0.1;
const TEXTRACT_MIN_LAYOUT_CANDIDATES = 8;

type CliOptions = {
    importAll: boolean;
    inputPdfPath: string | null;
    continueOnError: boolean;
    skipUpload: boolean;
    modelIds: string[];
    mergeExistingOnly: boolean;
    allowUnresolved: boolean;
    reviewReportDir: string;
    approveReviewPath: string | null;
    enableTextractFallback: boolean;
};

type ImportResult = {
    outputPath: string | null;
    s3Key: string | null;
    modelId: string;
    reviewReportPath?: string;
    unresolvedCount?: number;
};

type BloodworkDataFile = {
    path: string;
    fileName: string;
    lab: BloodworkLab;
};

type ConsolidatedMeasurementSelection = {
    measurement: BloodworkMeasurement;
    source: BloodworkDataFile;
    duplicateValues: BloodworkMeasurementDuplicateValue[];
};

type ConsolidationGroupSummary = {
    targetFileName: string;
    latestDate: string;
    sourceFileNames: string[];
    sourceDates: string[];
};

type ConsolidationSummary = {
    groupsProcessed: number;
    mergedGroups: number;
    filesBefore: number;
    filesAfter: number;
    writtenFiles: string[];
    removedFiles: string[];
    uploadedKeys: string[];
    deletedKeys: string[];
    groups: ConsolidationGroupSummary[];
};

type ExtractedPdfText = {
    fullText: string;
    pageTexts: string[];
};

type ScoredMeasurementCandidate = {
    measurement: BloodworkMeasurement;
    score: number;
    scoreBreakdown: string[];
};

type MeasurementConflictCandidate = {
    score: number;
    scoreBreakdown: string[];
    measurement: BloodworkMeasurement;
};

type MeasurementConflict = {
    measurementNameKey: string;
    measurementName: string;
    reason: string;
    candidateCount: number;
    recommendedCandidateIndex: number;
    selectedCandidateIndex?: number;
    candidates: MeasurementConflictCandidate[];
};

type MeasurementResolutionResult = {
    measurements: BloodworkMeasurement[];
    conflicts: MeasurementConflict[];
};

type ReviewReport = {
    version: 1;
    generatedAt: string;
    sourcePdfPath: string;
    suggestedOutputFileName: string;
    unresolvedCount: number;
    conflicts: MeasurementConflict[];
    labDraft: BloodworkLab;
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
const UNCATEGORIZED_CATEGORY = 'Uncategorized';

const CATEGORY_TRANSLATION_REPLACEMENTS: Array<{ pattern: RegExp; replacement: string }> = [
    { pattern: /\bhaematology\b/gi, replacement: 'Hematology' },
    { pattern: /\bhämatologie\b/gi, replacement: 'Hematology' },
    { pattern: /\bblutbild\b/gi, replacement: 'Complete Blood Count' },
    { pattern: /\blipidprofil\b/gi, replacement: 'Lipid Panel' },
    { pattern: /\bleber\b/gi, replacement: 'Liver' },
    { pattern: /\bnieren\b/gi, replacement: 'Kidney' },
    { pattern: /\bschilddr[uü]se\b/gi, replacement: 'Thyroid' },
    { pattern: /\bentzuendung\b/gi, replacement: 'Inflammation' },
    { pattern: /\bentzündung\b/gi, replacement: 'Inflammation' },
    { pattern: /\bvitamine?\b/gi, replacement: 'Vitamins' },
    { pattern: /\bmineralstoffe?\b/gi, replacement: 'Minerals' },
    { pattern: /\beisenstatus\b/gi, replacement: 'Iron Studies' },
];

const GLOSSARY_CANONICAL_CATEGORY_RULES: Array<{ pattern: RegExp; canonicalCategory: string }> = [
    { pattern: /\b(?:cbc|complete blood count|hematology)\b/i, canonicalCategory: 'Complete Blood Count' },
    { pattern: /\b(?:lipid|cholesterol|cardio|apolipoprotein)\b/i, canonicalCategory: 'Lipids & Cardiovascular' },
    { pattern: /\b(?:glucose|glycemic|diabet|insulin|hba1c|a1c|homa)\b/i, canonicalCategory: 'Glucose & Insulin' },
    { pattern: /\b(?:thyroid|tsh|ft3|ft4|triiodothyronine|thyroxine)\b/i, canonicalCategory: 'Thyroid' },
    { pattern: /\b(?:hormone|endocrine|testosterone|estradiol|prolactin|dhea|cortisol|pregnanediol|fsh|lh|shbg)\b/i, canonicalCategory: 'Hormones' },
    { pattern: /\b(?:liver|hepatic|alt|ast|bilirubin|alkaline phosphatase|ggt|fibrosis|steatosis|nash|albumin globulin)\b/i, canonicalCategory: 'Liver Function' },
    { pattern: /\b(?:kidney|renal|creatinine|egfr|bun|uric)\b/i, canonicalCategory: 'Kidney Function' },
    { pattern: /\b(?:inflamm|immune|infect|serology|culture|crp|igg|hcv|hep b|hbsag)\b/i, canonicalCategory: 'Inflammation & Immunity' },
    { pattern: /\b(?:vitamin|mineral|magnesium|folate|b12|b6|b2|d3|25 oh)\b/i, canonicalCategory: 'Vitamins & Minerals' },
    { pattern: /\b(?:iron|ferritin|transferrin|uibc|tibc)\b/i, canonicalCategory: 'Iron Studies' },
    { pattern: /\b(?:electrolyte|sodium|potassium|chloride|calcium)\b/i, canonicalCategory: 'Electrolytes' },
    { pattern: /\b(?:metabolic|chemistry|cmp)\b/i, canonicalCategory: 'Metabolic Panel' },
];

const MEASUREMENT_CANONICAL_CATEGORY_RULES: Array<{ pattern: RegExp; canonicalCategory: string }> = [
    { pattern: /\b(?:wbc|rbc|leukocyte|erythrocyte|hemoglobin|hematocrit|platelet|neutrophil|lymphocyte|monocyte|eosinophil|basophil)\b/i, canonicalCategory: 'Complete Blood Count' },
    { pattern: /\b(?:cholesterol|hdl|ldl|triglyceride|apolipoprotein|vldl)\b/i, canonicalCategory: 'Lipids & Cardiovascular' },
    { pattern: /\b(?:glucose|hemoglobin a1c|hba1c|insulin|homa)\b/i, canonicalCategory: 'Glucose & Insulin' },
    { pattern: /\b(?:tsh|free t3|free t4|triiodothyronine|thyroxine)\b/i, canonicalCategory: 'Thyroid' },
    { pattern: /\b(?:testosterone|estradiol|prolactin|dhea|cortisol|pregnanediol|luteinizing hormone|follicle stimulating hormone|sex hormone binding globulin|androsterone|etiocholanolone|estriol|estrone)\b/i, canonicalCategory: 'Hormones' },
    { pattern: /\b(?:alt|ast|bilirubin|alkaline phosphatase|ggt|fibrosis|steatosis|nash|albumin|globulin|amylase|lipase)\b/i, canonicalCategory: 'Liver Function' },
    { pattern: /\b(?:creatinine|egfr|bun|uric acid)\b/i, canonicalCategory: 'Kidney Function' },
    { pattern: /\b(?:c reactive protein|crp|igg|hcv|hep b|culture)\b/i, canonicalCategory: 'Inflammation & Immunity' },
    { pattern: /\b(?:vitamin|folate|magnesium)\b/i, canonicalCategory: 'Vitamins & Minerals' },
    { pattern: /\b(?:ferritin|iron|transferrin|uibc|tibc)\b/i, canonicalCategory: 'Iron Studies' },
    { pattern: /\b(?:sodium|potassium|chloride|calcium)\b/i, canonicalCategory: 'Electrolytes' },
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
    date: z.string().trim().min(1).transform(normalizeIsoDate).optional(),
    collectionDate: z.string().trim().min(1).transform(normalizeIsoDate).optional(),
    reportedDate: z.string().trim().min(1).transform(normalizeIsoDate).optional(),
    receivedDate: z.string().trim().min(1).transform(normalizeIsoDate).optional(),
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
    canonicalCategory: z.string().trim().min(1).default(UNCATEGORIZED_CATEGORY),
    aliases: z.array(z.string().trim().min(1)).default([]),
    categoryAliases: z.array(z.string().trim().min(1)).default([]),
    knownRanges: z.array(glossaryRangeSchema).default([]),
    unitHints: z.array(z.string().trim().min(1)).default([]),
});

const bloodworkGlossarySchema = z.object({
    version: z.literal(1),
    entries: z.array(bloodworkGlossaryEntrySchema),
});

const glossaryValidationDecisionSchema = z.object({
    index: z.number().int().nonnegative(),
    action: z.string().trim().min(1),
    targetCanonicalName: z.string().trim().min(1).optional(),
    canonicalName: z.string().trim().min(1).optional(),
    canonicalCategory: z.string().trim().min(1).optional(),
    aliases: z.array(z.string().trim().min(1)).max(6).optional(),
    categoryAliases: z.array(z.string().trim().min(1)).max(6).optional(),
    reason: z.string().trim().min(1).optional(),
});

const glossaryValidationBatchSchema = z.object({
    decisions: z.array(glossaryValidationDecisionSchema).max(MAX_GLOSSARY_DECISIONS),
});

const reviewConflictCandidateSchema = z.object({
    score: z.number().finite().min(0).max(1),
    scoreBreakdown: z.array(z.string().trim().min(1)),
    measurement: bloodworkMeasurementSchema,
});

const reviewConflictSchema = z.object({
    measurementNameKey: z.string().trim().min(1),
    measurementName: z.string().trim().min(1),
    reason: z.string().trim().min(1),
    candidateCount: z.number().int().nonnegative(),
    recommendedCandidateIndex: z.number().int().nonnegative(),
    selectedCandidateIndex: z.number().int().nonnegative().optional(),
    candidates: z.array(reviewConflictCandidateSchema).min(1),
});

const reviewReportSchema = z.object({
    version: z.literal(1),
    generatedAt: z.string().trim().min(1),
    sourcePdfPath: z.string().trim().min(1),
    suggestedOutputFileName: z.string().trim().min(1),
    unresolvedCount: z.number().int().nonnegative(),
    conflicts: z.array(reviewConflictSchema),
    labDraft: bloodworkLabSchema,
});

type BloodworkGlossary = z.infer<typeof bloodworkGlossarySchema>;
type BloodworkGlossaryEntry = z.infer<typeof bloodworkGlossaryEntrySchema>;
type GlossaryDecisionAction = 'alias' | 'new_valid' | 'invalid';

const GLOSSARY_ALIAS_ACTION_KEYS = new Set([
    'alias',
    'existingalias',
    'aliasexisting',
    'maptoalias',
    'maptoexisting',
    'usealias',
]);

const GLOSSARY_NEW_VALID_ACTION_KEYS = new Set([
    'newvalid',
    'validnew',
    'new',
    'newentry',
    'newvalidentry',
    'newcanonical',
    'addnew',
]);

const HELP_TEXT = [
    'Usage:',
    '  bun scripts/bloodwork-import.ts <path-to-pdf> [--skip-upload] [--model <openrouter-model-id>] [--allow-unresolved]',
    '  bun scripts/bloodwork-import.ts --all [--continue-on-error] [--skip-upload] [--model <openrouter-model-id>] [--allow-unresolved]',
    '  bun scripts/bloodwork-import.ts --merge-existing [--skip-upload]',
    '  bun scripts/bloodwork-import.ts --approve-review <path-to-review-report.json> [--skip-upload]',
    '',
    'Flags:',
    '  --all                 Import every .pdf file from data/to-import',
    '  --merge-existing      Merge existing bloodwork_*.json files by date proximity (<= 7 days)',
    '  --approve-review      Apply decisions from a review report and finalize the lab JSON',
    '  --continue-on-error   Continue processing other files when --all is used',
    '  --skip-upload         Skip S3 upload (useful for local validation)',
    '  --model <id>          Override model id (can be repeated)',
    '  --allow-unresolved    Write output even when unresolved conflicts were found',
    '  --review-report-dir   Directory for generated unresolved review reports (default: data/review)',
    '  --textract-fallback   Enable AWS Textract fallback when local extraction confidence is low',
].join('\n');

function parseCliOptions(argv: string[]): CliOptions {
    let importAll = false;
    let mergeExistingOnly = false;
    let continueOnError = false;
    let skipUpload = false;
    let allowUnresolved = false;
    let reviewReportDir = DEFAULT_REVIEW_REPORT_DIRECTORY;
    let approveReviewPath: string | null = null;
    let enableTextractFallback = false;
    const modelIds: string[] = [];
    const positional: string[] = [];

    for (let index = 0; index < argv.length; index++) {
        const token = argv[index];
        if (token === '--all') {
            importAll = true;
            continue;
        }
        if (token === '--merge-existing') {
            mergeExistingOnly = true;
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
        if (token === '--allow-unresolved') {
            allowUnresolved = true;
            continue;
        }
        if (token === '--textract-fallback') {
            enableTextractFallback = true;
            continue;
        }
        if (token === '--approve-review') {
            const reportPath = argv[index + 1];
            if (!reportPath || reportPath.startsWith('--')) {
                throw new Error(`Missing report path after --approve-review\n\n${HELP_TEXT}`);
            }
            approveReviewPath = reportPath;
            index++;
            continue;
        }
        if (token.startsWith('--approve-review=')) {
            const reportPath = token.slice('--approve-review='.length).trim();
            if (!reportPath) {
                throw new Error(`Missing report path in ${token}\n\n${HELP_TEXT}`);
            }
            approveReviewPath = reportPath;
            continue;
        }
        if (token === '--review-report-dir') {
            const directory = argv[index + 1];
            if (!directory || directory.startsWith('--')) {
                throw new Error(`Missing directory after --review-report-dir\n\n${HELP_TEXT}`);
            }
            reviewReportDir = directory;
            index++;
            continue;
        }
        if (token.startsWith('--review-report-dir=')) {
            const directory = token.slice('--review-report-dir='.length).trim();
            if (!directory) {
                throw new Error(`Missing directory in ${token}\n\n${HELP_TEXT}`);
            }
            reviewReportDir = directory;
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

    if (approveReviewPath && (importAll || mergeExistingOnly || positional.length > 0)) {
        throw new Error(`--approve-review cannot be combined with PDF inputs, --all, or --merge-existing\n\n${HELP_TEXT}`);
    }

    if (approveReviewPath && continueOnError) {
        throw new Error(`--continue-on-error is not used with --approve-review\n\n${HELP_TEXT}`);
    }

    if (approveReviewPath && modelIds.length > 0) {
        throw new Error(`--model is not used with --approve-review\n\n${HELP_TEXT}`);
    }

    if (approveReviewPath && allowUnresolved) {
        throw new Error(`--allow-unresolved is not used with --approve-review\n\n${HELP_TEXT}`);
    }

    if (importAll && positional.length > 0) {
        throw new Error(`Do not pass a file path when using --all\n\n${HELP_TEXT}`);
    }

    if (mergeExistingOnly && (importAll || positional.length > 0)) {
        throw new Error(`--merge-existing cannot be combined with PDF inputs or --all\n\n${HELP_TEXT}`);
    }

    if (mergeExistingOnly && continueOnError) {
        throw new Error(`--continue-on-error is only valid for PDF import mode\n\n${HELP_TEXT}`);
    }

    if (mergeExistingOnly && modelIds.length > 0) {
        throw new Error(`--model is not used with --merge-existing\n\n${HELP_TEXT}`);
    }

    if (!approveReviewPath && !mergeExistingOnly && !importAll && positional.length !== 1) {
        throw new Error(`Expected exactly one PDF path, --all, or --merge-existing\n\n${HELP_TEXT}`);
    }

    if (!importAll && continueOnError) {
        throw new Error(`--continue-on-error can only be used together with --all\n\n${HELP_TEXT}`);
    }

    return {
        importAll,
        inputPdfPath: importAll || mergeExistingOnly || approveReviewPath ? null : positional[0]!,
        continueOnError,
        skipUpload,
        modelIds,
        mergeExistingOnly,
        allowUnresolved,
        reviewReportDir: path.resolve(process.cwd(), reviewReportDir),
        approveReviewPath: approveReviewPath ? path.resolve(process.cwd(), approveReviewPath) : null,
        enableTextractFallback,
    };
}

function requireEnv(name: string): string {
    const value = process.env[name]?.trim();
    if (!value) {
        throw new Error(`Missing required environment variable: ${name}`);
    }
    return value;
}

function normalizeModelIds(modelIds: string[], context: string): string[] {
    const normalized = Array.from(new Set(modelIds.map(modelId => modelId.trim()).filter(Boolean)));
    if (normalized.length === 0) {
        throw new Error(`No model ids configured for ${context}`);
    }
    return normalized;
}

function resolveModelIds(cliModelIds: string[]): string[] {
    if (cliModelIds.length > 0) {
        return normalizeModelIds(cliModelIds, 'bloodwork extraction');
    }
    return normalizeModelIds(DEFAULT_MODEL_IDS, 'bloodwork extraction');
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
        'Return labName, optional location, optional weightKg, optional notes.',
        'Return optional dates: collectionDate, reportedDate, receivedDate, and optional date.',
        'Set date to collectionDate when available; otherwise reportedDate; otherwise receivedDate.',
        'Do not include measurements in this step.',
        '',
        'Extracted text (may be partial):',
        extractedSegment,
    ].join('\n');
}

function normalizeDateToken(
    value: string | undefined,
    options?: {
        preferMonthFirst?: boolean;
    },
): string | undefined {
    if (!value) {
        return undefined;
    }
    if (options?.preferMonthFirst) {
        const monthFirst = value.trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
        if (monthFirst) {
            const month = Number.parseInt(monthFirst[1]!, 10);
            const day = Number.parseInt(monthFirst[2]!, 10);
            const year = Number.parseInt(monthFirst[3]!, 10);
            if (
                Number.isFinite(year) &&
                Number.isFinite(month) &&
                Number.isFinite(day) &&
                month >= 1 &&
                month <= 12 &&
                day >= 1 &&
                day <= 31
            ) {
                return `${year.toString().padStart(4, '0')}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
            }
        }
    }
    try {
        return normalizeIsoDate(value);
    } catch {
        return undefined;
    }
}

function extractDateCandidatesFromText(text: string): {
    collectionDate?: string;
    reportedDate?: string;
    receivedDate?: string;
} {
    const normalizedText = text.replace(/\r/g, '\n');
    let collectionDate: string | undefined;
    let reportedDate: string | undefined;
    let receivedDate: string | undefined;

    const tableLikeDateMatch = normalizedText.match(
        /(?:date\/time[\s\S]{0,120})?collected[\s\S]{0,120}?reported[\s\S]{0,180}?(\d{4}-\d{2}-\d{2}|\d{1,2}[./-]\d{1,2}[./-]\d{4})\s+(\d{4}-\d{2}-\d{2}|\d{1,2}[./-]\d{1,2}[./-]\d{4})\s+(\d{4}-\d{2}-\d{2}|\d{1,2}[./-]\d{1,2}[./-]\d{4})/i,
    );
    if (tableLikeDateMatch) {
        collectionDate = normalizeDateToken(tableLikeDateMatch[1]);
        reportedDate = normalizeDateToken(tableLikeDateMatch[3]);
    }

    const linePatterns: Array<{
        regex: RegExp;
        assign: (value: string) => void;
    }> = [
        {
            regex: /\b(?:date\/time\s+)?collected\b[^\n]*?(\d{4}-\d{2}-\d{2}|\d{1,2}[./-]\d{1,2}[./-]\d{4})/i,
            assign: value => {
                collectionDate = collectionDate ?? normalizeDateToken(value);
            },
        },
        {
            regex: /\b(?:date\/time\s+)?reported\b[^\n]*?(\d{4}-\d{2}-\d{2}|\d{1,2}[./-]\d{1,2}[./-]\d{4})/i,
            assign: value => {
                reportedDate = reportedDate ?? normalizeDateToken(value);
            },
        },
        {
            regex: /\b(?:received on|received)\b[^\n]*?(\d{4}-\d{2}-\d{2}|\d{1,2}[./-]\d{1,2}[./-]\d{4})/i,
            assign: value => {
                receivedDate = receivedDate ?? normalizeDateToken(value, {
                    preferMonthFirst: true,
                });
            },
        },
    ];

    for (const pattern of linePatterns) {
        const match = normalizedText.match(pattern.regex);
        if (match?.[1]) {
            pattern.assign(match[1]);
        }
    }

    return {
        collectionDate,
        reportedDate,
        receivedDate,
    };
}

function resolveCanonicalLabDate({
    collectionDate,
    reportedDate,
    receivedDate,
    fallbackDate,
}: {
    collectionDate?: string;
    reportedDate?: string;
    receivedDate?: string;
    fallbackDate: string;
}): string {
    return collectionDate || reportedDate || receivedDate || fallbackDate;
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

function normalizeCategoryNameKey(name: string): string {
    return normalizeTextForMatch(name).replace(/[^a-z0-9]+/g, ' ').trim();
}

function normalizeCategoryNameForGlossary(category: string): string {
    let next = category
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    if (!next) {
        return UNCATEGORIZED_CATEGORY;
    }

    for (const replacement of CATEGORY_TRANSLATION_REPLACEMENTS) {
        next = next.replace(replacement.pattern, replacement.replacement);
    }

    next = next
        .replace(/\b(?:panel|profile|markers?|tests?|results?)\b/gi, ' ')
        .replace(/[,:;]+$/g, '')
        .replace(/\s+/g, ' ')
        .trim();

    const normalized = normalizeCategoryNameKey(next);
    if (!normalized) {
        return UNCATEGORIZED_CATEGORY;
    }

    for (const rule of GLOSSARY_CANONICAL_CATEGORY_RULES) {
        if (rule.pattern.test(normalized)) {
            return rule.canonicalCategory;
        }
    }

    return UNCATEGORIZED_CATEGORY;
}

function isUncategorizedCategory(category: string): boolean {
    return normalizeCategoryNameKey(category) === normalizeCategoryNameKey(UNCATEGORIZED_CATEGORY);
}

function inferCanonicalCategoryFromMeasurementName(measurementName: string): string {
    const normalizedName = normalizeGlossaryNameKey(measurementName);
    if (!normalizedName) {
        return UNCATEGORIZED_CATEGORY;
    }

    for (const rule of MEASUREMENT_CANONICAL_CATEGORY_RULES) {
        if (rule.pattern.test(normalizedName)) {
            return rule.canonicalCategory;
        }
    }

    return UNCATEGORIZED_CATEGORY;
}

function resolveCanonicalCategoryForMeasurement({
    measurementName,
    category,
    fallbackCategory,
}: {
    measurementName: string;
    category?: string;
    fallbackCategory?: string;
}): string {
    if (category?.trim()) {
        const normalizedCategory = normalizeCategoryNameForGlossary(category);
        if (!isUncategorizedCategory(normalizedCategory)) {
            return normalizedCategory;
        }
    }
    if (fallbackCategory?.trim()) {
        const normalizedFallbackCategory = normalizeCategoryNameForGlossary(fallbackCategory);
        if (!isUncategorizedCategory(normalizedFallbackCategory)) {
            return normalizedFallbackCategory;
        }
    }
    return inferCanonicalCategoryFromMeasurementName(measurementName);
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
    const category = resolveCanonicalCategoryForMeasurement({
        measurementName: canonicalName,
        category: measurement.category,
    });

    return {
        ...measurement,
        name: canonicalName,
        originalName: nextOriginalName,
        category,
    };
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
    const canonicalCategory = resolveCanonicalCategoryForMeasurement({
        measurementName: canonicalName,
        category: entry.canonicalCategory,
    });
    const canonicalCategoryKey = normalizeCategoryNameKey(canonicalCategory);
    const aliases = sortUniqueStrings(
        entry.aliases
            .map(alias => normalizeMeasurementNameForGlossary(alias))
            .filter(alias => isEnglishGlossaryName(alias))
            .filter(alias => normalizeGlossaryNameKey(alias) !== canonicalKey),
    );
    const categoryAliases = sortUniqueStrings(
        entry.categoryAliases
            .map(alias => normalizeCategoryNameForGlossary(alias))
            .filter(alias => !isUncategorizedCategory(alias))
            .filter(alias => normalizeCategoryNameKey(alias) !== canonicalCategoryKey),
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
        canonicalCategory,
        aliases,
        categoryAliases,
        knownRanges: Array.from(rangesByKey.values()),
        unitHints,
    };
}

function mergeGlossaryEntries(target: BloodworkGlossaryEntry, source: BloodworkGlossaryEntry): void {
    const targetCategory = resolveCanonicalCategoryForMeasurement({
        measurementName: target.canonicalName,
        category: target.canonicalCategory,
    });
    const sourceCategory = resolveCanonicalCategoryForMeasurement({
        measurementName: target.canonicalName,
        category: source.canonicalCategory,
    });
    target.canonicalCategory = targetCategory;
    upsertCategoryAliasIntoGlossaryEntry(target, sourceCategory);
    for (const categoryAlias of source.categoryAliases) {
        upsertCategoryAliasIntoGlossaryEntry(target, categoryAlias);
    }
    if (
        normalizeCategoryNameKey(targetCategory) === normalizeCategoryNameKey(UNCATEGORIZED_CATEGORY) &&
        normalizeCategoryNameKey(sourceCategory) !== normalizeCategoryNameKey(UNCATEGORIZED_CATEGORY)
    ) {
        target.canonicalCategory = sourceCategory;
    }

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
}

function normalizeGlossaryState(glossary: BloodworkGlossary): BloodworkGlossary {
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
    const normalizedGlossary = normalizeGlossaryState(glossary);
    fs.mkdirSync(path.dirname(glossaryPath), { recursive: true });
    fs.writeFileSync(glossaryPath, `${JSON.stringify(normalizedGlossary, null, 4)}\n`, 'utf8');
}

function resolveGlossaryValidatorModelIds(primaryModelIds: string[]): string[] {
    const envModels = process.env.VITALS_GLOSSARY_VALIDATOR_MODEL_IDS
        ?.split(',')
        .map(modelId => modelId.trim())
        .filter(Boolean);
    if (envModels && envModels.length > 0) {
        return normalizeModelIds(envModels, 'glossary validation');
    }

    if (primaryModelIds.length > 0) {
        return normalizeModelIds(primaryModelIds, 'glossary validation');
    }

    return normalizeModelIds(DEFAULT_GLOSSARY_VALIDATOR_MODEL_IDS, 'glossary validation');
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

function upsertCategoryAliasIntoGlossaryEntry(entry: BloodworkGlossaryEntry, alias: string): void {
    const normalizedAlias = normalizeCategoryNameForGlossary(alias);
    if (isUncategorizedCategory(normalizedAlias)) {
        return;
    }
    const aliasKey = normalizeCategoryNameKey(normalizedAlias);
    const canonicalKey = normalizeCategoryNameKey(entry.canonicalCategory);
    if (!aliasKey || aliasKey === canonicalKey) {
        return;
    }
    if (!entry.categoryAliases.some(existing => normalizeCategoryNameKey(existing) === aliasKey)) {
        entry.categoryAliases = sortUniqueStrings([...entry.categoryAliases, normalizedAlias]);
    }
}

function resolveEntryCanonicalCategoryFromMeasurement({
    entry,
    measurement,
    extraCategoryAliases = [],
}: {
    entry: BloodworkGlossaryEntry;
    measurement: BloodworkMeasurement;
    extraCategoryAliases?: string[];
}): string {
    const existingCanonicalCategory = resolveCanonicalCategoryForMeasurement({
        measurementName: entry.canonicalName,
        category: entry.canonicalCategory,
    });
    const measurementCanonicalCategory = resolveCanonicalCategoryForMeasurement({
        measurementName: entry.canonicalName,
        category: measurement.category,
        fallbackCategory: existingCanonicalCategory,
    });
    const hasDefaultExistingCategory =
        normalizeCategoryNameKey(existingCanonicalCategory) === normalizeCategoryNameKey(UNCATEGORIZED_CATEGORY);
    const hasDefaultMeasurementCategory =
        normalizeCategoryNameKey(measurementCanonicalCategory) === normalizeCategoryNameKey(UNCATEGORIZED_CATEGORY);

    if (hasDefaultExistingCategory && !hasDefaultMeasurementCategory) {
        entry.canonicalCategory = measurementCanonicalCategory;
    } else {
        entry.canonicalCategory = existingCanonicalCategory;
        if (normalizeCategoryNameKey(measurementCanonicalCategory) !== normalizeCategoryNameKey(existingCanonicalCategory)) {
            upsertCategoryAliasIntoGlossaryEntry(entry, measurementCanonicalCategory);
        }
    }

    for (const alias of extraCategoryAliases) {
        upsertCategoryAliasIntoGlossaryEntry(entry, alias);
    }

    return entry.canonicalCategory;
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

function updateGlossaryEntryWithMeasurement({
    entry,
    measurement,
    extraAliases = [],
    extraCategoryAliases = [],
}: {
    entry: BloodworkGlossaryEntry;
    measurement: BloodworkMeasurement;
    extraAliases?: string[];
    extraCategoryAliases?: string[];
}): string {
    const canonicalCategory = resolveEntryCanonicalCategoryFromMeasurement({
        entry,
        measurement,
        extraCategoryAliases,
    });
    upsertAliasIntoGlossaryEntry(entry, measurement.name);
    if (measurement.originalName) {
        upsertAliasIntoGlossaryEntry(entry, measurement.originalName);
    }
    for (const alias of extraAliases) {
        upsertAliasIntoGlossaryEntry(entry, alias);
    }
    upsertUnitHintIntoGlossaryEntry(entry, measurement.unit);
    upsertRangeIntoGlossaryEntry(entry, measurement);
    return canonicalCategory;
}

function createGlossaryEntryFromMeasurement({
    canonicalName,
    measurement,
}: {
    canonicalName: string;
    measurement: BloodworkMeasurement;
}): BloodworkGlossaryEntry {
    const canonicalCategory = resolveCanonicalCategoryForMeasurement({
        measurementName: canonicalName,
        category: measurement.category,
    });
    const entry: BloodworkGlossaryEntry = {
        canonicalName,
        canonicalCategory,
        aliases: [],
        categoryAliases: [],
        knownRanges: [],
        unitHints: [],
    };
    updateGlossaryEntryWithMeasurement({
        entry,
        measurement: {
            ...measurement,
            name: canonicalName,
            category: canonicalCategory,
        },
    });
    return entry;
}

function cleanMeasurementCandidate(measurement: BloodworkMeasurement): BloodworkMeasurement {
    const { notes: legacyNotes, ...measurementWithoutLegacyNotes } = measurement;
    const name = measurement.name.replace(/\s+/g, ' ').trim();
    const originalName = measurement.originalName?.replace(/\s+/g, ' ').trim() || undefined;
    let unit = measurement.unit?.replace(/\s+/g, ' ').trim() || undefined;
    const note = measurement.note?.replace(/\s+/g, ' ').trim() || legacyNotes?.replace(/\s+/g, ' ').trim() || undefined;
    const category = measurement.category?.replace(/\s+/g, ' ').trim()
        ? normalizeCategoryNameForGlossary(measurement.category)
        : undefined;
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

type UnitStandardizationConverter = {
    convert: (value: number) => number;
    requiresOriginal: boolean;
};

type MeasurementUnitStandardizationRule = {
    namePattern: RegExp;
    canonicalUnit: string;
    convertersByUnitKey: Record<string, UnitStandardizationConverter>;
};

const IDENTITY_UNIT_CONVERTER: UnitStandardizationConverter = {
    convert: value => value,
    requiresOriginal: false,
};

function scaledUnitConverter(factor: number): UnitStandardizationConverter {
    return {
        convert: value => value * factor,
        requiresOriginal: Math.abs(factor - 1) > Number.EPSILON,
    };
}

function shiftedScaledUnitConverter({
    factor,
    offset,
}: {
    factor: number;
    offset: number;
}): UnitStandardizationConverter {
    return {
        convert: value => value * factor + offset,
        requiresOriginal: true,
    };
}

function normalizeUnitKey(unit: string): string {
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
): Record<string, UnitStandardizationConverter> {
    const map: Record<string, UnitStandardizationConverter> = {};
    for (const entry of entries) {
        map[normalizeUnitKey(entry.unit)] = entry.converter;
    }
    return map;
}

const UNIT_CANONICAL_LABELS_BY_KEY: Record<string, string> = {
    '%': '%',
    '%oftotalhgb': '%',
    '%gesfs': '%',
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
    '/hpf': '/HPF',
    's/coratio': 's/co ratio',
    ratio: 'Ratio',
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
            { unit: 'mmol/mol', converter: shiftedScaledUnitConverter({ factor: 0.09148, offset: 2.152 }) },
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

function canonicalizeUnitLabel(unit: string): string {
    const normalizedKey = normalizeUnitKey(unit);
    return UNIT_CANONICAL_LABELS_BY_KEY[normalizedKey] || unit.replace(/\s+/g, ' ').trim();
}

function roundStandardizedNumber(value: number): number {
    const rounded = Number.parseFloat(value.toFixed(6));
    if (Object.is(rounded, -0)) {
        return 0;
    }
    return rounded;
}

function findMeasurementUnitStandardizationRule(
    measurementName: string,
): MeasurementUnitStandardizationRule | null {
    const normalizedName = normalizeMeasurementNameForGlossary(measurementName) || measurementName;
    for (const rule of MEASUREMENT_UNIT_STANDARDIZATION_RULES) {
        if (rule.namePattern.test(normalizedName)) {
            return rule;
        }
    }
    return null;
}

function parseComparableNumericValue(
    value: BloodworkMeasurement['value'],
): { comparator: string; numericValue: number; hasComparator: boolean } | null {
    if (typeof value === 'number') {
        if (!Number.isFinite(value)) {
            return null;
        }
        return {
            comparator: '',
            numericValue: value,
            hasComparator: false,
        };
    }

    if (typeof value !== 'string') {
        return null;
    }

    const match = value.trim().match(/^(<=|>=|<|>)?\s*(-?\d+(?:[.,]\d+)?)$/);
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

function convertMeasurementValueWithUnitConverter(
    value: BloodworkMeasurement['value'],
    converter: UnitStandardizationConverter,
): {
    nextValue: BloodworkMeasurement['value'];
    converted: boolean;
    changed: boolean;
} {
    const parsed = parseComparableNumericValue(value);
    if (!parsed) {
        return {
            nextValue: value,
            converted: false,
            changed: false,
        };
    }

    const convertedNumeric = roundStandardizedNumber(converter.convert(parsed.numericValue));
    const nextValue: BloodworkMeasurement['value'] = parsed.hasComparator
        ? `${parsed.comparator}${convertedNumeric}`
        : convertedNumeric;
    const changed = Math.abs(convertedNumeric - parsed.numericValue) > 1e-9;

    return {
        nextValue,
        converted: true,
        changed,
    };
}

function convertReferenceRangeWithUnitConverter(
    referenceRange: BloodworkMeasurement['referenceRange'],
    converter: UnitStandardizationConverter,
): {
    nextReferenceRange: BloodworkMeasurement['referenceRange'];
    converted: boolean;
    changed: boolean;
} {
    if (!referenceRange) {
        return {
            nextReferenceRange: referenceRange,
            converted: false,
            changed: false,
        };
    }

    let converted = false;
    let changed = false;
    const nextReferenceRange: NonNullable<BloodworkMeasurement['referenceRange']> = {};

    if (referenceRange.min !== undefined) {
        const nextMin = roundStandardizedNumber(converter.convert(referenceRange.min));
        nextReferenceRange.min = nextMin;
        converted = true;
        if (Math.abs(nextMin - referenceRange.min) > 1e-9) {
            changed = true;
        }
    }

    if (referenceRange.max !== undefined) {
        const nextMax = roundStandardizedNumber(converter.convert(referenceRange.max));
        nextReferenceRange.max = nextMax;
        converted = true;
        if (Math.abs(nextMax - referenceRange.max) > 1e-9) {
            changed = true;
        }
    }

    return {
        nextReferenceRange: converted ? nextReferenceRange : referenceRange,
        converted,
        changed,
    };
}

function buildMeasurementOriginalSnapshot(measurement: BloodworkMeasurement): BloodworkMeasurement['original'] {
    const original: NonNullable<BloodworkMeasurement['original']> = {
        value: measurement.value,
        unit: measurement.unit,
        referenceRange: measurement.referenceRange
            ? {
                min: measurement.referenceRange.min,
                max: measurement.referenceRange.max,
            }
            : undefined,
    };

    if (
        original.value === undefined &&
        original.unit === undefined &&
        original.referenceRange === undefined
    ) {
        return undefined;
    }

    return original;
}

function standardizeMeasurementUnit(measurement: BloodworkMeasurement): BloodworkMeasurement {
    const unit = measurement.unit?.trim();
    if (!unit) {
        return measurement;
    }

    const canonicalizedSourceUnit = canonicalizeUnitLabel(unit);
    const measurementWithCanonicalizedUnit =
        canonicalizedSourceUnit === unit
            ? measurement
            : {
                ...measurement,
                unit: canonicalizedSourceUnit,
            };

    const rule = findMeasurementUnitStandardizationRule(measurementWithCanonicalizedUnit.name);
    if (!rule) {
        return measurementWithCanonicalizedUnit;
    }

    const sourceUnitKey = normalizeUnitKey(canonicalizedSourceUnit);
    const targetUnitKey = normalizeUnitKey(rule.canonicalUnit);
    const converter = rule.convertersByUnitKey[sourceUnitKey];
    if (!converter) {
        if (sourceUnitKey === targetUnitKey) {
            return {
                ...measurementWithCanonicalizedUnit,
                unit: rule.canonicalUnit,
            };
        }
        return measurementWithCanonicalizedUnit;
    }

    const convertedValue = convertMeasurementValueWithUnitConverter(
        measurementWithCanonicalizedUnit.value,
        converter,
    );
    const convertedReferenceRange = convertReferenceRangeWithUnitConverter(
        measurementWithCanonicalizedUnit.referenceRange,
        converter,
    );
    const convertedAnyNumericField =
        convertedValue.converted || convertedReferenceRange.converted;

    if (
        sourceUnitKey !== targetUnitKey &&
        converter.requiresOriginal &&
        !convertedAnyNumericField
    ) {
        return measurementWithCanonicalizedUnit;
    }

    let nextMeasurement: BloodworkMeasurement = {
        ...measurementWithCanonicalizedUnit,
        unit: sourceUnitKey === targetUnitKey ? rule.canonicalUnit : measurementWithCanonicalizedUnit.unit,
    };

    if (convertedValue.converted) {
        nextMeasurement = {
            ...nextMeasurement,
            value: convertedValue.nextValue,
        };
    }
    if (convertedReferenceRange.converted) {
        nextMeasurement = {
            ...nextMeasurement,
            referenceRange: convertedReferenceRange.nextReferenceRange,
        };
    }
    if (sourceUnitKey !== targetUnitKey) {
        nextMeasurement = {
            ...nextMeasurement,
            unit: rule.canonicalUnit,
        };
    }

    if (sourceUnitKey !== targetUnitKey && converter.requiresOriginal) {
        const originalSnapshot = buildMeasurementOriginalSnapshot(measurementWithCanonicalizedUnit);
        if (originalSnapshot) {
            nextMeasurement = {
                ...nextMeasurement,
                original: originalSnapshot,
            };
        }
    }

    return nextMeasurement;
}

function standardizeMeasurementUnits(measurements: BloodworkMeasurement[]): BloodworkMeasurement[] {
    return measurements.map(standardizeMeasurementUnit);
}

type TableLikeRow = {
    line: string;
    category?: string;
};

function isLikelyCategoryHeading(line: string): boolean {
    const trimmed = line.trim();
    if (!trimmed || trimmed.length < 3 || trimmed.length > 90) {
        return false;
    }
    if (/\d/.test(trimmed) || VALUE_TOKEN_PATTERN.test(trimmed) || RANGE_TOKEN_PATTERN.test(trimmed)) {
        return false;
    }

    const normalized = normalizeTextForMatch(trimmed);
    if (!normalized) {
        return false;
    }
    if (NON_MEASUREMENT_NAME_EXACT.has(normalized)) {
        return false;
    }
    if (NON_MEASUREMENT_NAME_PATTERNS.some(pattern => pattern.test(normalized))) {
        return false;
    }
    if (
        LIKELY_ANALYTE_NAME_PATTERN.test(normalized) &&
        !/\b(?:panel|profile|section|markers?|hematology|cbc|lipid|hormone|thyroid|liver|kidney|metabolic|immune|vitamin|mineral)\b/i.test(normalized)
    ) {
        return false;
    }

    const tokenCount = normalized.split(' ').length;
    if (tokenCount > 7) {
        return false;
    }

    return (
        trimmed === trimmed.toUpperCase() ||
        trimmed.endsWith(':') ||
        /\b(?:panel|profile|section|markers?|hematology|cbc|lipid|hormone|thyroid|liver|kidney|metabolic|immune|vitamin|mineral|iron|electrolyte)\b/i.test(trimmed)
    );
}

function extractTableLikeRows(pageText: string): TableLikeRow[] {
    const rawLines = pageText
        .split('\n')
        .map(line => line.replace(/\t/g, ' ').replace(/\u00a0/g, ' '))
        .map(line => line.replace(/\s+$/g, ''));

    const selected = new Map<string, TableLikeRow>();
    let activeCategory: string | undefined;

    for (let index = 0; index < rawLines.length; index++) {
        const rawLine = rawLines[index]?.trim();
        if (!rawLine || rawLine.length > 220) {
            continue;
        }
        if (isLikelyCategoryHeading(rawLine)) {
            activeCategory = normalizeCategoryNameForGlossary(rawLine);
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

        const line = [mergedName, ...parts.slice(1)].join(' | ');
        const dedupeKey = `${line}|${normalizeCategoryNameKey(activeCategory || '')}`;
        if (!selected.has(dedupeKey)) {
            selected.set(dedupeKey, {
                line,
                category: activeCategory,
            });
        }
    }

    return Array.from(selected.values());
}

function parseNumericValueToken(raw: string): number | string {
    const trimmed = raw.trim();
    if (!trimmed) {
        return trimmed;
    }
    if (/^[<>]/.test(trimmed)) {
        return trimmed;
    }
    if (!/^-?\d+(?:[.,]\d+)?$/.test(trimmed)) {
        return trimmed;
    }
    const parsed = Number.parseFloat(trimmed.replace(',', '.'));
    if (Number.isFinite(parsed)) {
        return parsed;
    }
    return trimmed;
}

function parseReferenceRangeFromText(text: string): BloodworkMeasurement['referenceRange'] | undefined {
    return parseReferenceRangeBoundsFromText(text);
}

function parseMeasurementFromTableLine({
    line,
    category,
}: {
    line: string;
    category?: string;
}): BloodworkMeasurement | null {
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
        }
    }

    if (value === undefined) {
        return null;
    }

    return {
        name,
        originalName: name,
        category,
        value,
        unit,
        referenceRange,
        flag,
    };
}

function parseMeasurementsFromTableLikeRows(rows: TableLikeRow[]): BloodworkMeasurement[] {
    const parsed: BloodworkMeasurement[] = [];
    for (const row of rows) {
        const measurement = parseMeasurementFromTableLine({
            line: row.line,
            category: row.category,
        });
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
        let activeCategory: string | undefined;
        const lines = pageText
            .split('\n')
            .map(line => line.replace(/\s+/g, ' ').trim())
            .filter(Boolean);

        for (let lineIndex = 0; lineIndex < lines.length; lineIndex++) {
            const heading = lines[lineIndex]!;
            if (isLikelyCategoryHeading(heading)) {
                activeCategory = normalizeCategoryNameForGlossary(heading);
                continue;
            }
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
                category: activeCategory,
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
        'Use canonical English measurement names in `name` when possible.',
        'If source text is non-English, preserve the raw source label in `originalName` and translate `name` to English.',
        'Set optional `category` from the nearest section/panel heading when it is explicitly present in the document.',
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
        'The final `name` must be canonical English where possible. Keep `originalName` as source-language label when translation occurs.',
        'Return one canonical English `category` per measurement. Prefer explicit section/panel headings from the report text, otherwise infer from analyte meaning.',
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
        canonicalCategory: string;
        aliases: string[];
        categoryAliases: string[];
    }>;
    unknownMeasurements: Array<{ index: number; measurement: BloodworkMeasurement }>;
}): string {
    const candidates = unknownMeasurements.map(item => ({
        index: item.index,
        name: item.measurement.name,
        originalName: item.measurement.originalName,
        category: item.measurement.category,
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
        'Strict rule: canonicalName, aliases, canonicalCategory, and categoryAliases must be English-only terms using ASCII letters/digits/punctuation.',
        'If the candidate is non-English or uncertain, choose invalid.',
        'For alias, set targetCanonicalName exactly to one of the existing canonical names.',
        'For alias and new_valid, set canonicalCategory to one concise canonical category name (for example Lipids & Cardiovascular, Thyroid, Hormones).',
        'For new_valid, set canonicalName in English and include optional English aliases.',
        'Use optional categoryAliases only when they are true category synonyms.',
        'Never return non-English canonical names, aliases, categories, or category aliases.',
        '',
        'Existing glossary entries (canonical names with aliases/categories):',
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
            canonicalCategory: entry.canonicalCategory,
            aliases: entry.aliases,
            categoryAliases: entry.categoryAliases,
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
        canonicalCategory: resolveCanonicalCategoryForMeasurement({
            measurementName: canonicalName,
            category: measurement.category,
        }),
        aliases: measurement.originalName && isEnglishGlossaryName(measurement.originalName)
            ? [measurement.originalName.trim()]
            : [],
        reason: 'fallback-new-valid',
    };
}

function normalizeGlossaryDecisionAction(action: string): GlossaryDecisionAction {
    const normalizedAction = normalizeTextForMatch(action).replace(/[^a-z0-9]+/g, '');
    if (GLOSSARY_ALIAS_ACTION_KEYS.has(normalizedAction)) {
        return 'alias';
    }
    if (GLOSSARY_NEW_VALID_ACTION_KEYS.has(normalizedAction)) {
        return 'new_valid';
    }
    return 'invalid';
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
    const action = normalizeGlossaryDecisionAction(decision.action);
    const decisionCanonicalCategory = decision.canonicalCategory?.trim()
        ? normalizeCategoryNameForGlossary(decision.canonicalCategory)
        : undefined;
    const decisionCategoryAliases = (decision.categoryAliases ?? []).map(alias => normalizeCategoryNameForGlossary(alias));

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

        const aliasedMeasurement = standardizeMeasurementUnit({
            ...measurement,
            name: existingEntry.canonicalName,
        });
        const canonicalCategory = updateGlossaryEntryWithMeasurement({
            entry: existingEntry,
            measurement: aliasedMeasurement,
            extraAliases: decision.aliases,
            extraCategoryAliases: [
                ...(decisionCanonicalCategory ? [decisionCanonicalCategory] : []),
                ...decisionCategoryAliases,
            ],
        });
        acceptedMeasurements.push({
            ...aliasedMeasurement,
            category: canonicalCategory,
        });
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
    const measurementWithCanonicalName = standardizeMeasurementUnit({
        ...measurement,
        name: canonicalName,
        category: resolveCanonicalCategoryForMeasurement({
            measurementName: canonicalName,
            category: decisionCanonicalCategory ?? measurement.category,
        }),
    });

    let canonicalCategory = measurementWithCanonicalName.category;
    if (existingEntry) {
        canonicalCategory = updateGlossaryEntryWithMeasurement({
            entry: existingEntry,
            measurement: measurementWithCanonicalName,
            extraAliases: decision.aliases,
            extraCategoryAliases: decisionCategoryAliases,
        });
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
        for (const alias of decisionCategoryAliases) {
            upsertCategoryAliasIntoGlossaryEntry(entry, alias);
        }
        canonicalCategory = entry.canonicalCategory;
        appendGlossaryEntry({
            glossary,
            lookup,
            entry,
        });
    }
    acceptedMeasurements.push({
        ...measurementWithCanonicalName,
        category: canonicalCategory,
    });
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
        const normalizedMeasurement = normalizeMeasurementForGlossary(
            standardizeMeasurementUnit(measurement),
        );
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
            const canonicalCategory = updateGlossaryEntryWithMeasurement({
                entry: knownEntry,
                measurement: resolvedMeasurement,
            });
            accepted.push({
                ...resolvedMeasurement,
                category: canonicalCategory,
            });
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

function clampScore(value: number): number {
    if (value < 0) return 0;
    if (value > 1) return 1;
    return value;
}

function dedupeProvenanceEntries(measurement: BloodworkMeasurement): BloodworkMeasurement['provenance'] {
    if (!measurement.provenance || measurement.provenance.length === 0) {
        return undefined;
    }
    const unique = new Map<string, NonNullable<BloodworkMeasurement['provenance']>[number]>();
    for (const entry of measurement.provenance) {
        const key = [
            entry.extractor,
            entry.page,
            entry.rawName ?? '',
            entry.rawValue ?? '',
            entry.rawUnit ?? '',
            entry.rawRange ?? '',
            entry.confidence?.toString() ?? '',
        ].join('|');
        if (!unique.has(key)) {
            unique.set(key, entry);
        }
    }
    return Array.from(unique.values());
}

function buildDuplicateValueFromCandidate({
    measurement,
    date,
}: {
    measurement: BloodworkMeasurement;
    date: string;
}): BloodworkMeasurementDuplicateValue {
    const duplicateValue: BloodworkMeasurementDuplicateValue = {
        date,
    };
    if (measurement.value !== undefined) {
        duplicateValue.value = measurement.value;
    }
    if (measurement.unit) {
        duplicateValue.unit = measurement.unit;
    }
    if (measurement.referenceRange) {
        duplicateValue.referenceRange = cloneReferenceRange(measurement.referenceRange);
    }
    if (measurement.flag) {
        duplicateValue.flag = measurement.flag;
    }
    if (measurement.note) {
        duplicateValue.note = measurement.note;
    }
    return duplicateValue;
}

function hasHighSeverityCandidateContradiction(
    left: BloodworkMeasurement,
    right: BloodworkMeasurement,
): boolean {
    const leftComparable = parseComparableNumericValue(left.value);
    const rightComparable = parseComparableNumericValue(right.value);
    const leftUnit = left.unit ? normalizeUnitKey(left.unit) : '';
    const rightUnit = right.unit ? normalizeUnitKey(right.unit) : '';
    const unitsComparable = !leftUnit || !rightUnit || leftUnit === rightUnit;

    if (leftComparable && rightComparable && unitsComparable) {
        const leftAbs = Math.abs(leftComparable.numericValue);
        const rightAbs = Math.abs(rightComparable.numericValue);
        const max = Math.max(leftAbs, rightAbs);
        const min = Math.min(leftAbs, rightAbs);
        if (max > 0 && min > 0 && max / min >= 2) {
            return true;
        }
    }

    if (leftUnit && rightUnit && leftUnit !== rightUnit && UNIT_TOKEN_PATTERN.test(left.unit || '') && UNIT_TOKEN_PATTERN.test(right.unit || '')) {
        return true;
    }

    if (left.referenceRange && right.referenceRange) {
        const leftMin = left.referenceRange.min;
        const leftMax = left.referenceRange.max;
        const rightMin = right.referenceRange.min;
        const rightMax = right.referenceRange.max;
        if (leftMin !== undefined && leftMax !== undefined && rightMin !== undefined && rightMax !== undefined) {
            const overlaps = leftMin <= rightMax && rightMin <= leftMax;
            if (!overlaps) {
                return true;
            }
        }
    }

    return false;
}

function scoreMeasurementCandidate({
    measurement,
    glossaryLookup,
}: {
    measurement: BloodworkMeasurement;
    glossaryLookup: Map<string, BloodworkGlossaryEntry>;
}): ScoredMeasurementCandidate {
    let score = 0;
    const scoreBreakdown: string[] = [];
    const normalizedName = normalizeTextForMatch(measurement.name);
    const glossaryEntry = glossaryLookup.get(normalizeGlossaryNameKey(measurement.name));

    if (LIKELY_ANALYTE_NAME_PATTERN.test(normalizedName)) {
        score += 0.22;
        scoreBreakdown.push('name:likely-analyte(+0.22)');
    } else {
        score += 0.05;
        scoreBreakdown.push('name:weak(+0.05)');
    }

    const numericValue = parseComparableNumericValue(measurement.value);
    if (numericValue) {
        score += 0.2;
        scoreBreakdown.push('value:numeric(+0.20)');
    } else if (typeof measurement.value === 'string' && QUALITATIVE_RESULT_PATTERN.test(measurement.value)) {
        score += 0.12;
        scoreBreakdown.push('value:qualitative(+0.12)');
    }

    const hasUnit = Boolean(measurement.unit);
    const unitLooksValid = Boolean(measurement.unit && UNIT_TOKEN_PATTERN.test(measurement.unit));
    if (unitLooksValid) {
        score += 0.16;
        scoreBreakdown.push('unit:recognized(+0.16)');
    } else if (hasUnit) {
        score += 0.03;
        scoreBreakdown.push('unit:present-weak(+0.03)');
    }

    if (measurement.referenceRange) {
        score += 0.14;
        scoreBreakdown.push('range:present(+0.14)');
    }

    if (measurement.flag) {
        score += 0.04;
        scoreBreakdown.push('flag:present(+0.04)');
    }

    const provenanceConfidence = measurement.provenance
        ?.map(item => item.confidence)
        .find(value => value !== undefined);
    if (provenanceConfidence !== undefined) {
        score += provenanceConfidence * 0.12;
        scoreBreakdown.push(`provenance:${provenanceConfidence.toFixed(2)}(+${(provenanceConfidence * 0.12).toFixed(2)})`);
    }

    if (glossaryEntry) {
        score += 0.07;
        scoreBreakdown.push('glossary:name-match(+0.07)');
        if (measurement.unit && glossaryEntry.unitHints.some(unitHint => normalizeUnitKey(unitHint) === normalizeUnitKey(measurement.unit!))) {
            score += 0.06;
            scoreBreakdown.push('glossary:unit-match(+0.06)');
        }
    }

    if (
        numericValue &&
        numericValue.numericValue >= 1 &&
        numericValue.numericValue <= 5 &&
        !measurement.unit &&
        !measurement.referenceRange &&
        /\b(?:cholesterol|a1c|hemoglobin a1c|ratio)\b/i.test(normalizedName)
    ) {
        score -= 0.25;
        scoreBreakdown.push('penalty:suspicious-low-without-structure(-0.25)');
    }

    return {
        measurement,
        score: clampScore(score),
        scoreBreakdown,
    };
}

function resolveMeasurementCandidates({
    candidates,
    measurementDate,
    glossaryLookup,
}: {
    candidates: BloodworkMeasurement[];
    measurementDate: string;
    glossaryLookup: Map<string, BloodworkGlossaryEntry>;
}): MeasurementResolutionResult {
    const grouped = new Map<string, BloodworkMeasurement[]>();
    for (const candidate of candidates) {
        const key = buildMeasurementNameKey(candidate.name);
        if (!key) {
            continue;
        }
        const existing = grouped.get(key) ?? [];
        existing.push(candidate);
        grouped.set(key, existing);
    }

    const resolved: BloodworkMeasurement[] = [];
    const conflicts: MeasurementConflict[] = [];

    for (const [measurementNameKey, groupedCandidates] of grouped.entries()) {
        const dedupedByMeasurementKey = new Map<string, BloodworkMeasurement>();
        for (const candidate of groupedCandidates) {
            const key = buildMeasurementKey(candidate);
            if (!dedupedByMeasurementKey.has(key)) {
                dedupedByMeasurementKey.set(key, candidate);
            }
        }

        const scoredCandidates = Array.from(dedupedByMeasurementKey.values())
            .map(measurement => scoreMeasurementCandidate({
                measurement,
                glossaryLookup,
            }))
            .sort((left, right) => {
                const scoreCompare = right.score - left.score;
                if (scoreCompare !== 0) {
                    return scoreCompare;
                }
                return buildMeasurementKey(left.measurement).localeCompare(buildMeasurementKey(right.measurement));
            });

        const topCandidate = scoredCandidates[0];
        if (!topCandidate) {
            continue;
        }

        const secondCandidate = scoredCandidates[1];
        const margin = secondCandidate ? topCandidate.score - secondCandidate.score : 1;
        const hasContradiction = secondCandidate
            ? hasHighSeverityCandidateContradiction(topCandidate.measurement, secondCandidate.measurement)
            : false;

        const conflictReasons: string[] = [];
        if (topCandidate.score < RESOLUTION_MIN_CONFIDENCE) {
            conflictReasons.push(`top-score-below-threshold:${topCandidate.score.toFixed(2)}`);
        }
        if (secondCandidate && margin < RESOLUTION_MIN_MARGIN) {
            conflictReasons.push(`score-margin-below-threshold:${margin.toFixed(2)}`);
        }
        if (hasContradiction) {
            conflictReasons.push('high-severity-contradiction');
        }
        const needsReview = conflictReasons.length > 0;

        const selectedMeasurement = cloneMeasurement(topCandidate.measurement);
        selectedMeasurement.confidence = Number.parseFloat(topCandidate.score.toFixed(4));
        selectedMeasurement.reviewStatus = needsReview ? 'needs_review' : 'accepted';
        selectedMeasurement.provenance = dedupeProvenanceEntries(selectedMeasurement);

        const mergedDuplicateValues = dedupeDuplicateValues([
            ...(selectedMeasurement.duplicateValues?.map(cloneDuplicateValue) ?? []),
            ...scoredCandidates.slice(1).map(candidate => buildDuplicateValueFromCandidate({
                measurement: candidate.measurement,
                date: measurementDate,
            })),
        ]);
        if (mergedDuplicateValues.length > 0) {
            selectedMeasurement.duplicateValues = mergedDuplicateValues;
        } else {
            delete selectedMeasurement.duplicateValues;
        }

        if (needsReview) {
            selectedMeasurement.conflict = {
                reason: conflictReasons.join('; '),
                candidateCount: scoredCandidates.length,
            };
            conflicts.push({
                measurementNameKey,
                measurementName: selectedMeasurement.name,
                reason: conflictReasons.join('; '),
                candidateCount: scoredCandidates.length,
                recommendedCandidateIndex: 0,
                candidates: scoredCandidates.map(candidate => ({
                    score: Number.parseFloat(candidate.score.toFixed(4)),
                    scoreBreakdown: candidate.scoreBreakdown,
                    measurement: cloneMeasurement(candidate.measurement),
                })),
            });
        } else {
            delete selectedMeasurement.conflict;
        }

        resolved.push(selectedMeasurement);
    }

    return {
        measurements: resolved.sort((left, right) => left.name.localeCompare(right.name)),
        conflicts,
    };
}

function buildMeasurementNameKey(name: string): string {
    return normalizeTextForMatch(name).replace(/[^a-z0-9]+/g, ' ').trim();
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

        const tableLikeRows = extractTableLikeRows(pageText);
        if (tableLikeRows.length === 0) {
            continue;
        }

        extractedMeasurements.push(...parseMeasurementsFromTableLikeRows(tableLikeRows).map(measurement => ({
            ...measurement,
            provenance: dedupeProvenanceEntries({
                ...measurement,
                provenance: [{
                    extractor: 'layout_text',
                    page: pageIndex + 1,
                    rawName: measurement.originalName || measurement.name,
                    rawValue: measurement.value !== undefined ? String(measurement.value) : undefined,
                    rawUnit: measurement.unit,
                    rawRange: measurement.referenceRange
                        ? [measurement.referenceRange.min ?? '', measurement.referenceRange.max ?? ''].join('..')
                        : undefined,
                    confidence: 0.82,
                }],
            }),
        })));

        const prompt = buildMeasurementExtractionPrompt({
            sourcePath,
            pageText: tableLikeRows
                .map(row => (row.category ? `[Category: ${row.category}] ${row.line}` : row.line))
                .join('\n'),
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
            extractedMeasurements.push(...result.object.measurements.map(measurement => ({
                ...measurement,
                provenance: dedupeProvenanceEntries({
                    ...measurement,
                    provenance: [
                        ...(measurement.provenance ?? []),
                        {
                            extractor: 'llm_normalizer',
                            page: pageIndex + 1,
                            rawName: measurement.originalName || measurement.name,
                            rawValue: measurement.value !== undefined ? String(measurement.value) : undefined,
                            rawUnit: measurement.unit,
                            rawRange: measurement.referenceRange
                                ? [measurement.referenceRange.min ?? '', measurement.referenceRange.max ?? ''].join('..')
                                : undefined,
                            confidence: 0.66,
                        },
                    ],
                }),
            })));
        } catch {
            continue;
        }
    }

    return mergeUniqueMeasurements(extractedMeasurements);
}

function buildNameLookupKeys(measurement: BloodworkMeasurement): string[] {
    const keys = new Set<string>();
    const nameKey = buildMeasurementNameKey(measurement.name);
    if (nameKey) {
        keys.add(nameKey);
    }
    if (measurement.originalName) {
        const originalNameKey = buildMeasurementNameKey(measurement.originalName);
        if (originalNameKey) {
            keys.add(originalNameKey);
        }
    }
    return Array.from(keys.values());
}

function findSourceCandidateForNormalizedMeasurement({
    normalizedMeasurement,
    sourceCandidates,
}: {
    normalizedMeasurement: BloodworkMeasurement;
    sourceCandidates: BloodworkMeasurement[];
}): BloodworkMeasurement | null {
    const normalizedKeys = buildNameLookupKeys(normalizedMeasurement);
    for (const key of normalizedKeys) {
        const exact = sourceCandidates.find(candidate => buildNameLookupKeys(candidate).includes(key));
        if (exact) {
            return exact;
        }
    }

    const normalizedName = normalizeTextForMatch(normalizedMeasurement.name);
    if (!normalizedName) {
        return null;
    }
    for (const candidate of sourceCandidates) {
        const candidateName = normalizeTextForMatch(candidate.name);
        if (candidateName.includes(normalizedName) || normalizedName.includes(candidateName)) {
            return candidate;
        }
    }
    return null;
}

function projectStructuredFieldsFromSourceCandidate({
    normalizedMeasurement,
    sourceCandidate,
}: {
    normalizedMeasurement: BloodworkMeasurement;
    sourceCandidate: BloodworkMeasurement | null;
}): BloodworkMeasurement {
    if (!sourceCandidate) {
        return normalizedMeasurement;
    }

    return {
        ...normalizedMeasurement,
        value: sourceCandidate.value,
        unit: sourceCandidate.unit,
        referenceRange: sourceCandidate.referenceRange
            ? cloneReferenceRange(sourceCandidate.referenceRange)
            : undefined,
        flag: sourceCandidate.flag ?? normalizedMeasurement.flag,
        note: normalizedMeasurement.note ?? sourceCandidate.note,
        provenance: dedupeProvenanceEntries({
            ...normalizedMeasurement,
            provenance: [
                ...(sourceCandidate.provenance ?? []),
                ...(normalizedMeasurement.provenance ?? []),
            ],
        }),
    };
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
        return filterLikelyMeasurements(result.object.measurements.map(measurement =>
            projectStructuredFieldsFromSourceCandidate({
                normalizedMeasurement: measurement,
                sourceCandidate: findSourceCandidateForNormalizedMeasurement({
                    normalizedMeasurement: measurement,
                    sourceCandidates: filteredCandidates,
                }),
            })));
    } catch {
        return filteredCandidates;
    }
}

function extractTableLikeRowsFromPositionedTokenLines(
    tokenLines: Array<Array<{ x: number; token: string }>>,
): TableLikeRow[] {
    const selected = new Map<string, TableLikeRow>();
    let activeCategory: string | undefined;
    let pendingNamePrefix: string | undefined;

    for (const tokens of tokenLines) {
        if (tokens.length === 0) {
            continue;
        }
        const sortedTokens = [...tokens].sort((left, right) => left.x - right.x);
        const segments: string[] = [];
        let currentSegment = '';
        let previousX: number | null = null;
        for (const token of sortedTokens) {
            const gap = previousX === null ? 0 : token.x - previousX;
            if (previousX !== null && gap >= 26) {
                const trimmed = currentSegment.replace(/\s+/g, ' ').trim();
                if (trimmed) {
                    segments.push(trimmed);
                }
                currentSegment = token.token;
            } else {
                currentSegment = currentSegment ? `${currentSegment} ${token.token}` : token.token;
            }
            previousX = token.x;
        }
        const finalSegment = currentSegment.replace(/\s+/g, ' ').trim();
        if (finalSegment) {
            segments.push(finalSegment);
        }
        if (segments.length === 0) {
            continue;
        }

        const wholeLine = segments.join(' ').trim();
        if (!wholeLine || wholeLine.length > 220) {
            continue;
        }
        if (isLikelyCategoryHeading(wholeLine)) {
            activeCategory = normalizeCategoryNameForGlossary(wholeLine);
            pendingNamePrefix = undefined;
            continue;
        }

        const hasAnyValue = segments.slice(1).some(segment => VALUE_TOKEN_PATTERN.test(segment));
        if (!hasAnyValue && segments.length === 1) {
            const maybePrefix = segments[0];
            if (
                maybePrefix.length <= 80 &&
                !/\d/.test(maybePrefix) &&
                LIKELY_ANALYTE_NAME_PATTERN.test(normalizeTextForMatch(maybePrefix))
            ) {
                pendingNamePrefix = maybePrefix;
            }
            continue;
        }

        if (segments.length < 2) {
            continue;
        }

        let namePart = segments[0]!;
        if (pendingNamePrefix) {
            namePart = `${pendingNamePrefix} ${namePart}`.replace(/\s+/g, ' ').trim();
            pendingNamePrefix = undefined;
        }
        if (!namePart || namePart.length > 120) {
            continue;
        }
        const tail = segments.slice(1).join(' ');
        if (!VALUE_TOKEN_PATTERN.test(tail)) {
            continue;
        }

        const line = [namePart, ...segments.slice(1)].join(' | ');
        const dedupeKey = `${line}|${normalizeCategoryNameKey(activeCategory || '')}`;
        if (!selected.has(dedupeKey)) {
            selected.set(dedupeKey, {
                line,
                category: activeCategory,
            });
        }
    }

    return Array.from(selected.values());
}

async function extractLayoutMeasurementsFromPdfBytes(pdfBytes: Uint8Array): Promise<BloodworkMeasurement[]> {
    const document = await getDocument({ data: pdfBytes }).promise;
    const measurements: BloodworkMeasurement[] = [];

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

        const tokenLines = Array.from(lines.entries())
            .sort((left, right) => right[0] - left[0])
            .map(([, lineTokens]) => lineTokens.sort((left, right) => left.x - right.x));
        const rows = extractTableLikeRowsFromPositionedTokenLines(tokenLines);
        const parsed = parseMeasurementsFromTableLikeRows(rows).map(measurement => ({
            ...measurement,
            provenance: dedupeProvenanceEntries({
                ...measurement,
                provenance: [{
                    extractor: 'layout_text',
                    page: pageIndex,
                    rawName: measurement.originalName || measurement.name,
                    rawValue: measurement.value !== undefined ? String(measurement.value) : undefined,
                    rawUnit: measurement.unit,
                    rawRange: measurement.referenceRange
                        ? [measurement.referenceRange.min ?? '', measurement.referenceRange.max ?? ''].join('..')
                        : undefined,
                    confidence: 0.9,
                }],
            }),
        }));
        measurements.push(...parsed);
    }

    return filterLikelyMeasurements(measurements);
}

async function extractMeasurementsWithTextract({
    textractClient,
    pdfBytes,
}: {
    textractClient: TextractClient;
    pdfBytes: Uint8Array;
}): Promise<BloodworkMeasurement[]> {
    const response = await textractClient.send(new AnalyzeDocumentCommand({
        Document: {
            Bytes: pdfBytes,
        },
        FeatureTypes: ['TABLES', 'FORMS'],
    }));
    const blocks = response.Blocks ?? [];
    const lineBlocks = blocks
        .filter(block => block.BlockType === 'LINE' && block.Text && block.Page)
        .map(block => ({
            page: block.Page!,
            text: block.Text!.trim(),
            top: block.Geometry?.BoundingBox?.Top ?? 0,
            confidence: block.Confidence ?? 0,
        }))
        .filter(block => block.text.length > 0);

    const pageTexts = new Map<number, Array<{ text: string; top: number; confidence: number }>>();
    for (const line of lineBlocks) {
        const existing = pageTexts.get(line.page) ?? [];
        existing.push(line);
        pageTexts.set(line.page, existing);
    }

    const measurements: BloodworkMeasurement[] = [];
    for (const [page, lines] of pageTexts.entries()) {
        const sortedLines = lines.sort((left, right) => left.top - right.top);
        const text = sortedLines.map(item => item.text).join('\n');
        const parsed = parseMeasurementsFromTableLikeRows(extractTableLikeRows(text));
        const averageConfidence = sortedLines.reduce((sum, line) => sum + line.confidence, 0) / sortedLines.length / 100;
        measurements.push(...parsed.map(measurement => ({
            ...measurement,
            provenance: dedupeProvenanceEntries({
                ...measurement,
                provenance: [{
                    extractor: 'textract',
                    page,
                    rawName: measurement.originalName || measurement.name,
                    rawValue: measurement.value !== undefined ? String(measurement.value) : undefined,
                    rawUnit: measurement.unit,
                    rawRange: measurement.referenceRange
                        ? [measurement.referenceRange.min ?? '', measurement.referenceRange.max ?? ''].join('..')
                        : undefined,
                    confidence: clampScore(averageConfidence),
                }],
            }),
        })));
    }

    return filterLikelyMeasurements(measurements);
}

function shouldTriggerTextractFallback({
    layoutMeasurements,
    pageTexts,
}: {
    layoutMeasurements: BloodworkMeasurement[];
    pageTexts: string[];
}): boolean {
    const pagesWithNumbers = pageTexts.filter(page => /\d/.test(page)).length;
    if (pagesWithNumbers === 0) {
        return false;
    }
    if (layoutMeasurements.length >= TEXTRACT_MIN_LAYOUT_CANDIDATES) {
        return false;
    }
    return true;
}

async function generateLabObject({
    openRouterApiKey,
    modelIds,
    pdfPath,
    extractedText,
    pageTexts,
    glossary,
    pdfBytes,
    textractClient,
}: {
    openRouterApiKey: string;
    modelIds: string[];
    pdfPath: string;
    extractedText: string;
    pageTexts: string[];
    glossary: BloodworkGlossary;
    pdfBytes: Uint8Array;
    textractClient: TextractClient | null;
}): Promise<{ lab: BloodworkLab; modelId: string; conflicts: MeasurementConflict[] }> {
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
    const extractedDates = extractDateCandidatesFromText(extractedText);
    const collectionDate = extractedDates.collectionDate ?? metadata.collectionDate;
    const reportedDate = extractedDates.reportedDate ?? metadata.reportedDate;
    const receivedDate = extractedDates.receivedDate ?? metadata.receivedDate;
    const fallbackDate = metadata.date ?? inferredMetadata.date;
    if (!fallbackDate) {
        throw new Error(`Unable to determine canonical date for ${pdfPath}`);
    }
    const canonicalDate = resolveCanonicalLabDate({
        collectionDate,
        reportedDate,
        receivedDate,
        fallbackDate,
    });
    metadata = {
        ...metadata,
        date: canonicalDate,
        collectionDate,
        reportedDate,
        receivedDate,
    };

    const candidateMeasurements: BloodworkMeasurement[] = [];
    try {
        const layoutMeasurements = await extractLayoutMeasurementsFromPdfBytes(pdfBytes);
        candidateMeasurements.push(...layoutMeasurements);
        if (textractClient && shouldTriggerTextractFallback({
            layoutMeasurements,
            pageTexts,
        })) {
            try {
                const textractMeasurements = await extractMeasurementsWithTextract({
                    textractClient,
                    pdfBytes,
                });
                candidateMeasurements.push(...textractMeasurements);
            } catch {
                // textract fallback is best-effort and should not fail the import flow.
            }
        }
    } catch {
        // Keep pipeline resilient and continue with downstream extraction methods.
    }

    const cardStyleMeasurements = filterLikelyMeasurements(extractCardStyleMeasurements(pageTexts));
    candidateMeasurements.push(...cardStyleMeasurements);

    let modelMeasurements: BloodworkMeasurement[] = [];
    try {
        const extractedMeasurements = await extractMeasurementsFromPages({
            provider,
            modelIds,
            sourcePath: pdfPath,
            pageTexts,
        });
        modelMeasurements = await normalizeMeasurementsWithModel({
            provider,
            modelIds,
            sourcePath: pdfPath,
            extractedText,
            candidates: extractedMeasurements,
        });
    } catch {
        modelMeasurements = [];
    }
    candidateMeasurements.push(...modelMeasurements);

    if (candidateMeasurements.length === 0) {
        const heuristicFallback = heuristicExtractMeasurements(pageTexts);
        const narrativeFallback = extractNarrativeResultMeasurements(pageTexts);
        candidateMeasurements.push(...filterLikelyMeasurements([
            ...heuristicFallback,
            ...cardStyleMeasurements,
            ...narrativeFallback,
        ]));
    }

    let measurements = standardizeMeasurementUnits(filterLikelyMeasurements(candidateMeasurements));

    measurements = await applyGlossaryValidationToMeasurements({
        provider,
        glossary,
        sourcePath: pdfPath,
        primaryModelIds: modelIds,
        measurements,
    });

    measurements = standardizeMeasurementUnits(measurements);
    const glossaryLookup = buildGlossaryLookup(glossary);
    const resolved = resolveMeasurementCandidates({
        candidates: measurements,
        measurementDate: metadata.date || fallbackDate,
        glossaryLookup,
    });
    measurements = resolved.measurements;

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
        conflicts: resolved.conflicts,
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

function dateDifferenceInDays(leftDate: string, rightDate: string): number {
    const [leftYear, leftMonth, leftDay] = leftDate.split('-').map(part => Number.parseInt(part, 10));
    const [rightYear, rightMonth, rightDay] = rightDate.split('-').map(part => Number.parseInt(part, 10));
    const leftTimestamp = Date.UTC(leftYear!, (leftMonth ?? 1) - 1, leftDay!);
    const rightTimestamp = Date.UTC(rightYear!, (rightMonth ?? 1) - 1, rightDay!);
    return Math.abs(Math.round((leftTimestamp - rightTimestamp) / (24 * 60 * 60 * 1000)));
}

function cloneReferenceRange(
    referenceRange: BloodworkMeasurement['referenceRange'],
): BloodworkMeasurement['referenceRange'] {
    if (!referenceRange) {
        return undefined;
    }
    const nextRange: NonNullable<BloodworkMeasurement['referenceRange']> = {};
    if (referenceRange.min !== undefined) {
        nextRange.min = referenceRange.min;
    }
    if (referenceRange.max !== undefined) {
        nextRange.max = referenceRange.max;
    }
    if (nextRange.min === undefined && nextRange.max === undefined) {
        return undefined;
    }
    return nextRange;
}

function cloneMeasurementOriginal(
    original: BloodworkMeasurement['original'],
): BloodworkMeasurement['original'] {
    if (!original) {
        return undefined;
    }
    const nextOriginal: NonNullable<BloodworkMeasurement['original']> = {};
    if (original.value !== undefined) {
        nextOriginal.value = original.value;
    }
    if (original.unit !== undefined) {
        nextOriginal.unit = original.unit;
    }
    const originalRange = cloneReferenceRange(original.referenceRange);
    if (originalRange) {
        nextOriginal.referenceRange = originalRange;
    }
    if (
        nextOriginal.value === undefined &&
        nextOriginal.unit === undefined &&
        nextOriginal.referenceRange === undefined
    ) {
        return undefined;
    }
    return nextOriginal;
}

function cloneDuplicateValue(
    value: BloodworkMeasurementDuplicateValue,
): BloodworkMeasurementDuplicateValue {
    const cloned: BloodworkMeasurementDuplicateValue = {
        date: value.date,
    };
    if (value.value !== undefined) {
        cloned.value = value.value;
    }
    if (value.unit !== undefined) {
        cloned.unit = value.unit;
    }
    const range = cloneReferenceRange(value.referenceRange);
    if (range) {
        cloned.referenceRange = range;
    }
    if (value.flag !== undefined) {
        cloned.flag = value.flag;
    }
    if (value.note !== undefined) {
        cloned.note = value.note;
    }
    if (value.sourceFile !== undefined) {
        cloned.sourceFile = value.sourceFile;
    }
    if (value.sourceLabName !== undefined) {
        cloned.sourceLabName = value.sourceLabName;
    }
    if (value.importLocation !== undefined) {
        cloned.importLocation = value.importLocation;
    }
    return cloned;
}

function cloneMeasurement(measurement: BloodworkMeasurement): BloodworkMeasurement {
    const cloned: BloodworkMeasurement = {
        name: measurement.name,
    };
    if (measurement.originalName !== undefined) {
        cloned.originalName = measurement.originalName;
    }
    if (measurement.category !== undefined) {
        cloned.category = measurement.category;
    }
    if (measurement.value !== undefined) {
        cloned.value = measurement.value;
    }
    if (measurement.unit !== undefined) {
        cloned.unit = measurement.unit;
    }
    const range = cloneReferenceRange(measurement.referenceRange);
    if (range) {
        cloned.referenceRange = range;
    }
    const original = cloneMeasurementOriginal(measurement.original);
    if (original) {
        cloned.original = original;
    }
    if (measurement.flag !== undefined) {
        cloned.flag = measurement.flag;
    }
    if (measurement.note !== undefined) {
        cloned.note = measurement.note;
    }
    if (measurement.notes !== undefined) {
        cloned.notes = measurement.notes;
    }
    if (measurement.duplicateValues && measurement.duplicateValues.length > 0) {
        cloned.duplicateValues = measurement.duplicateValues.map(cloneDuplicateValue);
    }
    return cloned;
}

function buildDuplicateValueKey(value: BloodworkMeasurementDuplicateValue): string {
    const rawValue = value.value;
    const valuePart =
        rawValue === undefined || rawValue === null
            ? ''
            : typeof rawValue === 'number'
                ? rawValue.toString()
                : rawValue.trim().toLowerCase();
    const rangePart = value.referenceRange
        ? [
            value.referenceRange.min?.toString() ?? '',
            value.referenceRange.max?.toString() ?? '',
        ].join('|')
        : '';
    return [
        value.date,
        valuePart,
        value.unit?.trim().toLowerCase() ?? '',
        rangePart,
        value.flag ?? '',
        value.note?.trim().toLowerCase() ?? '',
        value.sourceFile?.trim().toLowerCase() ?? '',
        value.sourceLabName?.trim().toLowerCase() ?? '',
        value.importLocation?.trim().toLowerCase() ?? '',
    ].join('|');
}

function dedupeDuplicateValues(values: BloodworkMeasurementDuplicateValue[]): BloodworkMeasurementDuplicateValue[] {
    const deduped = new Map<string, BloodworkMeasurementDuplicateValue>();
    for (const value of values) {
        const key = buildDuplicateValueKey(value);
        if (!deduped.has(key)) {
            deduped.set(key, cloneDuplicateValue(value));
        }
    }
    return Array.from(deduped.values()).sort((left, right) => {
        const dateCompare = left.date.localeCompare(right.date);
        if (dateCompare !== 0) {
            return dateCompare;
        }
        const sourceCompare = (left.sourceFile ?? '').localeCompare(right.sourceFile ?? '');
        if (sourceCompare !== 0) {
            return sourceCompare;
        }
        return buildDuplicateValueKey(left).localeCompare(buildDuplicateValueKey(right));
    });
}

function buildDuplicateValueFromMeasurement({
    measurement,
    source,
}: {
    measurement: BloodworkMeasurement;
    source: BloodworkDataFile;
}): BloodworkMeasurementDuplicateValue {
    const duplicateValue: BloodworkMeasurementDuplicateValue = {
        date: source.lab.date,
    };
    if (measurement.value !== undefined) {
        duplicateValue.value = measurement.value;
    }
    if (measurement.unit !== undefined) {
        duplicateValue.unit = measurement.unit;
    }
    const range = cloneReferenceRange(measurement.referenceRange);
    if (range) {
        duplicateValue.referenceRange = range;
    }
    if (measurement.flag !== undefined) {
        duplicateValue.flag = measurement.flag;
    }
    const measurementNote = measurement.note?.trim() || measurement.notes?.trim();
    if (measurementNote) {
        duplicateValue.note = measurementNote;
    }
    duplicateValue.sourceFile = source.fileName;
    duplicateValue.sourceLabName = source.lab.labName;
    if (source.lab.importLocation) {
        duplicateValue.importLocation = source.lab.importLocation;
    }
    return duplicateValue;
}

function buildMergedFromKey(entry: BloodworkMergedSource): string {
    return [
        entry.fileName.trim().toLowerCase(),
        entry.date,
        entry.labName.trim().toLowerCase(),
        entry.importLocation?.trim().toLowerCase() ?? '',
    ].join('|');
}

function dedupeMergedFromEntries(entries: BloodworkMergedSource[]): BloodworkMergedSource[] {
    const deduped = new Map<string, BloodworkMergedSource>();
    for (const entry of entries) {
        const key = buildMergedFromKey(entry);
        if (!deduped.has(key)) {
            deduped.set(key, {
                fileName: entry.fileName,
                date: entry.date,
                labName: entry.labName,
                importLocation: entry.importLocation,
                measurementCount: entry.measurementCount,
            });
        }
    }
    return Array.from(deduped.values()).sort((left, right) => {
        const dateCompare = left.date.localeCompare(right.date);
        if (dateCompare !== 0) {
            return dateCompare;
        }
        return left.fileName.localeCompare(right.fileName);
    });
}

function buildMergedSourceFromFile(source: BloodworkDataFile): BloodworkMergedSource {
    return {
        fileName: source.fileName,
        date: source.lab.date,
        labName: source.lab.labName,
        importLocation: source.lab.importLocation,
        measurementCount: source.lab.measurements.length,
    };
}

function collectMergedFromEntries(group: BloodworkDataFile[]): BloodworkMergedSource[] {
    const entries: BloodworkMergedSource[] = [];
    for (const source of group) {
        if (source.lab.mergedFrom && source.lab.mergedFrom.length > 0) {
            entries.push(...source.lab.mergedFrom.map(entry => ({
                fileName: entry.fileName,
                date: entry.date,
                labName: entry.labName,
                importLocation: entry.importLocation,
                measurementCount: entry.measurementCount,
            })));
            continue;
        }
        entries.push(buildMergedSourceFromFile(source));
    }
    return dedupeMergedFromEntries(entries);
}

function compareSourceFreshness(left: BloodworkDataFile, right: BloodworkDataFile): number {
    const dateCompare = left.lab.date.localeCompare(right.lab.date);
    if (dateCompare !== 0) {
        return dateCompare;
    }
    const measurementCountCompare = left.lab.measurements.length - right.lab.measurements.length;
    if (measurementCountCompare !== 0) {
        return measurementCountCompare;
    }
    return left.fileName.localeCompare(right.fileName);
}

function pickLatestDefinedText(values: Array<string | undefined>): string | undefined {
    for (let index = values.length - 1; index >= 0; index--) {
        const value = values[index]?.trim();
        if (value) {
            return value;
        }
    }
    return undefined;
}

function pickLatestDefinedNumber(values: Array<number | undefined>): number | undefined {
    for (let index = values.length - 1; index >= 0; index--) {
        const value = values[index];
        if (value !== undefined) {
            return value;
        }
    }
    return undefined;
}

function pickLatestDefinedBoolean(values: Array<boolean | undefined>): boolean | undefined {
    for (let index = values.length - 1; index >= 0; index--) {
        const value = values[index];
        if (value !== undefined) {
            return value;
        }
    }
    return undefined;
}

function mergeNotes(group: BloodworkDataFile[]): string | undefined {
    const byNormalizedValue = new Map<string, string>();
    for (const source of group) {
        const note = source.lab.notes?.trim();
        if (!note) {
            continue;
        }
        const key = note.toLowerCase();
        if (!byNormalizedValue.has(key)) {
            byNormalizedValue.set(key, note);
        }
    }
    if (byNormalizedValue.size === 0) {
        return undefined;
    }
    return Array.from(byNormalizedValue.values()).join('\n\n');
}

function scoreConsolidationMeasurement({
    measurement,
}: {
    measurement: BloodworkMeasurement;
}): number {
    let score = measurement.confidence ?? 0;
    if (measurement.referenceRange) {
        score += 0.08;
    }
    if (measurement.unit && UNIT_TOKEN_PATTERN.test(measurement.unit)) {
        score += 0.08;
    }
    if (measurement.reviewStatus === 'accepted') {
        score += 0.08;
    }
    if (measurement.reviewStatus === 'needs_review') {
        score -= 0.12;
    }
    if (measurement.conflict) {
        score -= 0.1;
    }
    if (typeof measurement.value === 'number' && Number.isFinite(measurement.value)) {
        score += 0.05;
    }
    return score;
}

function mergeMeasurementsForGroup(group: BloodworkDataFile[]): BloodworkMeasurement[] {
    const selected = new Map<string, ConsolidatedMeasurementSelection>();

    for (const source of group) {
        for (const sourceMeasurement of source.lab.measurements) {
            const measurement = cloneMeasurement(sourceMeasurement);
            const measurementKey = buildMeasurementNameKey(measurement.name);
            if (!measurementKey) {
                continue;
            }

            const incomingDuplicateValues = measurement.duplicateValues?.map(cloneDuplicateValue) ?? [];
            delete measurement.duplicateValues;

            const existing = selected.get(measurementKey);
            if (!existing) {
                selected.set(measurementKey, {
                    measurement,
                    source,
                    duplicateValues: dedupeDuplicateValues(incomingDuplicateValues),
                });
                continue;
            }

            const existingScore = scoreConsolidationMeasurement({
                measurement: existing.measurement,
            });
            const incomingScore = scoreConsolidationMeasurement({
                measurement,
            });
            const shouldReplace =
                incomingScore > existingScore ||
                (Math.abs(incomingScore - existingScore) <= 1e-9 && compareSourceFreshness(source, existing.source) > 0);

            const mergedDuplicateValues = dedupeDuplicateValues([
                ...existing.duplicateValues,
                buildDuplicateValueFromMeasurement(shouldReplace
                    ? {
                        measurement: existing.measurement,
                        source: existing.source,
                    }
                    : {
                        measurement,
                        source,
                    }),
                ...incomingDuplicateValues,
            ]);

            selected.set(measurementKey, {
                measurement: shouldReplace ? measurement : existing.measurement,
                source: shouldReplace ? source : existing.source,
                duplicateValues: mergedDuplicateValues,
            });
        }
    }

    const mergedMeasurements = Array.from(selected.values())
        .map(({ measurement, duplicateValues }) => {
            if (duplicateValues.length === 0) {
                return measurement;
            }
            return {
                ...measurement,
                duplicateValues,
            };
        })
        .sort((left, right) => left.name.localeCompare(right.name));

    return mergeUniqueMeasurements(mergedMeasurements);
}

function listBloodworkDataFiles(outputDirectory: string): BloodworkDataFile[] {
    if (!fs.existsSync(outputDirectory)) {
        return [];
    }

    const files: BloodworkDataFile[] = [];
    for (const entry of fs.readdirSync(outputDirectory, { withFileTypes: true })) {
        if (!entry.isFile() || !/^bloodwork_.*\.json$/i.test(entry.name)) {
            continue;
        }

        const filePath = path.join(outputDirectory, entry.name);
        const raw = JSON.parse(fs.readFileSync(filePath, 'utf8')) as unknown;
        const parsed = bloodworkLabSchema.parse(raw);
        files.push({
            path: filePath,
            fileName: entry.name,
            lab: parsed,
        });
    }

    return files.sort((left, right) => left.fileName.localeCompare(right.fileName));
}

function groupBloodworkDataFilesByDateWindow(files: BloodworkDataFile[]): BloodworkDataFile[][] {
    if (files.length === 0) {
        return [];
    }

    const sorted = [...files].sort((left, right) => {
        const dateCompare = right.lab.date.localeCompare(left.lab.date);
        if (dateCompare !== 0) {
            return dateCompare;
        }
        return right.fileName.localeCompare(left.fileName);
    });

    const groups: BloodworkDataFile[][] = [];
    let currentGroup: BloodworkDataFile[] = [];
    let currentGroupLatestDate: string | null = null;

    for (const file of sorted) {
        if (!currentGroupLatestDate) {
            currentGroup = [file];
            currentGroupLatestDate = file.lab.date;
            continue;
        }

        if (dateDifferenceInDays(currentGroupLatestDate, file.lab.date) <= MERGE_WINDOW_DAYS) {
            currentGroup.push(file);
            continue;
        }

        groups.push(currentGroup);
        currentGroup = [file];
        currentGroupLatestDate = file.lab.date;
    }

    if (currentGroup.length > 0) {
        groups.push(currentGroup);
    }

    return groups.map(group =>
        group.sort((left, right) => {
            const dateCompare = left.lab.date.localeCompare(right.lab.date);
            if (dateCompare !== 0) {
                return dateCompare;
            }
            return left.fileName.localeCompare(right.fileName);
        }));
}

function mergeBloodworkDataFileGroup(group: BloodworkDataFile[]): {
    targetFileName: string;
    lab: BloodworkLab;
} {
    if (group.length === 0) {
        throw new Error('Cannot merge an empty bloodwork group');
    }

    const orderedGroup = [...group].sort((left, right) => {
        const dateCompare = left.lab.date.localeCompare(right.lab.date);
        if (dateCompare !== 0) {
            return dateCompare;
        }
        return left.fileName.localeCompare(right.fileName);
    });

    let primary = orderedGroup[0]!;
    for (const source of orderedGroup.slice(1)) {
        if (compareSourceFreshness(source, primary) > 0) {
            primary = source;
        }
    }

    const mergedFrom = collectMergedFromEntries(orderedGroup);
    const mergedLab = bloodworkLabSchema.parse({
        date: primary.lab.date,
        labName: primary.lab.labName,
        location: primary.lab.location ?? pickLatestDefinedText(orderedGroup.map(item => item.lab.location)),
        importLocation: primary.lab.importLocation ?? pickLatestDefinedText(orderedGroup.map(item => item.lab.importLocation)),
        importLocationIsInferred:
            primary.lab.importLocationIsInferred
                ?? pickLatestDefinedBoolean(orderedGroup.map(item => item.lab.importLocationIsInferred)),
        weightKg: primary.lab.weightKg ?? pickLatestDefinedNumber(orderedGroup.map(item => item.lab.weightKg)),
        measurements: mergeMeasurementsForGroup(orderedGroup),
        mergedFrom: mergedFrom.length > 1 ? mergedFrom : undefined,
        notes: mergeNotes(orderedGroup),
    });

    return {
        targetFileName: buildBloodworkFileName(mergedLab),
        lab: mergedLab,
    };
}

async function consolidateBloodworkDataFiles({
    outputDirectory,
    s3Client,
    s3Bucket,
    s3Prefix,
}: {
    outputDirectory: string;
    s3Client: S3Client | null;
    s3Bucket: string;
    s3Prefix: string;
}): Promise<ConsolidationSummary> {
    const sourceFiles = listBloodworkDataFiles(outputDirectory);
    const groups = groupBloodworkDataFilesByDateWindow(sourceFiles);
    const summary: ConsolidationSummary = {
        groupsProcessed: groups.length,
        mergedGroups: 0,
        filesBefore: sourceFiles.length,
        filesAfter: sourceFiles.length,
        writtenFiles: [],
        removedFiles: [],
        uploadedKeys: [],
        deletedKeys: [],
        groups: [],
    };

    if (groups.length === 0) {
        return summary;
    }

    const uploads = new Map<string, string>();
    const deletedFileNames = new Set<string>();

    for (const group of groups) {
        const { targetFileName, lab } = mergeBloodworkDataFileGroup(group);
        const targetPath = path.join(outputDirectory, targetFileName);
        const jsonPayload = `${JSON.stringify(lab, null, 4)}\n`;

        const currentPayload = fs.existsSync(targetPath)
            ? fs.readFileSync(targetPath, 'utf8')
            : null;
        if (currentPayload !== jsonPayload) {
            fs.writeFileSync(targetPath, jsonPayload, 'utf8');
            summary.writtenFiles.push(targetPath);
            uploads.set(targetFileName, jsonPayload);
        }

        if (group.length > 1) {
            summary.mergedGroups += 1;
        }
        summary.groups.push({
            targetFileName,
            latestDate: lab.date,
            sourceFileNames: group.map(item => item.fileName),
            sourceDates: group.map(item => item.lab.date),
        });

        for (const source of group) {
            if (source.fileName === targetFileName) {
                continue;
            }
            if (!fs.existsSync(source.path)) {
                continue;
            }
            fs.unlinkSync(source.path);
            summary.removedFiles.push(source.path);
            deletedFileNames.add(source.fileName);
        }
    }

    if (s3Client) {
        for (const [fileName, jsonPayload] of uploads.entries()) {
            const key = buildS3KeyFromFileName(fileName, s3Prefix);
            await s3Client.send(
                new PutObjectCommand({
                    Bucket: s3Bucket,
                    Key: key,
                    Body: jsonPayload,
                    ContentType: 'application/json; charset=utf-8',
                }),
            );
            summary.uploadedKeys.push(key);
        }

        for (const removedFileName of deletedFileNames) {
            const key = buildS3KeyFromFileName(removedFileName, s3Prefix);
            await s3Client.send(
                new DeleteObjectCommand({
                    Bucket: s3Bucket,
                    Key: key,
                }),
            );
            summary.deletedKeys.push(key);
        }
    }

    summary.filesAfter = listBloodworkDataFiles(outputDirectory).length;
    return summary;
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

function resolveAwsCredentialsForClients(): {
    region: string;
    accessKeyId: string;
    secretAccessKey: string;
    sessionToken?: string;
} {
    const region = process.env.AWS_REGION?.trim() || process.env.AWS_DEFAULT_REGION?.trim();
    if (!region) {
        throw new Error('Missing required environment variable: AWS_REGION (or AWS_DEFAULT_REGION)');
    }
    const accessKeyId = requireEnv('AWS_ACCESS_KEY_ID');
    const secretAccessKey = requireEnv('AWS_SECRET_ACCESS_KEY');
    const sessionToken = process.env.AWS_SESSION_TOKEN?.trim() || undefined;
    return {
        region,
        accessKeyId,
        secretAccessKey,
        sessionToken,
    };
}

function buildReviewReportFileName(pdfPath: string): string {
    const baseName = path.basename(pdfPath, path.extname(pdfPath));
    return `review_${slugifyForPath(baseName)}_${Date.now()}.json`;
}

function writeReviewReport({
    reportDir,
    pdfPath,
    suggestedOutputFileName,
    conflicts,
    labDraft,
}: {
    reportDir: string;
    pdfPath: string;
    suggestedOutputFileName: string;
    conflicts: MeasurementConflict[];
    labDraft: BloodworkLab;
}): string {
    fs.mkdirSync(reportDir, { recursive: true });
    const reportPath = path.join(reportDir, buildReviewReportFileName(pdfPath));
    const report: ReviewReport = {
        version: 1,
        generatedAt: new Date().toISOString(),
        sourcePdfPath: pdfPath,
        suggestedOutputFileName,
        unresolvedCount: conflicts.length,
        conflicts,
        labDraft,
    };
    fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 4)}\n`, 'utf8');
    return reportPath;
}

function applyReviewReportSelectionsToLab(report: z.infer<typeof reviewReportSchema>): BloodworkLab {
    const selectedByNameKey = new Map<string, BloodworkMeasurement>();
    for (const conflict of report.conflicts) {
        const selectedIndex = conflict.selectedCandidateIndex ?? conflict.recommendedCandidateIndex;
        const selectedCandidate = conflict.candidates[selectedIndex];
        if (!selectedCandidate) {
            throw new Error(
                `Invalid selected candidate index ${selectedIndex} for ${conflict.measurementName}`,
            );
        }
        const acceptedMeasurement = cloneMeasurement(selectedCandidate.measurement);
        acceptedMeasurement.reviewStatus = 'accepted';
        delete acceptedMeasurement.conflict;
        selectedByNameKey.set(conflict.measurementNameKey, acceptedMeasurement);
    }

    const nextMeasurements = report.labDraft.measurements.map(measurement => {
        const key = buildMeasurementNameKey(measurement.name);
        const selected = selectedByNameKey.get(key);
        if (!selected) {
            const clone = cloneMeasurement(measurement);
            if (clone.reviewStatus === 'needs_review') {
                clone.reviewStatus = 'accepted';
            }
            delete clone.conflict;
            return clone;
        }
        return selected;
    });

    return bloodworkLabSchema.parse({
        ...report.labDraft,
        reviewSummary: {
            unresolvedCount: 0,
        },
        measurements: nextMeasurements,
    });
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
    allowUnresolved,
    reviewReportDir,
    textractClient,
}: {
    pdfPath: string;
    openRouterApiKey: string;
    modelIds: string[];
    s3Client: S3Client | null;
    s3Bucket: string;
    s3Prefix: string;
    glossary: BloodworkGlossary;
    glossaryPath: string;
    allowUnresolved: boolean;
    reviewReportDir: string;
    textractClient: TextractClient | null;
}): Promise<ImportResult> {
    const pdfBytes = new Uint8Array(await Bun.file(pdfPath).arrayBuffer());
    assertPdfSignature(pdfBytes, pdfPath);
    const glossaryForFile = bloodworkGlossarySchema.parse(structuredClone(glossary));

    const extracted = await extractPdfText(pdfPath, pdfBytes);
    const { lab, modelId, conflicts } = await generateLabObject({
        openRouterApiKey,
        modelIds,
        pdfPath,
        extractedText: extracted.fullText,
        pageTexts: extracted.pageTexts,
        glossary: glossaryForFile,
        pdfBytes,
        textractClient,
    });

    const outputFileName = resolveOutputFileName({
        lab,
        sourcePath: pdfPath,
    });
    const unresolvedCount = conflicts.length;
    let reviewReportPath: string | undefined;

    if (unresolvedCount > 0) {
        reviewReportPath = writeReviewReport({
            reportDir: reviewReportDir,
            pdfPath,
            suggestedOutputFileName: outputFileName,
            conflicts,
            labDraft: bloodworkLabSchema.parse({
                ...lab,
                reviewSummary: {
                    unresolvedCount,
                },
            }),
        });
        if (!allowUnresolved) {
            throw new Error(
                [
                    `Unresolved measurement conflicts detected for ${pdfPath}`,
                    `Review report: ${reviewReportPath}`,
                    'Resolve candidate selections and rerun with --approve-review, or pass --allow-unresolved to write anyway.',
                ].join('\n'),
            );
        }
    }

    const labToWrite = bloodworkLabSchema.parse({
        ...lab,
        reviewSummary:
            unresolvedCount > 0
                ? {
                    unresolvedCount,
                    reportFile: reviewReportPath,
                }
                : undefined,
    });

    fs.mkdirSync(DEFAULT_OUTPUT_DIRECTORY, { recursive: true });
    const outputPath = path.join(DEFAULT_OUTPUT_DIRECTORY, outputFileName);
    const jsonPayload = JSON.stringify(labToWrite, null, 4);
    await Bun.write(outputPath, jsonPayload);

    const s3Key = await maybeUploadToS3({
        s3Client,
        s3Bucket,
        s3Prefix,
        fileName: outputFileName,
        jsonPayload,
    });

    glossary.version = glossaryForFile.version;
    glossary.entries = glossaryForFile.entries;
    saveBloodworkGlossary(glossaryPath, glossary);

    return {
        outputPath,
        s3Key,
        modelId,
        reviewReportPath,
        unresolvedCount,
    };
}

async function approveReviewReport({
    reviewReportPath,
    s3Client,
    s3Bucket,
    s3Prefix,
}: {
    reviewReportPath: string;
    s3Client: S3Client | null;
    s3Bucket: string;
    s3Prefix: string;
}): Promise<ImportResult> {
    if (!fs.existsSync(reviewReportPath)) {
        throw new Error(`Review report file does not exist: ${reviewReportPath}`);
    }

    const raw = JSON.parse(fs.readFileSync(reviewReportPath, 'utf8')) as unknown;
    const report = reviewReportSchema.parse(raw);
    const resolvedLab = applyReviewReportSelectionsToLab(report);

    fs.mkdirSync(DEFAULT_OUTPUT_DIRECTORY, { recursive: true });
    const outputFileName = resolveOutputFileName({
        lab: resolvedLab,
        sourcePath: report.sourcePdfPath,
    });
    const outputPath = path.join(DEFAULT_OUTPUT_DIRECTORY, outputFileName);
    const jsonPayload = JSON.stringify(resolvedLab, null, 4);
    await Bun.write(outputPath, jsonPayload);

    const s3Key = await maybeUploadToS3({
        s3Client,
        s3Bucket,
        s3Prefix,
        fileName: outputFileName,
        jsonPayload,
    });

    return {
        outputPath,
        s3Key,
        modelId: 'review-approval',
        unresolvedCount: 0,
    };
}

function createTextractClientIfNeeded(options: {
    enableTextractFallback: boolean;
}): TextractClient | null {
    if (!options.enableTextractFallback) {
        return null;
    }
    const credentials = resolveAwsCredentialsForClients();
    return new TextractClient({
        region: credentials.region,
        credentials: {
            accessKeyId: credentials.accessKeyId,
            secretAccessKey: credentials.secretAccessKey,
            sessionToken: credentials.sessionToken,
        },
    });
}

function createS3ClientIfNeeded(options: {
    skipUpload: boolean;
}): { s3Client: S3Client | null; s3Bucket: string; s3Prefix: string } {
    const s3Bucket = process.env.VITALS_S3_BUCKET?.trim() || DEFAULT_S3_BUCKET;
    const s3Prefix = process.env.VITALS_S3_PREFIX?.trim() || DEFAULT_S3_PREFIX;

    if (options.skipUpload) {
        return { s3Client: null, s3Bucket, s3Prefix };
    }

    const credentials = resolveAwsCredentialsForClients();

    return {
        s3Client: new S3Client({
            region: credentials.region,
            credentials: {
                accessKeyId: credentials.accessKeyId,
                secretAccessKey: credentials.secretAccessKey,
                sessionToken: credentials.sessionToken,
            },
        }),
        s3Bucket,
        s3Prefix,
    };
}

async function runBloodworkImporter(argv: string[] = process.argv.slice(2)): Promise<void> {
    const options = parseCliOptions(argv);
    const { s3Client, s3Bucket, s3Prefix } = createS3ClientIfNeeded({
        skipUpload: options.skipUpload,
    });
    const textractClient = createTextractClientIfNeeded({
        enableTextractFallback: options.enableTextractFallback,
    });

    if (options.approveReviewPath) {
        const result = await approveReviewReport({
            reviewReportPath: options.approveReviewPath,
            s3Client,
            s3Bucket,
            s3Prefix,
        });
        if (result.outputPath) {
            console.info(`Wrote ${result.outputPath}`);
        }
        if (result.s3Key) {
            console.info(`Uploaded s3://${s3Bucket}/${result.s3Key}`);
        }
        return;
    }

    if (options.mergeExistingOnly) {
        const consolidation = await consolidateBloodworkDataFiles({
            outputDirectory: DEFAULT_OUTPUT_DIRECTORY,
            s3Client,
            s3Bucket,
            s3Prefix,
        });

        console.info(`Consolidated ${consolidation.filesBefore} file(s) into ${consolidation.filesAfter} file(s)`);
        console.info(`Merged groups: ${consolidation.mergedGroups}/${consolidation.groupsProcessed}`);
        if (consolidation.groups.length > 0) {
            for (const group of consolidation.groups) {
                if (group.sourceFileNames.length < 2) {
                    continue;
                }
                console.info(
                    [
                        `Merged ${group.sourceFileNames.length} files into ${group.targetFileName}`,
                        `latest date ${group.latestDate}`,
                        `sources: ${group.sourceFileNames.join(', ')}`,
                    ].join(' | '),
                );
            }
        }
        if (consolidation.uploadedKeys.length > 0) {
            console.info(`Uploaded ${consolidation.uploadedKeys.length} consolidated file(s)`);
        }
        if (consolidation.deletedKeys.length > 0) {
            console.info(`Deleted ${consolidation.deletedKeys.length} stale S3 object(s)`);
        }
        return;
    }

    const openRouterApiKey = requireEnv('OPENROUTER_API_KEY');
    const modelIds = resolveModelIds(options.modelIds);
    const files = resolveInputFiles(options);
    const glossaryPath = process.env.VITALS_BLOODWORK_GLOSSARY_PATH?.trim() || DEFAULT_GLOSSARY_PATH;
    const glossary = loadBloodworkGlossary(glossaryPath);

    console.info(`Importing ${files.length} file(s)`);
    console.info(`Model candidates: ${modelIds.join(', ')}`);
    console.info(`Review report directory: ${options.reviewReportDir}`);
    if (options.allowUnresolved) {
        console.info('Unresolved conflicts will still be written (--allow-unresolved)');
    }
    if (textractClient) {
        console.info('AWS Textract fallback is enabled (--textract-fallback)');
    }
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
                allowUnresolved: options.allowUnresolved,
                reviewReportDir: options.reviewReportDir,
                textractClient,
            });

            successCount += 1;
            if (result.outputPath) {
                console.info(`Wrote ${result.outputPath}`);
            }
            if ((result.unresolvedCount ?? 0) > 0 && result.reviewReportPath) {
                console.info(`Unresolved conflicts: ${result.unresolvedCount} (review report: ${result.reviewReportPath})`);
            }
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

    const consolidation = await consolidateBloodworkDataFiles({
        outputDirectory: DEFAULT_OUTPUT_DIRECTORY,
        s3Client,
        s3Bucket,
        s3Prefix,
    });
    console.info(
        `Consolidated ${consolidation.filesBefore} file(s) into ${consolidation.filesAfter} file(s)`,
    );
    if (consolidation.mergedGroups > 0) {
        for (const group of consolidation.groups) {
            if (group.sourceFileNames.length < 2) {
                continue;
            }
            console.info(
                [
                    `Merged ${group.sourceFileNames.length} files into ${group.targetFileName}`,
                    `latest date ${group.latestDate}`,
                    `sources: ${group.sourceFileNames.join(', ')}`,
                ].join(' | '),
            );
        }
    }
    if (consolidation.uploadedKeys.length > 0) {
        console.info(`Uploaded ${consolidation.uploadedKeys.length} consolidated file(s)`);
    }
    if (consolidation.deletedKeys.length > 0) {
        console.info(`Deleted ${consolidation.deletedKeys.length} stale S3 object(s)`);
    }
}

export {
    parseCliOptions,
    resolveInputFiles,
    assertPdfSignature,
    normalizeModelOutput,
    filterLikelyMeasurements,
    standardizeMeasurementUnits,
    groupBloodworkDataFilesByDateWindow,
    mergeBloodworkDataFileGroup,
    isEnglishGlossaryName,
    normalizeGlossaryDecisionAction,
    resolveModelIds,
    parseNumericValueToken,
    extractDateCandidatesFromText,
    resolveCanonicalLabDate,
    resolveMeasurementCandidates,
    runBloodworkImporter,
};

if (import.meta.main) {
    createScript(async () => {
        await runBloodworkImporter();
    });
}
