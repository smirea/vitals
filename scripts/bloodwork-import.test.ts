import { describe, expect, test } from 'bun:test';

import {
    assertPdfSignature,
    extractDateCandidatesFromText,
    filterLikelyMeasurements,
    groupBloodworkDataFilesByDateWindow,
    isEnglishGlossaryName,
    mergeBloodworkDataFileGroup,
    normalizeGlossaryDecisionAction,
    parseNumericValueToken,
    parseCliOptions,
    resolveCanonicalLabDate,
    resolveMeasurementCandidates,
    resolveModelIds,
    standardizeMeasurementUnits,
} from './bloodwork-import.ts';
import { bloodworkLabSchema } from './bloodwork-schema.ts';

type MergeInputFile = Parameters<typeof groupBloodworkDataFilesByDateWindow>[0][number];

describe('parseCliOptions', () => {
    test('parses single file mode', () => {
        const options = parseCliOptions(['data/to-import/example.pdf', '--skip-upload']);
        expect(options.importAll).toBe(false);
        expect(options.inputPdfPath).toBe('data/to-import/example.pdf');
        expect(options.skipUpload).toBe(true);
    });

    test('parses bulk mode with options', () => {
        const options = parseCliOptions(['--all', '--continue-on-error', '--model', 'google/gemini-3-flash']);
        expect(options.importAll).toBe(true);
        expect(options.continueOnError).toBe(true);
        expect(options.modelIds).toEqual(['google/gemini-3-flash']);
    });

    test('parses merge-existing mode', () => {
        const options = parseCliOptions(['--merge-existing', '--skip-upload']);
        expect(options.mergeExistingOnly).toBe(true);
        expect(options.importAll).toBe(false);
        expect(options.inputPdfPath).toBeNull();
        expect(options.skipUpload).toBe(true);
    });

    test('parses review workflow flags', () => {
        const options = parseCliOptions([
            'data/to-import/example.pdf',
            '--allow-unresolved',
            '--review-report-dir',
            'tmp/review',
            '--textract-fallback',
        ]);
        expect(options.allowUnresolved).toBe(true);
        expect(options.enableTextractFallback).toBe(true);
        expect(options.reviewReportDir.endsWith('tmp/review')).toBe(true);
    });

    test('parses approve-review mode', () => {
        const options = parseCliOptions([
            '--approve-review',
            'data/review/report.json',
            '--skip-upload',
        ]);
        expect(options.approveReviewPath?.endsWith('data/review/report.json')).toBe(true);
        expect(options.inputPdfPath).toBeNull();
        expect(options.skipUpload).toBe(true);
    });

    test('rejects invalid combinations', () => {
        expect(() => parseCliOptions(['--all', 'file.pdf'])).toThrow();
        expect(() => parseCliOptions(['--continue-on-error', 'file.pdf'])).toThrow();
        expect(() => parseCliOptions([])).toThrow();
        expect(() => parseCliOptions(['--merge-existing', '--all'])).toThrow();
        expect(() => parseCliOptions(['--merge-existing', '--model', 'google/gemini-3-flash'])).toThrow();
        expect(() => parseCliOptions(['--approve-review', 'report.json', '--all'])).toThrow();
    });
});

describe('resolveModelIds', () => {
    test('uses cli model ids first', () => {
        expect(resolveModelIds(['custom/model'])).toEqual(['custom/model']);
    });

    test('normalizes and deduplicates model ids', () => {
        expect(resolveModelIds([' custom/model ', 'custom/model', 'other/model'])).toEqual([
            'custom/model',
            'other/model',
        ]);
    });

    test('defaults to gemini 3 flash preview when cli models are absent', () => {
        expect(resolveModelIds([])).toEqual(['google/gemini-3-flash-preview']);
    });
});

describe('normalizeGlossaryDecisionAction', () => {
    test('maps common alias and new-valid variants', () => {
        expect(normalizeGlossaryDecisionAction('alias')).toBe('alias');
        expect(normalizeGlossaryDecisionAction('existing_alias')).toBe('alias');
        expect(normalizeGlossaryDecisionAction('new_valid')).toBe('new_valid');
        expect(normalizeGlossaryDecisionAction('new-entry')).toBe('new_valid');
    });

    test('treats unknown actions as invalid', () => {
        expect(normalizeGlossaryDecisionAction('uncertain')).toBe('invalid');
    });
});

describe('assertPdfSignature', () => {
    test('accepts valid signature and rejects invalid signature', () => {
        const valid = new Uint8Array([0x25, 0x50, 0x44, 0x46, 0x2d]);
        const invalid = new Uint8Array([0x00, 0x01, 0x02, 0x03]);

        expect(() => assertPdfSignature(valid, '/tmp/ok.pdf')).not.toThrow();
        expect(() => assertPdfSignature(invalid, '/tmp/nope.pdf')).toThrow();
    });
});

describe('filterLikelyMeasurements', () => {
    test('removes non-analyte rows and keeps translated analytes', () => {
        const filtered = filterLikelyMeasurements([
            {
                name: 'Page',
                value: 1,
            },
            {
                name: 'Tel:',
                value: '030 / 443364-0',
            },
            {
                name: 'Leukozyten (EB)',
                value: 6.1,
                unit: 'Gpt/l',
            },
            {
                name: 'Comment: Canceled',
                value: 'Canceled',
            },
            {
                name: 'Hep B Core Ab, Tot',
                value: 'Negative',
            },
        ]);

        expect(filtered).toHaveLength(2);
        expect(filtered.map(item => item.name)).toEqual([
            'Leukocytes (EB)',
            'Hep B Core Ab, Tot',
        ]);
        expect(filtered[0]?.originalName).toBe('Leukozyten (EB)');
    });
});

describe('isEnglishGlossaryName', () => {
    test('accepts english analyte terms and rejects non-english names', () => {
        expect(isEnglishGlossaryName('Hemoglobin A1c')).toBe(true);
        expect(isEnglishGlossaryName('Upper Respiratory Culture')).toBe(true);
        expect(isEnglishGlossaryName('Hämatokrit')).toBe(false);
        expect(isEnglishGlossaryName('Leukozyten')).toBe(false);
    });
});

describe('standardizeMeasurementUnits', () => {
    test('converts glucose mmol/L to canonical mg/dL and captures original snapshot', () => {
        const [measurement] = standardizeMeasurementUnits([{
            name: 'Glucose',
            value: 6.1,
            unit: 'mmol/l',
            referenceRange: {
                min: 3.9,
                max: 5.5,
            },
        }]);

        expect(measurement?.unit).toBe('mg/dL');
        expect(typeof measurement?.value).toBe('number');
        expect(measurement?.value as number).toBeCloseTo(109.91102, 5);
        expect(measurement?.referenceRange?.min).toBeCloseTo(70.27098, 5);
        expect(measurement?.referenceRange?.max).toBeCloseTo(99.1001, 4);
        expect(measurement?.original).toEqual({
            value: 6.1,
            unit: 'mmol/L',
            referenceRange: {
                min: 3.9,
                max: 5.5,
            },
        });
    });

    test('converts hemoglobin a1c IFCC units to percent', () => {
        const [measurement] = standardizeMeasurementUnits([{
            name: 'Hemoglobin A1c',
            value: 39,
            unit: 'mmol/mol',
            referenceRange: {
                min: 20,
                max: 42,
            },
        }]);

        expect(measurement?.unit).toBe('%');
        expect(measurement?.value as number).toBeCloseTo(5.71972, 5);
        expect(measurement?.referenceRange?.min).toBeCloseTo(3.9816, 4);
        expect(measurement?.referenceRange?.max).toBeCloseTo(5.99416, 5);
        expect(measurement?.original?.unit).toBe('mmol/mol');
    });

    test('does not convert hemoglobin a1c from ambiguous mmol/L units', () => {
        const [measurement] = standardizeMeasurementUnits([{
            name: 'Hemoglobin A1c',
            value: 10.5,
            unit: 'mmol/L',
        }]);

        expect(measurement?.unit).toBe('mmol/L');
        expect(measurement?.value).toBe(10.5);
        expect(measurement?.original).toBeUndefined();
    });

    test('normalizes equivalent units without creating original snapshot', () => {
        const [measurement] = standardizeMeasurementUnits([{
            name: 'TSH',
            value: 2.1,
            unit: 'mU/l',
        }]);

        expect(measurement?.unit).toBe('uIU/mL');
        expect(measurement?.value).toBe(2.1);
        expect(measurement?.original).toBeUndefined();
    });

    test('does not force target unit when conversion needs numeric data but value is qualitative', () => {
        const [measurement] = standardizeMeasurementUnits([{
            name: 'Glucose',
            value: 'Negative',
            unit: 'mmol/l',
        }]);

        expect(measurement?.unit).toBe('mmol/L');
        expect(measurement?.value).toBe('Negative');
        expect(measurement?.original).toBeUndefined();
    });
});

describe('parseNumericValueToken', () => {
    test('rejects partial numeric fragments', () => {
        expect(parseNumericValueToken('1 90')).toBe('1 90');
        expect(parseNumericValueToken('abc 123')).toBe('abc 123');
    });

    test('accepts strict numeric tokens', () => {
        expect(parseNumericValueToken('190')).toBe(190);
        expect(parseNumericValueToken('190.5')).toBe(190.5);
    });
});

describe('extractDateCandidatesFromText + resolveCanonicalLabDate', () => {
    test('prefers collection date when present', () => {
        const text = [
            'Date/Time Collected Date Entered Reported',
            '2017-11-02 2017-11-03 2017-11-03',
            'Received on 11/03/2017',
        ].join('\n');
        const dates = extractDateCandidatesFromText(text);
        expect(dates.collectionDate).toBe('2017-11-02');
        expect(dates.reportedDate).toBe('2017-11-03');
        expect(dates.receivedDate).toBe('2017-11-03');
        expect(resolveCanonicalLabDate({
            collectionDate: dates.collectionDate,
            reportedDate: dates.reportedDate,
            receivedDate: dates.receivedDate,
            fallbackDate: '2017-11-09',
        })).toBe('2017-11-02');
    });
});

describe('resolveMeasurementCandidates', () => {
    test('selects higher confidence chol/hdl candidate and keeps alternate in duplicateValues', () => {
        const result = resolveMeasurementCandidates({
            candidates: [
                {
                    name: 'Cholesterol/HDL Ratio',
                    value: 2,
                    referenceRange: {
                        max: 100,
                    },
                },
                {
                    name: 'Cholesterol/HDL Ratio',
                    value: 6.4,
                    unit: 'calc',
                    referenceRange: {
                        max: 5,
                    },
                },
            ],
            measurementDate: '2017-11-02',
            glossaryLookup: new Map(),
        });

        expect(result.measurements).toHaveLength(1);
        expect(result.measurements[0]?.value).toBe(6.4);
        expect(result.measurements[0]?.duplicateValues?.[0]?.value).toBe(2);
    });

    test('marks low-margin ties as needs_review', () => {
        const result = resolveMeasurementCandidates({
            candidates: [
                {
                    name: 'WBC',
                    value: 7.2,
                    unit: 'Thous/mcL',
                    referenceRange: {
                        min: 3.8,
                        max: 10.8,
                    },
                },
                {
                    name: 'WBC',
                    value: 7.1,
                    unit: 'Thous/mcL',
                    referenceRange: {
                        min: 3.8,
                        max: 10.8,
                    },
                },
            ],
            measurementDate: '2026-01-20',
            glossaryLookup: new Map(),
        });

        expect(result.measurements[0]?.reviewStatus).toBe('needs_review');
        expect(result.conflicts.length).toBeGreaterThan(0);
    });
});

function makeMergeInputFile(input: {
    fileName: string;
    date: string;
    labName: string;
    measurements: Array<Record<string, unknown>>;
    notes?: string;
}): MergeInputFile {
    return {
        path: `/tmp/${input.fileName}`,
        fileName: input.fileName,
        lab: bloodworkLabSchema.parse({
            date: input.date,
            labName: input.labName,
            importLocation: input.fileName,
            measurements: input.measurements,
            notes: input.notes,
        }),
    };
}

describe('groupBloodworkDataFilesByDateWindow', () => {
    test('groups by proximity to latest date in each cluster', () => {
        const groups = groupBloodworkDataFilesByDateWindow([
            makeMergeInputFile({
                fileName: 'bloodwork_2026-01-20_lab-a.json',
                date: '2026-01-20',
                labName: 'Lab A',
                measurements: [{ name: 'Glucose', value: 95 }],
            }),
            makeMergeInputFile({
                fileName: 'bloodwork_2026-01-14_lab-b.json',
                date: '2026-01-14',
                labName: 'Lab B',
                measurements: [{ name: 'Glucose', value: 92 }],
            }),
            makeMergeInputFile({
                fileName: 'bloodwork_2026-01-03_lab-c.json',
                date: '2026-01-03',
                labName: 'Lab C',
                measurements: [{ name: 'Glucose', value: 90 }],
            }),
        ]);

        expect(groups).toHaveLength(2);
        expect(groups[0]?.map(item => item.fileName)).toEqual([
            'bloodwork_2026-01-14_lab-b.json',
            'bloodwork_2026-01-20_lab-a.json',
        ]);
        expect(groups[1]?.map(item => item.fileName)).toEqual([
            'bloodwork_2026-01-03_lab-c.json',
        ]);
    });
});

describe('mergeBloodworkDataFileGroup', () => {
    test('keeps latest measurement values and records replaced values as duplicate history', () => {
        const group = [
            makeMergeInputFile({
                fileName: 'bloodwork_2026-01-14_lab-a.json',
                date: '2026-01-14',
                labName: 'Lab A',
                measurements: [
                    {
                        name: 'Glucose',
                        value: 90,
                        unit: 'mg/dL',
                    },
                    {
                        name: 'Hemoglobin',
                        value: 14.1,
                        unit: 'g/dL',
                    },
                ],
                notes: 'Older run',
            }),
            makeMergeInputFile({
                fileName: 'bloodwork_2026-01-20_lab-b.json',
                date: '2026-01-20',
                labName: 'Lab B',
                measurements: [
                    {
                        name: 'Glucose',
                        value: 96,
                        unit: 'mg/dL',
                        duplicateValues: [{
                            date: '2026-01-19',
                            value: 94,
                            unit: 'mg/dL',
                        }],
                    },
                    {
                        name: 'Hemoglobin',
                        value: 13.8,
                        unit: 'g/dL',
                    },
                ],
                notes: 'Latest run',
            }),
        ];

        const merged = mergeBloodworkDataFileGroup(group);

        expect(merged.targetFileName).toBe('bloodwork_2026-01-20_lab-b.json');
        expect(merged.lab.date).toBe('2026-01-20');
        expect(merged.lab.labName).toBe('Lab B');
        expect(merged.lab.mergedFrom?.map(entry => entry.fileName)).toEqual([
            'bloodwork_2026-01-14_lab-a.json',
            'bloodwork_2026-01-20_lab-b.json',
        ]);

        const glucose = merged.lab.measurements.find(item => item.name === 'Glucose');
        expect(glucose?.value).toBe(96);
        expect(glucose?.duplicateValues).toEqual([
            {
                date: '2026-01-14',
                value: 90,
                unit: 'mg/dL',
                sourceFile: 'bloodwork_2026-01-14_lab-a.json',
                sourceLabName: 'Lab A',
                importLocation: 'bloodwork_2026-01-14_lab-a.json',
            },
            {
                date: '2026-01-19',
                value: 94,
                unit: 'mg/dL',
            },
        ]);
    });
});
