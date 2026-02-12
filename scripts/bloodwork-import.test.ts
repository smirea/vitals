import { describe, expect, test } from 'bun:test';

import {
    assertPdfSignature,
    filterLikelyMeasurements,
    isEnglishGlossaryName,
    normalizeGlossaryDecisionAction,
    parseCliOptions,
    resolveModelIds,
} from './bloodwork-import.ts';

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

    test('rejects invalid combinations', () => {
        expect(() => parseCliOptions(['--all', 'file.pdf'])).toThrow();
        expect(() => parseCliOptions(['--continue-on-error', 'file.pdf'])).toThrow();
        expect(() => parseCliOptions([])).toThrow();
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
