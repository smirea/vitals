import { describe, expect, test } from 'bun:test';

import {
    bloodworkLabSchema,
    bloodworkReferenceRangeSchema,
    buildBloodworkFileName,
    buildBloodworkS3Key,
    normalizeIsoDate,
    parseReferenceRangeBoundsFromText,
} from './bloodwork-schema.ts';

describe('normalizeIsoDate', () => {
    test('normalizes ISO-compatible values', () => {
        expect(normalizeIsoDate('2025-08-29')).toBe('2025-08-29');
        expect(normalizeIsoDate('2025/08/29')).toBe('2025-08-29');
        expect(normalizeIsoDate('29.08.2025')).toBe('2025-08-29');
    });

    test('rejects invalid dates', () => {
        expect(() => normalizeIsoDate('2025-13-01')).toThrow();
        expect(() => normalizeIsoDate('')).toThrow();
    });
});

describe('bloodworkLabSchema', () => {
    test('parses minimal valid payload and normalizes date', () => {
        const payload = bloodworkLabSchema.parse({
            date: '29.08.2025',
            labName: 'Muenchen Lab',
            measurements: [{
                name: 'Hemoglobin',
                value: 14.1,
                unit: 'g/dL',
                flag: 'normal',
            }],
        });

        expect(payload.date).toBe('2025-08-29');
        expect(payload.measurements).toHaveLength(1);
    });
});

describe('bloodworkReferenceRangeSchema', () => {
    test('normalizes comparator and legacy range shapes to min/max bounds', () => {
        expect(bloodworkReferenceRangeSchema.parse({ text: '<100' })).toEqual({ max: 100 });
        expect(bloodworkReferenceRangeSchema.parse({ text: '>59' })).toEqual({ min: 59 });
        expect(bloodworkReferenceRangeSchema.parse({ lower: 3.5, upper: 5.5 })).toEqual({
            min: 3.5,
            max: 5.5,
        });
    });
});

describe('parseReferenceRangeBoundsFromText', () => {
    test('parses common bounded formats', () => {
        expect(parseReferenceRangeBoundsFromText('2.5 - 4.9')).toEqual({ min: 2.5, max: 4.9 });
        expect(parseReferenceRangeBoundsFromText('2,5–4,9')).toEqual({ min: 2.5, max: 4.9 });
        expect(parseReferenceRangeBoundsFromText('<= 120')).toEqual({ max: 120 });
        expect(parseReferenceRangeBoundsFromText('>= -3.2')).toEqual({ min: -3.2 });
    });

    test('returns undefined for unsupported text', () => {
        expect(parseReferenceRangeBoundsFromText('see comment')).toBeUndefined();
        expect(parseReferenceRangeBoundsFromText('')).toBeUndefined();
    });
});

describe('measurement note support', () => {
    test('accepts optional measurement note', () => {
        const payload = bloodworkLabSchema.parse({
            date: '2025-08-29',
            labName: 'Muenchen Lab',
            measurements: [{
                name: 'Hemoglobin',
                value: 14.1,
                note: 'Fasting sample',
                referenceRange: { text: '<15.0' },
            }],
        });

        expect(payload.measurements[0]?.note).toBe('Fasting sample');
        expect(payload.measurements[0]?.referenceRange).toEqual({ max: 15 });
    });
});

describe('file and key naming', () => {
    test('creates deterministic file and s3 key names', () => {
        const fileName = buildBloodworkFileName({
            date: '2025-08-29',
            labName: 'Muenchen Prime / Lab',
        });
        const s3Key = buildBloodworkS3Key(
            { date: '2025-08-29', labName: 'Muenchen Prime / Lab' },
            'vitals',
        );

        expect(fileName).toBe('bloodwork_2025-08-29_muenchen-prime-lab.json');
        expect(s3Key).toBe('vitals/bloodwork_2025-08-29_muenchen-prime-lab.json');
    });
});
