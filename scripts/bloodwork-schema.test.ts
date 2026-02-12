import { describe, expect, test } from 'bun:test';

import {
    bloodworkLabSchema,
    buildBloodworkFileName,
    buildBloodworkS3Key,
    normalizeIsoDate,
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
