import { beforeEach, describe, expect, test } from 'bun:test';

import {
    assertPdfSignature,
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
    const originalModelValue = process.env.OPENROUTER_MODEL;

    beforeEach(() => {
        process.env.OPENROUTER_MODEL = originalModelValue;
    });

    test('uses cli model ids first', () => {
        expect(resolveModelIds(['custom/model'])).toEqual(['custom/model']);
    });

    test('uses environment model ids when cli models are absent', () => {
        process.env.OPENROUTER_MODEL = 'google/gemini-3-flash, google/gemini-2.5-flash';
        expect(resolveModelIds([])).toEqual(['google/gemini-3-flash', 'google/gemini-2.5-flash']);
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
