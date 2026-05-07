import { describe, expect, test } from 'bun:test';

import { cleanLabReferenceRange } from './lab-reference-ranges';

describe('cleanLabReferenceRange', () => {
	test('normalizes worded minimum inequalities', () => {
		expect(
			cleanLabReferenceRange({
				referenceText: 'Reference Range: > OR = 40mg/dL',
			}),
		).toEqual({
			referenceText: '>= 40mg/dL',
			referenceMin: 40,
		});
	});

	test('normalizes worded maximum inequalities', () => {
		expect(
			cleanLabReferenceRange({
				referenceText: '< or = 13.5',
			}),
		).toEqual({
			referenceText: '<= 13.5',
			referenceMax: 13.5,
		});
	});

	test('preserves zero bounds and qualitative ranges', () => {
		expect(
			cleanLabReferenceRange({
				referenceText: 'Low: <3.4 Borderline: 3.4-5.4 Normal: >5.4',
				referenceMin: 0,
			}),
		).toEqual({
			referenceText: 'Low: <3.4 Borderline: 3.4-5.4 Normal: >5.4',
			referenceMin: 0,
		});
	});

	test('does not treat titer ratios as decimal thresholds', () => {
		expect(
			cleanLabReferenceRange({
				referenceText: '<1:20 titer',
			}),
		).toEqual({
			referenceText: '<1:20 titer',
		});
	});
});
