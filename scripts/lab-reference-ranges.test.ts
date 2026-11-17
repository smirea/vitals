import { expect, test } from 'bun:test';

import { cleanLabReferenceRange } from './lab-reference-ranges';

test('a worded "> OR =" reference becomes ">=" and sets the minimum', () => {
	expect(cleanLabReferenceRange({ referenceText: 'Reference Range: > OR = 40mg/dL' })).toEqual({
		referenceText: '>= 40mg/dL',
		referenceMin: 40,
	});
});

test('a worded "< or =" reference becomes "<=" and sets the maximum', () => {
	expect(cleanLabReferenceRange({ referenceText: '< or = 13.5' })).toEqual({
		referenceText: '<= 13.5',
		referenceMax: 13.5,
	});
});

test('an explicit zero bound is kept and tiered qualitative text sets no bounds', () => {
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

test('a titer ratio like "<1:20" sets no numeric bound', () => {
	expect(cleanLabReferenceRange({ referenceText: '<1:20 titer' })).toEqual({
		referenceText: '<1:20 titer',
	});
});
