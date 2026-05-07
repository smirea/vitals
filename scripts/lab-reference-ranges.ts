type ReferenceRangeInput = {
	referenceText?: string | null;
	referenceMin?: number | null;
	referenceMax?: number | null;
};

type CleanReferenceRange = {
	referenceText?: string;
	referenceMin?: number;
	referenceMax?: number;
};

const referenceLabel = /^(?:reference\s+(?:range|interval)|normal\s+range)\s*:\s*/i;
const wordedInequality = /([<>])\s*or\s*=\s*/gi;
const leadingInequality = /^\s*(<=|>=|<|>)\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?)(?!\s*:)/i;

export function cleanLabReferenceRange({
	referenceText,
	referenceMin,
	referenceMax,
}: ReferenceRangeInput): CleanReferenceRange {
	const cleaned: CleanReferenceRange = {};
	let text = referenceText?.trim();

	if (text) {
		text = text
			.replace(referenceLabel, '')
			.replaceAll('≤', '<=')
			.replaceAll('≥', '>=')
			.replaceAll('&lt;', '<')
			.replaceAll('&gt;', '>')
			.replace(wordedInequality, '$1= ')
			.trim();
		if (text) cleaned.referenceText = text;
	}

	if (referenceMin != null) cleaned.referenceMin = referenceMin;
	if (referenceMax != null) cleaned.referenceMax = referenceMax;

	const match = text?.match(leadingInequality);
	if (!match) return cleaned;

	const operator = match[1];
	const value = Number(match[2]);
	if (!Number.isFinite(value)) return cleaned;

	if (operator.includes('>') && cleaned.referenceMin == null) cleaned.referenceMin = value;
	if (operator.includes('<') && cleaned.referenceMax == null) cleaned.referenceMax = value;

	return cleaned;
}

export function cleanLabReferenceRangeInPlace<T extends ReferenceRangeInput>(range: T): T {
	const cleaned = cleanLabReferenceRange(range);
	range.referenceText = cleaned.referenceText;
	range.referenceMin = cleaned.referenceMin;
	range.referenceMax = cleaned.referenceMax;
	return range;
}
