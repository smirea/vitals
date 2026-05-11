import type { inferRouterOutputs } from '@trpc/server';

import type { AppRouter } from 'server/trpc/index.ts';

import { API_BASE_URL } from '@/src/api/trpc';

type RouterOutput = inferRouterOutputs<AppRouter>;

export type PillsDashboard = RouterOutput['pills']['getDashboard'];
export type PillRecord = PillsDashboard['pills'][number];
export type PillComponent = PillRecord['components'][number];
export type PillImage = PillRecord['images'][number];
export type PillPeriod = PillRecord['periods'][number];
export type PillTag = PillRecord['tags'][number];
export type PillExtractionResult = RouterOutput['pills']['extractFromImages'];
export type TagRecord = RouterOutput['tags']['list'][number];

export type PillTiming = 'morning' | 'afternoon' | 'evening';
export type PillWeekday =
	| 'monday'
	| 'tuesday'
	| 'wednesday'
	| 'thursday'
	| 'friday'
	| 'saturday'
	| 'sunday';

export type PillSectionKey = 'future' | 'active' | 'notTracked' | 'past';

export type PillListRow = {
	key: string;
	pill: PillRecord;
	period: PillPeriod | null;
	section: PillSectionKey;
};

export type PillSection = {
	key: PillSectionKey;
	title: string;
	rows: PillListRow[];
};

export type PillImageFormValue = {
	id?: number;
	uid: string;
	fileName: string;
	dataUrl: string;
};

export type PillComponentFormValue = {
	name: string;
	value: string;
	unit: string;
};

export type PillPeriodFormValue = {
	id?: number;
	startDate: string;
	endDate: string;
	count: string;
	timing: PillTiming | '';
	daysOfWeek: PillWeekday[];
	tagText: string;
};

export type PillFormValues = {
	id?: number;
	name: string;
	value: string;
	unit: string;
	url: string;
	note: string;
	tagText: string;
	images: PillImageFormValue[];
	components: PillComponentFormValue[];
	periods: PillPeriodFormValue[];
};

type PillExportTableName = 'futurePills' | 'activePills' | 'notTrackedPills' | 'pastPills';

type PillExportRow = {
	components?: Record<string, string>;
	note?: string;
	tags?: string[];
	startDate?: string;
	endDate?: string;
	timing?: string;
};

export const timingOptions = [
	{ label: 'Morning', value: 'morning' },
	{ label: 'Afternoon', value: 'afternoon' },
	{ label: 'Evening', value: 'evening' },
] as const;

export const weekdayOptions = [
	{ label: 'Mon', value: 'monday' },
	{ label: 'Tue', value: 'tuesday' },
	{ label: 'Wed', value: 'wednesday' },
	{ label: 'Thu', value: 'thursday' },
	{ label: 'Fri', value: 'friday' },
	{ label: 'Sat', value: 'saturday' },
	{ label: 'Sun', value: 'sunday' },
] satisfies Array<{ label: string; value: PillWeekday }>;

const pillWeekdayValues = weekdayOptions.map(option => option.value);
const todayFormatter = new Intl.DateTimeFormat('en-CA', {
	year: 'numeric',
	month: '2-digit',
	day: '2-digit',
	timeZone: 'America/New_York',
});

export function getTodayDateString() {
	return todayFormatter.format(new Date());
}

export function createEmptyPillForm(): PillFormValues {
	return {
		name: '',
		value: '',
		unit: '',
		url: '',
		note: '',
		tagText: '',
		images: [],
		components: [{ name: '', value: '', unit: '' }],
		periods: [],
	};
}

export function createBlankPeriod(): PillPeriodFormValue {
	return {
		startDate: getTodayDateString(),
		endDate: '',
		count: '1',
		timing: '',
		daysOfWeek: [],
		tagText: '',
	};
}

export function createBlankComponent(): PillComponentFormValue {
	return { name: '', value: '', unit: '' };
}

export function buildPillSections(dashboard: PillsDashboard): PillSection[] {
	const futureRows = dashboard.futurePills
		.map(pill => ({ pill, period: getFuturePeriod(pill) }))
		.filter((row): row is { pill: PillRecord; period: PillPeriod } => row.period !== null)
		.sort((left, right) => left.period.startDate.localeCompare(right.period.startDate))
		.map(row => ({
			key: `future-${row.pill.id}`,
			pill: row.pill,
			period: row.period,
			section: 'future' as const,
		}));
	const activeRows = dashboard.activePills
		.map(pill => ({ pill, period: getActivePeriod(pill) }))
		.filter((row): row is { pill: PillRecord; period: PillPeriod } => row.period !== null)
		.map(row => ({
			key: `active-${row.pill.id}`,
			pill: row.pill,
			period: row.period,
			section: 'active' as const,
		}));
	const notTrackedRows = dashboard.pills
		.filter(pill => pill.periods.length === 0)
		.map(pill => ({
			key: `not-tracked-${pill.id}`,
			pill,
			period: null,
			section: 'notTracked' as const,
		}));
	const pastRows = dashboard.pastPills
		.flatMap(pill =>
			pill.periods
				.filter(period => Boolean(period.endDate))
				.map(period => ({
					key: `past-${pill.id}-${period.id}`,
					pill,
					period,
					section: 'past' as const,
				})),
		)
		.sort((left, right) => right.period.startDate.localeCompare(left.period.startDate));

	const sections: PillSection[] = [
		{ key: 'future', title: 'Future pills', rows: futureRows },
		{ key: 'active', title: 'Active pills', rows: activeRows },
		{ key: 'notTracked', title: 'Not tracked yet', rows: notTrackedRows },
		{ key: 'past', title: 'Past pills', rows: pastRows },
	];

	return sections.filter(section => section.rows.length > 0);
}

export function getActivePeriod(pill: PillRecord) {
	const today = getTodayDateString();
	return (
		[...pill.periods]
			.filter(period => period.startDate <= today && (!period.endDate || period.endDate > today))
			.sort((left, right) => left.startDate.localeCompare(right.startDate))
			.at(-1) ?? null
	);
}

export function getFuturePeriod(pill: PillRecord) {
	const today = getTodayDateString();
	return (
		[...pill.periods]
			.filter(period => period.startDate > today && (!period.endDate || period.endDate > today))
			.sort((left, right) => left.startDate.localeCompare(right.startDate))
			.at(0) ?? null
	);
}

export function pillToFormValues(pill: PillRecord, appendPeriod = false): PillFormValues {
	const periods = pill.periods.map(period => ({
		id: period.id,
		startDate: period.startDate,
		endDate: period.endDate ?? '',
		count: formatNumericValue(period.count),
		timing: (period.timing ?? '') as PillTiming | '',
		daysOfWeek: normalizeWeekdaySelection(period.daysOfWeek ?? []),
		tagText: period.tags.map(tag => tag.name).join(', '),
	}));

	return {
		id: pill.id,
		name: pill.name,
		value: pill.value ?? '',
		unit: pill.unit ?? '',
		url: pill.url ?? '',
		note: pill.note ?? '',
		tagText: pill.tags.map(tag => tag.name).join(', '),
		images: pill.images.map((image, index) => ({
			id: image.id,
			uid: createImageUid(image, index),
			fileName: image.fileName,
			dataUrl: normalizeImageUrl(image.dataUrl),
		})),
		components:
			pill.components.length > 0
				? pill.components.map(component => ({
						name: component.name,
						value: component.value ?? '',
						unit: component.unit ?? '',
					}))
				: [createBlankComponent()],
		periods: appendPeriod ? [...periods, createBlankPeriod()] : periods,
	};
}

export function formValuesToMutationInput(form: PillFormValues) {
	const isNewPill = form.id == null;

	return {
		id: form.id,
		name: form.name.trim(),
		value: form.value.trim(),
		unit: form.unit.trim(),
		url: form.url.trim(),
		note: form.note.trim(),
		tagNames: parseTagText(form.tagText),
		images: form.images.map(image => ({
			id: image.id,
			fileName: image.fileName,
			dataUrl: image.dataUrl,
		})),
		components: form.components.map(component => ({
			name: component.name.trim(),
			value: component.value.trim(),
			unit: component.unit.trim(),
		})),
		periods: form.periods.map(period => ({
			id: isNewPill ? undefined : period.id,
			startDate: period.startDate.trim(),
			endDate: period.endDate.trim(),
			count: normalizePeriodCount(period.count),
			timing: period.timing || undefined,
			daysOfWeek: normalizeWeekdaySelection(period.daysOfWeek),
			tagNames: parseTagText(period.tagText),
		})),
	};
}

export function extractionToFormPatch(extraction: PillExtractionResult): Partial<PillFormValues> {
	return {
		name: extraction.name ?? '',
		value: extraction.value ?? '',
		unit: extraction.unit ?? '',
		note: extraction.note ?? '',
		components:
			extraction.components.length > 0
				? extraction.components.map(component => ({
						name: component.name,
						value: component.value ?? '',
						unit: component.unit ?? '',
					}))
				: [createBlankComponent()],
	};
}

export function assertValidPillForm(form: PillFormValues) {
	if (!form.name.trim()) throw new Error('Enter a pill name.');
	if (!form.value.trim()) throw new Error('Enter the default pill value.');
	if (!form.unit.trim()) throw new Error('Enter the default pill unit.');
	for (const period of form.periods) {
		if (!period.startDate.trim()) throw new Error('Each date range needs a start date.');
		if (normalizePeriodCount(period.count) <= 0) {
			throw new Error('Each date range needs a positive count.');
		}
	}
}

export function buildPillsExport(sections: PillSection[]) {
	const exportSections: Partial<Record<PillExportTableName, Record<string, PillExportRow>>> = {};
	const sectionMap = new Map(sections.map(section => [section.key, section.rows]));
	const futurePills = buildPillExportTable(sectionMap.get('future') ?? []);
	const activePills = buildPillExportTable(sectionMap.get('active') ?? []);
	const notTrackedPills = buildPillExportTable(sectionMap.get('notTracked') ?? []);
	const pastPills = buildPillExportTable(sectionMap.get('past') ?? []);

	if (Object.keys(futurePills).length > 0) exportSections.futurePills = futurePills;
	if (Object.keys(activePills).length > 0) exportSections.activePills = activePills;
	if (Object.keys(notTrackedPills).length > 0) exportSections.notTrackedPills = notTrackedPills;
	if (Object.keys(pastPills).length > 0) exportSections.pastPills = pastPills;

	return exportSections;
}

function buildPillExportTable(rows: PillListRow[]) {
	return Object.fromEntries(
		rows.map(row => {
			const count = row.period?.count ?? 1;
			return [
				`${row.pill.name} - ${formatServing(multiplyServingValue(row.pill.value, count), row.pill.unit)}`,
				buildPillExportRow({
					pill: row.pill,
					count,
					tags: row.period?.tags ?? [],
					startDate: row.period?.startDate,
					endDate: row.period?.endDate ?? undefined,
					timing: row.period?.timing,
					daysOfWeek: row.period?.daysOfWeek,
				}),
			];
		}),
	);
}

function buildPillExportRow(args: {
	pill: PillRecord;
	count: number;
	tags: PillPeriod['tags'];
	startDate?: string;
	endDate?: string;
	timing?: PillPeriod['timing'];
	daysOfWeek?: PillPeriod['daysOfWeek'];
}): PillExportRow {
	const exportRow: PillExportRow = {};

	if (args.pill.components.length > 0) {
		exportRow.components = Object.fromEntries(
			args.pill.components.map(component => [
				component.name,
				formatServing(multiplyServingValue(component.value, args.count), component.unit),
			]),
		);
	}
	if (args.pill.note) exportRow.note = args.pill.note;
	if (args.tags.length > 0) exportRow.tags = args.tags.map(tag => `${tag.name}:${tag.note ?? ''}`);
	if (args.startDate) exportRow.startDate = args.startDate;
	if (args.endDate) exportRow.endDate = args.endDate;
	if (args.startDate || args.endDate || args.timing || (args.daysOfWeek?.length ?? 0) > 0) {
		exportRow.timing = formatPillSchedule({
			daysOfWeek: args.daysOfWeek ?? [],
			timing: args.timing,
		});
	}

	return exportRow;
}

export function parseTagText(value: string) {
	const namesByKey = new Map<string, string>();
	for (const rawValue of value.split(',')) {
		const name = rawValue.trim();
		if (!name) continue;
		const key = name.toLocaleLowerCase();
		if (!namesByKey.has(key)) namesByKey.set(key, name);
	}
	return [...namesByKey.values()];
}

export function normalizeWeekdaySelection(values: readonly PillWeekday[]) {
	const selectedValues = new Set(values);
	const orderedValues = pillWeekdayValues.filter(value => selectedValues.has(value));
	return orderedValues.length === pillWeekdayValues.length ? [] : orderedValues;
}

export function formatServing(value?: string | null, unit?: string | null) {
	const text = [value?.trim(), unit?.trim()].filter(Boolean).join(' ');
	return text || 'Not set';
}

export function formatSupplementFactsTitle(value?: string | null, unit?: string | null) {
	const serving = [value?.trim(), unit?.trim()].filter(Boolean).join(' ');
	return serving ? `Supplement Facts per ${serving}` : 'Supplement Facts';
}

export function normalizePeriodCount(value: string | number | null | undefined) {
	const numericValue =
		typeof value === 'number' ? value : Number.parseFloat(value?.replace(',', '.') ?? '');
	return Number.isFinite(numericValue) && numericValue > 0 ? numericValue : 1;
}

export function formatNumericValue(value: number) {
	return Number.isInteger(value) ? String(value) : Number(value.toFixed(4)).toString();
}

export function multiplyServingValue(
	value: string | null | undefined,
	count: number | null | undefined,
) {
	const trimmedValue = value?.trim() ?? '';
	const normalizedCount = normalizePeriodCount(count);

	if (!trimmedValue) return '';

	const numericValue = Number(trimmedValue.replace(/,/g, ''));
	if (!Number.isFinite(numericValue)) {
		return normalizedCount === 1
			? trimmedValue
			: `${formatNumericValue(normalizedCount)} x ${trimmedValue}`;
	}

	return formatNumericValue(numericValue * normalizedCount);
}

export function formatPeriodRange(period: Pick<PillPeriod, 'startDate' | 'endDate'>) {
	return period.endDate
		? `${period.startDate} to ${period.endDate}`
		: `${period.startDate} to ongoing`;
}

export function formatWeekdayFrequency(daysOfWeek: readonly PillWeekday[] | null | undefined) {
	const selectedDays = normalizeWeekdaySelection(daysOfWeek ?? []);
	if (selectedDays.length === 0) return 'daily';

	const labels = selectedDays.map(
		day => weekdayOptions.find(option => option.value === day)?.label ?? day,
	);
	return `every ${labels.join(', ')}`;
}

export function formatTimingPhrase(timing: PillTiming | null | undefined) {
	switch (timing) {
		case 'morning':
			return 'in the morning';
		case 'afternoon':
			return 'in the afternoon';
		case 'evening':
			return 'in the evening';
		default:
			return '';
	}
}

export function formatPillSchedule(period: {
	daysOfWeek?: readonly PillWeekday[] | null;
	timing?: PillTiming | null;
}) {
	return [formatWeekdayFrequency(period.daysOfWeek), formatTimingPhrase(period.timing)]
		.filter(Boolean)
		.join(' ');
}

export function formatRelativeDate(value: string) {
	const parsed = Date.parse(`${value}T00:00:00`);
	if (!Number.isFinite(parsed)) return value;
	const deltaMs = Date.now() - parsed;
	const days = Math.floor(deltaMs / 86_400_000);
	if (days < 1) return 'today';
	if (days === 1) return 'yesterday';
	if (days < 30) return `${days} days ago`;
	const months = Math.floor(days / 30);
	if (months < 12) return months === 1 ? 'about 1 month ago' : `about ${months} months ago`;
	const years = Math.floor(months / 12);
	return years === 1 ? 'about 1 year ago' : `about ${years} years ago`;
}

export function createImageUid(
	image: Pick<PillImageFormValue, 'id' | 'fileName' | 'dataUrl'>,
	index: number,
) {
	return image.id
		? `saved-${image.id}`
		: `image-${index}-${image.fileName}-${image.dataUrl.length}`;
}

export function normalizeImageUrl(dataUrl: string) {
	if (!dataUrl.includes('/api/db-image/')) return dataUrl;
	const [, path] = dataUrl.split('/api/db-image/');
	if (!path) return dataUrl;
	return `${API_BASE_URL}/db-image/${path}`;
}

export function isBase64DataImage(image: PillImageFormValue) {
	return image.dataUrl.startsWith('data:');
}
