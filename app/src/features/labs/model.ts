import type { inferRouterOutputs } from '@trpc/server';

import type { AppRouter } from 'server/trpc/index.ts';

type RouterOutput = inferRouterOutputs<AppRouter>;

export type LabsDashboard = RouterOutput['labs']['getDashboard'];
export type LabsDocument = LabsDashboard['documents'][number];
export type LabsMeasurement = LabsDashboard['measurements'][number];
export type LabsResult = LabsDashboard['results'][number];
export type LabsImportDocument = RouterOutput['labs']['listDocuments'][number];

export type SourceColumn = {
	id: string;
	documentIds: number[];
	documentCount: number;
	group: string | null;
	date: string;
	prettyDate: string;
	index: number;
};

export type LabValueStatus = 'in-range' | 'out-of-range' | 'unclassified';

export type LabValue = {
	sourceId: string;
	sourceIndex: number;
	documentId: number;
	date: string;
	prettyDate: string;
	display: string;
	numericValue: number | null;
	rangeMin: number | null;
	rangeMax: number | null;
	rangeCaption: string;
	status: LabValueStatus;
	note: string | null;
};

export type LabMeasurementRow = {
	key: string;
	name: string;
	category: string;
	searchText: string;
	values: LabValue[];
	latest: LabValue | null;
	inRange: number;
	outOfRange: number;
	unclassified: number;
};

export type CategoryOverviewItem = {
	category: string;
	inRange: number;
	outOfRange: number;
	unclassified: number;
	total: number;
};

export type MeaningfulChangeItem = {
	key: string;
	measurement: string;
	category: string;
	direction: 'improved' | 'worsened' | 'changed';
	score: number;
	deltaPercent: number | null;
	latest: LabValue;
	previous: LabValue;
};

export type PreviewRow = {
	id: number;
	key: string;
	name: string;
	valueText: string;
	rangeText: string | null;
	note: string | null;
	hasIssue: boolean;
	issueLabel: string | null;
};

const dateFormatter = new Intl.DateTimeFormat('en-US', {
	month: 'short',
	day: 'numeric',
	year: 'numeric',
	timeZone: 'UTC',
});
const uncategorized = 'Other';
const normalNotes = new Set(['n', 'normal', 'none', 'ok', 'n/a', 'na', 'in range', 'within range']);

export function formatPrettyDate(value: string) {
	const parsed = Date.parse(value);
	if (!Number.isFinite(parsed)) return value;
	return dateFormatter.format(new Date(parsed));
}

export function normalizeCategory(value: string | null | undefined) {
	const trimmed = value?.trim();
	return trimmed || uncategorized;
}

export function getSources(documents: LabsDocument[]): SourceColumn[] {
	const sortedDocuments = [...documents].sort((left, right) => {
		const dateCompare = (right.date ?? '').localeCompare(left.date ?? '');
		return dateCompare || right.id - left.id;
	});
	const sources: Array<Omit<SourceColumn, 'index'>> = [];
	const sourceIndexByGroup = new Map<string, number>();

	for (const document of sortedDocuments) {
		const date = document.date ?? document.queuedAt.slice(0, 10);
		const group = document.group?.trim() || null;
		if (!group) {
			sources.push({
				id: `document:${document.id}`,
				documentIds: [document.id],
				documentCount: 1,
				group: null,
				date,
				prettyDate: formatPrettyDate(date),
			});
			continue;
		}

		const existingIndex = sourceIndexByGroup.get(group);
		if (existingIndex === undefined) {
			sourceIndexByGroup.set(group, sources.length);
			sources.push({
				id: `group:${group}`,
				documentIds: [document.id],
				documentCount: 1,
				group,
				date,
				prettyDate: formatGroupedSourceLabel(1, date),
			});
			continue;
		}

		const existing = sources[existingIndex];
		if (!existing) continue;
		existing.documentIds.push(document.id);
		existing.documentCount = existing.documentIds.length;
		existing.prettyDate = formatGroupedSourceLabel(existing.documentCount, existing.date);
	}

	return sources.map((source, index) => ({ ...source, index }));
}

function formatGroupedSourceLabel(count: number, date: string) {
	const prettyDate = formatPrettyDate(date);
	return count === 1 ? `1 Lab from ${prettyDate}` : `${count} Labs from ${prettyDate}`;
}

export function buildMeasurementRows(dashboard: LabsDashboard): LabMeasurementRow[] {
	const sources = getSources(dashboard.documents);
	const measurementById = new Map(dashboard.measurements.map(item => [item.id, item]));
	const resultsByDocumentId = new Map<number, LabsResult[]>();

	for (const result of dashboard.results) {
		const existing = resultsByDocumentId.get(result.documentId);
		if (existing) {
			existing.push(result);
		} else {
			resultsByDocumentId.set(result.documentId, [result]);
		}
	}

	const rows = new Map<string, LabMeasurementRow>();
	for (const source of sources) {
		const resultByMeasurementId = new Map<number, LabsResult>();
		for (const documentId of source.documentIds) {
			for (const result of resultsByDocumentId.get(documentId) ?? []) {
				if (!resultByMeasurementId.has(result.measurementId)) {
					resultByMeasurementId.set(result.measurementId, result);
				}
			}
		}

		for (const result of resultByMeasurementId.values()) {
			const measurement = measurementById.get(result.measurementId);
			if (!measurement) continue;
			const key = measurement.key.trim().toLowerCase();
			if (!key) continue;
			const category = normalizeCategory(measurement.category);
			const value = formatValue(result, measurement, source);
			const existing = rows.get(key);
			if (existing) {
				existing.values.push(value);
				existing.inRange += value.status === 'in-range' ? 1 : 0;
				existing.outOfRange += value.status === 'out-of-range' ? 1 : 0;
				existing.unclassified += value.status === 'unclassified' ? 1 : 0;
				if (!existing.latest || value.date > existing.latest.date) {
					existing.latest = value;
				}
				continue;
			}

			rows.set(key, {
				key,
				name: measurement.name,
				category,
				searchText: `${measurement.name} ${category}`.toLowerCase(),
				values: [value],
				latest: value,
				inRange: value.status === 'in-range' ? 1 : 0,
				outOfRange: value.status === 'out-of-range' ? 1 : 0,
				unclassified: value.status === 'unclassified' ? 1 : 0,
			});
		}
	}

	return Array.from(rows.values())
		.map(row => ({
			...row,
			values: [...row.values].sort((left, right) => right.date.localeCompare(left.date)),
		}))
		.sort(
			(left, right) =>
				left.category.localeCompare(right.category) || left.name.localeCompare(right.name),
		);
}

function formatValue(
	result: LabsResult,
	measurement: LabsMeasurement,
	source: SourceColumn,
): LabValue {
	const valueText = result.valueText?.trim() ?? '';
	const unit = result.unit?.trim() ?? '';
	const numericValue = result.valueNumeric ?? parseNumericValue(valueText);
	const displayValue = numericValue === null ? valueText : formatNumericLabel(numericValue);
	const display = [displayValue, unit].filter(Boolean).join(' ').trim() || '--';
	const rangeMin = result.originalRangeMin ?? measurement.rangeMin ?? null;
	const rangeMax = result.originalRangeMax ?? measurement.rangeMax ?? null;
	const status = getValueStatus(numericValue, rangeMin, rangeMax);

	return {
		sourceId: source.id,
		sourceIndex: source.index,
		documentId: result.documentId,
		date: source.date,
		prettyDate: source.prettyDate,
		display,
		numericValue,
		rangeMin,
		rangeMax,
		rangeCaption: formatRangeCaption(rangeMin, rangeMax, unit || undefined),
		status,
		note: result.note?.trim() || null,
	};
}

function parseNumericValue(value: number | string | null | undefined): number | null {
	if (typeof value === 'number' && Number.isFinite(value)) return value;
	if (typeof value !== 'string') return null;
	const normalized = value
		.replace(',', '.')
		.replace(/[^0-9.+-]/g, '')
		.trim();
	if (!normalized) return null;
	const parsed = Number.parseFloat(normalized);
	return Number.isFinite(parsed) ? parsed : null;
}

function formatNumericLabel(value: number) {
	const decimals = Math.abs(value) < 10 ? 1 : 0;
	const rounded = Number.parseFloat(value.toFixed(decimals));
	if (Object.is(rounded, -0)) return decimals === 0 ? '0' : '0.0';
	return rounded.toFixed(decimals);
}

function formatRangeCaption(rangeMin: number | null, rangeMax: number | null, unit?: string) {
	const unitSuffix = unit ? ` ${unit}` : '';
	if (rangeMin !== null && rangeMax !== null) {
		const low = Math.min(rangeMin, rangeMax);
		const high = Math.max(rangeMin, rangeMax);
		return low === high
			? `ref ${formatNumericLabel(low)}${unitSuffix}`
			: `ref ${formatNumericLabel(low)} - ${formatNumericLabel(high)}${unitSuffix}`;
	}
	if (rangeMin !== null) return `ref >= ${formatNumericLabel(rangeMin)}${unitSuffix}`;
	if (rangeMax !== null) return `ref <= ${formatNumericLabel(rangeMax)}${unitSuffix}`;
	return '';
}

function getValueStatus(
	numericValue: number | null,
	rangeMin: number | null,
	rangeMax: number | null,
): LabValueStatus {
	if (numericValue === null || (rangeMin === null && rangeMax === null)) return 'unclassified';
	if (rangeMin !== null && numericValue < rangeMin) return 'out-of-range';
	if (rangeMax !== null && numericValue > rangeMax) return 'out-of-range';
	return 'in-range';
}

export function getCategoryOverview(rows: LabMeasurementRow[]): CategoryOverviewItem[] {
	const byCategory = new Map<string, CategoryOverviewItem>();
	for (const row of rows) {
		const item = byCategory.get(row.category) ?? {
			category: row.category,
			inRange: 0,
			outOfRange: 0,
			unclassified: 0,
			total: 0,
		};
		item.total += 1;
		if (row.latest?.status === 'in-range') item.inRange += 1;
		else if (row.latest?.status === 'out-of-range') item.outOfRange += 1;
		else item.unclassified += 1;
		byCategory.set(row.category, item);
	}

	return Array.from(byCategory.values()).sort((left, right) => right.total - left.total);
}

export function getMeaningfulChanges(rows: LabMeasurementRow[]): MeaningfulChangeItem[] {
	return rows
		.map(row => {
			const numericValues = row.values.filter(value => value.numericValue !== null);
			const latest = numericValues[0];
			const previous = numericValues.find(value => value.date < latest?.date);
			if (!latest || !previous || latest.numericValue === null || previous.numericValue === null) {
				return null;
			}
			const delta = latest.numericValue - previous.numericValue;
			const deltaPercent =
				previous.numericValue === 0 ? null : (delta / Math.abs(previous.numericValue)) * 100;
			const latestOut = latest.status === 'out-of-range';
			const previousOut = previous.status === 'out-of-range';
			const direction =
				previousOut && !latestOut ? 'improved' : !previousOut && latestOut ? 'worsened' : 'changed';
			const score = Math.abs(deltaPercent ?? delta) + (latestOut !== previousOut ? 100 : 0);

			return {
				key: row.key,
				measurement: row.name,
				category: row.category,
				direction,
				score,
				deltaPercent,
				latest,
				previous,
			} satisfies MeaningfulChangeItem;
		})
		.filter((item): item is MeaningfulChangeItem => item !== null)
		.sort((left, right) => right.score - left.score)
		.slice(0, 12);
}

export function getPreviewRows({
	documentId,
	dashboard,
}: {
	documentId: number;
	dashboard: LabsDashboard;
}): PreviewRow[] {
	const measurementById = new Map(
		dashboard.measurements.map(measurement => [measurement.id, measurement]),
	);
	return dashboard.results
		.filter(result => result.documentId === documentId)
		.map(result => {
			const measurement = measurementById.get(result.measurementId);
			const valueText = [result.valueText?.trim() ?? '', result.unit?.trim() ?? '']
				.filter(Boolean)
				.join(' ')
				.trim();
			const rangeMin = result.originalRangeMin ?? measurement?.rangeMin ?? null;
			const rangeMax = result.originalRangeMax ?? measurement?.rangeMax ?? null;
			const status = getValueStatus(
				result.valueNumeric ?? parseNumericValue(result.valueText),
				rangeMin,
				rangeMax,
			);
			const note = result.note?.trim() || null;
			const issueLabel = getPreviewIssueLabel(note, status === 'out-of-range');

			return {
				id: result.id,
				key: measurement?.key ?? '',
				name: measurement?.name ?? result.originalName ?? 'Unknown measurement',
				valueText: valueText || '--',
				rangeText: result.originalRangeText?.trim() || measurement?.range?.trim() || null,
				note: getPreviewDisplayNote(note),
				hasIssue: issueLabel !== null,
				issueLabel,
			};
		});
}

function getPreviewIssueLabel(note: string | null, isOutsideRange: boolean) {
	const value = note?.trim().toLowerCase();
	const compactFlag = getCompactFlagLabel(value, note);
	if (compactFlag) return compactFlag;
	if (value && !normalNotes.has(value)) return note;
	return isOutsideRange ? 'Outside reference range' : null;
}

function getPreviewDisplayNote(note: string | null) {
	const value = note?.trim().toLowerCase();
	if (!note || !value) return null;
	if (getCompactFlagLabel(value, note)) return null;
	return note;
}

function getCompactFlagLabel(value: string | undefined, note: string | null) {
	if (!value || normalNotes.has(value)) return null;
	if (value === 'h' || value === 'high') return 'High';
	if (value === 'l' || value === 'low') return 'Low';
	if (value === 'a' || value === 'abn' || value === 'abnormal') return 'Abnormal';
	if (value === 'critical' || value === 'crit') return 'Critical';
	if (value === 'critical h' || value === 'critical high') return 'Critical high';
	if (value === 'critical l' || value === 'critical low') return 'Critical low';
	if (/^[a-z]{1,2}$/.test(value)) return note?.toUpperCase() ?? value.toUpperCase();
	return null;
}

export function buildCsv(rows: LabMeasurementRow[], sources: SourceColumn[]) {
	const headers = [
		'Measurement',
		'Category',
		'In range',
		'Out of range',
		...sources.map(source => source.prettyDate),
	];
	const csvRows = rows.map(row => {
		const valueBySourceId = new Map(row.values.map(value => [value.sourceId, value.display]));
		return [
			row.name,
			row.category,
			row.inRange,
			row.outOfRange,
			...sources.map(source => valueBySourceId.get(source.id) ?? ''),
		];
	});

	return [headers, ...csvRows].map(row => row.map(escapeCsv).join(',')).join('\n');
}

function escapeCsv(value: string | number) {
	const text = String(value);
	if (!/[",\n]/.test(text)) return text;
	return `"${text.replace(/"/g, '""')}"`;
}
