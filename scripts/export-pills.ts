#!/usr/bin/env bun
import { createScript } from './createScript';

import { getDatabase } from 'server/db/client';
import { getPillsDashboard } from 'server/db/pills';

const db = getDatabase();
const headers = ['name', 'amount', 'timing', 'started', 'components(1component/line)', 'tags'];

void createScript(() => {
	const rows = getPillsDashboard(db)
		.activePills.map(pill => {
			const activePeriod = getActivePeriod(pill);
			if (!activePeriod) {
				return null;
			}

			return [
				pill.name,
				formatServing(multiplyServingValue(pill.value, activePeriod.count), pill.unit),
				formatPillSchedule(activePeriod),
				activePeriod.startDate,
				pill.components
					.map(component =>
						[
							component.name,
							formatServing(
								multiplyServingValue(component.value, activePeriod.count),
								component.unit,
							),
						]
							.filter(Boolean)
							.join(' '),
					)
					.join('\n'),
				activePeriod.tags.map(tag => tag.name).join('\n'),
			];
		})
		.filter((row): row is string[] => row !== null);

	process.stdout.write([headers, ...rows].map(formatCsvRow).join('\n') + '\n');
});

type PillRecord = ReturnType<typeof getPillsDashboard>['pills'][number];
type PillPeriod = PillRecord['periods'][number];
type PillTiming = PillPeriod['timing'];
type PillWeekday = PillPeriod['daysOfWeek'][number];

const weekdayOptions = [
	{ label: 'Mon', value: 'monday' },
	{ label: 'Tue', value: 'tuesday' },
	{ label: 'Wed', value: 'wednesday' },
	{ label: 'Thu', value: 'thursday' },
	{ label: 'Fri', value: 'friday' },
	{ label: 'Sat', value: 'saturday' },
	{ label: 'Sun', value: 'sunday' },
] satisfies Array<{ label: string; value: PillWeekday }>;
const pillWeekdayValues = weekdayOptions.map(option => option.value);

function getTodayDateString() {
	const date = new Date();
	const year = String(date.getFullYear());
	const month = String(date.getMonth() + 1).padStart(2, '0');
	const day = String(date.getDate()).padStart(2, '0');
	return `${year}-${month}-${day}`;
}

function getActivePeriod(pill: PillRecord) {
	const today = getTodayDateString();

	return (
		[...pill.periods]
			.filter(period => period.startDate <= today && (!period.endDate || period.endDate > today))
			.sort((left, right) => left.startDate.localeCompare(right.startDate))
			.at(-1) ?? null
	);
}

function normalizePeriodCount(value: number | null | undefined) {
	return typeof value === 'number' && Number.isFinite(value) && value > 0 ? value : 1;
}

function formatNumericValue(value: number) {
	return Number.isInteger(value) ? String(value) : Number(value.toFixed(4)).toString();
}

function multiplyServingValue(value: string | null | undefined, count: number | null | undefined) {
	const trimmedValue = value?.trim() ?? '';
	const normalizedCount = normalizePeriodCount(count);

	if (!trimmedValue) {
		return '';
	}

	const numericValue = Number(trimmedValue.replace(/,/g, ''));
	if (!Number.isFinite(numericValue)) {
		return normalizedCount === 1
			? trimmedValue
			: `${formatNumericValue(normalizedCount)} x ${trimmedValue}`;
	}

	return formatNumericValue(numericValue * normalizedCount);
}

function formatServing(value?: string | null, unit?: string | null) {
	return [value?.trim(), unit?.trim()].filter(Boolean).join(' ');
}

function formatCsvRow(values: string[]) {
	return values.map(formatCsvCell).join(',');
}

function normalizeWeekdaySelection(values: readonly PillWeekday[]) {
	const selectedValues = new Set(values);
	const orderedValues = pillWeekdayValues.filter(value => selectedValues.has(value));
	return orderedValues.length === pillWeekdayValues.length ? [] : orderedValues;
}

function formatWeekdayFrequency(daysOfWeek: readonly PillWeekday[] | null | undefined) {
	const selectedDays = normalizeWeekdaySelection(daysOfWeek ?? []);
	if (selectedDays.length === 0) {
		return 'daily';
	}

	const dayLabels = selectedDays.map(
		day => weekdayOptions.find(option => option.value === day)?.label ?? day,
	);
	return `every ${dayLabels.join(', ')}`;
}

function formatTimingPhrase(timing: PillTiming | null | undefined) {
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

function formatPillSchedule(period: Pick<PillPeriod, 'daysOfWeek' | 'timing'>) {
	return [formatWeekdayFrequency(period.daysOfWeek), formatTimingPhrase(period.timing)]
		.filter(Boolean)
		.join(' ');
}

function formatCsvCell(value: string) {
	if (!/[",\n\r]/.test(value)) {
		return value;
	}

	return `"${value.replaceAll('"', '""')}"`;
}
