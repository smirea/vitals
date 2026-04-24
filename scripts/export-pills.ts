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
				formatTiming(activePeriod.timing),
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

function formatTiming(timing?: PillPeriod['timing']) {
	if (!timing) {
		return '';
	}

	return timing.charAt(0).toUpperCase() + timing.slice(1);
}

function formatCsvRow(values: string[]) {
	return values.map(formatCsvCell).join(',');
}

function formatCsvCell(value: string) {
	if (!/[",\n\r]/.test(value)) {
		return value;
	}

	return `"${value.replaceAll('"', '""')}"`;
}
