#!/usr/bin/env bun
import { and, desc, eq, gte } from 'drizzle-orm';

import { createScript } from './createScript';

import { getDatabase } from 'server/db/client';
import { labDocuments, labMeasurements, labResults } from 'server/db/schema';

const db = getDatabase();
const headers = ['lab', 'date', 'value', 'unit', 'range', 'flags'];

void createScript(() => {
	const cutoff = getCutoffDate();
	const rowsByMeasurementKey = new Map<string, string[]>();
	const rows = db
		.select({
			measurementKey: labMeasurements.key,
			lab: labMeasurements.name,
			date: labDocuments.date,
			valueText: labResults.valueText,
			valueNumeric: labResults.valueNumeric,
			unit: labResults.unit,
			range: labMeasurements.range,
			rangeMin: labMeasurements.rangeMin,
			rangeMax: labMeasurements.rangeMax,
			flags: labResults.note,
		})
		.from(labResults)
		.innerJoin(labDocuments, eq(labResults.documentId, labDocuments.id))
		.innerJoin(labMeasurements, eq(labResults.measurementId, labMeasurements.id))
		.where(and(eq(labDocuments.status, 'completed'), gte(labDocuments.date, cutoff)))
		.orderBy(
			desc(labDocuments.date),
			desc(labDocuments.id),
			desc(labResults.sortOrder),
			desc(labResults.id),
		)
		.all();

	for (const row of rows) {
		const key = row.measurementKey.trim().toLowerCase();
		if (rowsByMeasurementKey.has(key)) {
			continue;
		}

		rowsByMeasurementKey.set(key, [
			row.lab,
			row.date ?? '',
			formatValue(row.valueText, row.valueNumeric),
			row.unit ?? '',
			row.range ?? formatRange(row.rangeMin, row.rangeMax),
			row.flags ?? '',
		]);
	}

	process.stdout.write(
		[headers, ...Array.from(rowsByMeasurementKey.values())].map(formatCsvRow).join('\n') + '\n',
	);
});

function getCutoffDate() {
	const cutoffIndex = process.argv.indexOf('--cutoff');
	if (cutoffIndex >= 0) {
		const value = process.argv[cutoffIndex + 1];
		if (!value) {
			throw new Error('Missing date after --cutoff.');
		}
		if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
			throw new Error('--cutoff must be a YYYY-MM-DD date.');
		}
		return value;
	}

	const date = new Date();
	date.setFullYear(date.getFullYear() - 1);
	const year = String(date.getFullYear());
	const month = String(date.getMonth() + 1).padStart(2, '0');
	const day = String(date.getDate()).padStart(2, '0');
	return `${year}-${month}-${day}`;
}

function formatValue(valueText: string | null, valueNumeric: number | null) {
	const text = valueText?.trim() ?? '';
	if (text) {
		return text;
	}
	if (valueNumeric === null) {
		return '';
	}
	return Number.isInteger(valueNumeric)
		? String(valueNumeric)
		: Number(valueNumeric.toFixed(4)).toString();
}

function formatRange(rangeMin: number | null, rangeMax: number | null) {
	if (rangeMin !== null && rangeMax !== null) {
		return `${formatNumber(rangeMin)} - ${formatNumber(rangeMax)}`;
	}
	if (rangeMin !== null) {
		return `>= ${formatNumber(rangeMin)}`;
	}
	if (rangeMax !== null) {
		return `<= ${formatNumber(rangeMax)}`;
	}
	return '';
}

function formatNumber(value: number) {
	return Number.isInteger(value) ? String(value) : Number(value.toFixed(4)).toString();
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
