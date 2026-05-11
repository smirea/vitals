import type { inferRouterOutputs } from '@trpc/server';

import type { AppRouter } from 'server/trpc/index.ts';

type RouterOutput = inferRouterOutputs<AppRouter>;

export type SensorsConfig = RouterOutput['sensors']['getConfig'];
export type SensorRunResult = RouterOutput['sensors']['runExtractor'];

export const sensorKeys = [
	'labs',
	'pills',
	'voiceMemos',
	'macrofactor',
	'whoop',
	'workouts',
] as const;
export const dependentSensorKeys = [
	'pills',
	'voiceMemos',
	'macrofactor',
	'whoop',
	'workouts',
] as const;
export const outputModes = ['text', 'json', 'csv'] as const;
export const voiceMemoContentOptions = ['raw', 'summary', 'both'] as const;

export type SensorKey = (typeof sensorKeys)[number];
export type DependentSensorKey = (typeof dependentSensorKeys)[number];
export type OutputMode = (typeof outputModes)[number];
export type VoiceMemoContent = (typeof voiceMemoContentOptions)[number];
export type RunStatus = 'idle' | 'running' | 'success' | 'error';

export type RunState = {
	status: RunStatus;
	error: string | null;
	completedAt: string | null;
};

export type LabsConfig = {
	textFilter: string;
	categories: string[];
	startDate: string | null;
	onlyLatest: boolean;
};

export type VoiceMemosConfig = {
	content: VoiceMemoContent;
};

export type MacrofactorConfig = {
	recipeDetails: boolean;
};

export const sensorLabels = {
	labs: 'Labs',
	pills: 'Pills',
	voiceMemos: "Captain's Log",
	macrofactor: 'MacroFactor',
	whoop: 'WHOOP',
	workouts: 'Workouts',
} satisfies Record<SensorKey, string>;

export function createDefaultRunStates() {
	return Object.fromEntries(
		sensorKeys.map(key => [key, { status: 'idle', error: null, completedAt: null }]),
	) as Record<SensorKey, RunState>;
}

export function getDateDaysAgo(days: number) {
	const date = new Date();
	date.setDate(date.getDate() - days);
	return formatDateInput(date);
}

export function getDateMonthsAgo(months: number) {
	const date = new Date();
	date.setMonth(date.getMonth() - months);
	return formatDateInput(date);
}

export function formatDateInput(date: Date) {
	const year = String(date.getFullYear());
	const month = String(date.getMonth() + 1).padStart(2, '0');
	const day = String(date.getDate()).padStart(2, '0');
	return `${year}-${month}-${day}`;
}

export function isDateInput(value: string) {
	return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

export function getDefaultLabStartDate(config: SensorsConfig | undefined) {
	const targetDate = getDateMonthsAgo(12);
	const sortedDates = [...(config?.labDates ?? [])].sort((left, right) =>
		right.localeCompare(left),
	);
	return sortedDates.find(date => date <= targetDate) ?? sortedDates.at(-1) ?? null;
}

export function buildCompiledJsonText(
	defaultStartDate: string,
	results: Partial<Record<SensorKey, SensorRunResult>>,
) {
	return JSON.stringify(
		{
			generatedAt: new Date().toISOString(),
			startDate: defaultStartDate,
			sections: Object.fromEntries(
				sensorKeys.flatMap((key): Array<[SensorKey, unknown]> => {
					const result = results[key];
					return result && result.json !== null ? [[key, result.json]] : [];
				}),
			),
		},
		null,
		2,
	);
}

export function buildCompiledText(results: Partial<Record<SensorKey, SensorRunResult>>) {
	return sensorKeys
		.map(key => {
			const result = results[key];
			if (!result?.text) return null;
			return `# ${result.label}\n\n${result.text.trim()}`;
		})
		.filter(Boolean)
		.join('\n\n');
}

export function getCsvFiles(results: Partial<Record<SensorKey, SensorRunResult>>) {
	return sensorKeys.flatMap(key => results[key]?.csvFiles ?? []);
}

export function dateStamp() {
	const now = new Date();
	const date = formatDateInput(now);
	const hours = String(now.getHours()).padStart(2, '0');
	const minutes = String(now.getMinutes()).padStart(2, '0');
	const seconds = String(now.getSeconds()).padStart(2, '0');
	return `${date}-${hours}${minutes}${seconds}`;
}

export function formatCompletedAt(value: string | null) {
	if (!value) return '';
	return new Date(value).toLocaleTimeString('en-US', {
		hour: 'numeric',
		minute: '2-digit',
	});
}

export function parseCategoryText(value: string) {
	const seen = new Set<string>();
	const categories: string[] = [];
	for (const rawCategory of value.split(',')) {
		const category = rawCategory.trim();
		const key = category.toLocaleLowerCase();
		if (!category || seen.has(key)) continue;
		seen.add(key);
		categories.push(category);
	}
	return categories;
}
