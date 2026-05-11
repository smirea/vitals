import type { inferRouterOutputs } from '@trpc/server';

import type { AppRouter } from 'server/trpc/index.ts';

type RouterOutput = inferRouterOutputs<AppRouter>;

export type TagRecord = RouterOutput['tags']['list'][number];

export const tagColorPresets = [
	'#D4380D',
	'#D48806',
	'#389E0D',
	'#08979C',
	'#096DD9',
	'#1D39C4',
	'#531DAB',
	'#C41D7F',
	'#CF1322',
	'#7A45D1',
] as const;

export type TagFormValues = {
	id: number | null;
	name: string;
	note: string;
	color: string;
};

export function createEmptyTagForm(): TagFormValues {
	return {
		id: null,
		name: '',
		note: '',
		color: tagColorPresets[0],
	};
}

export function tagToFormValues(tag: TagRecord): TagFormValues {
	return {
		id: tag.id,
		name: tag.name,
		note: tag.note ?? '',
		color: tag.color,
	};
}

export function assertValidTagForm(form: TagFormValues) {
	if (!form.name.trim()) {
		throw new Error('Enter a tag name.');
	}
	if (!form.color.trim()) {
		throw new Error('Choose a tag color.');
	}
}

export function formatCreatedDate(value: string) {
	const date = new Date(value);
	if (!Number.isFinite(date.getTime())) return value;

	const diffMs = Date.now() - date.getTime();
	const minuteMs = 60 * 1000;
	const hourMs = 60 * minuteMs;
	const dayMs = 24 * hourMs;
	if (Math.abs(diffMs) < minuteMs) return 'just now';
	if (Math.abs(diffMs) < hourMs) return `${Math.round(diffMs / minuteMs)} minutes ago`;
	if (Math.abs(diffMs) < dayMs) return `${Math.round(diffMs / hourMs)} hours ago`;
	return `${Math.round(diffMs / dayMs)} days ago`;
}
