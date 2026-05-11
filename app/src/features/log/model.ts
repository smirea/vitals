import type { inferRouterOutputs } from '@trpc/server';

import type { AppRouter } from 'server/trpc/index.ts';

import { API_BASE_URL } from '@/src/api/trpc';

type RouterOutput = inferRouterOutputs<AppRouter>;

export type DiaryEntry = RouterOutput['diary']['list'][number];
export type DiaryVoiceMemo = DiaryEntry['voiceMemos'][number];
export type DiaryPendingVoiceMemo = RouterOutput['diary']['listPendingVoiceMemos'][number];
export type DiaryPendingVoiceMemoRecovery =
	RouterOutput['diary']['listPendingVoiceMemoRecoveries'][number];
export type TagRecord = RouterOutput['tags']['list'][number];

export type DiaryLocationInput = {
	capturedAt: string;
	latitude: number;
	longitude: number;
	accuracy: number | null;
	altitude: number | null;
	altitudeAccuracy: number | null;
	heading: number | null;
	speed: number | null;
};

export function parseTagText(value: string) {
	const seen = new Set<string>();
	const tags: string[] = [];

	for (const rawTag of value.split(',')) {
		const tag = rawTag.trim();
		const key = tag.toLocaleLowerCase();
		if (!tag || seen.has(key)) continue;
		seen.add(key);
		tags.push(tag);
	}

	return tags;
}

export function appendTagText(value: string, tagName: string) {
	const tags = parseTagText(value);
	const seen = new Set(tags.map(tag => tag.toLocaleLowerCase()));
	if (!seen.has(tagName.toLocaleLowerCase())) tags.push(tagName);
	return tags.join(', ');
}

export function getEntryTranscriptText(entry: DiaryEntry) {
	const voiceTranscripts = entry.voiceMemos
		.map(memo => memo.transcript?.trim() ?? '')
		.filter(Boolean);

	if (voiceTranscripts.length > 0) {
		return voiceTranscripts.join('\n\n');
	}

	return entry.notes.trim();
}

export function getEntryPreview(entry: DiaryEntry) {
	return getEntryTranscriptText(entry) || entry.summary?.trim() || 'No notes or transcript';
}

export function formatDiaryDate(value: string) {
	return new Date(value).toLocaleDateString('en-US', {
		month: 'short',
		day: 'numeric',
		year: 'numeric',
	});
}

export function formatDiaryTime(value: string) {
	return new Date(value).toLocaleTimeString('en-US', {
		hour: 'numeric',
		minute: '2-digit',
	});
}

export function formatDiaryTimestamp(value: string) {
	return `${formatDiaryDate(value)} at ${formatDiaryTime(value)}`;
}

export function formatDuration(value: number | null) {
	if (!Number.isFinite(value)) {
		return 'unknown duration';
	}

	const minutes = Math.floor((value ?? 0) / 60);
	const seconds = Math.round((value ?? 0) % 60)
		.toString()
		.padStart(2, '0');
	return `${minutes}:${seconds}`;
}

export function formatBytes(value: number) {
	if (value < 1024) {
		return `${value} B`;
	}
	if (value < 1024 * 1024) {
		return `${(value / 1024).toFixed(1)} KB`;
	}

	return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

export function formatLocationLabel(location: {
	name?: string | null;
	latitude: number;
	longitude: number;
}) {
	return (
		location.name?.trim() || `${location.latitude.toFixed(4)}, ${location.longitude.toFixed(4)}`
	);
}

export function mapsUrl(location: { latitude: number; longitude: number }) {
	return `https://www.google.com/maps?q=${location.latitude},${location.longitude}`;
}

export function voiceMemoAudioUrl(voiceMemoId: number) {
	return `${API_BASE_URL}/diary/voice-memos/${voiceMemoId}/audio`;
}

export function audioFileNameFromUri(uri: string) {
	const extension = audioExtensionFromUri(uri);
	return `diary-${new Date().toISOString().replace(/[:.]/g, '-')}.${extension}`;
}

export function audioMimeTypeFromUri(uri: string) {
	const extension = audioExtensionFromUri(uri);
	if (extension === 'm4a' || extension === 'mp4') return 'audio/mp4';
	if (extension === 'wav') return 'audio/wav';
	if (extension === 'caf') return 'audio/x-caf';
	if (extension === 'webm') return 'audio/webm';
	if (extension === 'ogg') return 'audio/ogg';
	return `audio/${extension}`;
}

export function formatRecorderDuration(milliseconds: number) {
	return formatDuration(Math.max(milliseconds, 0) / 1000);
}

function audioExtensionFromUri(uri: string) {
	const path = uri.split('?')[0] ?? uri;
	const extension = path
		.split('.')
		.at(-1)
		?.toLowerCase()
		.replace(/[^a-z0-9]/g, '');
	if (!extension) {
		throw new Error(`Recording URI has no file extension: ${uri}`);
	}

	return extension;
}
