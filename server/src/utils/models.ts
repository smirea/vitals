import { createOpenRouter } from '@openrouter/ai-sdk-provider';
import { z } from 'zod';

import env from 'server/env';

const openrouter = createOpenRouter({ apiKey: env.OPENROUTER_API_KEY });

const xaiSpeechToTextResponseSchema = z.object({
	text: z.string(),
	language: z.string().optional(),
	duration: z.number().optional(),
	words: z
		.array(
			z.object({
				text: z.string(),
				start: z.number(),
				end: z.number(),
				speaker: z.number().optional(),
			}),
		)
		.optional(),
});

const models = {
	smart_and_expensive: openrouter('google/gemini-3.1-pro-preview'),
	document_parser_cheap: openrouter('google/gemini-3-flash-preview'),
} as const;

export async function transcribeAudioWithXai(input: {
	audioData: Buffer | Uint8Array | ArrayBuffer;
	fileName: string;
	mimeType: string;
	language?: string;
}) {
	const audioData =
		input.audioData instanceof ArrayBuffer ? input.audioData : copyToArrayBuffer(input.audioData);
	const formData = new FormData();
	formData.append('format', 'true');
	formData.append('language', input.language ?? 'en');
	formData.append('file', new Blob([audioData], { type: input.mimeType }), input.fileName);

	const response = await fetch('https://api.x.ai/v1/stt', {
		method: 'POST',
		headers: {
			Authorization: `Bearer ${env.XAI_API_KEY}`,
		},
		body: formData,
	});

	if (!response.ok) {
		const body = await response.text();
		throw new Error(`xAI speech-to-text failed (${response.status}): ${body}`);
	}

	return xaiSpeechToTextResponseSchema.parse(await response.json());
}

function copyToArrayBuffer(input: Uint8Array) {
	const copy = new Uint8Array(input.byteLength);
	copy.set(input);
	return copy.buffer;
}

export default models;
