import { createOpenRouter } from '@openrouter/ai-sdk-provider';
import { z } from 'zod';

import env from 'server/env';

const openrouter = createOpenRouter({ apiKey: env.OPENROUTER_API_KEY });

const elevenLabsSpeechToTextResponseSchema = z
	.object({
		text: z.string(),
		language_code: z.string().nullable().optional(),
	})
	.passthrough();

const models = {
	smart_and_expensive: openrouter('google/gemini-3.1-pro-preview'),
	document_parser_cheap: openrouter('google/gemini-3-flash-preview'),
} as const;

export async function transcribeAudioWithElevenLabs(input: {
	audioData: Buffer | Uint8Array | ArrayBuffer;
	fileName: string;
	mimeType: string;
}) {
	const audioData =
		input.audioData instanceof ArrayBuffer ? input.audioData : copyToArrayBuffer(input.audioData);
	const formData = new FormData();
	formData.append('model_id', 'scribe_v2');
	formData.append('language_code', 'eng');
	formData.append('file', new Blob([audioData], { type: input.mimeType }), input.fileName);

	const response = await fetch('https://api.elevenlabs.io/v1/speech-to-text', {
		method: 'POST',
		headers: {
			'xi-api-key': env.ELEVENLABS_API_KEY,
		},
		body: formData,
	});

	if (!response.ok) {
		const body = await response.text();
		throw new Error(`ElevenLabs speech-to-text failed (${response.status}): ${body}`);
	}

	return elevenLabsSpeechToTextResponseSchema.parse(await response.json());
}

function copyToArrayBuffer(input: Uint8Array) {
	const copy = new Uint8Array(input.byteLength);
	copy.set(input);
	return copy.buffer;
}

export default models;
