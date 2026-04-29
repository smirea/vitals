import { createOpenRouter } from '@openrouter/ai-sdk-provider';

import env from 'server/env';

const openrouter = createOpenRouter({ apiKey: env.OPENROUTER_API_KEY });

const models = {
	smart_and_expensive: openrouter('google/gemini-3.1-pro-preview'),
	document_parser_cheap: openrouter('google/gemini-3-flash-preview'),
} as const;

export default models;
