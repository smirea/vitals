import { createOpenRouter } from '@openrouter/ai-sdk-provider';
import env from 'server/env';

const openrouter = createOpenRouter({ apiKey: env.OPENROUTER_API_KEY });

const models = {
	smart_and_expensive: openrouter('google/gemini-3.1-pro'),
	document_parser_cheap: openrouter('google/gemini-2.5-flash'),
} as const;

export default models;
