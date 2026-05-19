import { QueryClient } from '@tanstack/react-query';
import { createTRPCClient, httpBatchLink, httpLink, splitLink } from '@trpc/client';
import { createTRPCContext } from '@trpc/tanstack-react-query';
import Constants from 'expo-constants';

import type { AppRouter } from 'server/trpc/index.ts';

const API_PORT = '6001';

function getApiBaseUrl() {
	const hostUri = Constants.expoConfig?.hostUri;
	if (!hostUri) {
		throw new Error(
			'Expo hostUri is missing; start Expo on LAN so Vitals can find the laptop API.',
		);
	}

	const expoUrl = new URL(`http://${hostUri}`);
	if (expoUrl.hostname.endsWith('.exp.direct')) {
		throw new Error(
			'Expo tunnel host cannot expose the local Vitals API; use LAN mode or set up an API tunnel.',
		);
	}

	expoUrl.protocol = 'http:';
	expoUrl.port = API_PORT;
	expoUrl.pathname = '';
	expoUrl.search = '';
	expoUrl.hash = '';

	return expoUrl.origin;
}

export const API_BASE_URL = getApiBaseUrl();

export const { TRPCProvider, useTRPC, useTRPCClient } = createTRPCContext<AppRouter>();

export function createQueryClient() {
	return new QueryClient({
		defaultOptions: {
			queries: {
				staleTime: 60_000,
				refetchOnWindowFocus: false,
			},
		},
	});
}

export function createNativeTrpcClient() {
	return createTRPCClient<AppRouter>({
		links: [
			splitLink({
				condition: op => op.path === 'sensors.runExtractor',
				true: httpLink({
					url: `${API_BASE_URL}/trpc`,
				}),
				false: httpBatchLink({
					url: `${API_BASE_URL}/trpc`,
				}),
			}),
		],
	});
}

export async function fetchServerStatus() {
	const response = await fetch(`${API_BASE_URL}/status`);
	if (!response.ok) {
		throw new Error(`Server status failed with ${response.status}`);
	}

	const payload = (await response.json()) as { ok?: boolean };
	if (payload.ok !== true) {
		throw new Error('Server status returned an invalid payload');
	}

	return { ok: true };
}
