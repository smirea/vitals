import { QueryClient } from '@tanstack/react-query';
import { createTRPCClient, httpBatchLink, httpLink, splitLink } from '@trpc/client';
import { createTRPCContext } from '@trpc/tanstack-react-query';

import type { AppRouter } from 'server/trpc/index.ts';

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

export function createBrowserTrpcClient() {
	return createTRPCClient<AppRouter>({
		links: [
			splitLink({
				condition: op => op.path === 'sensors.runExtractor',
				true: httpLink({
					url: `/api/trpc`,
				}),
				false: httpBatchLink({
					url: `/api/trpc`,
				}),
			}),
		],
	});
}
