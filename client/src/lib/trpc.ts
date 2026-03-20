import { QueryClient } from '@tanstack/react-query';
import { createTRPCClient, httpBatchLink } from '@trpc/client';
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
    const apiBaseUrl = import.meta.env.VITE_API_URL?.trim();

    return createTRPCClient<AppRouter>({
        links: [
            httpBatchLink({
                url: apiBaseUrl ? `${apiBaseUrl}/trpc` : '/trpc',
            }),
        ],
    });
}
