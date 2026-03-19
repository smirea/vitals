import { QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider, createRouter } from '@tanstack/react-router'
import { useState } from 'react'

import {
    TRPCProvider,
    createBrowserTrpcClient,
    createQueryClient,
} from './lib/trpc'
import { AutofillGuard } from './components/AutofillGuard'
import { routeTree } from './routeTree.gen'

const router = createRouter({
    routeTree,
    defaultPreload: 'intent',
})

declare module '@tanstack/react-router' {
    interface Register {
        router: typeof router
    }
}

export default function App() {
    const [queryClient] = useState(() => createQueryClient())
    const [trpcClient] = useState(() => createBrowserTrpcClient())

    return (
        <QueryClientProvider client={queryClient}>
            <TRPCProvider queryClient={queryClient} trpcClient={trpcClient}>
                <AutofillGuard />
                <RouterProvider router={router} />
            </TRPCProvider>
        </QueryClientProvider>
    )
}
