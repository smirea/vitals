import { QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider, createRouter } from '@tanstack/react-router'
import { ConfigProvider, theme as antdTheme } from 'antd'
import { useState } from 'react'

import {
    TRPCProvider,
    createBrowserTrpcClient,
    createQueryClient,
} from './utils/trpc'
import { AutofillGuard } from './components/AutofillGuard'
import { AppContextProvider } from './hooks/useAppContext'
import useAppContext from './hooks/useAppContext'
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
        <AppContextProvider>
            <ThemedApp queryClient={queryClient} trpcClient={trpcClient} />
        </AppContextProvider>
    )
}

function ThemedApp({
    queryClient,
    trpcClient,
}: {
    queryClient: ReturnType<typeof createQueryClient>
    trpcClient: ReturnType<typeof createBrowserTrpcClient>
}) {
    const { theme } = useAppContext()

    return (
        <ConfigProvider
            theme={{
                algorithm: theme === 'dark'
                    ? antdTheme.darkAlgorithm
                    : antdTheme.defaultAlgorithm,
                token: {
                    fontFamily: 'var(--font-body)',
                },
            }}
        >
            <QueryClientProvider client={queryClient}>
                <TRPCProvider queryClient={queryClient} trpcClient={trpcClient}>
                    <AutofillGuard />
                    <RouterProvider router={router} />
                </TRPCProvider>
            </QueryClientProvider>
        </ConfigProvider>
    )
}
