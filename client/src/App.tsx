import { QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider, createRouter } from '@tanstack/react-router';
import { TamaguiProvider } from 'tamagui';
import { useState } from 'react';

import tamaguiConfig from './tamagui.config';
import { TRPCProvider, createBrowserTrpcClient, createQueryClient } from './utils/trpc';
import { AutofillGuard } from './components/AutofillGuard';
import { MessageViewport } from './components/ui';
import { AppContextProvider } from './hooks/useAppContext';
import useAppContext from './hooks/useAppContext';
import { routeTree } from './routeTree.gen';

const router = createRouter({
	routeTree,
	defaultPreload: 'intent',
});

declare module '@tanstack/react-router' {
	interface Register {
		router: typeof router;
	}
}

export default function App() {
	const [queryClient] = useState(() => createQueryClient());
	const [trpcClient] = useState(() => createBrowserTrpcClient());

	return (
		<AppContextProvider>
			<ThemedApp queryClient={queryClient} trpcClient={trpcClient} />
		</AppContextProvider>
	);
}

function ThemedApp({
	queryClient,
	trpcClient,
}: {
	queryClient: ReturnType<typeof createQueryClient>;
	trpcClient: ReturnType<typeof createBrowserTrpcClient>;
}) {
	const { theme } = useAppContext();

	return (
		<TamaguiProvider config={tamaguiConfig} defaultTheme={theme}>
			<QueryClientProvider client={queryClient}>
				<TRPCProvider queryClient={queryClient} trpcClient={trpcClient}>
					<AutofillGuard />
					<MessageViewport />
					<RouterProvider router={router} />
				</TRPCProvider>
			</QueryClientProvider>
		</TamaguiProvider>
	);
}
