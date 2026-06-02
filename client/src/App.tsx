import { QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider, createRouter } from '@tanstack/react-router';
import { Alert, Button, ConfigProvider, Input, Space, Spin, theme as antdTheme } from 'antd';
import { useEffect, useState, type FormEvent } from 'react';

import { AutofillGuard } from './components/AutofillGuard';
import { AppContextProvider } from './hooks/useAppContext';
import useAppContext from './hooks/useAppContext';
import { requestServerToken, subscribeToAuthInvalid, verifyStoredServerToken } from './utils/auth';
import { TRPCProvider, createBrowserTrpcClient, createQueryClient } from './utils/trpc';
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
	const [authState, setAuthState] = useState<'checking' | 'authenticated' | 'unauthenticated'>(
		'checking',
	);

	useEffect(() => {
		let isMounted = true;

		void verifyStoredServerToken()
			.then(isAuthenticated => {
				if (isMounted) {
					setAuthState(isAuthenticated ? 'authenticated' : 'unauthenticated');
				}
			})
			.catch(error => {
				console.error(error);
				if (isMounted) {
					setAuthState('unauthenticated');
				}
			});

		const unsubscribe = subscribeToAuthInvalid(() => {
			queryClient.clear();
			setAuthState('unauthenticated');
		});

		return () => {
			isMounted = false;
			unsubscribe();
		};
	}, [queryClient]);

	return (
		<ConfigProvider
			theme={{
				algorithm: theme === 'dark' ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
				token: {
					fontFamily: 'var(--font-body)',
				},
			}}
		>
			{authState === 'authenticated' ? (
				<QueryClientProvider client={queryClient}>
					<TRPCProvider queryClient={queryClient} trpcClient={trpcClient}>
						<AutofillGuard />
						<RouterProvider router={router} />
					</TRPCProvider>
				</QueryClientProvider>
			) : (
				<PasswordScreen
					isChecking={authState === 'checking'}
					onAuthenticated={() => setAuthState('authenticated')}
				/>
			)}
		</ConfigProvider>
	);
}

function PasswordScreen({
	isChecking,
	onAuthenticated,
}: {
	isChecking: boolean;
	onAuthenticated: () => void;
}) {
	const [password, setPassword] = useState('');
	const [isSubmitting, setIsSubmitting] = useState(false);
	const [error, setError] = useState<string | null>(null);

	async function handleSubmit(event: FormEvent) {
		event.preventDefault();
		setIsSubmitting(true);
		setError(null);

		try {
			await requestServerToken(password);
			setPassword('');
			onAuthenticated();
		} catch (authError) {
			setError(authError instanceof Error ? authError.message : 'Authentication failed');
		} finally {
			setIsSubmitting(false);
		}
	}

	return (
		<main className='auth-screen'>
			<form className='auth-form' onSubmit={handleSubmit}>
				<Space.Compact className='auth-password-row'>
					<Input.Password
						autoFocus
						disabled={isChecking || isSubmitting}
						placeholder='Password'
						value={password}
						onChange={event => setPassword(event.target.value)}
					/>
					<Button
						htmlType='submit'
						type='primary'
						disabled={isChecking || !password}
						loading={isSubmitting}
					>
						Unlock
					</Button>
				</Space.Compact>
				{isChecking ? <Spin /> : null}
				{error ? <Alert type='error' showIcon message={error} /> : null}
			</form>
		</main>
	);
}
