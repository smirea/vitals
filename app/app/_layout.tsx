import 'react-native-gesture-handler';
import 'react-native-reanimated';

import { Provider as AntProvider } from '@ant-design/react-native';
import { QueryClientProvider } from '@tanstack/react-query';
import { DarkTheme, DefaultTheme, ThemeProvider } from '@react-navigation/native';
import { Stack } from 'expo-router';
import { StatusBar } from 'expo-status-bar';
import { useEffect, useState } from 'react';
import {
	ActivityIndicator,
	KeyboardAvoidingView,
	Platform,
	Pressable,
	Text,
	TextInput,
	View,
	useColorScheme,
} from 'react-native';

import {
	loadNativeAuthToken,
	requestNativeAuthToken,
	subscribeNativeAuthInvalid,
	verifyNativeAuthToken,
} from '@/src/api/auth';
import { TRPCProvider, createNativeTrpcClient, createQueryClient } from '@/src/api/trpc';
import { antTheme, appColors } from '@/src/theme/colors';

export { ErrorBoundary } from 'expo-router';

export const unstable_settings = {
	initialRouteName: '(tabs)',
};

type AuthState = 'checking' | 'authenticated' | 'unauthenticated';

export default function RootLayout() {
	const colorScheme = useColorScheme();
	const isDark = colorScheme === 'dark';
	const [queryClient] = useState(() => createQueryClient());
	const [trpcClient] = useState(() => createNativeTrpcClient());
	const [authState, setAuthState] = useState<AuthState>('checking');

	useEffect(() => {
		let isMounted = true;

		void (async () => {
			await loadNativeAuthToken();
			const isAuthenticated = await verifyNativeAuthToken();
			if (isMounted) {
				setAuthState(isAuthenticated ? 'authenticated' : 'unauthenticated');
			}
		})().catch(error => {
			console.error(error);
			if (isMounted) {
				setAuthState('unauthenticated');
			}
		});

		const unsubscribe = subscribeNativeAuthInvalid(() => {
			queryClient.clear();
			setAuthState('unauthenticated');
		});

		return () => {
			isMounted = false;
			unsubscribe();
		};
	}, [queryClient]);

	return (
		<AntProvider theme={antTheme(isDark)}>
			<ThemeProvider value={isDark ? DarkTheme : DefaultTheme}>
				{authState === 'authenticated' ? (
					<QueryClientProvider client={queryClient}>
						<TRPCProvider queryClient={queryClient} trpcClient={trpcClient}>
							<Stack>
								<Stack.Screen name='(tabs)' options={{ headerShown: false }} />
								<Stack.Screen name='+not-found' />
							</Stack>
						</TRPCProvider>
					</QueryClientProvider>
				) : (
					<NativePasswordScreen
						isChecking={authState === 'checking'}
						onAuthenticated={() => setAuthState('authenticated')}
					/>
				)}
				<StatusBar style='auto' />
			</ThemeProvider>
		</AntProvider>
	);
}

function NativePasswordScreen({
	isChecking,
	onAuthenticated,
}: {
	isChecking: boolean;
	onAuthenticated: () => void;
}) {
	const colors = appColors(useColorScheme() === 'dark');
	const [password, setPassword] = useState('');
	const [isSubmitting, setIsSubmitting] = useState(false);
	const [error, setError] = useState<string | null>(null);
	const isDisabled = isChecking || isSubmitting;

	async function handleSubmit() {
		if (!password || isDisabled) {
			return;
		}

		setIsSubmitting(true);
		setError(null);

		try {
			await requestNativeAuthToken(password);
			setPassword('');
			onAuthenticated();
		} catch (authError) {
			setError(authError instanceof Error ? authError.message : 'Authentication failed');
		} finally {
			setIsSubmitting(false);
		}
	}

	return (
		<KeyboardAvoidingView
			behavior={Platform.OS === 'ios' ? 'padding' : undefined}
			style={{
				alignItems: 'center',
				backgroundColor: colors.background,
				flex: 1,
				justifyContent: 'center',
				padding: 24,
			}}
		>
			<View style={{ gap: 12, width: '100%', maxWidth: 360 }}>
				<TextInput
					autoCapitalize='none'
					autoCorrect={false}
					editable={!isDisabled}
					onChangeText={setPassword}
					onSubmitEditing={handleSubmit}
					placeholder='Password'
					placeholderTextColor={colors.muted}
					secureTextEntry
					style={{
						backgroundColor: colors.surface,
						borderColor: colors.border,
						borderRadius: 10,
						borderWidth: 1,
						color: colors.text,
						fontSize: 16,
						height: 48,
						paddingHorizontal: 14,
					}}
					value={password}
				/>
				<Pressable
					accessibilityRole='button'
					disabled={isDisabled || !password}
					onPress={handleSubmit}
					style={{
						alignItems: 'center',
						backgroundColor: isDisabled || !password ? colors.border : colors.primary,
						borderRadius: 10,
						height: 48,
						justifyContent: 'center',
					}}
				>
					{isSubmitting || isChecking ? (
						<ActivityIndicator color='#ffffff' />
					) : (
						<Text style={{ color: '#ffffff', fontSize: 16, fontWeight: '700' }}>Unlock</Text>
					)}
				</Pressable>
				{error ? <Text style={{ color: colors.danger, fontSize: 14 }}>{error}</Text> : null}
			</View>
		</KeyboardAvoidingView>
	);
}
