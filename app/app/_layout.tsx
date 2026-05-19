import 'react-native-gesture-handler';
import 'react-native-reanimated';

import { Provider as AntProvider } from '@ant-design/react-native';
import { QueryClientProvider } from '@tanstack/react-query';
import { DarkTheme, DefaultTheme, ThemeProvider } from '@react-navigation/native';
import { Stack } from 'expo-router';
import { StatusBar } from 'expo-status-bar';
import { useState } from 'react';
import { useColorScheme } from 'react-native';

import { TRPCProvider, createNativeTrpcClient, createQueryClient } from '@/src/api/trpc';
import { antTheme } from '@/src/theme/colors';

export { ErrorBoundary } from 'expo-router';

export const unstable_settings = {
	initialRouteName: '(tabs)',
};

export default function RootLayout() {
	const colorScheme = useColorScheme();
	const isDark = colorScheme === 'dark';
	const [queryClient] = useState(() => createQueryClient());
	const [trpcClient] = useState(() => createNativeTrpcClient());

	return (
		<AntProvider theme={antTheme(isDark)}>
			<QueryClientProvider client={queryClient}>
				<TRPCProvider queryClient={queryClient} trpcClient={trpcClient}>
					<ThemeProvider value={isDark ? DarkTheme : DefaultTheme}>
						<Stack>
							<Stack.Screen name='(tabs)' options={{ headerShown: false }} />
							<Stack.Screen name='+not-found' />
						</Stack>
						<StatusBar style='auto' />
					</ThemeProvider>
				</TRPCProvider>
			</QueryClientProvider>
		</AntProvider>
	);
}
