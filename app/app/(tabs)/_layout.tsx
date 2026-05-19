import { Tabs } from 'expo-router';
import { SymbolView, type SymbolViewProps } from 'expo-symbols';
import { Text, useColorScheme } from 'react-native';

const activeTintColor = '#1677ff';

export const unstable_settings = {
	initialRouteName: 'log',
};

function TabIcon({
	color,
	name,
	fallback,
}: {
	color: string;
	name: SymbolViewProps['name'];
	fallback: string;
}) {
	return (
		<SymbolView
			name={name}
			size={23}
			tintColor={color}
			weight='semibold'
			resizeMode='scaleAspectFit'
			fallback={<Text style={{ color, fontSize: 16 }}>{fallback}</Text>}
		/>
	);
}

export default function TabLayout() {
	const colorScheme = useColorScheme();
	const isDark = colorScheme === 'dark';

	return (
		<Tabs
			initialRouteName='log'
			screenOptions={{
				headerShown: false,
				tabBarActiveTintColor: activeTintColor,
				tabBarInactiveTintColor: isDark ? '#a1a1aa' : '#71717a',
				tabBarLabelStyle: {
					fontSize: 12,
					fontWeight: '600',
				},
				tabBarStyle: {
					backgroundColor: isDark ? '#111827' : '#ffffff',
					borderTopColor: isDark ? '#27272a' : '#e5e7eb',
				},
			}}
		>
			<Tabs.Screen
				name='labs'
				options={{
					title: 'Labs',
					tabBarIcon: ({ color }) => (
						<TabIcon color={color} name='heart.text.square.fill' fallback='L' />
					),
				}}
			/>
			<Tabs.Screen
				name='pills'
				options={{
					title: 'Pills',
					tabBarIcon: ({ color }) => <TabIcon color={color} name='pills.fill' fallback='P' />,
				}}
			/>
			<Tabs.Screen
				name='log'
				options={{
					title: "Captain's Log",
					tabBarLabel: 'Log',
					tabBarIcon: ({ color }) => <TabIcon color={color} name='book.pages.fill' fallback='G' />,
				}}
			/>
			<Tabs.Screen
				name='sensors'
				options={{
					title: 'Sensors',
					tabBarIcon: ({ color }) => (
						<TabIcon color={color} name='waveform.path.ecg.rectangle.fill' fallback='N' />
					),
				}}
			/>
			<Tabs.Screen
				name='tags'
				options={{
					title: 'Tags',
					tabBarIcon: ({ color }) => <TabIcon color={color} name='tag.fill' fallback='T' />,
				}}
			/>
		</Tabs>
	);
}
