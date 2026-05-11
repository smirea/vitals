import { Link, Stack } from 'expo-router';
import { Text, View } from 'react-native';

export default function NotFoundScreen() {
	return (
		<>
			<Stack.Screen options={{ title: 'Not found' }} />
			<View
				style={{
					alignItems: 'center',
					flex: 1,
					gap: 12,
					justifyContent: 'center',
					padding: 20,
				}}
			>
				<Text style={{ fontSize: 20, fontWeight: '700' }}>Screen not found</Text>
				<Link href='/' style={{ color: '#1677ff', fontSize: 15 }}>
					Go to status
				</Link>
			</View>
		</>
	);
}
