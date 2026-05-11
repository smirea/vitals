import { ActivityIndicator, Button, Card, List, Tag } from '@ant-design/react-native';
import { useQuery } from '@tanstack/react-query';
import { ScrollView, Text, View, useColorScheme } from 'react-native';

import { API_BASE_URL, fetchServerStatus, useTRPC } from '@/src/api/trpc';
import { pageStyles } from '@/src/theme/page-styles';

export default function StatusScreen() {
	const trpc = useTRPC();
	const colorScheme = useColorScheme();
	const isDark = colorScheme === 'dark';
	const styles = pageStyles(isDark);
	const statusQuery = useQuery({
		queryKey: ['server-status'],
		queryFn: fetchServerStatus,
		retry: false,
	});
	const tagsQuery = useQuery({
		...trpc.tags.list.queryOptions(),
		retry: false,
	});
	const isLoading = statusQuery.isLoading || tagsQuery.isLoading;
	const isConnected = statusQuery.data?.ok === true && tagsQuery.isSuccess;
	const error = statusQuery.error ?? tagsQuery.error;

	return (
		<ScrollView contentInsetAdjustmentBehavior='automatic' contentContainerStyle={styles.page}>
			<View style={styles.header}>
				<Text style={styles.eyebrow}>Mobile setup</Text>
				<Text style={styles.title}>Vitals is wired to the local server.</Text>
				<Text style={styles.body}>
					This first native screen verifies HTTP and tRPC access before the page-by-page
					implementation starts.
				</Text>
			</View>

			<Card full>
				<Card.Header
					title='Connection'
					extra={
						<Tag small selected={isConnected}>
							{isConnected ? 'Online' : 'Waiting'}
						</Tag>
					}
				/>
				<Card.Body>
					{isLoading ? (
						<View style={styles.inlineStatus}>
							<ActivityIndicator animating />
							<Text style={styles.muted}>Checking server...</Text>
						</View>
					) : error ? (
						<Text selectable style={styles.errorText}>
							{error.message}
						</Text>
					) : (
						<Text selectable style={styles.body}>
							Server responded and returned {tagsQuery.data?.length ?? 0} tags through tRPC.
						</Text>
					)}
				</Card.Body>
				<Card.Footer
					content={
						<Button
							type='primary'
							size='small'
							onPress={() => {
								void statusQuery.refetch();
								void tagsQuery.refetch();
							}}
						>
							Refresh
						</Button>
					}
				/>
			</Card>

			<List renderHeader='Server'>
				<List.Item extra='Bun API'>Runtime</List.Item>
				<List.Item extra={API_BASE_URL} multipleLine wrap>
					Base URL
				</List.Item>
				<List.Item extra='/trpc/tags.list'>tRPC probe</List.Item>
			</List>
		</ScrollView>
	);
}
