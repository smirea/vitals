import { Card } from '@ant-design/react-native';
import { ScrollView, Text, useColorScheme } from 'react-native';

import { pageStyles } from '@/src/theme/page-styles';

type PendingRouteProps = {
	title: string;
};

export function PendingRoute({ title }: PendingRouteProps) {
	const styles = pageStyles(useColorScheme() === 'dark');

	return (
		<ScrollView contentInsetAdjustmentBehavior='automatic' contentContainerStyle={styles.page}>
			<Card full>
				<Card.Header title={title} />
				<Card.Body>
					<Text style={styles.body}>
						This route is intentionally empty in the setup commit. The web page screenshot, code
						read-through, native implementation, simulator pass, and route commit come next.
					</Text>
				</Card.Body>
			</Card>
		</ScrollView>
	);
}
