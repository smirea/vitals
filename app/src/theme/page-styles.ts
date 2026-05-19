import type { StyleProp, ViewStyle } from 'react-native';
import { appColors } from '@/src/theme/colors';

export function pageStyles(isDark: boolean) {
	const colors = appColors(isDark);

	return {
		page: {
			backgroundColor: colors.background,
			gap: 14,
			paddingBottom: 94,
			paddingHorizontal: 14,
			paddingTop: 12,
		} satisfies StyleProp<ViewStyle>,
		header: {
			gap: 6,
		},
		eyebrow: {
			color: colors.primary,
			fontSize: 12,
			fontWeight: '700' as const,
			textTransform: 'uppercase' as const,
		},
		title: {
			color: colors.text,
			fontSize: 26,
			fontWeight: '800' as const,
		},
		body: {
			color: colors.text,
			fontSize: 15,
			lineHeight: 21,
		},
		muted: {
			color: colors.muted,
			fontSize: 14,
		},
		errorText: {
			color: colors.danger,
			fontSize: 14,
			lineHeight: 20,
		},
		inlineStatus: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 10,
		},
	};
}
