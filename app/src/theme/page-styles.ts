import type { StyleProp, ViewStyle } from 'react-native';

export function pageStyles(isDark: boolean) {
	const text = isDark ? '#f9fafb' : '#111827';
	const muted = isDark ? '#a1a1aa' : '#71717a';

	return {
		page: {
			gap: 14,
			padding: 16,
			backgroundColor: isDark ? '#0f172a' : '#f6f7f9',
		} satisfies StyleProp<ViewStyle>,
		header: {
			gap: 6,
		},
		eyebrow: {
			color: '#1677ff',
			fontSize: 12,
			fontWeight: '700' as const,
			textTransform: 'uppercase' as const,
		},
		title: {
			color: text,
			fontSize: 26,
			fontWeight: '800' as const,
		},
		body: {
			color: text,
			fontSize: 15,
			lineHeight: 21,
		},
		muted: {
			color: muted,
			fontSize: 14,
		},
		errorText: {
			color: '#cf1322',
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
