export function appColors(isDark: boolean) {
	return {
		background: isDark ? '#0f172a' : '#f6f7f9',
		surface: isDark ? '#111827' : '#fff',
		surfaceRaised: isDark ? '#1f2937' : '#f4f4f5',
		border: isDark ? '#27272a' : '#e5e7eb',
		borderStrong: isDark ? '#334155' : '#bfdbfe',
		text: isDark ? '#f9fafb' : '#111827',
		muted: isDark ? '#a1a1aa' : '#71717a',
		primary: '#1677ff',
		primarySoft: isDark ? '#102a43' : '#e6f4ff',
		danger: '#cf1322',
		success: '#52c41a',
		warning: '#faad14',
	};
}

export function antTheme(isDark: boolean) {
	const colors = appColors(isDark);

	return {
		color_text_base: colors.text,
		color_text_caption: colors.muted,
		color_text_paragraph: colors.text,
		color_text_placeholder: colors.muted,
		color_icon_base: colors.muted,
		fill_body: colors.background,
		fill_base: colors.surface,
		fill_grey: colors.surfaceRaised,
		fill_tap: isDark ? '#334155' : '#dddddd',
		fill_disabled: isDark ? '#1f2937' : '#dddddd',
		fill_mask: isDark ? 'rgba(0, 0, 0, .68)' : 'rgba(0, 0, 0, .4)',
		brand_primary: colors.primary,
		brand_primary_tap: '#0958d9',
		brand_success: colors.success,
		brand_warning: colors.warning,
		brand_error: colors.danger,
		border_color_base: colors.border,
		border_color_thin: colors.border,
		primary_button_fill: colors.primary,
		primary_button_fill_tap: '#0958d9',
		ghost_button_color: colors.primary,
		ghost_button_fill_tap: isDark ? '#102a43' : '#e6f4ff',
		tab_bar_fill: colors.surface,
		search_bar_fill: colors.surfaceRaised,
		notice_bar_fill: colors.primarySoft,
		checkbox_border: colors.border,
		checkbox_fill_disabled: colors.surfaceRaised,
		switch_unchecked: isDark ? '#334155' : '#cccccc',
		toast_fill: isDark ? 'rgba(249, 250, 251, .92)' : 'rgba(0, 0, 0, .8)',
		tooltip_dark: isDark ? 'rgba(249, 250, 251, .94)' : 'rgba(0, 0, 0, .9)',
	};
}
