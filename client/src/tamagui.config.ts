import { shorthands } from '@tamagui/shorthands/v5';
import { themes, tokens } from '@tamagui/themes/v5';
import { createFont, createTamagui } from 'tamagui';

const appLightTheme = {
	...themes.light,
	background: '#ffffff',
	backgroundHover: '#f4f6fa',
	backgroundPress: '#e9edf4',
	backgroundFocus: '#eaf1ff',
	color: '#151922',
	colorHover: '#151922',
	colorPress: '#151922',
	colorFocus: '#151922',
	borderColor: '#d7dce5',
	borderColorHover: '#9fbcff',
	borderColorFocus: '#2563eb',
	borderColorPress: '#aac2f7',
	placeholderColor: '#8b93a1',
	bgLayout: '#f5f7fb',
	bgContainer: '#ffffff',
	borderSubtle: '#e8ebf1',
	textMuted: '#5f6878',
	textSubtle: '#8b93a1',
	fill: '#f4f6fa',
	fillStrong: '#e9edf4',
	fillSoft: '#f0f3f8',
	fillFaint: '#f8f9fc',
	primary: '#2563eb',
	primaryBg: '#eaf1ff',
	primaryBgHover: '#dbe8ff',
	primaryBorder: '#9fbcff',
	infoBg: '#eaf1ff',
	infoBorder: '#aac2f7',
	infoText: '#1d4ed8',
	success: '#15803d',
	successBg: '#e9f8ef',
	successBorder: '#9ed8b6',
	error: '#dc2626',
	errorBg: '#fff0f0',
	errorBgHover: '#ffe0e0',
	errorBorder: '#f4a7a7',
	warning: '#b45309',
	warningBg: '#fff7e6',
	warningBorder: '#f3c977',
	white: '#ffffff',
	shadowStrong: '0 12px 28px rgba(15, 23, 42, 0.14)',
} as const;

const appDarkTheme = {
	...themes.dark,
	background: '#151b23',
	backgroundHover: '#1b2330',
	backgroundPress: '#253041',
	backgroundFocus: '#14223b',
	color: '#e8edf4',
	colorHover: '#e8edf4',
	colorPress: '#e8edf4',
	colorFocus: '#e8edf4',
	borderColor: '#343b46',
	borderColorHover: '#365d9e',
	borderColorFocus: '#78a5ff',
	borderColorPress: '#365d9e',
	placeholderColor: '#788391',
	bgLayout: '#0d1117',
	bgContainer: '#151b23',
	borderSubtle: '#242c37',
	textMuted: '#a8b3c2',
	textSubtle: '#788391',
	fill: '#1b2330',
	fillStrong: '#253041',
	fillSoft: '#202938',
	fillFaint: '#111721',
	primary: '#78a5ff',
	primaryBg: '#14223b',
	primaryBgHover: '#1b3156',
	primaryBorder: '#365d9e',
	infoBg: '#14223b',
	infoBorder: '#365d9e',
	infoText: '#a9c5ff',
	success: '#65d086',
	successBg: '#10281a',
	successBorder: '#27663d',
	error: '#ff8585',
	errorBg: '#321717',
	errorBgHover: '#461f1f',
	errorBorder: '#773030',
	warning: '#f0bf5b',
	warningBg: '#30240d',
	warningBorder: '#7c5917',
	white: '#ffffff',
	shadowStrong: '0 18px 34px rgba(0, 0, 0, 0.42)',
} as const;

const fontSizes = {
	1: 12,
	2: 13,
	3: 14,
	4: 15,
	true: 15,
	5: 16,
	6: 18,
	7: 22,
	8: 26,
	9: 30,
	10: 40,
	11: 46,
	12: 52,
	13: 60,
	14: 70,
	15: 85,
	16: 100,
} as const;

function createAppFont(weight: Record<number, string> = { 1: '400' }) {
	return createFont({
		family:
			'-apple-system, system-ui, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif',
		size: fontSizes,
		lineHeight: Object.fromEntries(
			Object.entries(fontSizes).map(([key, value]) => [
				key,
				Math.round(Number(value) * (Number(value) >= 22 ? 1.35 : 1.5)),
			]),
		),
		weight,
		letterSpacing: { 4: 0 },
	});
}

const breakpoints = {
	xs: 460,
	sm: 640,
	md: 768,
	lg: 1024,
	xl: 1280,
	xxl: 1536,
};

const media = {
	touchable: { pointer: 'coarse' },
	hoverable: { hover: 'hover' },
	'max-xs': { maxWidth: breakpoints.xs - 0.02 },
	'max-sm': { maxWidth: breakpoints.sm - 0.02 },
	'max-md': { maxWidth: breakpoints.md - 0.02 },
	'max-lg': { maxWidth: breakpoints.lg - 0.02 },
	'max-xl': { maxWidth: breakpoints.xl - 0.02 },
	xs: { minWidth: breakpoints.xs },
	sm: { minWidth: breakpoints.sm },
	md: { minWidth: breakpoints.md },
	lg: { minWidth: breakpoints.lg },
	xl: { minWidth: breakpoints.xl },
	xxl: { minWidth: breakpoints.xxl },
} as const;

export const config = createTamagui({
	media,
	shorthands,
	themes: {
		...themes,
		light: appLightTheme,
		dark: appDarkTheme,
	},
	tokens,
	fonts: {
		body: createAppFont(),
		heading: createAppFont({
			0: '600',
			6: '700',
			9: '800',
		}),
	},
	settings: {
		defaultFont: 'body',
		fastSchemeChange: true,
		shouldAddPrefersColorThemes: true,
		allowedStyleValues: 'somewhat-strict-web',
		addThemeClassName: 'html',
		onlyAllowShorthands: false,
		styleCompat: 'react-native',
	},
});

type AppConfig = typeof config;

declare module 'tamagui' {
	interface TamaguiCustomConfig extends AppConfig {}
}

export default config;
