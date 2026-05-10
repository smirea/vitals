import { shorthands } from '@tamagui/shorthands/v5';
import { themes, tokens } from '@tamagui/themes/v5';
import { createFont, createTamagui } from 'tamagui';

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
	themes,
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
		onlyAllowShorthands: true,
		styleCompat: 'react-native',
	},
});

type AppConfig = typeof config;

declare module 'tamagui' {
	interface TamaguiCustomConfig extends AppConfig {}
}

export default config;
