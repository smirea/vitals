import { SymbolView, type SymbolViewProps } from 'expo-symbols';
import { createElement, type ReactNode } from 'react';
import {
	ActivityIndicator,
	Pressable,
	Text,
	View,
	useColorScheme,
	type PressableProps,
	type StyleProp,
	type TextStyle,
	type ViewStyle,
} from 'react-native';
import { appColors } from '@/src/theme/colors';

export type ButtonIntent = 'default' | 'primary' | 'success' | 'danger' | 'audio' | 'video';
export type ButtonVariant = 'solid' | 'outline' | 'ghost';
export type ButtonSize = 'small' | 'medium' | 'middle' | 'large';

type ButtonProps = {
	children?: ReactNode;
	label?: string;
	icon?: SymbolViewProps['name'];
	intent?: ButtonIntent;
	type?: 'primary';
	variant?: ButtonVariant;
	size?: ButtonSize;
	loading?: boolean;
	disabled?: boolean;
	active?: boolean;
	iconOnly?: boolean;
	fullWidth?: boolean;
	accessibilityLabel?: string;
	hitSlop?: PressableProps['hitSlop'];
	onPress?: () => void;
	style?: StyleProp<ViewStyle>;
	textStyle?: StyleProp<TextStyle>;
	iconColor?: string;
};

type ResolvedButtonColors = {
	backgroundColor: string;
	borderColor: string;
	textColor: string;
	activeBackgroundColor: string;
	pressedBackgroundColor: string;
	shadow: string;
};

export function Button({
	children,
	label,
	icon,
	intent,
	type,
	variant = 'solid',
	size = 'medium',
	loading = false,
	disabled = false,
	active = false,
	iconOnly = false,
	fullWidth = false,
	accessibilityLabel,
	hitSlop,
	onPress,
	style,
	textStyle,
	iconColor,
}: ButtonProps) {
	const isDark = useColorScheme() === 'dark';
	const colors = appColors(isDark);
	const resolvedIntent = intent ?? (type === 'primary' ? 'primary' : 'default');
	const resolvedColors = getButtonColors(colors, isDark, resolvedIntent, variant, disabled);
	const resolvedSize = getButtonSize(size);
	const content = label ?? children;
	const hasText = !iconOnly && content !== undefined && content !== null;
	const disabledOrLoading = disabled || loading;
	const resolvedAccessibilityLabel =
		accessibilityLabel ??
		(typeof content === 'string' || typeof content === 'number' ? String(content) : undefined);

	return createElement(
		Pressable,
		{
			accessibilityRole: 'button',
			accessibilityLabel: resolvedAccessibilityLabel,
			disabled: disabledOrLoading,
			hitSlop,
			onPress,
			style: ({ pressed }: { pressed: boolean }) => {
				const buttonStyle: ViewStyle = {
					alignItems: 'center',
					backgroundColor:
						pressed && !disabledOrLoading
							? resolvedColors.pressedBackgroundColor
							: active && !disabledOrLoading
								? resolvedColors.activeBackgroundColor
								: resolvedColors.backgroundColor,
					borderColor: resolvedColors.borderColor,
					borderRadius: 999,
					borderWidth: variant === 'ghost' ? 0 : 1,
					boxShadow: disabledOrLoading ? '0 0 0 rgba(0, 0, 0, 0)' : resolvedColors.shadow,
					flexDirection: 'row',
					gap: hasText && (icon || loading) ? 7 : 0,
					justifyContent: 'center',
					minHeight: resolvedSize.height,
					minWidth: iconOnly ? resolvedSize.height : undefined,
					opacity: disabledOrLoading ? 0.62 : pressed ? 0.82 : 1,
					paddingHorizontal: iconOnly ? 0 : resolvedSize.paddingHorizontal,
					paddingVertical: resolvedSize.paddingVertical,
					transform: pressed && !disabledOrLoading ? [{ scale: 0.98 }] : undefined,
					width: fullWidth ? '100%' : undefined,
				};

				return [buttonStyle, style] as StyleProp<ViewStyle>;
			},
		},
		loading
			? createElement(ActivityIndicator, {
					color: iconColor ?? resolvedColors.textColor,
					size: 'small',
				})
			: icon
				? createElement(
						View,
						{
							style: {
								alignItems: 'center',
								height: resolvedSize.iconBox,
								justifyContent: 'center',
								width: resolvedSize.iconBox,
							},
						},
						createElement(SymbolView, {
							name: icon,
							size: resolvedSize.icon,
							tintColor: iconColor ?? resolvedColors.textColor,
							weight: 'bold',
						}),
					)
				: null,
		hasText
			? createElement(
					Text,
					{
						style: [
							{
								color: resolvedColors.textColor,
								fontSize: resolvedSize.fontSize,
								fontWeight: '800',
							},
							textStyle,
						],
					},
					content,
				)
			: null,
	);
}

export function IconButton({
	icon,
	label,
	onPress,
	color,
}: {
	icon: SymbolViewProps['name'];
	label: string;
	onPress: () => void;
	color?: string;
}) {
	return createElement(Button, {
		accessibilityLabel: label,
		hitSlop: 10,
		icon,
		iconColor: color,
		iconOnly: true,
		intent: 'default',
		onPress,
		size: 'small',
		style: {
			height: 36,
			width: 36,
		},
	});
}

export function FloatingActionButton({
	icon,
	label,
	onPress,
	loading = false,
	style,
}: {
	icon: SymbolViewProps['name'];
	label: string;
	onPress: () => void;
	loading?: boolean;
	style?: StyleProp<ViewStyle>;
}) {
	return createElement(Button, {
		icon,
		intent: 'primary',
		label,
		loading,
		onPress,
		size: 'large',
		style: [
			{
				bottom: 18,
				position: 'absolute',
				right: 16,
				zIndex: 20,
			},
			style,
		],
	});
}

function getButtonSize(size: ButtonSize) {
	if (size === 'small') {
		return {
			fontSize: 13,
			height: 36,
			icon: 17,
			iconBox: 18,
			paddingHorizontal: 12,
			paddingVertical: 8,
		};
	}
	if (size === 'large') {
		return {
			fontSize: 15,
			height: 46,
			icon: 20,
			iconBox: 21,
			paddingHorizontal: 18,
			paddingVertical: 12,
		};
	}
	return {
		fontSize: 14,
		height: 40,
		icon: 18,
		iconBox: 19,
		paddingHorizontal: 14,
		paddingVertical: 9,
	};
}

function getButtonColors(
	colors: ReturnType<typeof appColors>,
	isDark: boolean,
	intent: ButtonIntent,
	variant: ButtonVariant,
	disabled: boolean,
): ResolvedButtonColors {
	const palette = {
		default: {
			background: colors.surfaceRaised,
			border: colors.border,
			pressed: isDark ? '#334155' : '#e5e7eb',
			shadow: '0 0 0 rgba(0, 0, 0, 0)',
			text: colors.text,
		},
		primary: {
			background: colors.primary,
			border: colors.primary,
			pressed: '#0958d9',
			shadow: '0 6px 16px rgba(22, 119, 255, 0.2)',
			text: '#fff',
		},
		success: {
			background: '#15803d',
			border: '#15803d',
			pressed: '#166534',
			shadow: '0 6px 16px rgba(21, 128, 61, 0.2)',
			text: '#fff',
		},
		danger: {
			background: colors.danger,
			border: colors.danger,
			pressed: '#a8071a',
			shadow: '0 6px 16px rgba(207, 19, 34, 0.22)',
			text: '#fff',
		},
		audio: {
			background: colors.danger,
			border: colors.danger,
			pressed: '#7f1d1d',
			shadow: '0 6px 16px rgba(207, 19, 34, 0.22)',
			text: '#fff',
		},
		video: {
			background: colors.primary,
			border: colors.primary,
			pressed: '#0958d9',
			shadow: '0 6px 16px rgba(22, 119, 255, 0.2)',
			text: '#fff',
		},
	}[intent];

	if (disabled) {
		return {
			backgroundColor: isDark ? '#3f3f46' : '#a1a1aa',
			borderColor: isDark ? '#3f3f46' : '#a1a1aa',
			activeBackgroundColor: isDark ? '#3f3f46' : '#a1a1aa',
			pressedBackgroundColor: isDark ? '#3f3f46' : '#a1a1aa',
			shadow: '0 0 0 rgba(0, 0, 0, 0)',
			textColor: '#fff',
		};
	}

	if (variant === 'outline') {
		return {
			backgroundColor: 'transparent',
			borderColor: palette.border,
			activeBackgroundColor: isDark ? '#1f2937' : '#eef6ff',
			pressedBackgroundColor: isDark ? '#1f2937' : '#eef6ff',
			shadow: '0 0 0 rgba(0, 0, 0, 0)',
			textColor: intent === 'default' ? colors.text : palette.border,
		};
	}

	if (variant === 'ghost') {
		return {
			backgroundColor: 'transparent',
			borderColor: 'transparent',
			activeBackgroundColor: isDark ? '#1f2937' : '#eef6ff',
			pressedBackgroundColor: isDark ? '#1f2937' : '#eef6ff',
			shadow: '0 0 0 rgba(0, 0, 0, 0)',
			textColor: intent === 'default' ? colors.text : palette.border,
		};
	}

	return {
		backgroundColor: palette.background,
		borderColor: palette.border,
		activeBackgroundColor: palette.pressed,
		pressedBackgroundColor: palette.pressed,
		shadow: palette.shadow,
		textColor: palette.text,
	};
}
