import { SymbolView, type SymbolViewProps } from 'expo-symbols';
import { useEffect, useRef, useState, type ReactNode } from 'react';
import {
	Animated,
	Easing,
	Modal,
	Pressable,
	ScrollView,
	Text,
	View,
	useColorScheme,
	useWindowDimensions,
	type StyleProp,
	type ViewStyle,
} from 'react-native';
import { appColors } from '@/src/theme/colors';

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
	return (
		<Pressable
			accessibilityRole='button'
			accessibilityLabel={label}
			onPress={onPress}
			disabled={loading}
			style={({ pressed }) => [
				{
					alignItems: 'center',
					backgroundColor: loading ? '#91caff' : '#1677ff',
					borderRadius: 999,
					bottom: 18,
					boxShadow: '0 8px 22px rgba(22, 119, 255, 0.28)',
					flexDirection: 'row',
					gap: 8,
					paddingHorizontal: 18,
					paddingVertical: 13,
					position: 'absolute',
					right: 16,
					zIndex: 20,
					opacity: pressed ? 0.82 : 1,
				},
				style,
			]}
		>
			<SymbolView name={icon} size={20} tintColor='#fff' weight='bold' />
			<Text style={{ color: '#fff', fontSize: 15, fontWeight: '800' }}>{label}</Text>
		</Pressable>
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
	const isDark = useColorScheme() === 'dark';
	const colors = appColors(isDark);
	return (
		<Pressable
			accessibilityRole='button'
			accessibilityLabel={label}
			onPress={onPress}
			hitSlop={10}
			style={({ pressed }) => ({
				alignItems: 'center',
				backgroundColor: colors.surfaceRaised,
				borderColor: colors.border,
				borderRadius: 999,
				borderWidth: 1,
				height: 36,
				justifyContent: 'center',
				opacity: pressed ? 0.7 : 1,
				width: 36,
			})}
		>
			<SymbolView name={icon} size={19} tintColor={color ?? colors.text} weight='semibold' />
		</Pressable>
	);
}

export function BottomSheet({
	visible,
	title,
	onClose,
	children,
	footer,
}: {
	visible: boolean;
	title: string;
	onClose: () => void;
	children: ReactNode;
	footer?: ReactNode;
}) {
	const isDark = useColorScheme() === 'dark';
	const colors = appColors(isDark);
	const { height } = useWindowDimensions();
	const [mounted, setMounted] = useState(visible);
	const animation = useRef(new Animated.Value(visible ? 1 : 0)).current;

	useEffect(() => {
		if (visible) setMounted(true);
	}, [visible]);

	useEffect(() => {
		if (!mounted) return;

		const timing = Animated.timing(animation, {
			toValue: visible ? 1 : 0,
			duration: visible ? 220 : 170,
			easing: visible ? Easing.out(Easing.cubic) : Easing.in(Easing.cubic),
			useNativeDriver: true,
		});

		timing.start(({ finished }) => {
			if (finished && !visible) setMounted(false);
		});

		return () => timing.stop();
	}, [animation, mounted, visible]);

	if (!mounted) return null;

	const backdropOpacity = animation.interpolate({
		inputRange: [0, 1],
		outputRange: [0, 0.38],
	});
	const sheetTranslateY = animation.interpolate({
		inputRange: [0, 1],
		outputRange: [Math.max(height, 1), 0],
	});

	return (
		<Modal visible={mounted} animationType='none' transparent onRequestClose={onClose}>
			<View style={{ flex: 1, justifyContent: 'flex-end' }}>
				<Animated.View
					pointerEvents={visible ? 'auto' : 'none'}
					style={{
						backgroundColor: 'rgb(15, 23, 42)',
						bottom: 0,
						left: 0,
						opacity: backdropOpacity,
						position: 'absolute',
						right: 0,
						top: 0,
					}}
				>
					<Pressable
						accessibilityRole='button'
						accessibilityLabel='Close'
						onPress={onClose}
						style={{ flex: 1 }}
					/>
				</Animated.View>
				<Animated.View
					style={{
						backgroundColor: colors.surface,
						borderTopLeftRadius: 22,
						borderTopRightRadius: 22,
						gap: 14,
						maxHeight: '86%',
						paddingBottom: 18,
						paddingHorizontal: 16,
						paddingTop: 10,
						transform: [{ translateY: sheetTranslateY }],
					}}
				>
					<Pressable accessibilityRole='button' accessibilityLabel='Close' onPress={onClose}>
						<View
							style={{
								alignSelf: 'center',
								backgroundColor: colors.border,
								borderRadius: 999,
								height: 4,
								width: 42,
							}}
						/>
					</Pressable>
					{title.trim() ? (
						<View
							style={{
								alignItems: 'center',
							}}
						>
							<Text
								style={{
									color: colors.text,
									fontSize: 18,
									fontWeight: '800',
								}}
							>
								{title}
							</Text>
						</View>
					) : null}
					<ScrollView
						contentInsetAdjustmentBehavior='automatic'
						keyboardShouldPersistTaps='handled'
						contentContainerStyle={{ gap: 14, paddingBottom: footer ? 4 : 12 }}
					>
						{children}
					</ScrollView>
					{footer ? <View style={{ gap: 10 }}>{footer}</View> : null}
				</Animated.View>
			</View>
		</Modal>
	);
}

export function SectionLabel({ children }: { children: ReactNode }) {
	const isDark = useColorScheme() === 'dark';
	const colors = appColors(isDark);
	return (
		<Text
			style={{
				color: colors.muted,
				fontSize: 12,
				fontWeight: '800',
				textTransform: 'uppercase',
			}}
		>
			{children}
		</Text>
	);
}
