import { SymbolView, type SymbolViewProps } from 'expo-symbols';
import type { ReactNode } from 'react';
import {
	Modal,
	Pressable,
	ScrollView,
	Text,
	View,
	useColorScheme,
	type StyleProp,
	type ViewStyle,
} from 'react-native';

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
	return (
		<Pressable
			accessibilityRole='button'
			accessibilityLabel={label}
			onPress={onPress}
			hitSlop={10}
			style={({ pressed }) => ({
				alignItems: 'center',
				backgroundColor: isDark ? '#1f2937' : '#fff',
				borderColor: isDark ? '#27272a' : '#e5e7eb',
				borderRadius: 999,
				borderWidth: 1,
				height: 36,
				justifyContent: 'center',
				opacity: pressed ? 0.7 : 1,
				width: 36,
			})}
		>
			<SymbolView
				name={icon}
				size={19}
				tintColor={color ?? (isDark ? '#f9fafb' : '#111827')}
				weight='semibold'
			/>
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
	const surface = isDark ? '#111827' : '#fff';
	const border = isDark ? '#27272a' : '#e5e7eb';

	return (
		<Modal visible={visible} animationType='slide' transparent onRequestClose={onClose}>
			<View style={{ flex: 1, justifyContent: 'flex-end' }}>
				<Pressable
					accessibilityRole='button'
					accessibilityLabel='Close'
					onPress={onClose}
					style={{
						backgroundColor: 'rgba(15, 23, 42, 0.38)',
						bottom: 0,
						left: 0,
						position: 'absolute',
						right: 0,
						top: 0,
					}}
				/>
				<View
					style={{
						backgroundColor: surface,
						borderTopLeftRadius: 22,
						borderTopRightRadius: 22,
						gap: 14,
						maxHeight: '86%',
						paddingBottom: 18,
						paddingHorizontal: 16,
						paddingTop: 10,
					}}
				>
					<Pressable accessibilityRole='button' accessibilityLabel='Close' onPress={onClose}>
						<View
							style={{
								alignSelf: 'center',
								backgroundColor: border,
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
									color: isDark ? '#f9fafb' : '#111827',
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
				</View>
			</View>
		</Modal>
	);
}

export function SectionLabel({ children }: { children: ReactNode }) {
	const isDark = useColorScheme() === 'dark';
	return (
		<Text
			style={{
				color: isDark ? '#a1a1aa' : '#71717a',
				fontSize: 12,
				fontWeight: '800',
				textTransform: 'uppercase',
			}}
		>
			{children}
		</Text>
	);
}
