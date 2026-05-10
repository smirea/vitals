import type { CSSProperties, ReactNode } from 'react';
import { X } from '@phosphor-icons/react';
import { Button, Text, XStack, useTheme } from 'tamagui';

type TagChipProps = {
	children: ReactNode;
	color?: string;
	closable?: boolean;
	onClose?: () => void;
	onPress?: () => void;
	icon?: ReactNode;
	className?: string;
	style?: CSSProperties;
};

export function TagChip({
	children,
	color,
	closable,
	onClose,
	onPress,
	icon,
	className,
	style,
}: TagChipProps) {
	const theme = useTheme();
	const resolvedColor = resolveTagColor(color, theme);

	return (
		<XStack
			className={className}
			alignItems='center'
			gap={5}
			borderWidth={1}
			borderRadius={999}
			paddingHorizontal={8}
			paddingVertical={2}
			cursor={onPress ? 'pointer' : undefined}
			onPress={onPress}
			style={{
				borderColor: resolvedColor ?? theme.borderColor?.get('web'),
				background: resolvedColor ? `${resolvedColor}22` : theme.fill?.get('web'),
				color: resolvedColor ?? theme.color?.get('web'),
				...style,
			}}
		>
			{icon}
			<Text fontSize={12} color='inherit'>
				{children}
			</Text>
			{closable ? (
				<Button
					size='$1'
					chromeless
					padding={0}
					width={16}
					height={16}
					onPress={event => {
						event.stopPropagation();
						onClose?.();
					}}
				>
					<X size={10} />
				</Button>
			) : null}
		</XStack>
	);
}

function resolveTagColor(color: string | undefined, theme: ReturnType<typeof useTheme>) {
	switch (color) {
		case 'success':
			return theme.success?.get('web');
		case 'error':
			return theme.error?.get('web');
		case 'processing':
			return theme.primary?.get('web');
		case 'default':
			return theme.textMuted?.get('web');
		default:
			return color;
	}
}
