import type { CSSProperties, ReactNode } from 'react';
import { Label, YStack } from 'tamagui';

type FormFieldProps = {
	label?: ReactNode;
	required?: boolean;
	children: ReactNode;
	style?: CSSProperties;
};

export function FormField({ label, required, children, style }: FormFieldProps) {
	return (
		<YStack gap={6} className='form-field' style={style}>
			{label ? (
				<Label className='form-field-label' padding={0} lineHeight={18}>
					{label}
					{required ? <span> *</span> : null}
				</Label>
			) : null}
			{children}
		</YStack>
	);
}
