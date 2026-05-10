import { useEffect, useRef } from 'react';
import { TextArea, type TextAreaProps } from 'tamagui';

type AutoResizeTextAreaProps = TextAreaProps & {
	minRows?: number;
	maxRows?: number;
};

function readPixelValue(value: string) {
	return Number.parseFloat(value) || 0;
}

function resizeTextArea(element: HTMLTextAreaElement, minRows?: number, maxRows?: number) {
	const styles = window.getComputedStyle(element);
	const lineHeight =
		readPixelValue(styles.lineHeight) || readPixelValue(styles.fontSize) * 1.2 || 24;
	const verticalPadding = readPixelValue(styles.paddingTop) + readPixelValue(styles.paddingBottom);
	const verticalBorder =
		styles.boxSizing === 'border-box'
			? readPixelValue(styles.borderTopWidth) + readPixelValue(styles.borderBottomWidth)
			: 0;
	const minHeight = minRows ? minRows * lineHeight + verticalPadding + verticalBorder : 0;
	const maxHeight = maxRows
		? maxRows * lineHeight + verticalPadding + verticalBorder
		: Number.POSITIVE_INFINITY;

	element.style.height = 'auto';
	const nextHeight = Math.max(
		minHeight,
		Math.min(element.scrollHeight + verticalBorder, maxHeight),
	);
	element.style.height = `${nextHeight}px`;
	element.style.overflowY = element.scrollHeight + verticalBorder > maxHeight ? 'auto' : 'hidden';
}

export function AutoResizeTextArea({
	minRows,
	maxRows,
	onChange,
	onInput,
	value,
	defaultValue,
	rows,
	style,
	...props
}: AutoResizeTextAreaProps) {
	const ref = useRef<HTMLTextAreaElement>(null);

	useEffect(() => {
		if (!ref.current) return;
		resizeTextArea(ref.current, minRows, maxRows);
	}, [minRows, maxRows, value, defaultValue]);

	return (
		<TextArea
			{...props}
			ref={ref as any}
			value={value}
			defaultValue={defaultValue}
			rows={minRows ?? rows}
			style={{ resize: 'none', ...(style as object) }}
			onChange={event => {
				onChange?.(event);
				resizeTextArea(event.currentTarget as unknown as HTMLTextAreaElement, minRows, maxRows);
			}}
			onInput={event => {
				onInput?.(event);
				resizeTextArea(event.currentTarget as unknown as HTMLTextAreaElement, minRows, maxRows);
			}}
		/>
	);
}
