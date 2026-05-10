import type { ReactNode } from 'react';
import { useId, useState } from 'react';
import { Input, Text, XStack, YStack } from 'tamagui';

import { TagChip } from './TagChip';

type TagInputOption = {
	label: ReactNode;
	value: string;
	color?: string;
};

type TagInputProps = {
	value: string[];
	options?: TagInputOption[];
	placeholder?: string;
	disabled?: boolean;
	onChange: (value: string[]) => void;
};

export function TagInput({ value, options = [], placeholder, disabled, onChange }: TagInputProps) {
	const listId = useId();
	const [draft, setDraft] = useState('');

	function addValue(raw: string) {
		const next = raw.trim();
		if (!next || value.some(item => item.toLocaleLowerCase() === next.toLocaleLowerCase())) {
			return;
		}
		onChange([...value, next]);
		setDraft('');
	}

	function removeValue(raw: string) {
		onChange(value.filter(item => item !== raw));
	}

	return (
		<YStack gap={6}>
			<XStack className='tag-input' flexWrap='wrap' gap={6}>
				{value.map(item => {
					const option = options.find(candidate => candidate.value === item);
					return (
						<TagChip key={item} color={option?.color} closable onClose={() => removeValue(item)}>
							{option?.label ?? item}
						</TagChip>
					);
				})}
				<Input
					className='tag-input-field'
					disabled={disabled}
					value={draft}
					placeholder={value.length === 0 ? placeholder : undefined}
					list={listId}
					onChange={event => {
						const next = event.target.value;
						if (next.includes(',')) {
							next.split(',').forEach(addValue);
							return;
						}
						setDraft(next);
					}}
					onKeyDown={event => {
						if (event.key === 'Enter') {
							event.preventDefault();
							addValue(draft);
						}
						if (event.key === 'Backspace' && !draft && value.length > 0) {
							onChange(value.slice(0, -1));
						}
					}}
					onBlur={() => addValue(draft)}
				/>
				<datalist id={listId}>
					{options.map(option => (
						<option key={option.value} value={option.value} />
					))}
				</datalist>
			</XStack>
			{value.length === 0 && !placeholder ? <Text color='$textMuted'>No tags</Text> : null}
		</YStack>
	);
}
