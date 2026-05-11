import { useMemo, useState } from 'react';
import { Pressable, Text, TextInput, View, useColorScheme } from 'react-native';

export type TagSelectorTag = {
	name: string;
	color?: string | null;
};

export function parseTagNames(value: string) {
	const seen = new Set<string>();
	const names: string[] = [];

	for (const rawName of value.split(',')) {
		const name = rawName.trim();
		const key = name.toLocaleLowerCase();
		if (!name || seen.has(key)) continue;
		seen.add(key);
		names.push(name);
	}

	return names;
}

export function formatTagNames(names: string[]) {
	return names.join(', ');
}

export function TagChips({ tags }: { tags: TagSelectorTag[] }) {
	const styles = tagSelectorStyles(useColorScheme() === 'dark');
	if (tags.length === 0) return null;

	return (
		<View style={styles.chipRow}>
			{tags.map(tag => (
				<View
					key={tag.name}
					style={[styles.readChip, tag.color ? { borderColor: tag.color } : null]}
				>
					<Text style={styles.readChipText}>{tag.name}</Text>
				</View>
			))}
		</View>
	);
}

export function TagSelector({
	value,
	availableTags,
	onChange,
	onSubmit,
	submitLabel = 'Add',
	loading = false,
	disabled = false,
}: {
	value: string;
	availableTags: TagSelectorTag[];
	onChange: (value: string) => void;
	onSubmit?: () => void;
	submitLabel?: string;
	loading?: boolean;
	disabled?: boolean;
}) {
	const isDark = useColorScheme() === 'dark';
	const styles = tagSelectorStyles(isDark);
	const selectedNames = useMemo(() => parseTagNames(value), [value]);
	const selectedKeys = useMemo(
		() => new Set(selectedNames.map(name => name.toLocaleLowerCase())),
		[selectedNames],
	);
	const [customName, setCustomName] = useState('');
	const visibleTags = useMemo(() => {
		const tagsByKey = new Map<string, TagSelectorTag>();
		for (const tag of availableTags) tagsByKey.set(tag.name.toLocaleLowerCase(), tag);
		for (const name of selectedNames) {
			const key = name.toLocaleLowerCase();
			if (!tagsByKey.has(key)) tagsByKey.set(key, { name });
		}
		return [...tagsByKey.values()].sort((left, right) => {
			const leftSelected = selectedKeys.has(left.name.toLocaleLowerCase());
			const rightSelected = selectedKeys.has(right.name.toLocaleLowerCase());
			if (leftSelected !== rightSelected) return leftSelected ? -1 : 1;
			return left.name.localeCompare(right.name);
		});
	}, [availableTags, selectedKeys, selectedNames]);

	function setSelected(names: string[]) {
		onChange(formatTagNames(names));
	}

	function toggleTag(name: string) {
		const key = name.toLocaleLowerCase();
		if (selectedKeys.has(key)) {
			setSelected(selectedNames.filter(item => item.toLocaleLowerCase() !== key));
			return;
		}
		setSelected([...selectedNames, name]);
	}

	function addCustomName() {
		const name = customName.trim();
		if (!name) return;
		const key = name.toLocaleLowerCase();
		if (!selectedKeys.has(key)) setSelected([...selectedNames, name]);
		setCustomName('');
	}

	return (
		<View style={styles.wrap}>
			{visibleTags.length > 0 ? (
				<View style={styles.chipRow}>
					{visibleTags.map(tag => {
						const selected = selectedKeys.has(tag.name.toLocaleLowerCase());
						return (
							<Pressable
								key={tag.name}
								disabled={disabled}
								onPress={() => toggleTag(tag.name)}
								style={[
									styles.pickChip,
									selected && styles.pickChipSelected,
									tag.color ? { borderColor: tag.color } : null,
								]}
							>
								<Text style={selected ? styles.pickChipTextSelected : styles.pickChipText}>
									{tag.name}
								</Text>
							</Pressable>
						);
					})}
				</View>
			) : null}
			<View style={styles.inputRow}>
				<TextInput
					value={customName}
					editable={!disabled}
					placeholder='Custom tag'
					placeholderTextColor={styles.placeholder.color}
					style={styles.input}
					autoCapitalize='none'
					onChangeText={setCustomName}
					onSubmitEditing={addCustomName}
				/>
				<Pressable
					disabled={disabled || customName.trim().length === 0}
					onPress={addCustomName}
					style={[styles.smallButton, customName.trim().length === 0 && styles.smallButtonDisabled]}
				>
					<Text style={styles.smallButtonText}>+</Text>
				</Pressable>
				{onSubmit ? (
					<Pressable
						disabled={disabled || loading || selectedNames.length === 0}
						onPress={onSubmit}
						style={[
							styles.submitButton,
							(disabled || loading || selectedNames.length === 0) && styles.submitButtonDisabled,
						]}
					>
						<Text style={styles.submitButtonText}>{loading ? 'Adding...' : submitLabel}</Text>
					</Pressable>
				) : null}
			</View>
		</View>
	);
}

function tagSelectorStyles(isDark: boolean) {
	const text = isDark ? '#f9fafb' : '#111827';
	const muted = isDark ? '#a1a1aa' : '#71717a';
	const surface = isDark ? '#111827' : '#fff';
	const border = isDark ? '#27272a' : '#e5e7eb';

	return {
		wrap: {
			gap: 8,
		},
		chipRow: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			flexWrap: 'wrap' as const,
			gap: 6,
		},
		pickChip: {
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 999,
			borderWidth: 1,
			paddingHorizontal: 9,
			paddingVertical: 5,
		},
		pickChipSelected: {
			backgroundColor: isDark ? '#102a43' : '#e6f4ff',
			borderColor: '#1677ff',
		},
		pickChipText: {
			color: muted,
			fontSize: 12,
			fontWeight: '700' as const,
		},
		pickChipTextSelected: {
			color: '#1677ff',
			fontSize: 12,
			fontWeight: '800' as const,
		},
		readChip: {
			backgroundColor: isDark ? '#1f2937' : '#f6f7f9',
			borderColor: border,
			borderRadius: 999,
			borderWidth: 1,
			paddingHorizontal: 8,
			paddingVertical: 4,
		},
		readChipText: {
			color: text,
			fontSize: 11,
			fontWeight: '700' as const,
		},
		inputRow: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 8,
		},
		input: {
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 8,
			borderWidth: 1,
			color: text,
			flex: 1,
			fontSize: 14,
			paddingHorizontal: 10,
			paddingVertical: 7,
		},
		placeholder: {
			color: muted,
		},
		smallButton: {
			alignItems: 'center' as const,
			backgroundColor: '#1677ff',
			borderRadius: 8,
			height: 36,
			justifyContent: 'center' as const,
			width: 36,
		},
		smallButtonDisabled: {
			backgroundColor: isDark ? '#334155' : '#d9d9d9',
		},
		smallButtonText: {
			color: '#fff',
			fontSize: 20,
			fontWeight: '800' as const,
			lineHeight: 22,
		},
		submitButton: {
			alignItems: 'center' as const,
			backgroundColor: '#1677ff',
			borderRadius: 8,
			justifyContent: 'center' as const,
			minHeight: 36,
			paddingHorizontal: 12,
		},
		submitButtonDisabled: {
			backgroundColor: isDark ? '#334155' : '#d9d9d9',
		},
		submitButtonText: {
			color: '#fff',
			fontSize: 13,
			fontWeight: '800' as const,
		},
	};
}
