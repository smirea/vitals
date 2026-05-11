import { SymbolView } from 'expo-symbols';
import { useMemo, useState } from 'react';
import { Pressable, Text, View, useColorScheme } from 'react-native';

import { BottomSheet } from '@/src/components/mobile-ui';

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
	disabled = false,
}: {
	value: string;
	availableTags: TagSelectorTag[];
	onChange: (value: string) => void;
	disabled?: boolean;
}) {
	const isDark = useColorScheme() === 'dark';
	const styles = tagSelectorStyles(isDark);
	const selectedNames = useMemo(() => parseTagNames(value), [value]);
	const selectedKeys = useMemo(
		() => new Set(selectedNames.map(name => name.toLocaleLowerCase())),
		[selectedNames],
	);
	const [selectorOpen, setSelectorOpen] = useState(false);
	const tagsByKey = useMemo(() => {
		const tagsByKey = new Map<string, TagSelectorTag>();
		for (const tag of availableTags) tagsByKey.set(tag.name.toLocaleLowerCase(), tag);
		return tagsByKey;
	}, [availableTags]);
	const sortedTags = useMemo(
		() => [...availableTags].sort((left, right) => left.name.localeCompare(right.name)),
		[availableTags],
	);
	const selectedTags = useMemo(
		() => selectedNames.map(name => tagsByKey.get(name.toLocaleLowerCase()) ?? { name }),
		[selectedNames, tagsByKey],
	);

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

	function openSelector() {
		if (!disabled) setSelectorOpen(true);
	}

	return (
		<>
			<View style={styles.chipRow}>
				{selectedTags.map(tag => (
					<Pressable
						key={tag.name}
						disabled={disabled}
						onPress={openSelector}
						style={[styles.pickChip, tag.color ? { borderColor: tag.color } : null]}
					>
						<Text style={styles.pickChipText}>{tag.name}</Text>
					</Pressable>
				))}
				<Pressable
					disabled={disabled}
					onPress={openSelector}
					style={[
						styles.addButton,
						selectedNames.length === 0 && styles.addButtonWithLabel,
						disabled && styles.addButtonDisabled,
					]}
				>
					<SymbolView
						name='tag'
						size={15}
						tintColor={disabled ? styles.disabledIcon.color : '#1677ff'}
						weight='semibold'
					/>
					{selectedNames.length === 0 ? <Text style={styles.addButtonText}>add tags</Text> : null}
				</Pressable>
			</View>
			<BottomSheet visible={selectorOpen} title='' onClose={() => setSelectorOpen(false)}>
				<View style={styles.optionList}>
					{sortedTags.map(tag => {
						const selected = selectedKeys.has(tag.name.toLocaleLowerCase());
						return (
							<Pressable
								key={tag.name}
								onPress={() => toggleTag(tag.name)}
								style={styles.optionRow}
							>
								<View style={styles.optionTextWrap}>
									<Text style={styles.optionText}>{tag.name}</Text>
								</View>
								<View style={[styles.checkbox, selected && styles.checkboxSelected]}>
									{selected ? (
										<SymbolView name='checkmark' size={15} tintColor='#fff' weight='bold' />
									) : null}
								</View>
							</Pressable>
						);
					})}
				</View>
			</BottomSheet>
		</>
	);
}

function tagSelectorStyles(isDark: boolean) {
	const text = isDark ? '#f9fafb' : '#111827';
	const muted = isDark ? '#a1a1aa' : '#71717a';
	const surface = isDark ? '#111827' : '#fff';
	const border = isDark ? '#27272a' : '#e5e7eb';

	return {
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
			height: 34,
			justifyContent: 'center' as const,
			paddingHorizontal: 10,
		},
		pickChipText: {
			color: text,
			fontSize: 12,
			fontWeight: '700' as const,
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
		addButton: {
			alignItems: 'center' as const,
			backgroundColor: isDark ? '#102a43' : '#e6f4ff',
			borderColor: '#91caff',
			borderRadius: 999,
			borderWidth: 1,
			flexDirection: 'row' as const,
			gap: 6,
			height: 34,
			justifyContent: 'center' as const,
			width: 34,
		},
		addButtonWithLabel: {
			paddingHorizontal: 11,
			width: 'auto' as const,
		},
		addButtonDisabled: {
			backgroundColor: isDark ? '#1f2937' : '#f4f4f5',
			borderColor: border,
		},
		addButtonText: {
			color: '#1677ff',
			fontSize: 12,
			fontWeight: '800' as const,
		},
		disabledIcon: {
			color: muted,
		},
		optionList: {
			gap: 4,
		},
		optionRow: {
			alignItems: 'center' as const,
			borderBottomColor: border,
			borderBottomWidth: 1,
			flexDirection: 'row' as const,
			gap: 12,
			justifyContent: 'space-between' as const,
			minHeight: 46,
			paddingVertical: 8,
		},
		optionTextWrap: {
			flex: 1,
		},
		optionText: {
			color: text,
			fontSize: 15,
			fontWeight: '600' as const,
		},
		checkbox: {
			alignItems: 'center' as const,
			borderColor: border,
			borderRadius: 6,
			borderWidth: 1,
			height: 24,
			justifyContent: 'center' as const,
			width: 24,
		},
		checkboxSelected: {
			backgroundColor: '#1677ff',
			borderColor: '#1677ff',
		},
	};
}
