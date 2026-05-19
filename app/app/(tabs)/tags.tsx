import type { inferRouterOutputs } from '@trpc/server';
import type { AppRouter } from 'server/trpc/index.ts';
import { ActivityIndicator, Card, Tag } from '@ant-design/react-native';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { Pressable, ScrollView, Text, TextInput, View, useColorScheme } from 'react-native';
import { useTRPC } from '@/src/api/trpc';
import { Button, FloatingActionButton } from '@/src/components/button';
import { BottomSheet } from '@/src/components/mobile-ui';
import { pageStyles } from '@/src/theme/page-styles';

type RouterOutput = inferRouterOutputs<AppRouter>;

type TagRecord = RouterOutput['tags']['list'][number];

const tagColorPresets = [
	'#D4380D',
	'#D48806',
	'#389E0D',
	'#08979C',
	'#096DD9',
	'#1D39C4',
	'#531DAB',
	'#C41D7F',
	'#CF1322',
	'#7A45D1',
] as const;

type TagFormValues = {
	id: number | null;
	name: string;
	note: string;
	color: string;
};

function createEmptyTagForm(): TagFormValues {
	return {
		id: null,
		name: '',
		note: '',
		color: tagColorPresets[0],
	};
}

function tagToFormValues(tag: TagRecord): TagFormValues {
	return {
		id: tag.id,
		name: tag.name,
		note: tag.note ?? '',
		color: tag.color,
	};
}

function assertValidTagForm(form: TagFormValues) {
	if (!form.name.trim()) {
		throw new Error('Enter a tag name.');
	}
	if (!form.color.trim()) {
		throw new Error('Choose a tag color.');
	}
}

function formatCreatedDate(value: string) {
	const date = new Date(value);
	if (!Number.isFinite(date.getTime())) return value;

	const diffMs = Date.now() - date.getTime();
	const minuteMs = 60 * 1000;
	const hourMs = 60 * minuteMs;
	const dayMs = 24 * hourMs;
	if (Math.abs(diffMs) < minuteMs) return 'just now';
	if (Math.abs(diffMs) < hourMs) return `${Math.round(diffMs / minuteMs)} minutes ago`;
	if (Math.abs(diffMs) < dayMs) return `${Math.round(diffMs / hourMs)} hours ago`;
	return `${Math.round(diffMs / dayMs)} days ago`;
}

export default function TagsScreen() {
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const isDark = useColorScheme() === 'dark';
	const styles = tagStyles(isDark);
	const sharedStyles = pageStyles(isDark);
	const [notice, setNotice] = useState<string | null>(null);
	const [form, setForm] = useState<TagFormValues>(() => createEmptyTagForm());
	const [formOpen, setFormOpen] = useState(false);

	const tagsQuery = useQuery(trpc.tags.list.queryOptions());
	const tags = tagsQuery.data ?? [];
	const isEditing = form.id !== null;

	const invalidateTags = async () => {
		await Promise.all([
			queryClient.invalidateQueries({ queryKey: [['tags']] }),
			queryClient.invalidateQueries({ queryKey: [['pills']] }),
			queryClient.invalidateQueries({ queryKey: [['diary']] }),
		]);
	};
	const createTagMutation = useMutation({
		...trpc.tags.create.mutationOptions(),
		onSuccess: async () => {
			await invalidateTags();
			setForm(createEmptyTagForm());
			setFormOpen(false);
			setNotice('Tag created.');
		},
		onError: error => setNotice(error.message),
	});
	const updateTagMutation = useMutation({
		...trpc.tags.update.mutationOptions(),
		onSuccess: async () => {
			await invalidateTags();
			setForm(createEmptyTagForm());
			setFormOpen(false);
			setNotice('Tag updated.');
		},
		onError: error => setNotice(error.message),
	});

	function patchForm(patch: Partial<TagFormValues>) {
		setForm(previous => ({ ...previous, ...patch }));
	}

	async function saveTag() {
		try {
			assertValidTagForm(form);
			if (form.id === null) {
				await createTagMutation.mutateAsync({
					name: form.name,
					color: form.color,
					note: form.note,
				});
				return;
			}

			await updateTagMutation.mutateAsync({
				id: form.id,
				name: form.name,
				color: form.color,
				note: form.note,
			});
		} catch (error) {
			setNotice(error instanceof Error ? error.message : String(error));
		}
	}

	if (tagsQuery.isLoading) {
		return (
			<View style={styles.loadingScreen}>
				<ActivityIndicator animating text='Loading tags...' />
			</View>
		);
	}

	if (tagsQuery.error) {
		return (
			<ScrollView
				contentInsetAdjustmentBehavior='automatic'
				contentContainerStyle={sharedStyles.page}
			>
				<Text selectable style={sharedStyles.errorText}>
					{tagsQuery.error.message}
				</Text>
			</ScrollView>
		);
	}

	return (
		<>
			<ScrollView
				contentInsetAdjustmentBehavior='automatic'
				contentContainerStyle={sharedStyles.page}
			>
				{notice ? (
					<Pressable onPress={() => setNotice(null)} style={styles.notice}>
						<Text selectable style={styles.noticeText}>
							{notice}
						</Text>
					</Pressable>
				) : null}

				<View style={styles.stack}>
					<View style={styles.rowBetween}>
						<Text style={styles.sectionTitle}>All tags</Text>
						<Text style={styles.muted}>{tags.length} rows</Text>
					</View>
					{tags.map(tag => (
						<TagCard
							key={tag.id}
							tag={tag}
							onPress={() => {
								setForm(tagToFormValues(tag));
								setFormOpen(true);
							}}
							styles={styles}
						/>
					))}
				</View>
			</ScrollView>
			<FloatingActionButton
				icon='plus'
				label='Tag'
				onPress={() => {
					setForm(createEmptyTagForm());
					setFormOpen(true);
				}}
			/>
			<BottomSheet
				visible={formOpen}
				title={isEditing ? 'Edit tag' : 'Create tag'}
				onClose={() => {
					setFormOpen(false);
					setForm(createEmptyTagForm());
				}}
				footer={
					<Button
						type='primary'
						onPress={() => void saveTag()}
						loading={createTagMutation.isPending || updateTagMutation.isPending}
					>
						{isEditing ? 'Save tag' : 'Create tag'}
					</Button>
				}
			>
				<View style={styles.stack}>
					<TextField
						label='Name'
						value={form.name}
						onChangeText={name => patchForm({ name })}
						styles={styles}
					/>
					<View style={styles.stack}>
						<Text style={styles.fieldLabel}>Color</Text>
						<View style={styles.swatchRow}>
							{tagColorPresets.map(color => (
								<Pressable
									key={color}
									onPress={() => patchForm({ color })}
									style={[
										styles.swatch,
										{ backgroundColor: color },
										form.color.toLocaleLowerCase() === color.toLocaleLowerCase() &&
											styles.swatchActive,
									]}
								/>
							))}
						</View>
						<TextInput
							value={form.color}
							placeholder='#1677ff'
							placeholderTextColor={styles.placeholder.color}
							style={styles.input}
							autoCapitalize='none'
							onChangeText={color => patchForm({ color })}
						/>
					</View>
					<TextField
						label='Note'
						value={form.note}
						onChangeText={note => patchForm({ note })}
						multiline
						styles={styles}
					/>
				</View>
			</BottomSheet>
		</>
	);
}

function TagCard({
	tag,
	onPress,
	styles,
}: {
	tag: TagRecord;
	onPress: () => void;
	styles: ReturnType<typeof tagStyles>;
}) {
	return (
		<Pressable onPress={onPress}>
			<Card full>
				<Card.Body>
					<View style={styles.stack}>
						<View style={styles.rowBetween}>
							<Tag small>{tag.name}</Tag>
							<View style={[styles.colorDot, { backgroundColor: tag.color }]} />
						</View>
						{tag.note ? <Text style={styles.body}>{tag.note}</Text> : null}
						<View style={styles.countRow}>
							<CountPill
								label='Pill ranges'
								value={tag.attachmentCounts.pillPeriods}
								styles={styles}
							/>
							<CountPill
								label='Diary entries'
								value={tag.attachmentCounts.diaryEntries}
								styles={styles}
							/>
						</View>
						<Text style={styles.muted}>
							Created {formatCreatedDate(tag.createdDate)} - {tag.createdDate}
						</Text>
					</View>
				</Card.Body>
			</Card>
		</Pressable>
	);
}

function CountPill({
	label,
	value,
	styles,
}: {
	label: string;
	value: number;
	styles: ReturnType<typeof tagStyles>;
}) {
	return (
		<View style={styles.countPill}>
			<Text style={styles.countValue}>{value}</Text>
			<Text style={styles.muted}>{label}</Text>
		</View>
	);
}

function TextField({
	label,
	styles,
	...props
}: {
	label: string;
	styles: ReturnType<typeof tagStyles>;
} & React.ComponentProps<typeof TextInput>) {
	return (
		<View style={styles.field}>
			<Text style={styles.fieldLabel}>{label}</Text>
			<TextInput
				placeholderTextColor={styles.placeholder.color}
				style={[styles.input, props.multiline && styles.multilineInput]}
				{...props}
			/>
		</View>
	);
}

function tagStyles(isDark: boolean) {
	const text = isDark ? '#f9fafb' : '#111827';
	const muted = isDark ? '#a1a1aa' : '#71717a';
	const bg = isDark ? '#0f172a' : '#f6f7f9';
	const border = isDark ? '#27272a' : '#e5e7eb';
	const surface = isDark ? '#111827' : '#fff';

	return {
		loadingScreen: {
			alignItems: 'center' as const,
			backgroundColor: bg,
			flex: 1,
			justifyContent: 'center' as const,
		},
		headerRow: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 12,
			justifyContent: 'space-between' as const,
		},
		stack: {
			gap: 12,
		},
		rowBetween: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 10,
			justifyContent: 'space-between' as const,
		},
		notice: {
			backgroundColor: isDark ? '#102a43' : '#e6f4ff',
			borderColor: '#91caff',
			borderRadius: 8,
			borderWidth: 1,
			padding: 10,
		},
		noticeText: {
			color: text,
			fontSize: 14,
		},
		title: {
			color: text,
			fontSize: 24,
			fontWeight: '800' as const,
		},
		sectionTitle: {
			color: muted,
			fontSize: 13,
			fontWeight: '700' as const,
			textTransform: 'uppercase' as const,
		},
		body: {
			color: text,
			fontSize: 14,
			lineHeight: 20,
		},
		muted: {
			color: muted,
			fontSize: 12,
		},
		field: {
			gap: 4,
		},
		fieldLabel: {
			color: muted,
			fontSize: 12,
			fontWeight: '700' as const,
			textTransform: 'uppercase' as const,
		},
		input: {
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 8,
			borderWidth: 1,
			color: text,
			fontSize: 15,
			paddingHorizontal: 10,
			paddingVertical: 8,
		},
		multilineInput: {
			minHeight: 76,
			textAlignVertical: 'top' as const,
		},
		placeholder: {
			color: muted,
		},
		swatchRow: {
			flexDirection: 'row' as const,
			flexWrap: 'wrap' as const,
			gap: 8,
		},
		swatch: {
			borderColor: 'transparent',
			borderRadius: 8,
			borderWidth: 3,
			height: 34,
			width: 34,
		},
		swatchActive: {
			borderColor: text,
		},
		colorDot: {
			borderRadius: 999,
			height: 20,
			width: 20,
		},
		countRow: {
			flexDirection: 'row' as const,
			gap: 8,
		},
		countPill: {
			backgroundColor: isDark ? '#1f2937' : '#f4f4f5',
			borderColor: border,
			borderRadius: 8,
			borderWidth: 1,
			flex: 1,
			padding: 10,
		},
		countValue: {
			color: text,
			fontSize: 17,
			fontWeight: '800' as const,
		},
	};
}
