import { ActivityIndicator, Button, Card, Modal, Tag } from '@ant-design/react-native';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import * as FileSystem from 'expo-file-system/legacy';
import * as ImagePicker from 'expo-image-picker';
import {
	Linking,
	Pressable,
	ScrollView,
	Text,
	TextInput,
	View,
	Image,
	useColorScheme,
} from 'react-native';
import * as Sharing from 'expo-sharing';
import { useEffect, useMemo, useState } from 'react';

import { useTRPC } from '@/src/api/trpc';
import {
	assertValidPillForm,
	buildPillSections,
	buildPillsExport,
	createBlankComponent,
	createBlankPeriod,
	createEmptyPillForm,
	createImageUid,
	extractionToFormPatch,
	formatPeriodRange,
	formatPillSchedule,
	formatRelativeDate,
	formatServing,
	formatSupplementFactsTitle,
	formValuesToMutationInput,
	isBase64DataImage,
	multiplyServingValue,
	normalizeImageUrl,
	normalizeWeekdaySelection,
	pillToFormValues,
	timingOptions,
	weekdayOptions,
	type PillComponentFormValue,
	type PillFormValues,
	type PillImageFormValue,
	type PillListRow,
	type PillPeriodFormValue,
	type PillRecord,
	type PillSection,
	type PillsDashboard,
	type PillWeekday,
} from '@/src/features/pills/model';
import { pageStyles } from '@/src/theme/page-styles';

type EditorSection = 'details' | 'images' | 'ranges' | 'components';

export function PillsScreen() {
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const isDark = useColorScheme() === 'dark';
	const styles = pillStyles(isDark);
	const sharedStyles = pageStyles(isDark);
	const [notice, setNotice] = useState<string | null>(null);
	const [editorOpen, setEditorOpen] = useState(false);
	const [form, setForm] = useState<PillFormValues>(() => createEmptyPillForm());

	const dashboardQuery = useQuery(trpc.pills.getDashboard.queryOptions());
	const tagsQuery = useQuery(trpc.tags.list.queryOptions());
	const dashboard = dashboardQuery.data;
	const sections = useMemo(() => (dashboard ? buildPillSections(dashboard) : []), [dashboard]);
	const editingPill = useMemo(
		() => dashboard?.pills.find(pill => pill.id === form.id) ?? null,
		[dashboard, form.id],
	);

	const invalidatePills = async () => {
		await queryClient.invalidateQueries({ queryKey: [['pills']] });
		await queryClient.invalidateQueries({ queryKey: [['tags']] });
	};
	const upsertMutation = useMutation({
		...trpc.pills.upsert.mutationOptions(),
		onSuccess: async () => {
			await invalidatePills();
			setEditorOpen(false);
			setForm(createEmptyPillForm());
			setNotice('Pill saved.');
		},
		onError: error => setNotice(error.message),
	});
	const deletePillMutation = useMutation({
		...trpc.table.pills.deleteMany.mutationOptions(),
		onSuccess: async data => {
			await invalidatePills();
			setEditorOpen(false);
			setForm(createEmptyPillForm());
			setNotice(`${data.deletedCount} pill${data.deletedCount === 1 ? '' : 's'} removed.`);
		},
		onError: error => setNotice(error.message),
	});
	const deletePeriodMutation = useMutation({
		...trpc.table.pillPeriods.deleteMany.mutationOptions(),
		onSuccess: async data => {
			await queryClient.invalidateQueries({ queryKey: [['pills']] });
			setNotice(`${data.deletedCount} date range${data.deletedCount === 1 ? '' : 's'} removed.`);
		},
		onError: error => setNotice(error.message),
	});
	const extractionMutation = useMutation({
		...trpc.pills.extractFromImages.mutationOptions(),
		onSuccess: extraction => {
			if (!extraction.detected) {
				setNotice(
					extraction.extractionNotes ?? 'No pill label was detected in the selected images.',
				);
				return;
			}
			setForm(previous => ({ ...previous, ...extractionToFormPatch(extraction) }));
			setNotice(`Filled pill details from images.`);
		},
		onError: error => setNotice(error.message),
	});

	const openNewPill = () => {
		setForm(createEmptyPillForm());
		setEditorOpen(true);
	};
	const openExistingPill = (pill: PillRecord) => {
		setForm(pillToFormValues(pill));
		setEditorOpen(true);
	};
	const closeEditor = () => {
		setEditorOpen(false);
		setForm(createEmptyPillForm());
	};
	const patchForm = (patch: Partial<PillFormValues>) => {
		setForm(previous => ({ ...previous, ...patch }));
	};
	const onSave = async () => {
		try {
			assertValidPillForm(form);
			await upsertMutation.mutateAsync(formValuesToMutationInput(form));
		} catch (error) {
			setNotice(error instanceof Error ? error.message : String(error));
		}
	};
	const onDeletePill = () => {
		if (!form.id) return;
		Modal.alert('Remove pill?', form.name, [
			{ text: 'Cancel' },
			{
				text: 'Remove',
				onPress: () => {
					void deletePillMutation.mutateAsync({
						where: [{ column: 'id', operator: 'eq', value: form.id }],
					});
				},
			},
		]);
	};
	const onShareExport = async () => {
		try {
			if (!FileSystem.documentDirectory) throw new Error('Document directory is unavailable.');
			if (!(await Sharing.isAvailableAsync())) throw new Error('Sharing is unavailable.');
			const fileUri = `${FileSystem.documentDirectory}vitals-pills-${new Date().toISOString().slice(0, 10)}.json`;
			await FileSystem.writeAsStringAsync(
				fileUri,
				JSON.stringify(buildPillsExport(sections), null, '\t'),
				{
					encoding: FileSystem.EncodingType.UTF8,
				},
			);
			await Sharing.shareAsync(fileUri, {
				mimeType: 'application/json',
				UTI: 'public.json',
				dialogTitle: 'Share pills export',
			});
		} catch (error) {
			setNotice(error instanceof Error ? error.message : String(error));
		}
	};
	const onPickImages = async () => {
		try {
			const result = await ImagePicker.launchImageLibraryAsync({
				mediaTypes: ['images'],
				allowsMultipleSelection: true,
				base64: true,
				quality: 0.9,
			});
			if (result.canceled) return;
			const nextImages = result.assets.map((asset, index) => {
				if (!asset.base64) throw new Error(`Unable to read ${asset.fileName ?? 'selected image'}.`);
				const fileName = asset.fileName ?? `pill-image-${Date.now()}-${index}.jpg`;
				const mimeType = asset.mimeType ?? 'image/jpeg';
				const image = {
					fileName,
					dataUrl: `data:${mimeType};base64,${asset.base64}`,
				};
				return {
					...image,
					uid: createImageUid(image, form.images.length + index),
				} satisfies PillImageFormValue;
			});
			patchForm({ images: [...form.images, ...nextImages] });
		} catch (error) {
			setNotice(error instanceof Error ? error.message : String(error));
		}
	};
	const onParseImages = () => {
		const parseableImages = form.images.filter(isBase64DataImage);
		if (parseableImages.length === 0) {
			setNotice('Add a newly selected image before parsing.');
			return;
		}
		extractionMutation.mutate({ images: parseableImages });
	};
	const onDeletePeriod = async (period: PillPeriodFormValue, index: number) => {
		if (period.id) {
			await deletePeriodMutation.mutateAsync({
				where: [{ column: 'id', operator: 'eq', value: period.id }],
			});
		}
		setForm(previous => ({
			...previous,
			periods: previous.periods.filter((_, periodIndex) => periodIndex !== index),
		}));
	};
	if (dashboardQuery.isLoading || tagsQuery.isLoading) {
		return (
			<View style={styles.loadingScreen}>
				<ActivityIndicator animating text='Loading pills...' />
			</View>
		);
	}

	const error = dashboardQuery.error ?? tagsQuery.error;
	if (error) {
		return (
			<ScrollView
				contentInsetAdjustmentBehavior='automatic'
				contentContainerStyle={sharedStyles.page}
			>
				<Text selectable style={sharedStyles.errorText}>
					{error.message}
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
				<View style={styles.actionRow}>
					<Button size='small' onPress={onShareExport} disabled={!dashboard}>
						Export
					</Button>
					<Button type='primary' size='small' onPress={openNewPill}>
						Log pill
					</Button>
				</View>

				{notice ? (
					<Pressable onPress={() => setNotice(null)} style={styles.notice}>
						<Text selectable style={styles.noticeText}>
							{notice}
						</Text>
					</Pressable>
				) : null}

				{dashboard ? <PillsTotals dashboard={dashboard} styles={styles} /> : null}

				{sections.map(section => (
					<PillSectionView
						key={section.key}
						section={section}
						onOpenPill={openExistingPill}
						styles={styles}
					/>
				))}
			</ScrollView>

			<PillEditorModal
				form={form}
				editingPill={editingPill}
				availableTags={tagsQuery.data ?? []}
				visible={editorOpen}
				isSaving={upsertMutation.isPending}
				isDeleting={deletePillMutation.isPending}
				isParsing={extractionMutation.isPending}
				onClose={closeEditor}
				onPatch={patchForm}
				onSave={onSave}
				onDeletePill={onDeletePill}
				onPickImages={onPickImages}
				onParseImages={onParseImages}
				onDeletePeriod={(period, index) => {
					void onDeletePeriod(period, index).catch(error =>
						setNotice(error instanceof Error ? error.message : String(error)),
					);
				}}
				styles={styles}
			/>
		</>
	);
}

function PillsTotals({
	dashboard,
	styles,
}: {
	dashboard: PillsDashboard;
	styles: ReturnType<typeof pillStyles>;
}) {
	const totals = {
		all: dashboard.pills.length,
		active: dashboard.activePills.length,
		future: dashboard.futurePills.length,
		past: dashboard.pastPills.reduce(
			(total, pill) => total + pill.periods.filter(period => Boolean(period.endDate)).length,
			0,
		),
	};
	return (
		<View style={styles.totalsRow}>
			<TotalCard label='Active' value={totals.active} styles={styles} />
			<TotalCard label='Future' value={totals.future} styles={styles} />
			<TotalCard label='Past' value={totals.past} styles={styles} />
			<TotalCard label='All' value={totals.all} styles={styles} />
		</View>
	);
}

function TotalCard({
	label,
	value,
	styles,
}: {
	label: string;
	value: number;
	styles: ReturnType<typeof pillStyles>;
}) {
	return (
		<View style={styles.totalCard}>
			<Text style={styles.totalValue}>{value}</Text>
			<Text style={styles.muted}>{label}</Text>
		</View>
	);
}

function PillSectionView({
	section,
	onOpenPill,
	styles,
}: {
	section: PillSection;
	onOpenPill: (pill: PillRecord) => void;
	styles: ReturnType<typeof pillStyles>;
}) {
	return (
		<View style={styles.stack}>
			<View style={styles.rowBetween}>
				<Text style={styles.sectionTitle}>{section.title}</Text>
				<Text style={styles.muted}>{section.rows.length} rows</Text>
			</View>
			{section.rows.map(row => (
				<PillRow key={row.key} row={row} onOpenPill={onOpenPill} styles={styles} />
			))}
		</View>
	);
}

function PillRow({
	row,
	onOpenPill,
	styles,
}: {
	row: PillListRow;
	onOpenPill: (pill: PillRecord) => void;
	styles: ReturnType<typeof pillStyles>;
}) {
	const { pill, period } = row;
	const count = period?.count ?? 1;
	const firstComponent = pill.components[0];
	const firstImage = pill.images[0];
	const tagNames = mergeTagNames(pill, period?.tags ?? []);

	return (
		<Pressable onPress={() => onOpenPill(pill)}>
			<Card full>
				<Card.Body>
					<View style={styles.pillRow}>
						<View style={styles.pillMain}>
							<View style={styles.rowBetween}>
								<Text style={styles.pillTitle} numberOfLines={2}>
									{pill.name}
								</Text>
								<Text style={styles.amountText}>
									{formatServing(multiplyServingValue(pill.value, count), pill.unit)}
								</Text>
							</View>
							{tagNames.length > 0 ? (
								<View style={styles.tagRow}>
									{tagNames.map(tagName => (
										<Tag key={tagName} small>
											{tagName}
										</Tag>
									))}
								</View>
							) : null}
							<Text style={styles.muted}>{formatRowTiming(row)}</Text>
							{firstComponent ? (
								<Text style={styles.body} numberOfLines={2}>
									{firstComponent.name}:{' '}
									{formatServing(
										multiplyServingValue(firstComponent.value, count),
										firstComponent.unit,
									)}
								</Text>
							) : (
								<Text style={styles.muted}>No components</Text>
							)}
							{pill.components.length > 1 ? (
								<Text style={styles.muted}>+{pill.components.length - 1} more</Text>
							) : null}
						</View>
						{firstImage ? (
							<Image
								source={{ uri: normalizeImageUrl(firstImage.dataUrl) }}
								style={styles.thumbnail}
								accessibilityLabel={firstImage.fileName}
							/>
						) : null}
					</View>
				</Card.Body>
			</Card>
		</Pressable>
	);
}

function PillEditorModal({
	form,
	editingPill,
	availableTags,
	visible,
	isSaving,
	isDeleting,
	isParsing,
	onClose,
	onPatch,
	onSave,
	onDeletePill,
	onPickImages,
	onParseImages,
	onDeletePeriod,
	styles,
}: {
	form: PillFormValues;
	editingPill: PillRecord | null;
	availableTags: Array<{ name: string; color: string }>;
	visible: boolean;
	isSaving: boolean;
	isDeleting: boolean;
	isParsing: boolean;
	onClose: () => void;
	onPatch: (patch: Partial<PillFormValues>) => void;
	onSave: () => void;
	onDeletePill: () => void;
	onPickImages: () => void;
	onParseImages: () => void;
	onDeletePeriod: (period: PillPeriodFormValue, index: number) => void;
	styles: ReturnType<typeof pillStyles>;
}) {
	const [activeSection, setActiveSection] = useState<EditorSection>('details');
	const setComponent = (index: number, patch: Partial<PillComponentFormValue>) => {
		onPatch({
			components: form.components.map((component, componentIndex) =>
				componentIndex === index ? { ...component, ...patch } : component,
			),
		});
	};
	const setPeriod = (index: number, patch: Partial<PillPeriodFormValue>) => {
		onPatch({
			periods: form.periods.map((period, periodIndex) =>
				periodIndex === index ? { ...period, ...patch } : period,
			),
		});
	};
	useEffect(() => {
		if (visible) setActiveSection('details');
	}, [visible]);

	return (
		<Modal
			visible={visible}
			title={editingPill ? 'Edit pill' : 'Log pill'}
			transparent
			animationType='slide-up'
			onClose={onClose}
			closable
			footer={[
				{ text: 'Cancel', onPress: onClose },
				{ text: isSaving ? 'Saving...' : 'Save', onPress: onSave },
			]}
		>
			<ScrollView
				style={styles.modalScroll}
				contentContainerStyle={styles.modalContent}
				keyboardShouldPersistTaps='handled'
			>
				<View style={styles.stack}>
					{editingPill ? (
						<View style={styles.rowBetween}>
							<Text style={styles.muted}>Pill #{editingPill.id}</Text>
							<Button size='small' onPress={onDeletePill} loading={isDeleting}>
								Remove
							</Button>
						</View>
					) : null}

					<View style={styles.segment}>
						<SegmentButton
							label='Details'
							active={activeSection === 'details'}
							onPress={() => setActiveSection('details')}
							styles={styles}
						/>
						<SegmentButton
							label='Images'
							active={activeSection === 'images'}
							onPress={() => setActiveSection('images')}
							styles={styles}
						/>
						<SegmentButton
							label='Ranges'
							active={activeSection === 'ranges'}
							onPress={() => setActiveSection('ranges')}
							styles={styles}
						/>
						<SegmentButton
							label='Facts'
							active={activeSection === 'components'}
							onPress={() => setActiveSection('components')}
							styles={styles}
						/>
					</View>

					{activeSection === 'details' ? (
						<View style={styles.fieldGrid}>
							<TextField
								label='Pill name'
								value={form.name}
								onChangeText={name => onPatch({ name })}
								styles={styles}
							/>
							<View style={styles.twoColumn}>
								<TextField
									label='Value'
									value={form.value}
									onChangeText={value => onPatch({ value })}
									styles={styles}
								/>
								<TextField
									label='Unit'
									value={form.unit}
									onChangeText={unit => onPatch({ unit })}
									styles={styles}
								/>
							</View>
							<TextField
								label='URL'
								value={form.url}
								onChangeText={url => onPatch({ url })}
								autoCapitalize='none'
								styles={styles}
							/>
							{form.url.trim() ? (
								<Button
									size='small'
									onPress={() => {
										void Linking.openURL(form.url.trim());
									}}
								>
									Open URL
								</Button>
							) : null}
							<TextField
								label='Tags'
								value={form.tagText}
								onChangeText={tagText => onPatch({ tagText })}
								placeholder='blueprint, sleep'
								styles={styles}
							/>
							{availableTags.length > 0 ? (
								<ScrollView horizontal showsHorizontalScrollIndicator={false}>
									<View style={styles.tagRow}>
										{availableTags.map(tag => (
											<Pressable
												key={tag.name}
												onPress={() => onPatch({ tagText: appendTagText(form.tagText, tag.name) })}
											>
												<Tag small>{tag.name}</Tag>
											</Pressable>
										))}
									</View>
								</ScrollView>
							) : null}
							<TextField
								label='Note'
								value={form.note}
								onChangeText={note => onPatch({ note })}
								multiline
								styles={styles}
							/>
						</View>
					) : null}

					{activeSection === 'images' ? (
						<View style={styles.stack}>
							<View style={styles.rowBetween}>
								<Text style={styles.sectionTitle}>Images</Text>
								<View style={styles.inline}>
									<Button size='small' onPress={onPickImages}>
										Add
									</Button>
									<Button
										size='small'
										onPress={onParseImages}
										loading={isParsing}
										disabled={!form.images.some(isBase64DataImage)}
									>
										Parse
									</Button>
								</View>
							</View>
							<ScrollView horizontal showsHorizontalScrollIndicator={false}>
								<View style={styles.imageRow}>
									{form.images.map(image => (
										<View key={image.uid} style={styles.imageTile}>
											<Image source={{ uri: image.dataUrl }} style={styles.imagePreview} />
											<Button
												size='small'
												onPress={() =>
													onPatch({ images: form.images.filter(item => item.uid !== image.uid) })
												}
											>
												Remove
											</Button>
										</View>
									))}
									{form.images.length === 0 ? <Text style={styles.muted}>No images</Text> : null}
								</View>
							</ScrollView>
						</View>
					) : null}

					{activeSection === 'ranges' ? (
						<View style={styles.stack}>
							<View style={styles.rowBetween}>
								<Text style={styles.sectionTitle}>Date Ranges</Text>
								<Button
									size='small'
									onPress={() => onPatch({ periods: [...form.periods, createBlankPeriod()] })}
								>
									Add range
								</Button>
							</View>
							{form.periods.length === 0 ? (
								<Text style={styles.muted}>Not tracked yet.</Text>
							) : null}
							{form.periods.map((period, index) => (
								<PeriodEditor
									key={period.id ?? `period-${index}`}
									period={period}
									index={index}
									onPatch={patch => setPeriod(index, patch)}
									onDelete={() => onDeletePeriod(period, index)}
									styles={styles}
								/>
							))}
						</View>
					) : null}

					{activeSection === 'components' ? (
						<View style={styles.stack}>
							<View style={styles.rowBetween}>
								<Text style={styles.sectionTitle}>
									{formatSupplementFactsTitle(form.value, form.unit)}
								</Text>
								<Button
									size='small'
									onPress={() =>
										onPatch({ components: [...form.components, createBlankComponent()] })
									}
								>
									Add component
								</Button>
							</View>
							{form.components.map((component, index) => (
								<ComponentEditor
									key={`component-${index}`}
									component={component}
									onPatch={patch => setComponent(index, patch)}
									onDelete={() =>
										onPatch({
											components:
												form.components.length === 1
													? [createBlankComponent()]
													: form.components.filter((_, componentIndex) => componentIndex !== index),
										})
									}
									styles={styles}
								/>
							))}
						</View>
					) : null}
				</View>
			</ScrollView>
		</Modal>
	);
}

function PeriodEditor({
	period,
	index,
	onPatch,
	onDelete,
	styles,
}: {
	period: PillPeriodFormValue;
	index: number;
	onPatch: (patch: Partial<PillPeriodFormValue>) => void;
	onDelete: () => void;
	styles: ReturnType<typeof pillStyles>;
}) {
	const daily = period.daysOfWeek.length === 0;
	return (
		<Card full>
			<Card.Body>
				<View style={styles.stack}>
					<View style={styles.rowBetween}>
						<Text style={styles.cardTitle}>Range {index + 1}</Text>
						<Button size='small' onPress={onDelete}>
							Delete
						</Button>
					</View>
					<View style={styles.twoColumn}>
						<TextField
							label='Start'
							value={period.startDate}
							onChangeText={startDate => onPatch({ startDate })}
							placeholder='YYYY-MM-DD'
							styles={styles}
						/>
						<TextField
							label='End'
							value={period.endDate}
							onChangeText={endDate => onPatch({ endDate })}
							placeholder='ongoing'
							styles={styles}
						/>
					</View>
					<TextField
						label='Count'
						value={period.count}
						onChangeText={count => onPatch({ count })}
						keyboardType='decimal-pad'
						styles={styles}
					/>
					<View style={styles.chipRow}>
						<Chip
							label='Daily'
							active={daily}
							onPress={() => onPatch({ daysOfWeek: [] })}
							styles={styles}
						/>
						{weekdayOptions.map(option => (
							<Chip
								key={option.value}
								label={option.label}
								active={period.daysOfWeek.includes(option.value)}
								onPress={() =>
									onPatch({ daysOfWeek: toggleWeekday(period.daysOfWeek, option.value) })
								}
								styles={styles}
							/>
						))}
					</View>
					<View style={styles.chipRow}>
						<Chip
							label='Any time'
							active={!period.timing}
							onPress={() => onPatch({ timing: '' })}
							styles={styles}
						/>
						{timingOptions.map(option => (
							<Chip
								key={option.value}
								label={option.label}
								active={period.timing === option.value}
								onPress={() => onPatch({ timing: option.value })}
								styles={styles}
							/>
						))}
					</View>
					<TextField
						label='Range tags'
						value={period.tagText}
						onChangeText={tagText => onPatch({ tagText })}
						styles={styles}
					/>
				</View>
			</Card.Body>
		</Card>
	);
}

function ComponentEditor({
	component,
	onPatch,
	onDelete,
	styles,
}: {
	component: PillComponentFormValue;
	onPatch: (patch: Partial<PillComponentFormValue>) => void;
	onDelete: () => void;
	styles: ReturnType<typeof pillStyles>;
}) {
	return (
		<Card full>
			<Card.Body>
				<View style={styles.stack}>
					<TextField
						label='Name'
						value={component.name}
						onChangeText={name => onPatch({ name })}
						styles={styles}
					/>
					<View style={styles.twoColumn}>
						<TextField
							label='Value'
							value={component.value}
							onChangeText={value => onPatch({ value })}
							styles={styles}
						/>
						<TextField
							label='Unit'
							value={component.unit}
							onChangeText={unit => onPatch({ unit })}
							styles={styles}
						/>
					</View>
					<Button size='small' onPress={onDelete}>
						Delete component
					</Button>
				</View>
			</Card.Body>
		</Card>
	);
}

function TextField({
	label,
	styles,
	...props
}: {
	label: string;
	styles: ReturnType<typeof pillStyles>;
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

function Chip({
	label,
	active,
	onPress,
	styles,
}: {
	label: string;
	active: boolean;
	onPress: () => void;
	styles: ReturnType<typeof pillStyles>;
}) {
	return (
		<Pressable onPress={onPress} style={[styles.chip, active && styles.chipActive]}>
			<Text style={active ? styles.chipActiveText : styles.chipText}>{label}</Text>
		</Pressable>
	);
}

function SegmentButton({
	label,
	active,
	onPress,
	styles,
}: {
	label: string;
	active: boolean;
	onPress: () => void;
	styles: ReturnType<typeof pillStyles>;
}) {
	return (
		<Pressable
			onPress={onPress}
			style={[styles.segmentButton, active && styles.segmentButtonActive]}
		>
			<Text style={active ? styles.segmentButtonActiveText : styles.segmentButtonText}>
				{label}
			</Text>
		</Pressable>
	);
}

function mergeTagNames(pill: PillRecord, periodTags: PillPeriodFormValue[] | PillRecord['tags']) {
	const names = pill.tags.map(tag => tag.name);
	const seen = new Set(names.map(name => name.toLocaleLowerCase()));
	for (const tag of periodTags as Array<{ name?: string }>) {
		const name = tag.name?.trim();
		if (!name || seen.has(name.toLocaleLowerCase())) continue;
		seen.add(name.toLocaleLowerCase());
		names.push(name);
	}
	return names;
}

function formatRowTiming(row: PillListRow) {
	if (!row.period) return 'Not tracked yet';
	if (row.section === 'active') {
		return `${formatPillSchedule(row.period)}, started ${formatRelativeDate(row.period.startDate)}`;
	}
	if (row.section === 'future') {
		return `${formatPillSchedule(row.period)}, starts ${row.period.startDate}`;
	}
	return `${formatPillSchedule(row.period)}, ${formatPeriodRange(row.period)}`;
}

function appendTagText(value: string, tagName: string) {
	const existing = value
		.split(',')
		.map(tag => tag.trim())
		.filter(Boolean);
	const seen = new Set(existing.map(tag => tag.toLocaleLowerCase()));
	if (!seen.has(tagName.toLocaleLowerCase())) existing.push(tagName);
	return existing.join(', ');
}

function toggleWeekday(days: PillWeekday[], day: PillWeekday) {
	const next = days.includes(day) ? days.filter(value => value !== day) : [...days, day];
	return normalizeWeekdaySelection(next);
}

function pillStyles(isDark: boolean) {
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
		stack: {
			gap: 12,
		},
		segment: {
			backgroundColor: isDark ? '#1f2937' : '#e5e7eb',
			borderRadius: 8,
			flexDirection: 'row' as const,
			gap: 4,
			padding: 4,
		},
		segmentButton: {
			borderRadius: 7,
			flex: 1,
			paddingVertical: 8,
		},
		segmentButtonActive: {
			backgroundColor: '#1677ff',
		},
		segmentButtonText: {
			color: muted,
			fontSize: 12,
			fontWeight: '700' as const,
			textAlign: 'center' as const,
		},
		segmentButtonActiveText: {
			color: '#fff',
			fontSize: 12,
			fontWeight: '700' as const,
			textAlign: 'center' as const,
		},
		actionRow: {
			flexDirection: 'row' as const,
			gap: 8,
			justifyContent: 'flex-end' as const,
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
		rowBetween: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 10,
			justifyContent: 'space-between' as const,
		},
		inline: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 8,
		},
		totalsRow: {
			flexDirection: 'row' as const,
			gap: 8,
		},
		totalCard: {
			alignItems: 'center' as const,
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 8,
			borderWidth: 1,
			flex: 1,
			padding: 10,
		},
		totalValue: {
			color: text,
			fontSize: 18,
			fontWeight: '800' as const,
		},
		sectionTitle: {
			color: muted,
			fontSize: 13,
			fontWeight: '700' as const,
			textTransform: 'uppercase' as const,
		},
		cardTitle: {
			color: text,
			fontSize: 14,
			fontWeight: '700' as const,
		},
		body: {
			color: text,
			fontSize: 13,
			lineHeight: 18,
		},
		muted: {
			color: muted,
			fontSize: 12,
		},
		pillRow: {
			alignItems: 'flex-start' as const,
			flexDirection: 'row' as const,
			gap: 12,
		},
		pillMain: {
			flex: 1,
			gap: 6,
			minWidth: 0,
		},
		pillTitle: {
			color: text,
			flex: 1,
			fontSize: 15,
			fontWeight: '700' as const,
		},
		amountText: {
			color: text,
			fontSize: 13,
			fontWeight: '700' as const,
			maxWidth: 120,
			textAlign: 'right' as const,
		},
		tagRow: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			flexWrap: 'wrap' as const,
			gap: 4,
		},
		thumbnail: {
			backgroundColor: isDark ? '#1f2937' : '#f4f4f5',
			borderRadius: 8,
			height: 58,
			width: 58,
		},
		modalScroll: {
			maxHeight: 520,
		},
		modalContent: {
			paddingBottom: 12,
		},
		fieldGrid: {
			gap: 10,
		},
		twoColumn: {
			flexDirection: 'row' as const,
			gap: 10,
		},
		field: {
			flex: 1,
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
		imageRow: {
			flexDirection: 'row' as const,
			gap: 10,
		},
		imageTile: {
			gap: 6,
			width: 104,
		},
		imagePreview: {
			backgroundColor: isDark ? '#1f2937' : '#f4f4f5',
			borderRadius: 8,
			height: 96,
			width: 96,
		},
		chipRow: {
			flexDirection: 'row' as const,
			flexWrap: 'wrap' as const,
			gap: 6,
		},
		chip: {
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 999,
			borderWidth: 1,
			paddingHorizontal: 10,
			paddingVertical: 6,
		},
		chipActive: {
			backgroundColor: '#1677ff',
			borderColor: '#1677ff',
		},
		chipText: {
			color: muted,
			fontSize: 12,
			fontWeight: '600' as const,
		},
		chipActiveText: {
			color: '#fff',
			fontSize: 12,
			fontWeight: '700' as const,
		},
	};
}
