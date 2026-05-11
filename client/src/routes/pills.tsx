import {
	ArrowDown as ArrowDownOutlined,
	ArrowUp as ArrowUpOutlined,
	Copy as CopyOutlined,
	PencilSimple as EditOutlined,
	Plus as PlusOutlined,
	Trash as DeleteOutlined,
	UploadSimple as UploadOutlined,
	WarningCircle,
} from '@phosphor-icons/react';
import { toast } from '@tamagui/toast/v2';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { formatDistanceToNow, parseISO } from 'date-fns';
import dayjs from 'dayjs';
import type { CSSProperties, Key, ReactNode } from 'react';
import { useCallback, useDeferredValue, useEffect, useMemo, useState } from 'react';
import {
	Button,
	Card,
	Checkbox,
	H3,
	Input,
	Paragraph,
	Spinner,
	Text,
	XStack,
	YStack,
	useTheme,
} from 'tamagui';
import { z } from 'zod';

import { AutoResizeTextArea } from '../components/AutoResizeTextArea';
import { type DataColumn, DataTable } from '../components/DataTable';
import { FileDropzone } from '../components/FileDropzone';
import { FormField } from '../components/FormField';
import { PageNav } from '../components/PageNav';
import { TagChip } from '../components/TagChip';
import { TagInput } from '../components/TagInput';
import type {
	PillComponent,
	PillExtractionResult,
	PillImage,
	PillPeriod,
	PillRecord,
} from '../utils/api';
import { useTRPC } from '../utils/trpc';

const pillsSearchSchema = z.object({
	edit: z.coerce.number().int().positive().optional(),
});

const DATE_FORMAT = 'YYYY-MM-DD';
const IMAGE_TILE_SIZE = 104;

export const Route = createFileRoute('/pills')({
	validateSearch: search => pillsSearchSchema.parse(search),
	component: PillsRouteComponent,
});

type PillTiming = 'morning' | 'afternoon' | 'evening';
type PillWeekday =
	| 'monday'
	| 'tuesday'
	| 'wednesday'
	| 'thursday'
	| 'friday'
	| 'saturday'
	| 'sunday';

type PillImageFormValue = {
	id?: number;
	uid: string;
	fileName: string;
	dataUrl: string;
};

type PillImageErrorDetails = {
	message: string;
	details: string;
};

type PillComponentFormValue = {
	name: string;
	value: string;
	unit: string;
};

type PillPeriodFormValue = {
	id?: number;
	startDate?: string;
	endDate?: string;
	count?: number;
	timing?: PillTiming;
	daysOfWeek?: PillWeekday[];
	tagNames?: string[];
};

type PillFormValues = {
	id?: number;
	name: string;
	value: string;
	unit: string;
	url: string;
	note: string;
	tagNames: string[];
	images: PillImageFormValue[];
	components: PillComponentFormValue[];
	periods: PillPeriodFormValue[];
};

type ActivePillRow = {
	pill: PillRecord;
	activePeriod: PillPeriod;
};

type FuturePillRow = {
	pill: PillRecord;
	futurePeriod: PillPeriod;
};

type PastPillRow = {
	key: string;
	pill: PillRecord;
	period: PillPeriod;
};

type NotTrackedPillRow = {
	pill: PillRecord;
};

type PillExportTableName = 'futurePills' | 'activePills' | 'notTrackedPills' | 'pastPills';
type PillExportRow = {
	components?: Record<string, string>;
	note?: string;
	tags?: string[];
	startDate?: string;
	endDate?: string;
	timing?: string;
};

const timingOptions = [
	{ label: 'Morning', value: 'morning' },
	{ label: 'Afternoon', value: 'afternoon' },
	{ label: 'Evening', value: 'evening' },
] as const;
const weekdayOptions = [
	{ label: 'Mon', value: 'monday' },
	{ label: 'Tue', value: 'tuesday' },
	{ label: 'Wed', value: 'wednesday' },
	{ label: 'Thu', value: 'thursday' },
	{ label: 'Fri', value: 'friday' },
	{ label: 'Sat', value: 'saturday' },
	{ label: 'Sun', value: 'sunday' },
] satisfies Array<{ label: string; value: PillWeekday }>;
const pillWeekdayValues = weekdayOptions.map(option => option.value);

function PillsRouteComponent() {
	const search = Route.useSearch();
	const navigate = Route.useNavigate();
	const editPillId = search.edit ?? null;
	const onEditPillChange = useCallback(
		(nextEditPillId: number | null) =>
			navigate({
				search: nextEditPillId ? { edit: nextEditPillId } : {},
			}),
		[navigate],
	);

	const token = getPillsThemeToken(useTheme());
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const [formValues, setFormValues] = useState(createEmptyFormValues);
	const [isCreateDrawerOpen, setIsCreateDrawerOpen] = useState(false);
	const [pillQuery, setPillQuery] = useState('');
	const [hydratedEditPillId, setHydratedEditPillId] = useState<number | null>(null);
	const [deletingPeriodId, setDeletingPeriodId] = useState<number | null>(null);
	const [expandedComponentRowKeys, setExpandedComponentRowKeys] = useState<string[]>([]);
	const [imageError, setImageError] = useState<PillImageErrorDetails | null>(null);
	const deferredPillQuery = useDeferredValue(pillQuery);
	const isEditMode = editPillId !== null;
	const isDrawerRequestedOpen = isCreateDrawerOpen || isEditMode;

	const dashboardQuery = useQuery(trpc.pills.getDashboard.queryOptions());
	const tagsQuery = useQuery(trpc.tags.list.queryOptions());
	const searchQuery = useQuery({
		...trpc.pills.search.queryOptions({
			query: deferredPillQuery,
			limit: 8,
		}),
		enabled: isDrawerRequestedOpen,
	});

	const extractionMutation = useMutation({
		...trpc.pills.extractFromImages.mutationOptions(),
		onSuccess: extraction => {
			setImageError(null);
			if (!extraction.detected) {
				toast.info(
					extraction.extractionNotes ??
						'No pill or supplement label was confidently detected in the uploaded images.',
				);
				return;
			}

			setFormValues(current => ({
				...current,
				...extractionToFormPatch(extraction),
			}));
			toast.success(`Filled pill details from images using ${extraction.model}.`);
		},
		onError: error => {
			const errorDetails = formatPillImageError(error, 'Image parsing failed.');
			setImageError(errorDetails);
			toast.error(errorDetails.message);
		},
	});
	const isParsingImages = extractionMutation.isPending;

	const upsertMutation = useMutation({
		...trpc.pills.upsert.mutationOptions(),
		onSuccess: () => {
			void queryClient.invalidateQueries({ queryKey: [['pills']] });
			void queryClient.invalidateQueries({ queryKey: [['tags']] });
			setIsCreateDrawerOpen(false);
			onEditPillChange(null);
			resetPillForm();
			toast.success('Pill saved.');
		},
		onError: error => {
			toast.error(error.message);
		},
	});

	const deletePillMutation = useMutation({
		...trpc.table.pills.deleteMany.mutationOptions(),
		onSuccess: () => {
			void queryClient.invalidateQueries({ queryKey: [['pills']] });
			void queryClient.invalidateQueries({ queryKey: [['tags']] });
			setIsCreateDrawerOpen(false);
			onEditPillChange(null);
			resetPillForm();
			toast.success('Pill removed.');
		},
		onError: error => {
			toast.error(error.message);
		},
	});

	const deletePeriodMutation = useMutation({
		...trpc.table.pillPeriods.deleteMany.mutationOptions(),
		onSuccess: async () => {
			await queryClient.invalidateQueries({ queryKey: [['pills']] });
		},
		onSettled: () => {
			setDeletingPeriodId(null);
		},
	});

	const dashboard = dashboardQuery.data;
	const availableTags = tagsQuery.data ?? [];
	const searchResults = (searchQuery.data ?? []) as PillRecord[];
	const tagOptions = useMemo(
		() =>
			availableTags.map(tag => ({
				label: tag.name,
				value: tag.name,
				color: tag.color ?? undefined,
			})),
		[availableTags],
	);

	const allPills = dashboard?.pills ?? [];
	const activePills = dashboard?.activePills ?? [];
	const futurePills = dashboard?.futurePills ?? [];
	const pastPills = dashboard?.pastPills ?? [];
	const editedPill = useMemo(
		() => allPills.find(pill => pill.id === editPillId) ?? null,
		[allPills, editPillId],
	);
	const isDrawerOpen =
		isCreateDrawerOpen ||
		(editPillId !== null && (dashboardQuery.isLoading || editedPill !== null));
	const activeRows = useMemo<ActivePillRow[]>(
		() =>
			activePills
				.map(pill => ({
					pill,
					activePeriod: getActivePeriod(pill),
				}))
				.filter((row): row is ActivePillRow => row.activePeriod !== null),
		[activePills],
	);
	const futureRows = useMemo<FuturePillRow[]>(
		() =>
			futurePills
				.map(pill => ({
					pill,
					futurePeriod: getFuturePeriod(pill),
				}))
				.filter((row): row is FuturePillRow => row.futurePeriod !== null)
				.sort((left, right) =>
					left.futurePeriod.startDate.localeCompare(right.futurePeriod.startDate),
				),
		[futurePills],
	);
	const pastRows = useMemo<PastPillRow[]>(
		() =>
			pastPills
				.flatMap(pill =>
					pill.periods
						.filter(period => period.endDate)
						.map(period => ({
							key: `${pill.id}-${period.id}`,
							pill,
							period,
						})),
				)
				.sort((left, right) => right.period.startDate.localeCompare(left.period.startDate)),
		[pastPills],
	);
	const notTrackedRows = useMemo<NotTrackedPillRow[]>(
		() => allPills.filter(pill => pill.periods.length === 0).map(pill => ({ pill })),
		[allPills],
	);
	const isSaveDisabled =
		upsertMutation.isPending ||
		!formValues.name.trim() ||
		!formValues.value.trim() ||
		!formValues.unit.trim() ||
		formValues.periods.some(period => !period.startDate || !normalizePeriodCount(period.count));

	const resetPillForm = useCallback(() => {
		setImageError(null);
		setFormValues({
			...createEmptyFormValues(),
			id: undefined,
		});
	}, []);

	useEffect(() => {
		setFormValues(current => {
			const nextPeriods = current.periods.map(period => ({
				...period,
				count: normalizePeriodCount(period.count),
			}));
			const hasChanged = nextPeriods.some(
				(period, index) => period.count !== current.periods[index]?.count,
			);
			return hasChanged ? { ...current, periods: nextPeriods } : current;
		});
	}, [formValues.periods]);

	useEffect(() => {
		if (!isDrawerOpen) {
			return;
		}

		const onPaste = (event: ClipboardEvent) => {
			const pastedText = event.clipboardData?.getData('text')?.trim() ?? '';
			if (isValidPastedUrl(pastedText)) {
				event.preventDefault();
				patchFormValues({ url: pastedText });
				return;
			}

			const pastedFiles = getPastedImageFiles(event);
			if (pastedFiles.length === 0) {
				return;
			}

			event.preventDefault();

			void addImages(pastedFiles, {
				successText:
					pastedFiles.length === 1 ? 'Pasted 1 image.' : `Pasted ${pastedFiles.length} images.`,
				errorText: 'Unable to process pasted images.',
				pasted: true,
			});
		};

		window.addEventListener('paste', onPaste);

		return () => {
			window.removeEventListener('paste', onPaste);
		};
	}, [isDrawerOpen]);

	useEffect(() => {
		if (editPillId === null) {
			setHydratedEditPillId(null);
			return;
		}

		if (dashboardQuery.isLoading) {
			return;
		}

		if (hydratedEditPillId === editPillId) {
			return;
		}

		if (!editedPill) {
			setPillQuery('');
			setHydratedEditPillId(null);
			onEditPillChange(null);
			resetPillForm();
			return;
		}

		setPillQuery(editedPill.name);
		setFormValues(pillToFormValues(editedPill));
		setHydratedEditPillId(editPillId);
	}, [
		dashboardQuery.isLoading,
		editPillId,
		editedPill,
		hydratedEditPillId,
		onEditPillChange,
		resetPillForm,
	]);

	function patchFormValues(patch: Partial<PillFormValues>) {
		setFormValues(current => ({ ...current, ...patch }));
	}

	function openNewPillDrawer() {
		setIsCreateDrawerOpen(true);
		setHydratedEditPillId(null);
		setPillQuery('');
		onEditPillChange(null);
		resetPillForm();
	}

	const openExistingPillDrawer = useCallback(
		(pill: PillRecord) => {
			setIsCreateDrawerOpen(false);
			setHydratedEditPillId(null);
			setImageError(null);
			onEditPillChange(pill.id);
		},
		[onEditPillChange],
	);

	function handleCloseDrawer() {
		setIsCreateDrawerOpen(false);
		setImageError(null);
		onEditPillChange(null);
		setPillQuery('');
		setHydratedEditPillId(null);
		resetPillForm();
	}

	const toggleExpandedComponentsRow = useCallback((rowKey: string) => {
		setExpandedComponentRowKeys(currentKeys =>
			currentKeys.includes(rowKey)
				? currentKeys.filter(currentKey => currentKey !== rowKey)
				: [...currentKeys, rowKey],
		);
	}, []);

	async function handleExport() {
		try {
			const exportPayload = buildPillsExport({
				futureRows,
				activeRows,
				notTrackedRows,
				pastRows,
			});
			await copyTextToClipboard(JSON.stringify(exportPayload, null, '\t'));
			toast.success('Pills export copied to clipboard.');
		} catch (error) {
			toast.error(getErrorMessage(error, 'Unable to export pills.'));
		}
	}

	function handleNameChange(value: string) {
		setPillQuery(value);

		if (!isEditMode) {
			const selectedPill = searchResults.find(result => result.name === value);
			if (selectedPill) {
				setFormValues(pillToFormValues(selectedPill, { appendNewPeriod: true }));
				return;
			}
		}

		setFormValues(current => ({
			...current,
			id: isEditMode ? current.id : undefined,
			name: value,
		}));
	}

	async function addImages(
		files: File[],
		options?: { successText?: string; errorText?: string; pasted?: boolean },
	) {
		try {
			setImageError(null);
			const nextImages = options?.pasted
				? await filesToFormImages(files)
				: await Promise.all(
						files.map(async (file, index) => ({
							uid: `upload-${Date.now()}-${index}-${Math.random().toString(36).slice(2, 10)}`,
							fileName: file.name,
							dataUrl: await readFileAsDataUrl(file),
						})),
					);
			setFormValues(current => ({ ...current, images: [...current.images, ...nextImages] }));
			if (options?.successText) {
				toast.success(options.successText);
			}
		} catch (error) {
			const errorDetails = formatPillImageError(
				error,
				options?.errorText ?? 'Unable to process uploaded images.',
			);
			setImageError(errorDetails);
			toast.error(errorDetails.message);
		}
	}

	function removeImage(uid: string) {
		setImageError(null);
		setFormValues(current => ({ ...current, images: removeImageByUid(current.images, uid) }));
	}

	function handleParseImages() {
		if (formValues.images.length === 0) {
			return;
		}

		setImageError(null);
		extractionMutation.mutate({
			images: getImagePayload(formValues.images),
		});
	}

	async function handleSubmit(values: PillFormValues) {
		const isNewPill = values.id == null;
		const submittedPeriods = values.periods.map(period => ({
			...period,
			id: isNewPill ? undefined : period.id,
			count: normalizePeriodCount(period.count),
			daysOfWeek: normalizeWeekdaySelection(period.daysOfWeek ?? []),
			tagNames: normalizeTagNames(period.tagNames ?? []),
		}));

		await upsertMutation.mutateAsync({
			id: values.id,
			name: values.name,
			value: values.value,
			unit: values.unit,
			url: values.url,
			note: values.note,
			tagNames: normalizeTagNames(values.tagNames ?? []),
			images: getImagePayload(values.images),
			components: values.components,
			periods: submittedPeriods,
		});
	}

	async function handleDeleteSavedPeriod(periodId: number, index: number) {
		setDeletingPeriodId(periodId);

		try {
			const result = await deletePeriodMutation.mutateAsync({
				where: [
					{
						column: 'id',
						operator: 'eq',
						value: periodId,
					},
				],
			});

			removePeriod(index);
			toast.success(
				result.deletedCount === 1
					? 'Date range deleted.'
					: `${result.deletedCount} date ranges deleted.`,
			);
		} catch (error) {
			toast.error(getErrorMessage(error, 'Unable to delete date range.'));
		}
	}

	function updatePeriod(index: number, patch: Partial<PillPeriodFormValue>) {
		setFormValues(current => ({
			...current,
			periods: current.periods.map((period, periodIndex) =>
				periodIndex === index ? { ...period, ...patch } : period,
			),
		}));
	}

	function removePeriod(index: number) {
		setFormValues(current => ({
			...current,
			periods: current.periods.filter((_, periodIndex) => periodIndex !== index),
		}));
	}

	function handlePeriodDailyChange(index: number, isDaily: boolean) {
		const rowValue = formValues.periods[index];
		updatePeriod(index, {
			daysOfWeek: isDaily ? [] : [getDefaultWeekdayForPeriod(rowValue)],
		});
	}

	function handlePeriodWeekdayToggle(index: number, weekday: PillWeekday, checked: boolean) {
		const selectedDays = normalizeWeekdaySelection(formValues.periods[index]?.daysOfWeek ?? []);
		const nextSet = new Set(selectedDays);
		if (checked) {
			nextSet.add(weekday);
		} else {
			nextSet.delete(weekday);
		}
		updatePeriod(index, {
			daysOfWeek: normalizeWeekdaySelection([...nextSet]),
		});
	}

	function updateComponent(index: number, patch: Partial<PillComponentFormValue>) {
		setFormValues(current => ({
			...current,
			components: current.components.map((component, componentIndex) =>
				componentIndex === index ? { ...component, ...patch } : component,
			),
		}));
	}

	function insertComponent(index: number) {
		setFormValues(current => ({
			...current,
			components: [
				...current.components.slice(0, index),
				getComponentInsertValue(),
				...current.components.slice(index),
			],
		}));
	}

	function removeComponent(index: number) {
		setFormValues(current => ({
			...current,
			components: current.components.filter((_, componentIndex) => componentIndex !== index),
		}));
	}

	function getMergedTagNames(pill: PillRecord, periodTagNames: string[]) {
		const pillTagNames = pill.tags.map(tag => tag.name);
		const seen = new Set(pillTagNames.map(n => n.toLocaleLowerCase()));
		const deduped = [...pillTagNames];
		for (const name of periodTagNames) {
			if (!seen.has(name.toLocaleLowerCase())) {
				seen.add(name.toLocaleLowerCase());
				deduped.push(name);
			}
		}
		return deduped;
	}

	function renderPillNameCell(pill: PillRecord, periodTagNames: string[]) {
		const tagNames = getMergedTagNames(pill, periodTagNames);

		return (
			<XStack gap={6} flexWrap='wrap' alignItems='center'>
				<Button
					chromeless
					padding={0}
					height='auto'
					icon={<EditOutlined />}
					onPress={() => openExistingPillDrawer(pill)}
				>
					<Text fontWeight='700'>{pill.name}</Text>
				</Button>

				{tagNames.map(tagName => {
					const tagRecord = availableTags.find(tag => tag.name === tagName);
					return (
						<TagChip key={`${pill.id}-${tagName}`} color={tagRecord?.color ?? undefined}>
							{tagName}
						</TagChip>
					);
				})}
			</XStack>
		);
	}

	const activeColumns = useMemo<Array<DataColumn<ActivePillRow>>>(
		() => [
			{
				key: 'name',
				header: 'Pill',
				width: 360,
				cell: row =>
					renderPillNameCell(
						row.pill,
						row.activePeriod.tags.map(tag => tag.name),
					),
			},
			{
				key: 'amount',
				header: 'Amount',
				cell: row =>
					formatServing(
						multiplyServingValue(row.pill.value, row.activePeriod.count),
						row.pill.unit,
					),
			},
			{
				key: 'frequency',
				header: 'Frequency',
				cell: row => formatPillSchedule(row.activePeriod),
			},
			{
				key: 'started',
				header: 'Started',
				cell: row => formatRelativeDate(row.activePeriod.startDate),
			},
			{
				key: 'components',
				header: 'Components',
				cell: row => renderComponents(row.pill.components, row.activePeriod.count),
				getCellProps: row =>
					row.pill.components.length > 0
						? {
								onClick: () => toggleExpandedComponentsRow(String(row.pill.id)),
								style: { cursor: 'pointer' },
							}
						: {},
			},
			{
				key: 'images',
				header: 'Images',
				cell: row => renderImages(row.pill.images),
			},
		],
		[availableTags, openExistingPillDrawer, toggleExpandedComponentsRow],
	);

	const futureColumns = useMemo<Array<DataColumn<FuturePillRow>>>(
		() => [
			{
				key: 'name',
				header: 'Pill',
				width: 360,
				cell: row =>
					renderPillNameCell(
						row.pill,
						row.futurePeriod.tags.map(tag => tag.name),
					),
			},
			{
				key: 'amount',
				header: 'Amount',
				cell: row =>
					formatServing(
						multiplyServingValue(row.pill.value, row.futurePeriod.count),
						row.pill.unit,
					),
			},
			{
				key: 'frequency',
				header: 'Frequency',
				cell: row => formatPillSchedule(row.futurePeriod),
			},
			{
				key: 'starts',
				header: 'Starts',
				cell: row => formatRelativeDate(row.futurePeriod.startDate),
			},
			{
				key: 'components',
				header: 'Components',
				cell: row => renderComponents(row.pill.components, row.futurePeriod.count),
				getCellProps: row =>
					row.pill.components.length > 0
						? {
								onClick: () => toggleExpandedComponentsRow(`future-${row.pill.id}`),
								style: { cursor: 'pointer' },
							}
						: {},
			},
			{
				key: 'images',
				header: 'Images',
				cell: row => renderImages(row.pill.images),
			},
		],
		[availableTags, openExistingPillDrawer, toggleExpandedComponentsRow],
	);

	const pastColumns = useMemo<Array<DataColumn<PastPillRow>>>(
		() => [
			{
				key: 'name',
				header: 'Pill',
				width: 360,
				cell: row =>
					renderPillNameCell(
						row.pill,
						row.period.tags.map(tag => tag.name),
					),
			},
			{
				key: 'amount',
				header: 'Amount',
				cell: row =>
					formatServing(multiplyServingValue(row.pill.value, row.period.count), row.pill.unit),
			},
			{
				key: 'frequency',
				header: 'Frequency',
				cell: row => formatPillSchedule(row.period),
			},
			{
				key: 'period',
				header: 'Period',
				cell: row => formatPeriodRange(row.period),
			},
			{
				key: 'components',
				header: 'Components',
				cell: row => renderComponents(row.pill.components, row.period.count),
				getCellProps: row =>
					row.pill.components.length > 0
						? {
								onClick: () => toggleExpandedComponentsRow(row.key),
								style: { cursor: 'pointer' },
							}
						: {},
			},
			{
				key: 'images',
				header: 'Images',
				cell: row => renderImages(row.pill.images),
			},
		],
		[availableTags, openExistingPillDrawer, toggleExpandedComponentsRow],
	);

	const notTrackedColumns = useMemo<Array<DataColumn<NotTrackedPillRow>>>(
		() => [
			{
				key: 'name',
				header: 'Pill',
				width: 360,
				cell: row => renderPillNameCell(row.pill, []),
			},
			{
				key: 'amount',
				header: 'Amount',
				cell: row => formatServing(row.pill.value, row.pill.unit),
			},
			{
				key: 'frequency',
				header: 'Frequency',
				cell: () => null,
			},
			{
				key: 'components',
				header: 'Components',
				cell: row => renderComponents(row.pill.components),
				getCellProps: row =>
					row.pill.components.length > 0
						? {
								onClick: () => toggleExpandedComponentsRow(String(row.pill.id)),
								style: { cursor: 'pointer' },
							}
						: {},
			},
			{
				key: 'images',
				header: 'Images',
				cell: row => renderImages(row.pill.images),
			},
		],
		[availableTags, openExistingPillDrawer, toggleExpandedComponentsRow],
	);

	const onExpandedRowsChange = useCallback((keys: readonly Key[]) => {
		setExpandedComponentRowKeys(keys.map(key => String(key)));
	}, []);

	const periodRows = formValues.periods.map((period, index) => ({
		key: period.id ? `period-${period.id}` : `draft-${index}`,
		period,
		index,
	}));
	const periodColumns = useMemo<Array<DataColumn<(typeof periodRows)[number]>>>(
		() => [
			{
				key: 'period',
				header: 'Period',
				width: 250,
				cell: row => (
					<XStack gap={6} flexWrap='wrap'>
						<Input
							type='date'
							value={row.period.startDate ?? ''}
							onChange={event => updatePeriod(row.index, { startDate: event.target.value })}
						/>
						<Input
							type='date'
							value={row.period.endDate ?? ''}
							onChange={event => updatePeriod(row.index, { endDate: event.target.value })}
						/>
					</XStack>
				),
			},
			{
				key: 'count',
				header: 'Count',
				width: 100,
				cell: row => (
					<Input
						type='number'
						min={0.01}
						step={0.5}
						value={String(row.period.count ?? '')}
						onChange={event =>
							updatePeriod(row.index, {
								count: Number(event.target.value),
							})
						}
					/>
				),
			},
			{
				key: 'frequency',
				header: 'Frequency',
				width: 220,
				cell: row => <PeriodFrequencyEditor row={row.period} index={row.index} />,
			},
			{
				key: 'timing',
				header: 'Timing',
				width: 150,
				cell: row => (
					<NativeSelect
						value={row.period.timing ?? ''}
						options={[{ label: 'Optional', value: '' }, ...timingOptions]}
						onChange={value =>
							updatePeriod(row.index, {
								timing: value ? (value as PillTiming) : undefined,
							})
						}
					/>
				),
			},
			{
				key: 'tags',
				header: 'Tags',
				width: 250,
				cell: row => (
					<TagInput
						value={row.period.tagNames ?? []}
						options={tagOptions}
						placeholder='Tags'
						onChange={tagNames =>
							updatePeriod(row.index, { tagNames: normalizeTagNames(tagNames) })
						}
					/>
				),
			},
			{
				key: 'actions',
				header: '',
				width: 70,
				align: 'right',
				cell: row => {
					const isSavedRow = Boolean(row.period.id);
					return (
						<Button
							icon={
								isSavedRow && deletingPeriodId === row.period.id ? (
									<Spinner size='small' />
								) : (
									<DeleteOutlined />
								)
							}
							disabled={isSavedRow && deletingPeriodId === row.period.id}
							onPress={() => {
								if (isSavedRow && row.period.id) {
									void handleDeleteSavedPeriod(row.period.id, row.index);
									return;
								}
								removePeriod(row.index);
							}}
						/>
					);
				},
			},
		],
		[deletingPeriodId, tagOptions],
	);

	const componentRows = formValues.components.map((component, index) => ({
		key: `component-${index}`,
		component,
		index,
	}));
	const componentColumns = useMemo<Array<DataColumn<(typeof componentRows)[number]>>>(
		() => [
			{
				key: 'name',
				header: 'Name',
				cell: row => (
					<Input
						value={row.component.name}
						placeholder='Vitamin D3'
						onChange={event => updateComponent(row.index, { name: event.target.value })}
					/>
				),
			},
			{
				key: 'value',
				header: 'Value',
				width: 130,
				cell: row => (
					<Input
						value={row.component.value}
						placeholder='125'
						onChange={event => updateComponent(row.index, { value: event.target.value })}
					/>
				),
			},
			{
				key: 'unit',
				header: 'Unit',
				width: 140,
				cell: row => (
					<Input
						value={row.component.unit}
						placeholder='mcg'
						onChange={event => updateComponent(row.index, { unit: event.target.value })}
					/>
				),
			},
			{
				key: 'actions',
				header: 'Action',
				width: 140,
				cell: row => (
					<XStack gap={4}>
						<Button
							icon={<ArrowUpOutlined />}
							aria-label='Insert component before'
							onPress={() => insertComponent(row.index)}
						/>
						<Button
							icon={<ArrowDownOutlined />}
							aria-label='Insert component after'
							onPress={() => insertComponent(row.index + 1)}
						/>
						<Button
							icon={<DeleteOutlined />}
							aria-label='Delete component'
							onPress={() => removeComponent(row.index)}
						/>
					</XStack>
				),
			},
		],
		[],
	);

	function PeriodFrequencyEditor(props: { row: PillPeriodFormValue; index: number }) {
		const selectedDays = normalizeWeekdaySelection(props.row.daysOfWeek ?? []);
		const isDaily = selectedDays.length === 0;

		return (
			<div className='pills-frequency-control'>
				<CheckControl
					checked={isDaily}
					onCheckedChange={checked => handlePeriodDailyChange(props.index, checked)}
				>
					Daily
				</CheckControl>

				{isDaily ? null : (
					<div className='pills-weekday-selector'>
						{weekdayOptions.map(option => (
							<CheckControl
								key={option.value}
								checked={selectedDays.includes(option.value)}
								onCheckedChange={checked =>
									handlePeriodWeekdayToggle(props.index, option.value, checked)
								}
							>
								{option.label}
							</CheckControl>
						))}
					</div>
				)}
			</div>
		);
	}

	return (
		<main className='pills-page' style={{ background: token.colorBgLayout }}>
			<PageNav
				title='Pills'
				actions={
					<XStack gap={8}>
						<Button
							icon={<CopyOutlined />}
							onPress={() => {
								void handleExport();
							}}
							disabled={dashboardQuery.isLoading}
						>
							Export
						</Button>
						<Button
							className='app-button-primary'
							backgroundColor='$primary'
							style={{ color: 'white' }}
							icon={<PlusOutlined color='white' />}
							onPress={openNewPillDrawer}
						>
							<Text color='$white'>Log pill</Text>
						</Button>
					</XStack>
				}
			/>

			<div className='pills-page-inner'>
				{futureRows.length > 0 ? (
					<PillTableSection
						title='Future pills'
						rowCount={futureRows.length}
						rows={futureRows}
						columns={futureColumns}
						loading={dashboardQuery.isLoading}
						getRowKey={row => `future-${row.pill.id}`}
						expandedRowKeys={expandedComponentRowKeys}
						onExpandedRowKeysChange={onExpandedRowsChange}
						renderExpandedRow={row =>
							renderExpandedComponents(row.pill.components, row.futurePeriod.count)
						}
						canExpandRow={row => row.pill.components.length > 0}
					/>
				) : null}

				{activeRows.length > 0 ? (
					<PillTableSection
						title='Active pills'
						rowCount={activePills.length}
						rows={activeRows}
						columns={activeColumns}
						loading={dashboardQuery.isLoading}
						getRowKey={row => String(row.pill.id)}
						expandedRowKeys={expandedComponentRowKeys}
						onExpandedRowKeysChange={onExpandedRowsChange}
						renderExpandedRow={row =>
							renderExpandedComponents(row.pill.components, row.activePeriod.count)
						}
						canExpandRow={row => row.pill.components.length > 0}
					/>
				) : null}

				{notTrackedRows.length > 0 ? (
					<PillTableSection
						title='Not tracked yet'
						rowCount={notTrackedRows.length}
						rows={notTrackedRows}
						columns={notTrackedColumns}
						loading={dashboardQuery.isLoading}
						getRowKey={row => `not-${row.pill.id}`}
						expandedRowKeys={expandedComponentRowKeys}
						onExpandedRowKeysChange={onExpandedRowsChange}
						renderExpandedRow={row => renderExpandedComponents(row.pill.components)}
						canExpandRow={row => row.pill.components.length > 0}
					/>
				) : null}

				{pastRows.length > 0 ? (
					<PillTableSection
						title='Past pills'
						rowCount={pastRows.length}
						rows={pastRows}
						columns={pastColumns}
						loading={dashboardQuery.isLoading}
						getRowKey={row => row.key}
						expandedRowKeys={expandedComponentRowKeys}
						onExpandedRowKeysChange={onExpandedRowsChange}
						renderExpandedRow={row =>
							renderExpandedComponents(row.pill.components, row.period.count)
						}
						canExpandRow={row => row.pill.components.length > 0}
					/>
				) : null}
			</div>

			{isDrawerOpen ? (
				<DrawerOverlay
					title={isEditMode ? 'Edit pill' : 'Log pill'}
					onClose={handleCloseDrawer}
					actions={
						<XStack gap={8} flexWrap='wrap'>
							{isEditMode && editPillId !== null ? (
								<Button
									disabled={deletePillMutation.isPending}
									onPress={() => {
										if (
											window.confirm(
												'Remove this pill? This removes the pill and all its date ranges, components, and images.',
											)
										) {
											deletePillMutation.mutate({
												where: [{ column: 'id', operator: 'eq', value: editPillId }],
											});
										}
									}}
								>
									{deletePillMutation.isPending ? 'Removing...' : 'Remove'}
								</Button>
							) : null}
							<Button onPress={handleCloseDrawer}>Cancel</Button>
							<Button
								className='app-button-primary'
								backgroundColor='$primary'
								style={{ color: 'white' }}
								disabled={isSaveDisabled}
								onPress={() => {
									void handleSubmit(formValues);
								}}
							>
								<Text color='$white'>{upsertMutation.isPending ? 'Saving...' : 'Save'}</Text>
							</Button>
						</XStack>
					}
				>
					<YStack gap={14}>
						<div className='pills-primary-fields'>
							<FormField label='Pill name' required>
								<Input
									value={formValues.name}
									list='pill-search-options'
									placeholder='Start typing to reuse a canonical pill'
									onChange={event => handleNameChange(event.target.value)}
								/>
								<datalist id='pill-search-options'>
									{searchResults.map(result => (
										<option key={result.id} value={result.name} />
									))}
								</datalist>
							</FormField>

							<FormField label='Value' required>
								<Input
									value={formValues.value}
									placeholder='e.g. 2'
									onChange={event => patchFormValues({ value: event.target.value })}
								/>
							</FormField>

							<FormField label='Unit' required>
								<Input
									value={formValues.unit}
									placeholder='e.g. capsules'
									onChange={event => patchFormValues({ unit: event.target.value })}
								/>
							</FormField>
						</div>

						<FormField label='URL'>
							<Input
								value={formValues.url}
								placeholder='Optional product URL'
								onChange={event => patchFormValues({ url: event.target.value })}
							/>
						</FormField>

						<FormField label='Tags'>
							<TagInput
								value={formValues.tagNames}
								options={tagOptions}
								placeholder='Type to attach or create tags'
								onChange={tagNames => patchFormValues({ tagNames: normalizeTagNames(tagNames) })}
							/>
						</FormField>

						<FormField label='Note'>
							<AutoResizeTextArea
								value={formValues.note}
								placeholder='Optional note'
								minRows={1}
								maxRows={6}
								onChange={event =>
									patchFormValues({
										note: (event.target as unknown as HTMLTextAreaElement).value,
									})
								}
							/>
						</FormField>

						<div className='pills-images-header'>
							<H3 className='pills-images-divider'>Images</H3>

							{formValues.images.length > 0 ? (
								<Button
									disabled={isParsingImages}
									onPress={handleParseImages}
									icon={isParsingImages ? <Spinner size='small' /> : undefined}
								>
									{isParsingImages ? 'Parsing...' : 'Parse images'}
								</Button>
							) : null}
						</div>

						<div className='pills-images-grid'>
							{formValues.images.map(image => (
								<div
									key={image.uid}
									className='pills-image-tile'
									style={{
										border: `1px solid ${token.colorBorderSecondary}`,
										background: token.colorBgContainer,
									}}
								>
									<img
										src={image.dataUrl}
										alt={image.fileName}
										width={IMAGE_TILE_SIZE}
										height={IMAGE_TILE_SIZE}
										style={{ objectFit: 'cover' }}
									/>
									<Button
										icon={<DeleteOutlined />}
										onPress={() => removeImage(image.uid)}
										className='pills-image-remove'
									/>
								</div>
							))}

							<FileDropzone
								accept='image/*'
								multiple
								disabled={isParsingImages}
								className='pills-upload-tile'
								style={{
									width: IMAGE_TILE_SIZE,
									height: IMAGE_TILE_SIZE,
									border: `1px dashed ${token.colorBorderSecondary}`,
									borderRadius: 5,
									background: token.colorFillAlter,
								}}
								onFiles={files => {
									void addImages(files);
								}}
							>
								<div className='pills-upload-inner'>
									<UploadOutlined size={20} />
									<Text className='pills-upload-label' color='$textMuted'>
										Drop or upload
									</Text>
								</div>
							</FileDropzone>
						</div>

						{isParsingImages ? (
							<div
								className='pills-parsing-banner'
								style={{
									borderColor: token.colorInfoBorder,
									background: token.colorInfoBg,
									color: token.colorInfoText,
								}}
							>
								<Spinner size='small' />
								<Text className='pills-parsing-banner-text'>
									Parsing uploaded images and filling the form...
								</Text>
							</div>
						) : null}
						{imageError ? (
							<InlineAlert title={imageError.message}>
								<pre className='pills-image-error-details'>{imageError.details}</pre>
							</InlineAlert>
						) : null}

						<SectionDivider>Date Ranges</SectionDivider>

						<YStack gap={8} className='pills-list-space'>
							{periodRows.length > 0 ? (
								<DataTable
									rows={periodRows}
									columns={periodColumns}
									getRowKey={row => row.key}
									minWidth={1040}
									className='pills-periods-table'
								/>
							) : null}

							<Button
								icon={<PlusOutlined />}
								onPress={() =>
									setFormValues(current => ({
										...current,
										periods: [...current.periods, createBlankPeriod()],
									}))
								}
							>
								Add {periodRows.length > 0 ? 'another' : 'a'} range
							</Button>
						</YStack>

						<SectionDivider>
							{formatSupplementFactsTitle(formValues.value, formValues.unit)}
						</SectionDivider>

						<YStack gap={8} className='pills-list-space'>
							<DataTable
								rows={componentRows}
								columns={componentColumns}
								getRowKey={row => row.key}
								minWidth={760}
							/>

							<Button
								icon={<PlusOutlined />}
								onPress={() =>
									setFormValues(current => ({
										...current,
										components: [...current.components, getComponentInsertValue()],
									}))
								}
							>
								Add component
							</Button>
						</YStack>
					</YStack>
				</DrawerOverlay>
			) : null}
		</main>
	);
}

function PillTableSection<T>(props: {
	title: string;
	rowCount: number;
	rows: T[];
	columns: Array<DataColumn<T>>;
	loading: boolean;
	getRowKey: (row: T, index: number) => Key;
	expandedRowKeys: readonly Key[];
	onExpandedRowKeysChange: (keys: Key[]) => void;
	renderExpandedRow: (row: T) => ReactNode;
	canExpandRow: (row: T) => boolean;
}) {
	return (
		<SectionCard
			title={props.title}
			actions={<Text color='$textMuted'>{props.rowCount} rows</Text>}
		>
			<DataTable
				rows={props.rows}
				columns={props.columns}
				getRowKey={props.getRowKey}
				loading={props.loading}
				expandedRowKeys={props.expandedRowKeys}
				onExpandedRowKeysChange={props.onExpandedRowKeysChange}
				renderExpandedRow={props.renderExpandedRow}
				canExpandRow={props.canExpandRow}
				showExpandColumn={false}
			/>
		</SectionCard>
	);
}

function themeValue(theme: ReturnType<typeof useTheme>, name: string) {
	const token = (theme as any)[name];
	if (!token?.get) {
		throw new Error(`Missing Tamagui theme token: ${name}`);
	}
	return token.get('web') as string;
}

function getPillsThemeToken(theme: ReturnType<typeof useTheme>) {
	return {
		colorBgLayout: themeValue(theme, 'bgLayout'),
		colorBgContainer: themeValue(theme, 'bgContainer'),
		colorBorderSecondary: themeValue(theme, 'borderSubtle'),
		colorFillAlter: themeValue(theme, 'fill'),
		colorTextSecondary: themeValue(theme, 'textMuted'),
		colorError: themeValue(theme, 'error'),
		colorErrorBg: themeValue(theme, 'errorBg'),
		colorErrorBorder: themeValue(theme, 'errorBorder'),
		colorInfoBg: themeValue(theme, 'infoBg'),
		colorInfoBorder: themeValue(theme, 'infoBorder'),
		colorInfoText: themeValue(theme, 'infoText'),
	};
}

function SectionCard(props: { title: ReactNode; actions?: ReactNode; children: ReactNode }) {
	const token = getPillsThemeToken(useTheme());

	return (
		<Card
			borderWidth={1}
			borderColor='$borderSubtle'
			backgroundColor='$bgContainer'
			borderRadius={5}
			overflow='hidden'
		>
			<XStack
				justifyContent='space-between'
				alignItems='center'
				gap={12}
				flexWrap='wrap'
				paddingHorizontal={12}
				paddingVertical={10}
				style={{ borderBottom: `1px solid ${token.colorBorderSecondary}` }}
			>
				{typeof props.title === 'string' ? <H3>{props.title}</H3> : props.title}
				{props.actions}
			</XStack>
			<div style={{ padding: 12 }}>{props.children}</div>
		</Card>
	);
}

function SectionDivider(props: { children: ReactNode }) {
	const token = getPillsThemeToken(useTheme());

	return (
		<XStack alignItems='center' gap={12}>
			<div style={{ height: 1, flex: 1, background: token.colorBorderSecondary }} />
			<Text fontWeight='700'>{props.children}</Text>
			<div style={{ height: 1, flex: 1, background: token.colorBorderSecondary }} />
		</XStack>
	);
}

function DrawerOverlay(props: {
	title: ReactNode;
	actions: ReactNode;
	children: ReactNode;
	onClose: () => void;
}) {
	const token = getPillsThemeToken(useTheme());
	const panelStyle: CSSProperties = {
		width: 920,
		maxWidth: 'calc(100vw - 32px)',
		height: 'calc(100vh - 32px)',
		background: token.colorBgContainer,
		border: `1px solid ${token.colorBorderSecondary}`,
		borderRadius: 6,
		boxShadow: '0 18px 48px rgba(15, 23, 42, 0.24)',
		overflow: 'hidden',
	};

	return (
		<div
			style={{
				position: 'fixed',
				inset: 0,
				zIndex: 80,
				display: 'flex',
				justifyContent: 'flex-end',
				padding: 16,
				background: 'rgba(15, 23, 42, 0.38)',
			}}
		>
			<div style={panelStyle}>
				<XStack
					justifyContent='space-between'
					alignItems='center'
					gap={12}
					paddingHorizontal={12}
					paddingVertical={10}
					style={{ borderBottom: `1px solid ${token.colorBorderSecondary}` }}
				>
					<H3>{props.title}</H3>
					{props.actions}
				</XStack>
				<div style={{ height: 'calc(100% - 53px)', overflowY: 'auto', padding: 12 }}>
					{props.children}
				</div>
			</div>
		</div>
	);
}

function InlineAlert(props: { title: ReactNode; children: ReactNode }) {
	const token = getPillsThemeToken(useTheme());

	return (
		<XStack
			gap={10}
			padding={10}
			borderWidth={1}
			borderRadius={5}
			className='pills-image-error'
			style={{ background: token.colorErrorBg, borderColor: token.colorErrorBorder }}
		>
			<WarningCircle size={18} weight='fill' color={token.colorError} />
			<YStack flex={1} minWidth={0}>
				<Text fontWeight='700'>{props.title}</Text>
				<Paragraph color='$textMuted'>{props.children}</Paragraph>
			</YStack>
		</XStack>
	);
}

function CheckControl(props: {
	checked: boolean;
	children: ReactNode;
	onCheckedChange: (checked: boolean) => void;
}) {
	return (
		<XStack alignItems='center' gap={6}>
			<Checkbox
				checked={props.checked}
				onCheckedChange={value => props.onCheckedChange(Boolean(value))}
			>
				<Checkbox.Indicator />
			</Checkbox>
			<Text>{props.children}</Text>
		</XStack>
	);
}

function NativeSelect(props: {
	value: string;
	options: Array<{ label: ReactNode; value: string }>;
	onChange: (value: string) => void;
}) {
	return (
		<select
			className='native-select'
			value={props.value}
			onChange={event => props.onChange(event.target.value)}
		>
			{props.options.map(option => (
				<option key={option.value || 'empty'} value={option.value}>
					{option.label}
				</option>
			))}
		</select>
	);
}

function getTodayDateString() {
	return dayjs().format(DATE_FORMAT);
}

function createBlankPeriod(): PillPeriodFormValue {
	return {
		startDate: getTodayDateString(),
		endDate: '',
		count: 1,
		daysOfWeek: [],
		tagNames: [],
	};
}

function getPillPeriodFormValues(pill: PillRecord) {
	return pill.periods.map(period => ({
		id: period.id,
		startDate: period.startDate,
		endDate: period.endDate ?? '',
		count: normalizePeriodCount(period.count),
		timing: period.timing ?? undefined,
		daysOfWeek: normalizeWeekdaySelection(period.daysOfWeek ?? []),
		tagNames: period.tags.map(tag => tag.name),
	}));
}

function createEmptyFormValues(): PillFormValues {
	return {
		name: '',
		value: '',
		unit: '',
		url: '',
		note: '',
		tagNames: [],
		images: [],
		components: [{ name: '', value: '', unit: '' }],
		periods: [],
	};
}

function normalizeTagNames(values: string[]) {
	const namesByKey = new Map<string, string>();

	for (const rawValue of values) {
		const value = rawValue.trim();
		if (!value) {
			continue;
		}

		const key = value.toLocaleLowerCase();
		if (!namesByKey.has(key)) {
			namesByKey.set(key, value);
		}
	}

	return [...namesByKey.values()];
}

function formatServing(value?: string | null, unit?: string | null) {
	const text = [value?.trim(), unit?.trim()].filter(Boolean).join(' ');
	return text || 'Not set';
}

function formatSupplementFactsTitle(value?: string | null, unit?: string | null) {
	const serving = [value?.trim(), unit?.trim()].filter(Boolean).join(' ');
	return serving ? `Supplement Facts per ${serving}` : 'Supplement Facts';
}

function normalizePeriodCount(value: number | null | undefined) {
	return typeof value === 'number' && Number.isFinite(value) && value > 0 ? value : 1;
}

function formatNumericValue(value: number) {
	return Number.isInteger(value) ? String(value) : Number(value.toFixed(4)).toString();
}

function multiplyServingValue(value: string | null | undefined, count: number | null | undefined) {
	const trimmedValue = value?.trim() ?? '';
	const normalizedCount = normalizePeriodCount(count);

	if (!trimmedValue) {
		return '';
	}

	const numericValue = Number(trimmedValue.replace(/,/g, ''));
	if (!Number.isFinite(numericValue)) {
		return normalizedCount === 1
			? trimmedValue
			: `${formatNumericValue(normalizedCount)} x ${trimmedValue}`;
	}

	return formatNumericValue(numericValue * normalizedCount);
}

function getActivePeriod(pill: PillRecord) {
	const today = getTodayDateString();

	return (
		[...pill.periods]
			.filter(period => period.startDate <= today && (!period.endDate || period.endDate > today))
			.sort((left, right) => left.startDate.localeCompare(right.startDate))
			.at(-1) ?? null
	);
}

function getFuturePeriod(pill: PillRecord) {
	const today = getTodayDateString();

	return (
		[...pill.periods]
			.filter(period => period.startDate > today && (!period.endDate || period.endDate > today))
			.sort((left, right) => left.startDate.localeCompare(right.startDate))
			.at(0) ?? null
	);
}

function formatPeriodRange(period: Pick<PillPeriod, 'startDate' | 'endDate'>) {
	return period.endDate
		? `${period.startDate} to ${period.endDate}`
		: `${period.startDate} to ongoing`;
}

function isPillWeekday(value: unknown): value is PillWeekday {
	return typeof value === 'string' && (pillWeekdayValues as readonly string[]).includes(value);
}

function normalizeWeekdaySelection(values: readonly PillWeekday[]) {
	const selectedValues = new Set(values.filter(isPillWeekday));
	const orderedValues = pillWeekdayValues.filter(value => selectedValues.has(value));
	return orderedValues.length === pillWeekdayValues.length ? [] : orderedValues;
}

function getDefaultWeekdayForPeriod(period: PillPeriodFormValue | undefined) {
	const date = period?.startDate ? dayjs(period.startDate, DATE_FORMAT) : dayjs();
	const weekdayIndex = ((date.isValid() ? date.day() : dayjs().day()) + 6) % 7;
	return pillWeekdayValues[weekdayIndex];
}

function formatWeekdayFrequency(daysOfWeek: readonly PillWeekday[] | null | undefined) {
	const selectedDays = normalizeWeekdaySelection(daysOfWeek ?? []);
	if (selectedDays.length === 0) {
		return 'daily';
	}

	const dayLabels = selectedDays.map(
		day => weekdayOptions.find(option => option.value === day)?.label ?? day,
	);
	return `every ${dayLabels.join(', ')}`;
}

function formatTimingPhrase(timing: PillTiming | null | undefined) {
	switch (timing) {
		case 'morning':
			return 'in the morning';
		case 'afternoon':
			return 'in the afternoon';
		case 'evening':
			return 'in the evening';
		default:
			return '';
	}
}

function formatPillSchedule(period: {
	daysOfWeek?: readonly PillWeekday[] | null;
	timing?: PillTiming | null;
}) {
	return [formatWeekdayFrequency(period.daysOfWeek), formatTimingPhrase(period.timing)]
		.filter(Boolean)
		.join(' ');
}

function createImageUid(
	image: Pick<PillImageFormValue, 'id' | 'fileName' | 'dataUrl'>,
	index: number,
) {
	return image.id
		? `saved-${image.id}`
		: `image-${index}-${image.fileName}-${image.dataUrl.length}`;
}

function pillToFormValues(
	pill: PillRecord,
	options?: {
		appendNewPeriod?: boolean;
	},
): PillFormValues {
	const periodFormValues = getPillPeriodFormValues(pill);
	const shouldAppendNewPeriod = options?.appendNewPeriod ?? false;

	return {
		id: pill.id,
		name: pill.name,
		value: pill.value ?? '',
		unit: pill.unit ?? '',
		url: pill.url ?? '',
		note: pill.note ?? '',
		tagNames: pill.tags.map(tag => tag.name),
		images: pill.images.map((image, index) => ({
			id: image.id,
			uid: createImageUid(
				{
					id: image.id,
					fileName: image.fileName,
					dataUrl: image.dataUrl,
				},
				index,
			),
			fileName: image.fileName,
			dataUrl: image.dataUrl,
		})),
		components:
			pill.components.length > 0
				? pill.components.map(component => ({
						name: component.name,
						value: component.value ?? '',
						unit: component.unit ?? '',
					}))
				: [{ name: '', value: '', unit: '' }],
		periods: shouldAppendNewPeriod ? [...periodFormValues, createBlankPeriod()] : periodFormValues,
	};
}

function extractionToFormPatch(extraction: PillExtractionResult) {
	return {
		name: extraction.name ?? '',
		value: extraction.value ?? '',
		unit: extraction.unit ?? '',
		note: extraction.note ?? '',
		components:
			extraction.components.length > 0
				? extraction.components.map(component => ({
						name: component.name,
						value: component.value ?? '',
						unit: component.unit ?? '',
					}))
				: [{ name: '', value: '', unit: '' }],
	} satisfies Partial<PillFormValues>;
}

function getImagePayload(images: PillImageFormValue[]) {
	return images.map(image => ({
		id: image.id,
		fileName: image.fileName,
		dataUrl: image.dataUrl,
	}));
}

function removeImageByUid(images: PillImageFormValue[], uid: string) {
	return images.filter(image => image.uid !== uid);
}

function getComponentInsertValue() {
	return { name: '', value: '', unit: '' };
}

function readFileAsDataUrl(file: File) {
	return new Promise<string>((resolve, reject) => {
		const reader = new FileReader();
		reader.onload = () => {
			if (typeof reader.result !== 'string') {
				reject(new Error(`Unable to read ${file.name}.`));
				return;
			}

			resolve(reader.result);
		};
		reader.onerror = () => reject(reader.error ?? new Error(`Unable to read ${file.name}.`));
		reader.readAsDataURL(file);
	});
}

function replaceFileExtension(fileName: string, nextExtension: string) {
	const trimmedFileName = fileName.trim();
	if (!trimmedFileName) {
		return `pasted-image-${Date.now()}.${nextExtension}`;
	}

	const sanitizedExtension = nextExtension.replace(/^\./, '');
	const extensionPattern = /\.[^./\\]+$/;
	return extensionPattern.test(trimmedFileName)
		? trimmedFileName.replace(extensionPattern, `.${sanitizedExtension}`)
		: `${trimmedFileName}.${sanitizedExtension}`;
}

async function readPastedImageAsDataUrl(file: File) {
	if (typeof createImageBitmap !== 'function' || typeof document === 'undefined') {
		throw new Error('Pasted images are unavailable in this browser.');
	}

	const bitmap = await createImageBitmap(file);

	try {
		const canvas = document.createElement('canvas');
		canvas.width = bitmap.width;
		canvas.height = bitmap.height;

		const context = canvas.getContext('2d');
		if (!context) {
			throw new Error(`Unable to process ${file.name || 'pasted image'}.`);
		}

		context.drawImage(bitmap, 0, 0);
		return canvas.toDataURL('image/png');
	} finally {
		bitmap.close();
	}
}

function buildPastedImageFileName(file: File, index: number) {
	const defaultName = `pasted-image-${Date.now()}-${index}`;
	return replaceFileExtension(file.name.trim() || defaultName, 'png');
}

async function filesToFormImages(files: File[]) {
	return Promise.all(
		files.map(async (file, index) => {
			const fileName = buildPastedImageFileName(file, index);
			return {
				uid: `pasted-${Date.now()}-${index}-${Math.random().toString(36).slice(2, 10)}`,
				fileName,
				dataUrl: await readPastedImageAsDataUrl(file),
			} satisfies PillImageFormValue;
		}),
	);
}

function getPastedImageFiles(event: ClipboardEvent) {
	const clipboardItems = Array.from(event.clipboardData?.items ?? [])
		.filter(item => item.kind === 'file' && item.type.startsWith('image/'))
		.map(item => item.getAsFile())
		.filter((file): file is File => file !== null);

	if (clipboardItems.length > 0) {
		return clipboardItems;
	}

	return Array.from(event.clipboardData?.files ?? []).filter(file =>
		file.type.startsWith('image/'),
	);
}

function isValidPastedUrl(value: string) {
	if (!value) {
		return false;
	}

	try {
		const url = new URL(value);
		return url.protocol === 'http:' || url.protocol === 'https:';
	} catch {
		return false;
	}
}

function formatPillImageError(error: unknown, fallbackMessage: string): PillImageErrorDetails {
	const message =
		error instanceof Error ? getFirstErrorLine(error.message) || fallbackMessage : fallbackMessage;
	const details = [
		error instanceof Error ? `${error.name}: ${error.message}` : `Error: ${String(error)}`,
		safeJsonStringify(serializeError(error)),
	]
		.filter(Boolean)
		.join('\n\n');

	return {
		message,
		details: details || message,
	};
}

function getFirstErrorLine(value: string) {
	return value
		.split('\n')
		.map(line => line.trim())
		.find(Boolean);
}

function serializeError(error: unknown): unknown {
	if (typeof error === 'bigint') {
		return error.toString();
	}
	if (typeof error !== 'object' || error === null) {
		return error;
	}

	const output: Record<string, unknown> = {};
	for (const key of Reflect.ownKeys(error)) {
		output[String(key)] = (error as Record<PropertyKey, unknown>)[key];
	}
	if (error instanceof Error) {
		output.name = error.name;
		output.message = error.message;
		output.stack = error.stack;
		output.cause = serializeError(error.cause);
	}

	return output;
}

function safeJsonStringify(value: unknown) {
	try {
		return JSON.stringify(value, getDebugJsonReplacer(), 2);
	} catch {
		return String(value);
	}
}

function getDebugJsonReplacer() {
	const seen = new WeakSet<object>();
	return (_key: string, value: unknown) => {
		if (typeof value === 'bigint') {
			return value.toString();
		}
		if (value instanceof Error) {
			return serializeError(value);
		}
		if (typeof Response !== 'undefined' && value instanceof Response) {
			return {
				status: value.status,
				statusText: value.statusText,
				url: value.url,
				headers: Object.fromEntries(value.headers.entries()),
			};
		}
		if (typeof Headers !== 'undefined' && value instanceof Headers) {
			return Object.fromEntries(value.entries());
		}
		if (typeof value !== 'object' || value === null) {
			return value;
		}
		if (seen.has(value)) {
			return '[Circular]';
		}
		seen.add(value);
		return value;
	};
}

function renderComponents(components: PillComponent[], count = 1) {
	if (components.length === 0) {
		return <Text color='$textMuted'>No components</Text>;
	}

	const [firstComponent, ...remainingComponents] = components;

	return (
		<YStack gap={0}>
			<Text>
				{firstComponent.name}:{' '}
				{formatServing(multiplyServingValue(firstComponent.value, count), firstComponent.unit)}
			</Text>
			{remainingComponents.length > 0 ? (
				<Text color='$textMuted'>+{remainingComponents.length} more</Text>
			) : null}
		</YStack>
	);
}

function renderExpandedComponents(components: PillComponent[], count = 1) {
	const columns: Array<DataColumn<PillComponent>> = [
		{
			key: 'name',
			header: 'Name',
			cell: component => component.name,
		},
		{
			key: 'value',
			header: 'Value',
			width: 180,
			cell: component =>
				formatServing(multiplyServingValue(component.value, count), component.unit),
		},
	];

	return (
		<DataTable
			rows={components}
			columns={columns}
			getRowKey={component => component.id}
			minWidth={420}
		/>
	);
}

function renderImages(images: PillImage[]) {
	if (images.length === 0) {
		return <Text color='$textMuted'>No images</Text>;
	}

	return (
		<div className='pills-image-list'>
			{images.map(image => (
				<img
					key={image.id}
					src={image.dataUrl}
					alt={image.fileName}
					width={34}
					height={34}
					className='pills-image-preview'
				/>
			))}
		</div>
	);
}

function buildPillsExport(args: {
	futureRows: FuturePillRow[];
	activeRows: ActivePillRow[];
	notTrackedRows: NotTrackedPillRow[];
	pastRows: PastPillRow[];
}): Partial<Record<PillExportTableName, Record<string, PillExportRow>>> {
	const exportSections: Partial<Record<PillExportTableName, Record<string, PillExportRow>>> = {};
	const futurePills = buildPillExportTable(
		args.futureRows.map(row => ({
			pill: row.pill,
			count: row.futurePeriod.count,
			tags: row.futurePeriod.tags,
			startDate: row.futurePeriod.startDate,
			endDate: row.futurePeriod.endDate ?? undefined,
			timing: row.futurePeriod.timing,
			daysOfWeek: row.futurePeriod.daysOfWeek,
		})),
	);
	const activePills = buildPillExportTable(
		args.activeRows.map(row => ({
			pill: row.pill,
			count: row.activePeriod.count,
			tags: row.activePeriod.tags,
			startDate: row.activePeriod.startDate,
			endDate: row.activePeriod.endDate ?? undefined,
			timing: row.activePeriod.timing,
			daysOfWeek: row.activePeriod.daysOfWeek,
		})),
	);
	const notTrackedPills = buildPillExportTable(
		args.notTrackedRows.map(row => ({
			pill: row.pill,
			count: 1,
			tags: [],
		})),
	);
	const pastPills = buildPillExportTable(
		args.pastRows.map(row => ({
			pill: row.pill,
			count: row.period.count,
			tags: row.period.tags,
			startDate: row.period.startDate,
			endDate: row.period.endDate ?? undefined,
			timing: row.period.timing,
			daysOfWeek: row.period.daysOfWeek,
		})),
	);

	if (Object.keys(futurePills).length > 0) {
		exportSections.futurePills = futurePills;
	}
	if (Object.keys(activePills).length > 0) {
		exportSections.activePills = activePills;
	}
	if (Object.keys(notTrackedPills).length > 0) {
		exportSections.notTrackedPills = notTrackedPills;
	}
	if (Object.keys(pastPills).length > 0) {
		exportSections.pastPills = pastPills;
	}

	return exportSections;
}

function buildPillExportTable(
	rows: Array<{
		pill: PillRecord;
		count: number;
		tags: PillPeriod['tags'];
		startDate?: string;
		endDate?: string;
		timing?: PillPeriod['timing'];
		daysOfWeek?: PillPeriod['daysOfWeek'];
	}>,
) {
	return Object.fromEntries(
		rows.map(({ pill, count, tags, startDate, endDate, timing, daysOfWeek }) => [
			`${pill.name} - ${formatServing(multiplyServingValue(pill.value, count), pill.unit)}`,
			buildPillExportRow({
				pill,
				count,
				tags,
				startDate,
				endDate,
				timing,
				daysOfWeek,
			}),
		]),
	);
}

function buildPillExportRow(args: {
	pill: PillRecord;
	count: number;
	tags: PillPeriod['tags'];
	startDate?: string;
	endDate?: string;
	timing?: PillPeriod['timing'];
	daysOfWeek?: PillPeriod['daysOfWeek'];
}): PillExportRow {
	const exportRow: PillExportRow = {};

	if (args.pill.components.length > 0) {
		exportRow.components = Object.fromEntries(
			args.pill.components.map(component => [
				component.name,
				formatServing(multiplyServingValue(component.value, args.count), component.unit),
			]),
		);
	}
	if (args.pill.note) {
		exportRow.note = args.pill.note;
	}
	if (args.tags.length > 0) {
		exportRow.tags = args.tags.map(tag => `${tag.name}:${tag.note ?? ''}`);
	}
	if (args.startDate) {
		exportRow.startDate = args.startDate;
	}
	if (args.endDate) {
		exportRow.endDate = args.endDate;
	}
	if (args.startDate || args.endDate || args.timing || (args.daysOfWeek?.length ?? 0) > 0) {
		exportRow.timing = formatPillSchedule({
			daysOfWeek: args.daysOfWeek ?? [],
			timing: args.timing,
		});
	}

	return exportRow;
}

async function copyTextToClipboard(value: string) {
	if (typeof navigator !== 'undefined' && navigator.clipboard?.writeText) {
		await navigator.clipboard.writeText(value);
		return;
	}

	if (typeof document === 'undefined') {
		throw new Error('Clipboard is unavailable in this environment.');
	}

	const textArea = document.createElement('textarea');
	textArea.value = value;
	textArea.setAttribute('readonly', '');
	textArea.style.position = 'fixed';
	textArea.style.opacity = '0';
	document.body.appendChild(textArea);
	textArea.select();

	try {
		const didCopy = document.execCommand('copy');
		if (!didCopy) {
			throw new Error('Clipboard copy failed.');
		}
	} finally {
		textArea.remove();
	}
}

function formatRelativeDate(value: string) {
	try {
		return formatDistanceToNow(parseISO(value), { addSuffix: true });
	} catch {
		return value;
	}
}

function getErrorMessage(error: unknown, fallback = 'Something went wrong.') {
	if (error instanceof Error) {
		return error.message || fallback;
	}
	return String(error || fallback);
}
