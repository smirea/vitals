import {
	ArrowDownOutlined,
	ArrowUpOutlined,
	CopyOutlined,
	DeleteOutlined,
	EditOutlined,
	PlusOutlined,
	TagOutlined,
	UploadOutlined,
} from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import {
	Alert,
	AutoComplete,
	Badge,
	Button,
	Card,
	DatePicker,
	Divider,
	Drawer,
	Form,
	Image,
	Input,
	InputNumber,
	Popconfirm,
	Popover,
	Select,
	Space,
	Spin,
	Tag,
	Table,
	Typography,
	Upload,
	message,
	theme as antdTheme,
} from 'antd';
import type { FormInstance, TableColumnsType } from 'antd';
import type { UploadChangeParam, UploadFile, UploadProps } from 'antd/es/upload/interface';
import { formatDistanceToNow, parseISO } from 'date-fns';
import dayjs from 'dayjs';
import type { Key } from 'react';
import { useCallback, useDeferredValue, useEffect, useMemo, useState } from 'react';
import { z } from 'zod';

import type {
	PillComponent,
	PillExtractionResult,
	PillImage,
	PillPeriod,
	PillRecord,
} from '../utils/api';
import { PageNav } from '../components/PageNav';
import { useTRPC } from '../utils/trpc';

const pillsSearchSchema = z.object({
	edit: z.coerce.number().int().positive().optional(),
});

const DATE_FORMAT = 'YYYY-MM-DD';
const IMAGE_TILE_SIZE = 104;
const { Dragger } = Upload;
const { RangePicker } = DatePicker;

export const Route = createFileRoute('/pills')({
	validateSearch: search => pillsSearchSchema.parse(search),
	component: PillsRouteComponent,
});

type PillTiming = 'morning' | 'afternoon' | 'evening';

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

function PillsRouteComponent() {
	const search = Route.useSearch();
	const navigate = Route.useNavigate();
	const editPillId = search.edit ?? null;
	const onEditPillChange = (nextEditPillId: number | null) =>
		navigate({
			search: nextEditPillId ? { edit: nextEditPillId } : {},
		});

	const { token } = antdTheme.useToken();
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const [form] = Form.useForm<PillFormValues>();
	const [isCreateDrawerOpen, setIsCreateDrawerOpen] = useState(false);
	const [pillQuery, setPillQuery] = useState('');
	const [hydratedEditPillId, setHydratedEditPillId] = useState<number | null>(null);
	const [deletingPeriodId, setDeletingPeriodId] = useState<number | null>(null);
	const [isSaveDisabled, setIsSaveDisabled] = useState(true);
	const [expandedComponentRowKeys, setExpandedComponentRowKeys] = useState<string[]>([]);
	const [imageError, setImageError] = useState<PillImageErrorDetails | null>(null);
	const [openPeriodTagEditorKey, setOpenPeriodTagEditorKey] = useState<string | null>(null);
	const deferredPillQuery = useDeferredValue(pillQuery);
	const watchedFormValues = Form.useWatch([], form) as PillFormValues | undefined;
	const watchedImages = (Form.useWatch('images', form) ?? []) as PillImageFormValue[];
	const watchedPeriods = (Form.useWatch('periods', form) ?? []) as PillPeriodFormValue[];
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
				message.info(
					extraction.extractionNotes ??
						'No pill or supplement label was confidently detected in the uploaded images.',
				);
				return;
			}

			form.setFieldsValue(extractionToFormPatch(extraction));
			message.success(`Filled pill details from images using ${extraction.model}.`);
		},
		onError: error => {
			const errorDetails = formatPillImageError(error, 'Image parsing failed.');
			setImageError(errorDetails);
			message.error(errorDetails.message);
		},
	});
	const isParsingImages = extractionMutation.isPending;

	const upsertMutation = useMutation({
		...trpc.pills.upsert.mutationOptions(),
		onSuccess: () => {
			void queryClient.invalidateQueries({ queryKey: [['pills']] });
			void queryClient.invalidateQueries({ queryKey: [['tags']] });
			setIsCreateDrawerOpen(false);
			setOpenPeriodTagEditorKey(null);
			onEditPillChange(null);
			message.success('Pill saved.');
		},
		onError: error => {
			message.error(error.message);
		},
	});

	const deletePillMutation = useMutation({
		...trpc.table.pills.deleteMany.mutationOptions(),
		onSuccess: () => {
			void queryClient.invalidateQueries({ queryKey: [['pills']] });
			void queryClient.invalidateQueries({ queryKey: [['tags']] });
			setIsCreateDrawerOpen(false);
			setOpenPeriodTagEditorKey(null);
			onEditPillChange(null);
			message.success('Pill removed.');
		},
		onError: error => {
			message.error(error.message);
		},
	});

	const deletePeriodMutation = useMutation({
		...trpc.table.pillPeriods.deleteMany.mutationOptions(),
		onSuccess: async () => {
			await queryClient.invalidateQueries({ queryKey: [['pills']] });
		},
		onError: error => {
			message.error(error.message);
		},
		onSettled: () => {
			setDeletingPeriodId(null);
		},
	});

	const dashboard = dashboardQuery.data;
	const availableTags = tagsQuery.data ?? [];
	const searchResults = (searchQuery.data ?? []) as PillRecord[];

	const autocompleteOptions = useMemo(
		() =>
			searchResults.map(result => ({
				label: result.name,
				value: result.name,
				pill: result,
			})),
		[searchResults],
	);
	const tagAutocompleteOptions = useMemo(
		() =>
			availableTags.map(tag => ({
				label: tag.name,
				value: tag.name,
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

	useEffect(() => {
		if (!isDrawerRequestedOpen) {
			return;
		}

		let isCancelled = false;

		const syncSaveDisabledState = async () => {
			const hasErrors = await hasFormErrors(form);
			if (!isCancelled) {
				setIsSaveDisabled(hasErrors);
			}
		};

		void syncSaveDisabledState();

		return () => {
			isCancelled = true;
		};
	}, [form, isDrawerRequestedOpen, watchedFormValues]);

	useEffect(() => {
		if (!isDrawerRequestedOpen || watchedPeriods.length === 0) {
			return;
		}

		const nextPeriods = watchedPeriods.map(period => ({
			...period,
			count: normalizePeriodCount(period.count),
		}));

		const hasChanged = nextPeriods.some(
			(period, index) => period.count !== watchedPeriods[index]?.count,
		);

		if (hasChanged) {
			form.setFieldValue('periods', nextPeriods);
		}
	}, [form, isDrawerRequestedOpen, watchedPeriods]);

	useEffect(() => {
		if (!isDrawerOpen) {
			return;
		}

		const onPaste = (event: ClipboardEvent) => {
			const pastedText = event.clipboardData?.getData('text')?.trim() ?? '';
			if (isValidPastedUrl(pastedText)) {
				event.preventDefault();
				form.setFieldValue('url', pastedText);
				return;
			}

			const pastedFiles = getPastedImageFiles(event);
			if (pastedFiles.length === 0) {
				return;
			}

			event.preventDefault();

			void (async () => {
				try {
					setImageError(null);
					const nextImages = await filesToFormImages(pastedFiles);
					const currentImages = (form.getFieldValue('images') ?? []) as PillImageFormValue[];
					form.setFieldValue('images', [...currentImages, ...nextImages]);
					message.success(
						pastedFiles.length === 1 ? 'Pasted 1 image.' : `Pasted ${pastedFiles.length} images.`,
					);
				} catch (error) {
					const errorDetails = formatPillImageError(error, 'Unable to process pasted images.');
					setImageError(errorDetails);
					message.error(errorDetails.message);
				}
			})();
		};

		window.addEventListener('paste', onPaste);

		return () => {
			window.removeEventListener('paste', onPaste);
		};
	}, [form, isDrawerOpen]);

	function resetPillForm() {
		setImageError(null);
		form.resetFields();
		form.setFieldsValue({
			...createEmptyFormValues(),
			id: undefined,
		});
	}

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
		form.setFieldsValue(pillToFormValues(editedPill));
		setHydratedEditPillId(editPillId);
	}, [
		dashboardQuery.isLoading,
		editPillId,
		editedPill,
		form,
		hydratedEditPillId,
		onEditPillChange,
	]);

	function openNewPillDrawer() {
		setIsCreateDrawerOpen(true);
		setHydratedEditPillId(null);
		setPillQuery('');
		setOpenPeriodTagEditorKey(null);
		onEditPillChange(null);
		resetPillForm();
	}

	const openExistingPillDrawer = useCallback(
		(pill: PillRecord) => {
			setIsCreateDrawerOpen(false);
			setHydratedEditPillId(null);
			setImageError(null);
			setOpenPeriodTagEditorKey(null);
			onEditPillChange(pill.id);
		},
		[onEditPillChange],
	);

	function handleCloseDrawer() {
		setIsCreateDrawerOpen(false);
		setImageError(null);
		setOpenPeriodTagEditorKey(null);
		onEditPillChange(null);
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
			message.success('Pills export copied to clipboard.');
		} catch (error) {
			message.error(error instanceof Error ? error.message : 'Unable to export pills.');
		}
	}

	function handleAutocompleteSelect(_: string, option: { pill?: PillRecord }) {
		if (!option.pill || isEditMode) {
			return;
		}

		form.setFieldsValue(
			pillToFormValues(option.pill, {
				appendNewPeriod: true,
			}),
		);
	}

	async function syncImagesFromUpload(event: UploadChangeParam<UploadFile<any>>) {
		try {
			setImageError(null);
			const nextImages = await Promise.all(
				event.fileList.map(async file => {
					if (typeof file.url === 'string' && !file.originFileObj) {
						return {
							uid: file.uid,
							fileName: file.name,
							dataUrl: file.url,
						} satisfies PillImageFormValue;
					}

					const originalFile = file.originFileObj;
					if (!originalFile) {
						throw new Error(`Unable to process ${file.name}.`);
					}

					return {
						uid: file.uid,
						fileName: originalFile.name,
						dataUrl: await readFileAsDataUrl(originalFile),
						id:
							typeof file.uid === 'string' && file.uid.startsWith('saved-')
								? Number(file.uid.replace('saved-', '')) || undefined
								: undefined,
					} satisfies PillImageFormValue;
				}),
			);

			form.setFieldValue('images', nextImages);
		} catch (error) {
			const errorDetails = formatPillImageError(error, 'Unable to process uploaded images.');
			setImageError(errorDetails);
			message.error(errorDetails.message);
		}
	}

	function handleParseImages() {
		if (watchedImages.length === 0) {
			return;
		}

		setImageError(null);
		extractionMutation.mutate({
			images: getImagePayload(watchedImages),
		});
	}

	const uploadProps: UploadProps = {
		accept: 'image/*',
		beforeUpload: () => false,
		disabled: isParsingImages,
		multiple: true,
		fileList: buildUploadFileList(watchedImages),
		showUploadList: false,
		onChange: info => {
			void syncImagesFromUpload(info);
		},
		onRemove: file => {
			setImageError(null);
			form.setFieldValue('images', removeImageByUid(watchedImages, file.uid));
			return true;
		},
	};

	async function handleSubmit(values: PillFormValues) {
		const isNewPill = values.id == null;
		const submittedPeriods = (
			(form.getFieldValue('periods') ?? values.periods) as PillPeriodFormValue[]
		).map(period => ({
			...period,
			id: isNewPill ? undefined : period.id,
			count: normalizePeriodCount(period.count),
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

	async function handleDeleteSavedPeriod(
		periodId: number,
		fieldIndex: number,
		remove: (index: number | number[]) => void,
	) {
		setOpenPeriodTagEditorKey(null);
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

			remove(fieldIndex);

			message.success(
				result.deletedCount === 1
					? 'Date range deleted.'
					: `${result.deletedCount} date ranges deleted.`,
			);
		} catch {}
	}

	function handlePeriodTagNamesChange(fieldIndex: number, values: string[]) {
		form.setFieldValue(['periods', fieldIndex, 'tagNames'], normalizeTagNames(values));
	}

	function renderPeriodTagEditor(fieldIndex: number) {
		const rowValue = watchedPeriods[fieldIndex];
		const currentTagNames = rowValue?.tagNames ?? [];
		const editorKey = getPeriodTagEditorKey(fieldIndex, rowValue);

		return (
			<Popover
				trigger='click'
				open={openPeriodTagEditorKey === editorKey}
				onOpenChange={open => {
					setOpenPeriodTagEditorKey(open ? editorKey : null);
				}}
				placement='leftTop'
				destroyOnHidden
				content={
					<Space direction='vertical' size={8} style={{ width: 280 }}>
						<Typography.Text strong>Tags</Typography.Text>
						<Select
							mode='tags'
							value={currentTagNames}
							options={tagAutocompleteOptions}
							placeholder='Type to attach or create tags'
							style={{ width: '100%' }}
							getPopupContainer={trigger => trigger.parentElement ?? document.body}
							onChange={values => {
								handlePeriodTagNamesChange(fieldIndex, values);
							}}
							tokenSeparators={[',']}
						/>
						<div>
							{currentTagNames.length > 0 ? (
								<Space size={[4, 4]} wrap>
									{currentTagNames.map(tagName => {
										const tagRecord = availableTags.find(tag => tag.name === tagName);

										return (
											<Tag key={tagName} color={tagRecord?.color}>
												{tagName}
											</Tag>
										);
									})}
								</Space>
							) : (
								<Typography.Text type='secondary'>
									No tags attached to this date range yet.
								</Typography.Text>
							)}
						</div>
					</Space>
				}
			>
				<Badge count={currentTagNames.length} size='small' offset={[-2, 2]}>
					<Button
						icon={<TagOutlined />}
						aria-label='Edit date range tags'
						title={formatPeriodTagButtonTitle(currentTagNames)}
					/>
				</Badge>
			</Popover>
		);
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
			<Space size={[4, 4]} wrap align='center'>
				<Button
					type='link'
					size='small'
					className='pills-link-button'
					icon={<EditOutlined />}
					onClick={() => openExistingPillDrawer(pill)}
				>
					{pill.name}
				</Button>

				{tagNames.map(tagName => {
					const tagRecord = availableTags.find(tag => tag.name === tagName);

					return (
						<Tag
							key={`${pill.id}-${tagName}`}
							color={tagRecord?.color}
							style={{ marginInlineEnd: 0 }}
						>
							{tagName}
						</Tag>
					);
				})}
			</Space>
		);
	}

	const activeColumns: TableColumnsType<ActivePillRow> = useMemo(
		() => [
			{
				title: 'Pill',
				key: 'name',
				width: 360,
				render: (_: unknown, row: ActivePillRow) =>
					renderPillNameCell(
						row.pill,
						row.activePeriod.tags.map(tag => tag.name),
					),
			},
			{
				title: 'Amount',
				key: 'amount',
				render: (_: unknown, row: ActivePillRow) => (
					<Typography.Text>
						{formatServing(
							multiplyServingValue(row.pill.value, row.activePeriod.count),
							row.pill.unit,
						)}
					</Typography.Text>
				),
			},
			{
				title: 'Timing',
				key: 'timing',
				render: (_: unknown, row: ActivePillRow) => (
					<Typography.Text>{formatTiming(row.activePeriod.timing)}</Typography.Text>
				),
			},
			{
				title: 'Started',
				key: 'started',
				render: (_: unknown, row: ActivePillRow) => (
					<Typography.Text>{formatRelativeDate(row.activePeriod.startDate)}</Typography.Text>
				),
			},
			{
				title: 'Components',
				key: 'components',
				render: (_: unknown, row: ActivePillRow) =>
					renderComponents(row.pill.components, row.activePeriod.count),
				onCell: row =>
					row.pill.components.length > 0
						? {
								onClick: () => {
									toggleExpandedComponentsRow(String(row.pill.id));
								},
								style: { cursor: 'pointer' },
							}
						: {},
			},
			{
				title: 'Images',
				key: 'images',
				render: (_: unknown, row: ActivePillRow) => renderImages(row.pill.images),
			},
		],
		[availableTags, openExistingPillDrawer, toggleExpandedComponentsRow],
	);

	const futureColumns: TableColumnsType<FuturePillRow> = useMemo(
		() => [
			{
				title: 'Pill',
				key: 'name',
				width: 360,
				render: (_: unknown, row: FuturePillRow) =>
					renderPillNameCell(
						row.pill,
						row.futurePeriod.tags.map(tag => tag.name),
					),
			},
			{
				title: 'Amount',
				key: 'amount',
				render: (_: unknown, row: FuturePillRow) => (
					<Typography.Text>
						{formatServing(
							multiplyServingValue(row.pill.value, row.futurePeriod.count),
							row.pill.unit,
						)}
					</Typography.Text>
				),
			},
			{
				title: 'Timing',
				key: 'timing',
				render: (_: unknown, row: FuturePillRow) => (
					<Typography.Text>{formatTiming(row.futurePeriod.timing)}</Typography.Text>
				),
			},
			{
				title: 'Starts',
				key: 'starts',
				render: (_: unknown, row: FuturePillRow) => (
					<Typography.Text>{formatRelativeDate(row.futurePeriod.startDate)}</Typography.Text>
				),
			},
			{
				title: 'Components',
				key: 'components',
				render: (_: unknown, row: FuturePillRow) =>
					renderComponents(row.pill.components, row.futurePeriod.count),
				onCell: row =>
					row.pill.components.length > 0
						? {
								onClick: () => {
									toggleExpandedComponentsRow(`future-${row.pill.id}`);
								},
								style: { cursor: 'pointer' },
							}
						: {},
			},
			{
				title: 'Images',
				key: 'images',
				render: (_: unknown, row: FuturePillRow) => renderImages(row.pill.images),
			},
		],
		[availableTags, openExistingPillDrawer, toggleExpandedComponentsRow],
	);

	const pastColumns: TableColumnsType<PastPillRow> = useMemo(
		() => [
			{
				title: 'Pill',
				key: 'name',
				width: 360,
				render: (_: unknown, row: PastPillRow) =>
					renderPillNameCell(
						row.pill,
						row.period.tags.map(tag => tag.name),
					),
			},
			{
				title: 'Amount',
				key: 'amount',
				render: (_: unknown, row: PastPillRow) => (
					<Typography.Text>
						{formatServing(multiplyServingValue(row.pill.value, row.period.count), row.pill.unit)}
					</Typography.Text>
				),
			},
			{
				title: 'Timing',
				key: 'timing',
				render: (_: unknown, row: PastPillRow) => (
					<Typography.Text>{formatTiming(row.period.timing)}</Typography.Text>
				),
			},
			{
				title: 'Period',
				key: 'period',
				render: (_: unknown, row: PastPillRow) => (
					<Typography.Text>{formatPeriodRange(row.period)}</Typography.Text>
				),
			},
			{
				title: 'Components',
				key: 'components',
				render: (_: unknown, row: PastPillRow) =>
					renderComponents(row.pill.components, row.period.count),
				onCell: row =>
					row.pill.components.length > 0
						? {
								onClick: () => {
									toggleExpandedComponentsRow(row.key);
								},
								style: { cursor: 'pointer' },
							}
						: {},
			},
			{
				title: 'Images',
				key: 'images',
				render: (_: unknown, row: PastPillRow) => renderImages(row.pill.images),
			},
		],
		[availableTags, openExistingPillDrawer, toggleExpandedComponentsRow],
	);

	const notTrackedColumns: TableColumnsType<NotTrackedPillRow> = useMemo(
		() => [
			{
				title: 'Pill',
				key: 'name',
				width: 360,
				render: (_: unknown, row: NotTrackedPillRow) => renderPillNameCell(row.pill, []),
			},
			{
				title: 'Amount',
				key: 'amount',
				render: (_: unknown, row: NotTrackedPillRow) => (
					<Typography.Text>{formatServing(row.pill.value, row.pill.unit)}</Typography.Text>
				),
			},
			{
				title: 'Timing',
				key: 'timing',
				render: () => <Typography.Text />,
			},
			{
				title: 'Components',
				key: 'components',
				render: (_: unknown, row: NotTrackedPillRow) => renderComponents(row.pill.components),
				onCell: row =>
					row.pill.components.length > 0
						? {
								onClick: () => {
									toggleExpandedComponentsRow(String(row.pill.id));
								},
								style: { cursor: 'pointer' },
							}
						: {},
			},
			{
				title: 'Images',
				key: 'images',
				render: (_: unknown, row: NotTrackedPillRow) => renderImages(row.pill.images),
			},
		],
		[availableTags, openExistingPillDrawer, toggleExpandedComponentsRow],
	);

	const onExpandedRowsChange = useCallback((keys: readonly Key[]) => {
		setExpandedComponentRowKeys(keys.map(key => String(key)));
	}, []);

	const activeTableExpandable = useMemo(
		() => ({
			expandedRowKeys: expandedComponentRowKeys,
			expandedRowRender: (row: ActivePillRow) =>
				renderExpandedComponents(row.pill.components, row.activePeriod.count),
			onExpandedRowsChange,
			rowExpandable: (row: ActivePillRow) => row.pill.components.length > 0,
			showExpandColumn: false,
		}),
		[expandedComponentRowKeys, onExpandedRowsChange],
	);

	const futureTableExpandable = useMemo(
		() => ({
			expandedRowKeys: expandedComponentRowKeys,
			expandedRowRender: (row: FuturePillRow) =>
				renderExpandedComponents(row.pill.components, row.futurePeriod.count),
			onExpandedRowsChange,
			rowExpandable: (row: FuturePillRow) => row.pill.components.length > 0,
			showExpandColumn: false,
		}),
		[expandedComponentRowKeys, onExpandedRowsChange],
	);

	const pastTableExpandable = useMemo(
		() => ({
			expandedRowKeys: expandedComponentRowKeys,
			expandedRowRender: (row: PastPillRow) =>
				renderExpandedComponents(row.pill.components, row.period.count),
			onExpandedRowsChange,
			rowExpandable: (row: PastPillRow) => row.pill.components.length > 0,
			showExpandColumn: false,
		}),
		[expandedComponentRowKeys, onExpandedRowsChange],
	);

	const notTrackedTableExpandable = useMemo(
		() => ({
			expandedRowKeys: expandedComponentRowKeys,
			expandedRowRender: (row: NotTrackedPillRow) => renderExpandedComponents(row.pill.components),
			onExpandedRowsChange,
			rowExpandable: (row: NotTrackedPillRow) => row.pill.components.length > 0,
			showExpandColumn: false,
		}),
		[expandedComponentRowKeys, onExpandedRowsChange],
	);

	return (
		<main className='pills-page' style={{ background: token.colorBgLayout }}>
			<PageNav
				title='Pills'
				actions={
					<Space>
						<Button
							size='large'
							icon={<CopyOutlined />}
							onClick={() => {
								void handleExport();
							}}
							disabled={dashboardQuery.isLoading}
						>
							Export
						</Button>
						<Button type='primary' size='large' icon={<PlusOutlined />} onClick={openNewPillDrawer}>
							Log pill
						</Button>
					</Space>
				}
			/>

			<div className='pills-page-inner'>
				{futureRows.length > 0 ? (
					<Card
						title='Future pills'
						extra={<Typography.Text type='secondary'>{futureRows.length} rows</Typography.Text>}
					>
						<Table
							rowKey={row => String(row.pill.id)}
							size='small'
							columns={futureColumns}
							dataSource={futureRows}
							loading={dashboardQuery.isLoading}
							pagination={false}
							expandable={futureTableExpandable}
						/>
					</Card>
				) : null}

				{activeRows.length > 0 ? (
					<Card
						title='Active pills'
						extra={<Typography.Text type='secondary'>{activePills.length} rows</Typography.Text>}
					>
						<Table
							rowKey={row => String(row.pill.id)}
							size='small'
							columns={activeColumns}
							dataSource={activeRows}
							loading={dashboardQuery.isLoading}
							pagination={false}
							expandable={activeTableExpandable}
						/>
					</Card>
				) : null}

				{notTrackedRows.length > 0 ? (
					<Card
						title='Not tracked yet'
						extra={<Typography.Text type='secondary'>{notTrackedRows.length} rows</Typography.Text>}
					>
						<Table
							rowKey={row => String(row.pill.id)}
							size='small'
							columns={notTrackedColumns}
							dataSource={notTrackedRows}
							loading={dashboardQuery.isLoading}
							pagination={false}
							expandable={notTrackedTableExpandable}
						/>
					</Card>
				) : null}

				{pastRows.length > 0 ? (
					<Card
						title='Past pills'
						extra={<Typography.Text type='secondary'>{pastRows.length} rows</Typography.Text>}
					>
						<Table
							rowKey='key'
							size='small'
							columns={pastColumns}
							dataSource={pastRows}
							loading={dashboardQuery.isLoading}
							pagination={false}
							expandable={pastTableExpandable}
						/>
					</Card>
				) : null}
			</div>

			<Drawer
				title={isEditMode ? 'Edit pill' : 'Log pill'}
				placement='right'
				width={920}
				open={isDrawerOpen}
				onClose={handleCloseDrawer}
				afterOpenChange={open => {
					if (open) {
						return;
					}

					setPillQuery('');
					setHydratedEditPillId(null);
					setOpenPeriodTagEditorKey(null);
					resetPillForm();
				}}
				destroyOnHidden={false}
				styles={{
					body: {
						padding: 16,
					},
				}}
				extra={
					<Space>
						{isEditMode && editPillId !== null && (
							<Popconfirm
								title='Remove this pill?'
								description='This removes the pill and all its date ranges, components, and images.'
								okText='Remove'
								okButtonProps={{ danger: true }}
								onConfirm={() =>
									deletePillMutation.mutate({
										where: [{ column: 'id', operator: 'eq', value: editPillId }],
									})
								}
							>
								<Button danger loading={deletePillMutation.isPending}>
									Remove
								</Button>
							</Popconfirm>
						)}
						<Button onClick={handleCloseDrawer}>Cancel</Button>
						<Button
							type='primary'
							loading={upsertMutation.isPending}
							disabled={isSaveDisabled}
							onClick={() => void form.submit()}
						>
							Save
						</Button>
					</Space>
				}
			>
				<Form<PillFormValues>
					form={form}
					layout='vertical'
					initialValues={createEmptyFormValues()}
					onFinish={values => {
						void handleSubmit(values);
					}}
				>
					<Form.Item name='id' hidden>
						<Input />
					</Form.Item>

					<div className='pills-primary-fields'>
						<Form.Item
							label='Pill name'
							name='name'
							rules={[{ required: true, message: 'Enter a pill name.' }]}
							style={{ marginBottom: 8 }}
						>
							<AutoComplete
								options={autocompleteOptions}
								onSearch={value => setPillQuery(value)}
								onSelect={handleAutocompleteSelect}
								onChange={value => {
									setPillQuery(value);

									if (value !== form.getFieldValue('name')) {
										form.setFieldValue('name', value);
									}

									if (!isEditMode) {
										const currentId = form.getFieldValue('id');
										const selectedPill = searchResults.find(result => result.id === currentId);
										if (selectedPill && selectedPill.name !== value) {
											form.setFieldValue('id', undefined);
										}
									}
								}}
								placeholder='Start typing to reuse a canonical pill'
								filterOption={false}
							/>
						</Form.Item>

						<Form.Item
							label='Value'
							name='value'
							rules={[{ required: true, message: 'Enter the default pill value.' }]}
							style={{ marginBottom: 8 }}
						>
							<Input placeholder='e.g. 2' />
						</Form.Item>

						<Form.Item
							label='Unit'
							name='unit'
							rules={[{ required: true, message: 'Enter the default pill unit.' }]}
							style={{ marginBottom: 8 }}
						>
							<Input placeholder='e.g. capsules' />
						</Form.Item>
					</div>

					<Form.Item
						label='URL'
						name='url'
						layout='horizontal'
						labelCol={{ flex: '50px' }}
						wrapperCol={{ flex: 'auto' }}
						labelAlign='left'
						style={{ marginBottom: 8 }}
					>
						<Input placeholder='Optional product URL' />
					</Form.Item>

					<Form.Item
						label='Tags'
						name='tagNames'
						layout='horizontal'
						labelCol={{ flex: '50px' }}
						wrapperCol={{ flex: 'auto' }}
						labelAlign='left'
						style={{ marginBottom: 8 }}
					>
						<Select
							mode='tags'
							options={tagAutocompleteOptions}
							placeholder='Type to attach or create tags'
							tokenSeparators={[',']}
							tagRender={props => {
								const tagRecord = availableTags.find(tag => tag.name === props.value);
								return (
									<Tag
										color={tagRecord?.color}
										closable={props.closable}
										onClose={props.onClose}
										style={{ marginInlineEnd: 4 }}
									>
										{props.label}
									</Tag>
								);
							}}
						/>
					</Form.Item>

					<Form.Item
						label='Note'
						name='note'
						layout='horizontal'
						labelCol={{ flex: '50px' }}
						wrapperCol={{ flex: 'auto' }}
						labelAlign='left'
						style={{ marginBottom: 8 }}
					>
						<Input.TextArea autoSize={{ minRows: 1, maxRows: 6 }} placeholder='Optional note' />
					</Form.Item>

					<div className='pills-images-header'>
						<Divider className='pills-images-divider'>Images</Divider>

						{watchedImages.length > 0 ? (
							<Button
								size='small'
								loading={isParsingImages}
								disabled={isParsingImages}
								onClick={handleParseImages}
							>
								Parse images
							</Button>
						) : null}
					</div>

					<Form.Item name='images' hidden>
						<Input />
					</Form.Item>

					<Image.PreviewGroup>
						<div className='pills-images-grid'>
							{watchedImages.map(image => (
								<div
									key={image.uid}
									className='pills-image-tile'
									style={{
										border: '1px solid var(--ant-color-border-secondary)',
										background: 'var(--ant-color-bg-container)',
									}}
								>
									<Image
										src={image.dataUrl}
										alt={image.fileName}
										width={IMAGE_TILE_SIZE}
										height={IMAGE_TILE_SIZE}
										style={{ objectFit: 'cover' }}
									/>
									<Button
										size='small'
										danger
										icon={<DeleteOutlined />}
										onClick={() => {
											setImageError(null);
											form.setFieldValue('images', removeImageByUid(watchedImages, image.uid));
										}}
										className='pills-image-remove'
									/>
								</div>
							))}

							<Dragger
								{...uploadProps}
								className='pills-upload-tile'
								style={{
									width: IMAGE_TILE_SIZE,
									height: IMAGE_TILE_SIZE,
								}}
							>
								<div className='pills-upload-inner'>
									<UploadOutlined style={{ fontSize: 20 }} />
									<Typography.Text className='pills-upload-label' type='secondary'>
										Drop or upload
									</Typography.Text>
								</div>
							</Dragger>
						</div>
					</Image.PreviewGroup>

					{isParsingImages ? (
						<div
							className='pills-parsing-banner'
							style={{
								borderColor: token.colorInfoBorder,
								background: token.colorInfoBg,
								color: token.colorInfoText,
							}}
						>
							<Spin size='small' />
							<Typography.Text className='pills-parsing-banner-text'>
								Parsing uploaded images and filling the form…
							</Typography.Text>
						</div>
					) : null}
					{imageError ? (
						<Alert
							type='error'
							showIcon
							message={imageError.message}
							description={<pre className='pills-image-error-details'>{imageError.details}</pre>}
							className='pills-image-error'
						/>
					) : null}

					<Divider>Date Ranges</Divider>

					<Form.List name='periods'>
						{(fields, { add, remove }) => (
							<Space direction='vertical' size={8} className='pills-list-space'>
								{fields.length > 0 && (
									<Table
										size='small'
										pagination={false}
										rowKey='key'
										dataSource={fields}
										scroll={{ x: 860 }}
										columns={[
											{
												title: 'Period',
												width: 290,
												render: (_: unknown, field) => (
													<div style={{ width: '100%' }}>
														<Form.Item name={[field.name, 'id']} hidden>
															<Input />
														</Form.Item>
														<Form.Item
															name={[field.name, 'startDate']}
															rules={[{ required: true, message: 'Required' }]}
															hidden
															style={{ marginBottom: 0 }}
														>
															<Input />
														</Form.Item>
														<Form.Item
															name={[field.name, 'endDate']}
															hidden
															style={{ marginBottom: 0 }}
														>
															<Input />
														</Form.Item>
														<RangePicker
															style={{ width: '100%' }}
															format={DATE_FORMAT}
															allowEmpty={[false, true]}
															value={getPeriodRangePickerValue(watchedPeriods[field.name])}
															onChange={(_, dateStrings) => {
																const [startDate, endDate] = dateStrings;
																form.setFieldValue(
																	['periods', field.name, 'startDate'],
																	startDate ?? '',
																);
																form.setFieldValue(
																	['periods', field.name, 'endDate'],
																	endDate ?? '',
																);
															}}
														/>
													</div>
												),
											},
											{
												title: 'Count',
												width: 120,
												render: (_: unknown, field) => (
													<Form.Item
														name={[field.name, 'count']}
														rules={[{ required: true, message: 'Required' }]}
														style={{ marginBottom: 0 }}
													>
														<InputNumber
															min={0.01}
															step={0.5}
															placeholder='Count'
															style={{ width: '100%' }}
														/>
													</Form.Item>
												),
											},
											{
												title: 'Timing',
												width: 140,
												render: (_: unknown, field) => (
													<Form.Item name={[field.name, 'timing']} style={{ marginBottom: 0 }}>
														<Select
															allowClear
															placeholder='Optional'
															options={
																timingOptions as unknown as { label: string; value: string }[]
															}
														/>
													</Form.Item>
												),
											},
											{
												width: 170,
												render: (_: unknown, field) => {
													const rowValue = watchedPeriods[field.name] as
														| PillPeriodFormValue
														| undefined;
													const isSavedRow = Boolean(rowValue?.id);

													return (
														<Space size='small' align='center'>
															<Form.Item
																name={[field.name, 'tagNames']}
																hidden
																style={{ marginBottom: 0 }}
																getValueProps={value => ({
																	value: value ?? [],
																})}
															>
																<Select mode='multiple' />
															</Form.Item>

															{renderPeriodTagEditor(field.name)}

															<Button
																danger
																icon={<DeleteOutlined />}
																loading={isSavedRow && deletingPeriodId === rowValue?.id}
																onClick={() => {
																	if (isSavedRow && rowValue?.id) {
																		void handleDeleteSavedPeriod(rowValue.id, field.name, remove);
																		return;
																	}

																	remove(field.name);
																	setOpenPeriodTagEditorKey(null);
																}}
															/>
														</Space>
													);
												},
											},
										]}
									/>
								)}

								<Button
									size='small'
									icon={<PlusOutlined />}
									onClick={() => add(createBlankPeriod())}
								>
									Add {fields.length > 0 ? 'another' : 'a'} range
								</Button>
							</Space>
						)}
					</Form.List>

					<Divider>
						{formatSupplementFactsTitle(watchedFormValues?.value, watchedFormValues?.unit)}
					</Divider>

					<Form.List name='components'>
						{(fields, { add, remove }) => (
							<Space direction='vertical' size={8} className='pills-list-space'>
								<Table
									size='small'
									pagination={false}
									rowKey='key'
									dataSource={fields}
									scroll={{ x: 760 }}
									columns={[
										{
											title: 'Name',
											render: (_: unknown, field) => (
												<Form.Item name={[field.name, 'name']} style={{ marginBottom: 0 }}>
													<Input placeholder='Vitamin D3' />
												</Form.Item>
											),
										},
										{
											title: 'Value',
											width: 130,
											render: (_: unknown, field) => (
												<Form.Item name={[field.name, 'value']} style={{ marginBottom: 0 }}>
													<Input placeholder='125' />
												</Form.Item>
											),
										},
										{
											title: 'Unit',
											width: 140,
											render: (_: unknown, field) => (
												<Form.Item name={[field.name, 'unit']} style={{ marginBottom: 0 }}>
													<Input placeholder='mcg' />
												</Form.Item>
											),
										},
										{
											title: 'Action',
											width: 124,
											render: (_: unknown, field) => (
												<Space size='small'>
													<Button
														size='small'
														icon={<ArrowUpOutlined />}
														onClick={() => add(getComponentInsertValue(), field.name)}
														aria-label='Insert component before'
														title='Insert component before'
													/>
													<Button
														size='small'
														icon={<ArrowDownOutlined />}
														onClick={() => add(getComponentInsertValue(), field.name + 1)}
														aria-label='Insert component after'
														title='Insert component after'
													/>
													<Button
														size='small'
														danger
														icon={<DeleteOutlined />}
														onClick={() => remove(field.name)}
														aria-label='Delete component'
														title='Delete component'
													/>
												</Space>
											),
										},
									]}
								/>

								<Button
									size='small'
									icon={<PlusOutlined />}
									onClick={() => add(getComponentInsertValue())}
								>
									Add component
								</Button>
							</Space>
						)}
					</Form.List>
				</Form>
			</Drawer>
		</main>
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

function getPeriodTagEditorKey(fieldIndex: number, period: PillPeriodFormValue | undefined) {
	return period?.id ? `period-${period.id}` : `draft-${fieldIndex}`;
}

function getPeriodRangePickerValue(period: PillPeriodFormValue | undefined) {
	return [
		period?.startDate ? dayjs(period.startDate, DATE_FORMAT) : null,
		period?.endDate ? dayjs(period.endDate, DATE_FORMAT) : null,
	] as [dayjs.Dayjs | null, dayjs.Dayjs | null];
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

function formatPeriodTagButtonTitle(tagNames: string[]) {
	if (tagNames.length === 0) {
		return 'Edit date range tags';
	}

	return `Tags: ${tagNames.join(', ')}`;
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
			: `${formatNumericValue(normalizedCount)} × ${trimmedValue}`;
	}

	return formatNumericValue(numericValue * normalizedCount);
}

function formatTiming(timing?: PillTiming | null) {
	if (!timing) {
		return '';
	}

	return timing.charAt(0).toUpperCase() + timing.slice(1);
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

function buildUploadFileList(images: PillImageFormValue[]): UploadFile[] {
	return images.map(image => ({
		uid: image.uid,
		name: image.fileName,
		status: 'done',
		url: image.dataUrl,
	}));
}

function getImagePayload(images: PillImageFormValue[]) {
	return images.map(image => ({
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

async function hasFormErrors(form: FormInstance<PillFormValues>) {
	try {
		await form.validateFields({
			validateOnly: true,
			recursive: true,
		});
		return false;
	} catch {
		return true;
	}
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
		return <Typography.Text type='secondary'>No components</Typography.Text>;
	}

	const [firstComponent, ...remainingComponents] = components;

	return (
		<Space direction='vertical' size={0}>
			<Typography.Text>
				{firstComponent.name}:{' '}
				{formatServing(multiplyServingValue(firstComponent.value, count), firstComponent.unit)}
			</Typography.Text>
			{remainingComponents.length > 0 ? (
				<Typography.Text type='secondary'>+{remainingComponents.length} more</Typography.Text>
			) : null}
		</Space>
	);
}

function renderExpandedComponents(components: PillComponent[], count = 1) {
	return (
		<Table
			size='small'
			pagination={false}
			rowKey='id'
			dataSource={components}
			columns={[
				{
					title: 'Name',
					dataIndex: 'name',
					key: 'name',
				},
				{
					title: 'Value',
					key: 'value',
					width: 180,
					render: (_: unknown, component: PillComponent) =>
						formatServing(multiplyServingValue(component.value, count), component.unit),
				},
			]}
		/>
	);
}

function renderImages(images: PillImage[]) {
	if (images.length === 0) {
		return <Typography.Text type='secondary'>No images</Typography.Text>;
	}

	return (
		<Image.PreviewGroup>
			<div className='pills-image-list'>
				{images.map(image => (
					<Image
						key={image.id}
						src={image.dataUrl}
						alt={image.fileName}
						width={44}
						height={44}
						className='pills-image-preview'
					/>
				))}
			</div>
		</Image.PreviewGroup>
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
	}>,
) {
	return Object.fromEntries(
		rows.map(({ pill, count, tags, startDate, endDate, timing }) => [
			`${pill.name} - ${formatServing(multiplyServingValue(pill.value, count), pill.unit)}`,
			buildPillExportRow({
				pill,
				count,
				tags,
				startDate,
				endDate,
				timing,
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
	if (args.timing) {
		exportRow.timing = args.timing;
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
