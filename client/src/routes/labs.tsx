import { ChartLineUp, UploadSimple, WarningCircle } from '@phosphor-icons/react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import {
	Alert,
	Button,
	Card,
	Checkbox,
	Drawer,
	Empty,
	Flex,
	Popconfirm,
	Splitter,
	Spin,
	Tag,
	Tabs,
	Typography,
	message,
	theme as antdTheme,
} from 'antd';
import {
	type ChangeEvent,
	useCallback,
	useDeferredValue,
	useEffect,
	useMemo,
	useRef,
	useState,
} from 'react';
import { z } from 'zod';

import {
	CategoriesOverview,
	MeaningfulChanges,
	TrendChart,
	VitalsControls,
	VitalsTable,
} from './labs/_components';
import {
	getAllMeasurementRows,
	getCategoryOverviewByLatestAcrossAllLabs,
	getCategorySelectionByName,
	getChartSeries,
	getChartSources,
	getDateBounds,
	getFilteredMeasurementRows,
	getMeasurementKeysByCategory,
	getMeasurementOverviewByKey,
	getOrderedDocuments,
	getSourceCountsBySourceId,
	getPrunedSelectedRowKeys,
	getRowsMatchingSourceFilters,
	getRowsWithVisibleData,
	getSelectedRows,
	getSixMonthMeaningfulChanges,
	getSources,
	getTableRows,
	getTableSources,
	getVisibleSources,
	MEASUREMENT_COLUMN_WIDTH,
	MIN_CHART_PANE_WIDTH,
	OVERVIEW_COLUMN_WIDTH,
	SELECTION_COLUMN_WIDTH,
	SOURCE_COLUMN_WIDTH,
	clamp,
	isCellOutsideReferenceRange,
	formatCell,
	type VitalsRowModel,
	type SourceFilter,
	type SourceFilterMode,
} from './labs/_labs';
import type { LabImportDocument } from '../utils/api';
import { withAuthToken } from '../utils/auth';
import createLocalStorage from '../utils/createLocalStorage';
import { useTRPC } from '../utils/trpc';

const { useLocalStorage } = createLocalStorage({
	namespace: 'vitals.labs',
	getDefaults: () => ({
		starredMeasurements: [] as string[],
		selectedRows: [] as string[],
		groupByCategory: true,
		sourceFilters: [] as SourceFilter[],
	}),
});

const labsSearchSchema = z.object({
	tab: z.enum(['overview', 'documents']).optional(),
	doc: z.coerce.number().int().positive().optional(),
	m: z.string().optional(),
});

export const Route = createFileRoute('/labs')({
	validateSearch: search => labsSearchSchema.parse(search),
	component: LabsPage,
});

const BLOODWORK_IMPORT_POLL_INTERVAL_MS = 3_000;
const PREVIEW_DRAWER_CONTENT_HEIGHT = 'calc(100vh - 108px)';
const PREVIEW_RESULTS_PANEL_WIDTH = 360;
const NORMAL_PREVIEW_FLAG_NOTES = new Set([
	'n',
	'normal',
	'none',
	'ok',
	'n/a',
	'na',
	'in range',
	'within range',
]);

function LabsPage() {
	const search = Route.useSearch();
	const navigate = Route.useNavigate();
	const viewport = useViewport();
	const isMobileViewport = viewport.width < 900;
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const { token } = antdTheme.useToken();
	const [messageApi, messageContextHolder] = message.useMessage();
	const importInputRef = useRef<HTMLInputElement | null>(null);

	const [measurementFilter, setMeasurementFilter] = useState('');
	const [deletingDocumentId, setDeletingDocumentId] = useState<number | null>(null);
	const [selectedImportDocumentIds, setSelectedImportDocumentIds] = useState<number[]>([]);
	const [selectedRowKeys, setSelectedRowKeys] = useLocalStorage('selectedRows');
	const [starredMeasurementKeys, setStarredMeasurementKeys] =
		useLocalStorage('starredMeasurements');
	const [rawDateRangeStart, setDateRangeStart] = useState('');
	const [rawDateRangeEnd, setDateRangeEnd] = useState('');
	const [showFlaggedPreviewRowsOnly, setShowFlaggedPreviewRowsOnly] = useState(false);
	const [groupByCategory, setGroupByCategory] = useLocalStorage('groupByCategory');
	const [sourceFilters, setSourceFilters] = useLocalStorage('sourceFilters');
	const activeTab = search.tab ?? 'overview';
	const previewDocumentId = search.doc ?? null;
	const previewMeasurementKey = search.m ?? null;

	const deferredMeasurementFilter = useDeferredValue(measurementFilter);

	const documentsQuery = useQuery({
		...trpc.labs.listDocuments.queryOptions(),
		refetchInterval: query =>
			hasActiveLabsImports((query.state.data ?? []) as LabImportDocument[])
				? BLOODWORK_IMPORT_POLL_INTERVAL_MS
				: false,
	});
	const importDocuments = documentsQuery.data ?? [];
	const hasActiveImportDocuments = hasActiveLabsImports(importDocuments);
	const dashboardQuery = useQuery({
		...trpc.labs.getDashboard.queryOptions(),
		refetchInterval: hasActiveImportDocuments ? BLOODWORK_IMPORT_POLL_INTERVAL_MS : false,
	});
	const uploadDocumentsMutation = useMutation({
		...trpc.labs.uploadDocuments.mutationOptions(),
		onSuccess: async data => {
			await queryClient.invalidateQueries({ queryKey: [['labs']] });
			const queuedCount = data.documents.filter(document => !document.deduplicated).length;
			const deduplicatedCount = data.documents.length - queuedCount;
			const parts = [`Queued ${queuedCount} PDF${queuedCount === 1 ? '' : 's'}.`];
			if (deduplicatedCount > 0) {
				parts.push(`${deduplicatedCount} duplicate${deduplicatedCount === 1 ? '' : 's'} skipped.`);
			}
			messageApi.success(parts.join(' '));
		},
		onError: error => {
			messageApi.error(error.message);
		},
	});
	const deleteDocumentMutation = useMutation({
		...trpc.table.labDocuments.deleteMany.mutationOptions(),
		onSuccess: async data => {
			await queryClient.invalidateQueries({ queryKey: [['labs']] });
			messageApi.success(
				data.deletedCount === 1 ? 'Document deleted.' : `${data.deletedCount} documents deleted.`,
			);
		},
		onError: error => {
			messageApi.error(error.message);
		},
		onSettled: () => {
			setDeletingDocumentId(null);
		},
	});
	const updateDocumentsMutation = useMutation({
		...trpc.table.labDocuments.updateMany.mutationOptions(),
		onSuccess: async data => {
			await queryClient.invalidateQueries({ queryKey: [['labs']] });
			messageApi.success(
				data.updatedCount === 1 ? '1 document updated.' : `${data.updatedCount} documents updated.`,
			);
			setSelectedImportDocumentIds([]);
		},
		onError: error => {
			messageApi.error(error.message);
		},
	});
	const retryDocumentMutation = useMutation({
		...trpc.labs.retryDocument.mutationOptions(),
		onSuccess: async () => {
			await queryClient.invalidateQueries({ queryKey: [['labs']] });
			messageApi.success('Document queued for retry.');
		},
		onError: error => {
			messageApi.error(error.message);
		},
	});
	const reprocessDocumentMutation = useMutation({
		...trpc.labs.reprocessDocument.mutationOptions(),
		onSuccess: async () => {
			await queryClient.invalidateQueries({ queryKey: [['labs']] });
			messageApi.success('Document queued for reprocess.');
		},
		onError: error => {
			messageApi.error(error.message);
		},
	});
	const dashboard = dashboardQuery.data;
	const documents = dashboard?.documents ?? [];
	const measurements = dashboard?.measurements ?? [];
	const results = dashboard?.results ?? [];
	const previousHasActiveImportDocumentsRef = useRef(hasActiveImportDocuments);
	const previewDocument = useMemo(
		() => importDocuments.find(document => document.id === previewDocumentId) ?? null,
		[importDocuments, previewDocumentId],
	);
	const previewMeasurementById = useMemo(
		() => new Map(measurements.map(measurement => [measurement.id, measurement])),
		[measurements],
	);
	const previewRows = useMemo(() => {
		if (previewDocumentId === null) {
			return [];
		}

		return results
			.filter(result => result.documentId === previewDocumentId)
			.map(result => {
				const measurement = previewMeasurementById.get(result.measurementId);
				const valueText = [result.valueText?.trim() ?? '', result.unit?.trim() ?? '']
					.filter(Boolean)
					.join(' ')
					.trim();
				const cell = formatCell({
					key: measurement?.key ?? '',
					name: measurement?.name ?? result.originalName ?? 'Unknown measurement',
					category: measurement?.category ?? null,
					documentId: result.documentId,
					valueText: result.valueText,
					valueNumeric: result.valueNumeric,
					unit: result.unit,
					referenceRangeMin: result.originalRangeMin ?? measurement?.rangeMin ?? null,
					referenceRangeMax: result.originalRangeMax ?? measurement?.rangeMax ?? null,
					flag: null,
					note: result.note,
				});
				const note = result.note?.trim() || null;
				const isOutsideRange = isCellOutsideReferenceRange(cell);
				const issueLabel = getPreviewIssueLabel(note, isOutsideRange);

				return {
					id: result.id,
					key: measurement?.key ?? '',
					name: measurement?.name ?? result.originalName ?? 'Unknown measurement',
					valueText: valueText || '—',
					rangeText: result.originalRangeText?.trim() || measurement?.range?.trim() || null,
					note: getPreviewDisplayNote(note),
					hasIssue: issueLabel !== null,
					issueLabel,
				};
			});
	}, [previewDocumentId, previewMeasurementById, results]);
	const flaggedPreviewRowCount = useMemo(
		() => previewRows.filter(row => row.hasIssue).length,
		[previewRows],
	);
	const visiblePreviewRows = useMemo(
		() => (showFlaggedPreviewRowsOnly ? previewRows.filter(row => row.hasIssue) : previewRows),
		[previewRows, showFlaggedPreviewRowsOnly],
	);

	useEffect(() => {
		const previousHadActiveImports = previousHasActiveImportDocumentsRef.current;
		previousHasActiveImportDocumentsRef.current = hasActiveImportDocuments;

		if (previousHadActiveImports && !hasActiveImportDocuments) {
			void dashboardQuery.refetch();
		}
	}, [dashboardQuery.refetch, hasActiveImportDocuments]);

	const orderedDocuments = useMemo(() => getOrderedDocuments(documents), [documents]);
	const sources = useMemo(() => getSources(orderedDocuments), [orderedDocuments]);
	const availableDates = useMemo(
		() =>
			Array.from(new Set(sources.map(source => source.date))).sort((left, right) =>
				left.localeCompare(right),
			),
		[sources],
	);
	const dateBounds = useMemo(() => getDateBounds(sources), [sources]);

	const { dateRangeStart, dateRangeEnd } = useMemo(() => {
		if (availableDates.length === 0) {
			return { dateRangeStart: '', dateRangeEnd: '' };
		}

		const clampToAvailable = (raw: string, fallback: string) => {
			if (!raw || !dateBounds.min || !dateBounds.max) return fallback;
			const clamped =
				raw < dateBounds.min ? dateBounds.min : raw > dateBounds.max ? dateBounds.max : raw;
			return availableDates.includes(clamped) ? clamped : fallback;
		};

		return {
			dateRangeStart: clampToAvailable(rawDateRangeStart, availableDates[0] ?? ''),
			dateRangeEnd: clampToAvailable(
				rawDateRangeEnd,
				availableDates[availableDates.length - 1] ?? '',
			),
		};
	}, [availableDates, dateBounds.min, dateBounds.max, rawDateRangeStart, rawDateRangeEnd]);

	const visibleSources = useMemo(
		() =>
			getVisibleSources({
				sources,
				dateRangeStart,
				dateRangeEnd,
			}),
		[dateRangeEnd, dateRangeStart, sources],
	);

	const allMeasurementRows = useMemo(
		() =>
			getAllMeasurementRows({
				sources,
				measurements,
				results,
			}),
		[measurements, results, sources],
	);

	const effectiveSelectedImportDocumentIds = useMemo(() => {
		const documentIds = new Set(importDocuments.map(document => document.id));
		const pruned = selectedImportDocumentIds.filter(documentId => documentIds.has(documentId));
		return pruned.length === selectedImportDocumentIds.length ? selectedImportDocumentIds : pruned;
	}, [importDocuments, selectedImportDocumentIds]);

	const effectiveStarredMeasurementKeys = useMemo(() => {
		if (allMeasurementRows.length === 0) return starredMeasurementKeys;
		const availableRowIds = new Set(allMeasurementRows.map(item => item.key));
		const pruned = starredMeasurementKeys.filter(item => availableRowIds.has(item));
		return pruned.length === starredMeasurementKeys.length ? starredMeasurementKeys : pruned;
	}, [allMeasurementRows, starredMeasurementKeys]);

	const starredMeasurementSet = useMemo(
		() => new Set(effectiveStarredMeasurementKeys),
		[effectiveStarredMeasurementKeys],
	);

	const filteredMeasurementRows = useMemo(
		() =>
			getFilteredMeasurementRows({
				allMeasurementRows,
				measurementFilter: deferredMeasurementFilter,
				starredMeasurementSet,
			}),
		[allMeasurementRows, deferredMeasurementFilter, starredMeasurementSet],
	);

	const rowsWithVisibleData = useMemo(
		() =>
			getRowsWithVisibleData({
				filteredMeasurementRows,
				visibleSources,
			}),
		[filteredMeasurementRows, visibleSources],
	);

	const baseTableSources = useMemo(
		() =>
			getTableSources({
				filteredMeasurementRows: rowsWithVisibleData,
				visibleSources,
			}),
		[rowsWithVisibleData, visibleSources],
	);

	const baseTableMeasurementRows = useMemo(
		() =>
			getRowsWithVisibleData({
				filteredMeasurementRows: rowsWithVisibleData,
				visibleSources: baseTableSources,
			}),
		[baseTableSources, rowsWithVisibleData],
	);

	const effectiveSourceFilters = useMemo(() => {
		const availableSourceIds = new Set(baseTableSources.map(source => source.id));
		const pruned = sourceFilters.filter(f => availableSourceIds.has(f.sourceId));
		return pruned.length === sourceFilters.length ? sourceFilters : pruned;
	}, [baseTableSources, sourceFilters]);

	const tableMeasurementRows = useMemo(
		() =>
			getRowsMatchingSourceFilters({
				filteredMeasurementRows: baseTableMeasurementRows,
				tableSources: baseTableSources,
				sourceFilters: effectiveSourceFilters,
			}),
		[baseTableMeasurementRows, baseTableSources, effectiveSourceFilters],
	);

	const effectiveSelectedRowKeys = useMemo(() => {
		if (allMeasurementRows.length === 0) return selectedRowKeys;
		const pruned = getPrunedSelectedRowKeys({
			selectedRowKeys,
			filteredMeasurementRows: tableMeasurementRows,
		});
		return pruned.length === selectedRowKeys.length ? selectedRowKeys : pruned;
	}, [allMeasurementRows.length, selectedRowKeys, tableMeasurementRows]);

	const selectedRowKeySet = useMemo(
		() => new Set(effectiveSelectedRowKeys),
		[effectiveSelectedRowKeys],
	);

	const sourceCountsBySourceId = useMemo(
		() =>
			getSourceCountsBySourceId({
				filteredMeasurementRows: baseTableMeasurementRows,
				tableSources: baseTableSources,
			}),
		[baseTableMeasurementRows, baseTableSources],
	);

	const tableRows = useMemo(
		() =>
			getTableRows({
				filteredMeasurementRows: tableMeasurementRows,
				groupByCategory,
				starredMeasurementSet,
			}),
		[groupByCategory, starredMeasurementSet, tableMeasurementRows],
	);

	const measurementKeysByCategory = useMemo(
		() =>
			getMeasurementKeysByCategory({
				filteredMeasurementRows: tableMeasurementRows,
				groupByCategory,
				starredMeasurementSet,
			}),
		[groupByCategory, starredMeasurementSet, tableMeasurementRows],
	);

	const categorySelectionByName = useMemo(
		() =>
			getCategorySelectionByName({
				measurementKeysByCategory,
				selectedRowKeySet,
			}),
		[measurementKeysByCategory, selectedRowKeySet],
	);

	const selectedRows = useMemo(
		() =>
			getSelectedRows({
				filteredMeasurementRows: tableMeasurementRows,
				selectedRowKeySet,
			}),
		[selectedRowKeySet, tableMeasurementRows],
	);

	const chartSources = useMemo(
		() =>
			getChartSources({
				visibleSources,
				selectedRows,
			}),
		[selectedRows, visibleSources],
	);

	const measurementOverviewByKey = useMemo(
		() =>
			getMeasurementOverviewByKey({
				filteredMeasurementRows: tableMeasurementRows,
				tableSources: baseTableSources,
			}),
		[tableMeasurementRows, baseTableSources],
	);

	const chartSeries = useMemo(
		() =>
			getChartSeries({
				selectedRows,
				chartSources,
			}),
		[chartSources, selectedRows],
	);

	const categoryOverview = useMemo(
		() =>
			getCategoryOverviewByLatestAcrossAllLabs({
				allMeasurementRows,
				sources,
			}),
		[allMeasurementRows, sources],
	);

	const sixMonthChanges = useMemo(
		() =>
			getSixMonthMeaningfulChanges({
				allMeasurementRows,
				sources,
			}),
		[allMeasurementRows, sources],
	);

	const onMeasurementFilterChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
		setMeasurementFilter(event.target.value);
	}, []);

	const dateRangeSliderValue = useMemo<[number, number]>(() => {
		if (availableDates.length === 0) {
			return [0, 0];
		}
		const minIndex = 0;
		const maxIndex = availableDates.length - 1;
		const startIndex = availableDates.indexOf(dateRangeStart);
		const endIndex = availableDates.indexOf(dateRangeEnd);
		const safeStart = startIndex >= 0 ? startIndex : minIndex;
		const safeEnd = endIndex >= 0 ? endIndex : maxIndex;

		const newestHandle = maxIndex - safeEnd;
		const oldestHandle = maxIndex - safeStart;

		if (newestHandle <= oldestHandle) {
			return [newestHandle, oldestHandle];
		}
		return [oldestHandle, newestHandle];
	}, [availableDates, dateRangeEnd, dateRangeStart]);

	const onDateRangeSliderChange = useCallback(
		(nextRange: [number, number]) => {
			if (availableDates.length === 0) {
				return;
			}

			const maxIndex = availableDates.length - 1;
			const rawNewestHandle = Math.round(Math.min(nextRange[0], nextRange[1]));
			const rawOldestHandle = Math.round(Math.max(nextRange[0], nextRange[1]));
			const newestHandle = clamp(rawNewestHandle, 0, maxIndex);
			const oldestHandle = clamp(rawOldestHandle, 0, maxIndex);
			const endIndex = maxIndex - newestHandle;
			const startIndex = maxIndex - oldestHandle;
			const nextStartDate = availableDates[startIndex];
			const nextEndDate = availableDates[endIndex];
			if (!nextStartDate || !nextEndDate) {
				return;
			}

			setDateRangeStart(nextStartDate);
			setDateRangeEnd(nextEndDate);
		},
		[availableDates],
	);

	const onGroupByCategoryChange = useCallback((checked: boolean) => {
		setGroupByCategory(checked);
	}, []);

	const onToggleRow = useCallback((key: string, checked: boolean) => {
		setSelectedRowKeys(previous => {
			if (checked) {
				if (previous.includes(key)) return previous;
				return [...previous, key];
			}
			if (!previous.includes(key)) return previous;
			return previous.filter(item => item !== key);
		});
	}, []);

	const onToggleAllRows = useCallback(
		(checked: boolean) => {
			setSelectedRowKeys(previous => {
				const visibleMeasurementKeys = tableMeasurementRows.map(row => row.key);
				if (!checked) {
					const visibleSet = new Set(visibleMeasurementKeys);
					const next = previous.filter(key => !visibleSet.has(key));
					return next.length === previous.length ? previous : next;
				}

				const nextSet = new Set(previous);
				visibleMeasurementKeys.forEach(key => nextSet.add(key));
				if (nextSet.size === previous.length) return previous;
				return Array.from(nextSet);
			});
		},
		[tableMeasurementRows],
	);

	const onToggleCategory = useCallback(
		(category: string, shouldSelect: boolean) => {
			const categoryMeasurementKeys = measurementKeysByCategory.get(category);
			if (!categoryMeasurementKeys || categoryMeasurementKeys.length === 0) {
				return;
			}

			setSelectedRowKeys(previous => {
				const next = new Set(previous);
				if (shouldSelect) {
					categoryMeasurementKeys.forEach(key => next.add(key));
				} else {
					categoryMeasurementKeys.forEach(key => next.delete(key));
				}
				return Array.from(next);
			});
		},
		[measurementKeysByCategory],
	);

	const onToggleStar = useCallback((measurementKey: string) => {
		setStarredMeasurementKeys(previous =>
			previous.includes(measurementKey)
				? previous.filter(item => item !== measurementKey)
				: [...previous, measurementKey],
		);
	}, []);

	const onToggleSourceFilter = useCallback((sourceId: string, mode: SourceFilterMode) => {
		setSourceFilters(previous => {
			const existing = previous.find(f => f.sourceId === sourceId);
			if (existing?.mode === mode) {
				return previous.filter(f => f.sourceId !== sourceId);
			}
			return [...previous.filter(f => f.sourceId !== sourceId), { sourceId, mode }];
		});
	}, []);

	const hasAnyData = documents.length > 0;
	const hasSelectedRows = selectedRows.length > 0;
	const showSplitLayout = hasSelectedRows && !isMobileViewport;

	const tableScrollX = useMemo(
		() =>
			SELECTION_COLUMN_WIDTH +
			MEASUREMENT_COLUMN_WIDTH +
			OVERVIEW_COLUMN_WIDTH +
			baseTableSources.length * SOURCE_COLUMN_WIDTH,
		[baseTableSources.length],
	);

	const tableScrollY = useMemo(
		() => (showSplitLayout ? Math.max(viewport.height - 390, 320) : Math.max(viewport.height, 420)),
		[showSplitLayout, viewport.height],
	);

	const csvMeasurementRows = useMemo(
		() => tableRows.filter((row): row is VitalsRowModel => row.rowType === 'measurement'),
		[tableRows],
	);

	const isDownloadCsvDisabled = csvMeasurementRows.length === 0 || baseTableSources.length === 0;

	const onDownloadCsv = useCallback(() => {
		if (isDownloadCsvDisabled || typeof document === 'undefined') {
			return;
		}

		const escapeCsv = (value: string | number) => {
			const text = String(value);
			if (!/[",\n]/.test(text)) {
				return text;
			}
			return `"${text.replace(/"/g, '""')}"`;
		};

		const headers = [
			'Measurement',
			'Category',
			'In range',
			'Out of range',
			...baseTableSources.map(source => source.prettyDate),
		];

		const rows = csvMeasurementRows.map(row => {
			const overview = measurementOverviewByKey.get(row.key) ?? { inRange: 0, outOfRange: 0 };
			return [
				row.measurement,
				row.category,
				overview.inRange,
				overview.outOfRange,
				...baseTableSources.map(source => {
					const cell = row.valuesBySourceIndex[source.index];
					if (!cell || cell.display === '—' || cell.display === '--') {
						return '';
					}
					return cell.display;
				}),
			];
		});

		const csv = [headers, ...rows].map(row => row.map(escapeCsv).join(',')).join('\n');
		const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
		const href = URL.createObjectURL(blob);
		const link = document.createElement('a');
		link.href = href;
		link.download = `vitals-visible-data-${new Date().toISOString().slice(0, 10)}.csv`;
		document.body.append(link);
		link.click();
		link.remove();
		window.setTimeout(() => URL.revokeObjectURL(href), 0);
	}, [csvMeasurementRows, isDownloadCsvDisabled, measurementOverviewByKey, baseTableSources]);

	const onOpenImportPicker = useCallback(() => {
		importInputRef.current?.click();
	}, []);

	const onImportFiles = useCallback(
		async (event: ChangeEvent<HTMLInputElement>) => {
			const selectedFiles = Array.from(event.target.files ?? []).filter(file =>
				file.name.toLowerCase().endsWith('.pdf'),
			);
			event.target.value = '';

			if (selectedFiles.length === 0) {
				return;
			}

			await uploadDocumentsMutation.mutateAsync({
				files: await Promise.all(
					selectedFiles.map(async file => ({
						fileName: file.name,
						mimeType: file.type || 'application/pdf',
						dataBase64: await readFileAsBase64(file),
					})),
				),
			});
		},
		[uploadDocumentsMutation],
	);

	const onDeleteDocument = useCallback(
		async (documentId: number) => {
			setDeletingDocumentId(documentId);
			await deleteDocumentMutation
				.mutateAsync({
					where: [
						{
							column: 'id',
							operator: 'eq',
							value: documentId,
						},
					],
				})
				.catch(() => undefined);
		},
		[deleteDocumentMutation],
	);

	const scrollToPreviewRow = useCallback((el: HTMLDivElement | null) => {
		if (!el) return;
		if ('scrollIntoViewIfNeeded' in el) {
			(el as any).scrollIntoViewIfNeeded(true);
		} else {
			el.scrollIntoView({ block: 'center' });
		}
	}, []);

	const onOpenDocumentPreview = useCallback(
		(documentId: number, measurementKey?: string) => {
			void navigate({
				search: previous => ({
					...previous,
					doc: documentId,
					m: measurementKey,
				}),
			});
		},
		[navigate],
	);

	const onCloseDocumentPreview = useCallback(() => {
		void navigate({
			search: previous => {
				const next = { ...previous };
				delete next.doc;
				delete next.m;
				return next;
			},
		});
	}, [navigate]);

	const onTabChange = useCallback(
		(nextTab: string) => {
			const normalizedTab = nextTab === 'documents' ? 'documents' : 'overview';
			void navigate({
				search: previous => ({
					...previous,
					tab: normalizedTab,
				}),
			});
		},
		[navigate],
	);

	const onRetryDocument = useCallback(
		async (documentId: number) => {
			await retryDocumentMutation.mutateAsync({
				documentId,
			});
		},
		[retryDocumentMutation],
	);

	const onReprocessDocument = useCallback(
		async (documentId: number) => {
			await reprocessDocumentMutation.mutateAsync({
				documentId,
			});
		},
		[reprocessDocumentMutation],
	);

	const onToggleImportDocument = useCallback((documentId: number, checked: boolean) => {
		setSelectedImportDocumentIds(previous => {
			if (checked) {
				if (previous.includes(documentId)) {
					return previous;
				}
				return [...previous, documentId];
			}
			return previous.filter(item => item !== documentId);
		});
	}, []);

	const onGroupDocuments = useCallback(async () => {
		const documentIds = importDocuments
			.filter(document => effectiveSelectedImportDocumentIds.includes(document.id))
			.map(document => document.id);
		if (documentIds.length < 2) {
			return;
		}

		await updateDocumentsMutation.mutateAsync({
			where: [
				{
					column: 'id',
					operator: 'in',
					value: documentIds,
				},
			],
			values: {
				group: buildLabsDocumentGroupId(),
			},
		});
	}, [importDocuments, effectiveSelectedImportDocumentIds, updateDocumentsMutation]);

	const onClearDocumentGroup = useCallback(async () => {
		const documentIds = importDocuments
			.filter(document => effectiveSelectedImportDocumentIds.includes(document.id))
			.filter(document => Boolean(document.group))
			.map(document => document.id);
		if (documentIds.length === 0) {
			return;
		}

		await updateDocumentsMutation.mutateAsync({
			where: [
				{
					column: 'id',
					operator: 'in',
					value: documentIds,
				},
			],
			values: {
				group: null,
			},
		});
	}, [importDocuments, effectiveSelectedImportDocumentIds, updateDocumentsMutation]);

	const selectedGroupableDocuments = useMemo(
		() =>
			importDocuments.filter(document => effectiveSelectedImportDocumentIds.includes(document.id)),
		[importDocuments, effectiveSelectedImportDocumentIds],
	);
	const hasSelectedDocumentGroup = selectedGroupableDocuments.some(document =>
		Boolean(document.group),
	);

	const importPanel = (
		<Card
			size='small'
			title='Documents'
			extra={
				<Flex align='center' gap={8} wrap>
					<input
						ref={importInputRef}
						type='file'
						accept='application/pdf,.pdf'
						multiple
						hidden
						onChange={event => {
							void onImportFiles(event);
						}}
					/>
					<Button
						type='default'
						onClick={() => {
							void onGroupDocuments();
						}}
						disabled={selectedGroupableDocuments.length < 2 || updateDocumentsMutation.isPending}
						loading={updateDocumentsMutation.isPending}
					>
						Group documents
					</Button>
					<Button
						type='default'
						onClick={() => {
							void onClearDocumentGroup();
						}}
						disabled={!hasSelectedDocumentGroup || updateDocumentsMutation.isPending}
					>
						Clear group
					</Button>
					<Button
						type='default'
						icon={<UploadSimple size={16} />}
						onClick={onOpenImportPicker}
						loading={uploadDocumentsMutation.isPending}
					>
						Import File
					</Button>
				</Flex>
			}
			styles={{ body: { padding: 0 } }}
		>
			{importDocuments.length === 0 ? (
				<div style={{ padding: 16 }}>
					<Empty description='No imported documents yet.' image={Empty.PRESENTED_IMAGE_SIMPLE} />
				</div>
			) : (
				<div>
					{importDocuments.map((item, index) => {
						const statusColor =
							item.status === 'completed'
								? 'success'
								: item.status === 'failed'
									? 'error'
									: item.status === 'processing'
										? 'processing'
										: 'default';
						const previousItem = importDocuments[index - 1];
						const nextItem = importDocuments[index + 1];
						const hasGroup = Boolean(item.group);
						const isGroupStart = hasGroup && previousItem?.group !== item.group;
						const isGroupEnd = hasGroup && nextItem?.group !== item.group;
						return (
							<div
								key={item.id}
								style={{
									padding: '12px 16px',
									marginTop: isGroupStart ? 12 : 0,
									marginBottom: isGroupEnd ? 12 : 0,
									borderTop: index === 0 ? 'none' : `1px solid ${token.colorBorderSecondary}`,
									background: hasGroup ? token.colorFillAlter : undefined,
									borderRadius:
										isGroupStart || isGroupEnd
											? `${isGroupStart ? 8 : 0}px ${isGroupStart ? 8 : 0}px ${isGroupEnd ? 8 : 0}px ${isGroupEnd ? 8 : 0}px`
											: 0,
								}}
							>
								<Flex vertical style={{ width: '100%' }} gap={4}>
									<Flex justify='space-between' align='center' gap={12} wrap>
										<Flex align='center' gap={8} wrap>
											<Checkbox
												checked={effectiveSelectedImportDocumentIds.includes(item.id)}
												onChange={event => onToggleImportDocument(item.id, event.target.checked)}
												disabled={updateDocumentsMutation.isPending}
											/>
											<Tag color={statusColor}>
												{item.status}
												{(item.status === 'pending' || item.status === 'processing') &&
												item.statusUpdatedAt ? (
													<>
														{' '}
														<ElapsedTime since={item.statusUpdatedAt} />
													</>
												) : null}
											</Tag>
											<Button
												type='link'
												style={{ padding: 0, height: 'auto', fontWeight: 600 }}
												onClick={() => onOpenDocumentPreview(item.id)}
											>
												{item.fileName}
											</Button>
											{item.group ? <Tag>{'Grouped'}</Tag> : null}
											{item.statusText ? (
												<Typography.Text type='secondary'>{item.statusText}</Typography.Text>
											) : null}
										</Flex>
										<Flex align='center' gap={12} wrap>
											<Typography.Text type='secondary'>
												{item.date ?? item.queuedAt.slice(0, 10)}
												{item.labName ? ` · ${item.labName}` : ''}
											</Typography.Text>
											{item.status === 'completed' ? (
												<Popconfirm
													title='Reprocess imported file?'
													description='This clears all derived data for the document and imports it again from the PDF.'
													okText='Reprocess'
													onConfirm={() => onReprocessDocument(item.id)}
													disabled={reprocessDocumentMutation.isPending}
												>
													<Button
														size='small'
														type='text'
														style={{ color: token.colorWarning }}
														loading={reprocessDocumentMutation.isPending}
														disabled={reprocessDocumentMutation.isPending}
													>
														Reprocess
													</Button>
												</Popconfirm>
											) : null}
											{item.status === 'failed' ? (
												<Button
													size='small'
													type='text'
													onClick={() => {
														void onRetryDocument(item.id);
													}}
													loading={retryDocumentMutation.isPending}
													disabled={retryDocumentMutation.isPending}
												>
													Retry
												</Button>
											) : null}
											<Popconfirm
												title='Delete imported file?'
												description='This removes the document and every result derived from it.'
												okText='Delete'
												okButtonProps={{ danger: true }}
												onConfirm={() => onDeleteDocument(item.id)}
												disabled={item.status === 'processing' || deleteDocumentMutation.isPending}
											>
												<Button
													size='small'
													type='text'
													danger
													disabled={
														item.status === 'processing' || deleteDocumentMutation.isPending
													}
													loading={
														deleteDocumentMutation.isPending && deletingDocumentId === item.id
													}
												>
													Delete
												</Button>
											</Popconfirm>
										</Flex>
									</Flex>
									{item.status === 'failed' && item.lastError ? (
										<div style={{ maxHeight: '10rem', overflow: 'auto' }}>
											<Typography.Text
												type='danger'
												style={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', fontSize: 12 }}
											>
												{item.lastError}
											</Typography.Text>
										</div>
									) : null}
								</Flex>
							</div>
						);
					})}
				</div>
			)}
		</Card>
	);

	const chartCard = (
		<Card
			size='small'
			title={
				<Flex align='center' gap={8}>
					<ChartLineUp size={18} weight='duotone' />
					<span>Trend view</span>
				</Flex>
			}
			styles={{ body: { padding: 12 } }}
		>
			{chartSeries.length > 0 ? (
				<TrendChart
					series={chartSeries}
					orderedSources={chartSources}
					isMobile={isMobileViewport}
				/>
			) : (
				<Empty description='No numeric values in the selected rows for this date range.' />
			)}
		</Card>
	);

	const tablePanel = (
		<div>
			<CategoriesOverview items={categoryOverview} />
			<MeaningfulChanges items={sixMonthChanges} />
			<VitalsControls
				measurementFilter={measurementFilter}
				onMeasurementFilterChange={onMeasurementFilterChange}
				availableDates={availableDates}
				dateRangeValue={dateRangeSliderValue}
				onDateRangeSliderChange={onDateRangeSliderChange}
				groupByCategory={groupByCategory}
				onGroupByCategoryChange={onGroupByCategoryChange}
				onDownloadCsv={onDownloadCsv}
				isDownloadCsvDisabled={isDownloadCsvDisabled}
			/>
			<VitalsTable
				rows={tableRows}
				tableSources={baseTableSources}
				sourceFilters={effectiveSourceFilters}
				sourceCountsBySourceId={sourceCountsBySourceId}
				selectedRowKeySet={selectedRowKeySet}
				categorySelectionByName={categorySelectionByName}
				starredMeasurementSet={starredMeasurementSet}
				measurementOverviewByKey={measurementOverviewByKey}
				tableScrollX={tableScrollX}
				tableScrollY={tableScrollY}
				onToggleRow={onToggleRow}
				onToggleAllRows={onToggleAllRows}
				onToggleCategory={onToggleCategory}
				onToggleStar={onToggleStar}
				onToggleSourceFilter={onToggleSourceFilter}
				onOpenSourceDocument={onOpenDocumentPreview}
			/>
		</div>
	);
	const overviewPanel = !hasAnyData ? (
		<Card styles={{ body: { padding: 24 } }}>
			<Flex justify='center' align='center' style={{ minHeight: '40vh' }}>
				<Empty description='No lab data found yet.' />
			</Flex>
		</Card>
	) : showSplitLayout ? (
		<Splitter
			style={{ height: Math.max(viewport.height - 96, 680) }}
			styles={{
				root: { height: Math.max(viewport.height - 96, 680) },
				panel: { overflow: 'hidden' },
				dragger: {
					default: { background: token.colorFillSecondary },
					active: { background: token.colorPrimary },
				},
			}}
		>
			<Splitter.Panel defaultSize='68%' min={560}>
				<div style={{ height: '100%', overflowY: 'auto', paddingRight: 12 }}>{tablePanel}</div>
			</Splitter.Panel>
			<Splitter.Panel min={MIN_CHART_PANE_WIDTH}>
				<div style={{ height: '100%', overflowY: 'auto', paddingLeft: 12 }}>{chartCard}</div>
			</Splitter.Panel>
		</Splitter>
	) : (
		<Flex vertical gap={16}>
			{tablePanel}
			{hasSelectedRows ? chartCard : null}
		</Flex>
	);

	return (
		<main
			style={{
				minHeight: '100dvh',
				padding: 0,
				background: token.colorBgLayout,
			}}
		>
			{messageContextHolder}
			{dashboardQuery.isLoading ? (
				<Card styles={{ body: { padding: 24 } }}>
					<Flex justify='center' align='center' style={{ minHeight: '50vh' }}>
						<Spin size='large' />
					</Flex>
				</Card>
			) : dashboardQuery.error ? (
				<Alert
					type='error'
					showIcon
					message='Unable to load lab data'
					description={dashboardQuery.error.message}
				/>
			) : (
				<Tabs
					activeKey={activeTab}
					onChange={onTabChange}
					items={[
						{
							key: 'overview',
							label: 'Overview',
							children: overviewPanel,
						},
						{
							key: 'documents',
							label: 'Documents',
							children: importPanel,
						},
					]}
				/>
			)}
			<Drawer
				title={previewDocument?.fileName ?? 'Document preview'}
				open={previewDocumentId !== null}
				onClose={onCloseDocumentPreview}
				width={Math.min(viewport.width - 32, 1080)}
				destroyOnHidden
				styles={{ body: { padding: 0 } }}
			>
				{previewDocumentId !== null ? (
					<Flex
						style={{
							height: PREVIEW_DRAWER_CONTENT_HEIGHT,
							minHeight: PREVIEW_DRAWER_CONTENT_HEIGHT,
						}}
					>
						<div
							style={{
								width: PREVIEW_RESULTS_PANEL_WIDTH,
								minWidth: PREVIEW_RESULTS_PANEL_WIDTH,
								borderRight: `1px solid ${token.colorBorderSecondary}`,
								display: 'flex',
								flexDirection: 'column',
								minHeight: 0,
								background: token.colorBgContainer,
							}}
						>
							<div
								style={{
									padding: '10px 14px',
									borderBottom: `1px solid ${token.colorBorderSecondary}`,
								}}
							>
								<Flex justify='space-between' align='center' gap={12}>
									<Typography.Text strong>
										Parsed Values ({visiblePreviewRows.length})
									</Typography.Text>
									<Checkbox
										checked={showFlaggedPreviewRowsOnly}
										onChange={event => setShowFlaggedPreviewRowsOnly(event.target.checked)}
									>
										Flagged ({flaggedPreviewRowCount})
									</Checkbox>
								</Flex>
							</div>
							<div style={{ overflowY: 'auto', minHeight: 0, flex: 1 }}>
								{visiblePreviewRows.length === 0 ? (
									<Empty
										image={Empty.PRESENTED_IMAGE_SIMPLE}
										description={
											previewRows.length === 0
												? 'No processed values for this document'
												: 'No flagged values for this document'
										}
										style={{ marginTop: 24 }}
									/>
								) : (
									<div>
										{visiblePreviewRows.map(row => {
											const isHighlighted = previewMeasurementKey === row.key;
											return (
												<div
													key={row.id}
													ref={isHighlighted ? scrollToPreviewRow : undefined}
													style={{
														padding: '8px 14px',
														borderBottom: `1px solid ${token.colorBorderSecondary}`,
														background: isHighlighted ? token.colorPrimaryBg : undefined,
													}}
												>
													<Flex justify='space-between' align='flex-start' gap={10}>
														<div style={{ minWidth: 0, flex: 1 }}>
															<Flex align='flex-start' gap={6}>
																<Typography.Text
																	strong
																	style={{
																		display: 'block',
																		lineHeight: 1.25,
																		overflowWrap: 'break-word',
																	}}
																>
																	{row.name}
																</Typography.Text>
																{row.issueLabel ? (
																	<WarningCircle
																		size={15}
																		weight='fill'
																		color={token.colorWarning}
																		style={{ flexShrink: 0, marginTop: 2 }}
																		aria-label={row.issueLabel}
																	/>
																) : null}
															</Flex>
														</div>
														<div
															style={{
																textAlign: 'right',
																flexShrink: 0,
																maxWidth: 138,
															}}
														>
															<Typography.Text
																style={{
																	display: 'block',
																	overflowWrap: 'anywhere',
																}}
															>
																{row.valueText}
															</Typography.Text>
															{row.rangeText ? (
																<Typography.Text
																	type='secondary'
																	style={{
																		display: 'block',
																		marginTop: 2,
																		fontSize: 11,
																		lineHeight: 1.2,
																	}}
																>
																	ref {row.rangeText}
																</Typography.Text>
															) : null}
														</div>
													</Flex>
													{row.note ? (
														<Typography.Text
															type='secondary'
															style={{ display: 'block', marginTop: 3, fontSize: 12 }}
														>
															{row.note}
														</Typography.Text>
													) : null}
												</div>
											);
										})}
									</div>
								)}
							</div>
						</div>
						<div style={{ flex: 1, minWidth: 0 }}>
							<iframe
								src={buildLabsDocumentPreviewUrl(previewDocumentId)}
								title={previewDocument?.fileName ?? `Labs document ${previewDocumentId}`}
								style={{ width: '100%', height: '100%', border: 0 }}
							/>
						</div>
					</Flex>
				) : null}
			</Drawer>
		</main>
	);
}

function getPreviewIssueLabel(note: string | null, isOutsideRange: boolean) {
	const value = note?.trim().toLowerCase();
	const compactFlagLabel = getPreviewCompactFlagLabel(value, note);
	if (compactFlagLabel) return compactFlagLabel;
	if (value && !NORMAL_PREVIEW_FLAG_NOTES.has(value)) return note;
	return isOutsideRange ? 'Outside reference range' : null;
}

function getPreviewDisplayNote(note: string | null) {
	const value = note?.trim().toLowerCase();
	if (!note || !value) return null;
	if (getPreviewCompactFlagLabel(value, note)) return null;
	return note;
}

function getPreviewCompactFlagLabel(value: string | undefined, note: string | null) {
	if (!value || NORMAL_PREVIEW_FLAG_NOTES.has(value)) return null;
	if (value === 'h' || value === 'high') return 'High';
	if (value === 'l' || value === 'low') return 'Low';
	if (value === 'a' || value === 'abn' || value === 'abnormal') return 'Abnormal';
	if (value === 'critical' || value === 'crit') return 'Critical';
	if (value === 'critical h' || value === 'critical high') return 'Critical high';
	if (value === 'critical l' || value === 'critical low') return 'Critical low';
	if (/^[a-z]{1,2}$/.test(value)) return note?.toUpperCase() ?? value.toUpperCase();
	return null;
}

function hasActiveLabsImports(documents: LabImportDocument[]) {
	return documents.some(
		document => document.status === 'pending' || document.status === 'processing',
	);
}

function buildLabsDocumentGroupId() {
	return `group_${Date.now()}_${crypto.randomUUID().slice(0, 8)}`;
}

function buildLabsDocumentPreviewUrl(documentId: number) {
	return withAuthToken(
		`${import.meta.env.VITE_API_URL.trim()}/asset/lab_documents/${documentId}/pdf`,
	);
}

function useViewport() {
	const [size, setSize] = useState({
		width: window.innerWidth,
		height: window.innerHeight,
	});

	useEffect(() => {
		const onResize = () => {
			setSize({
				width: window.innerWidth,
				height: window.innerHeight,
			});
		};

		window.addEventListener('resize', onResize);
		return () => window.removeEventListener('resize', onResize);
	}, []);

	return size;
}

function readFileAsBase64(file: File): Promise<string> {
	return new Promise((resolve, reject) => {
		const reader = new FileReader();

		reader.onerror = () => {
			reject(new Error(`Failed to read ${file.name}.`));
		};

		reader.onload = () => {
			if (typeof reader.result !== 'string') {
				reject(new Error(`Failed to read ${file.name}.`));
				return;
			}

			const [, dataBase64] = reader.result.split(',', 2);
			if (!dataBase64) {
				reject(new Error(`Failed to encode ${file.name}.`));
				return;
			}

			resolve(dataBase64);
		};

		reader.readAsDataURL(file);
	});
}

function ElapsedTime({ since }: { since: string }) {
	const [elapsed, setElapsed] = useState(() => formatElapsed(since));

	useEffect(() => {
		setElapsed(formatElapsed(since));
		const interval = setInterval(() => setElapsed(formatElapsed(since)), 1000);
		return () => clearInterval(interval);
	}, [since]);

	return <span>{elapsed}</span>;
}

function formatElapsed(since: string): string {
	const ms = Date.now() - Date.parse(since);
	if (!Number.isFinite(ms) || ms < 0) return '0s';
	const totalSeconds = Math.floor(ms / 1000);
	if (totalSeconds < 60) return `${totalSeconds}s`;
	const minutes = Math.floor(totalSeconds / 60);
	const seconds = totalSeconds % 60;
	if (minutes < 60) return `${minutes}m ${seconds}s`;
	const hours = Math.floor(minutes / 60);
	return `${hours}h ${minutes % 60}m`;
}
