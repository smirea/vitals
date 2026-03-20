import { ChartLineUp, UploadSimple } from '@phosphor-icons/react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import {
	Alert,
	Button,
	Card,
	Empty,
	Flex,
	Splitter,
	Spin,
	Tag,
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

import {
	CategoriesOverview,
	MeaningfulChanges,
	TrendChart,
	VitalsControls,
	VitalsTable,
} from './bloodwork/_components';
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
	getMeasurementRangesTooltipByKey,
	getOrderedDocuments,
	getOutOfRangeMeasurementCountBySourceId,
	getPrunedSelectedRowKeys,
	getRowsMatchingOutOfRangeSources,
	getRowsWithVisibleData,
	getSelectedRows,
	getSixMonthMeaningfulChanges,
	getSources,
	getTableRows,
	getTableSources,
	getVisibleSources,
	GROUP_BY_CATEGORY_STORAGE_KEY,
	MEASUREMENT_COLUMN_WIDTH,
	MIN_CHART_PANE_WIDTH,
	OUT_OF_RANGE_SOURCE_FILTERS_STORAGE_KEY,
	OVERVIEW_COLUMN_WIDTH,
	readStoredGroupByCategory,
	readStoredOutOfRangeSourceFilterIds,
	readStoredSelectedRowKeys,
	readStoredStarredMeasurementKeys,
	SELECTED_ROWS_STORAGE_KEY,
	SELECTION_COLUMN_WIDTH,
	SOURCE_COLUMN_WIDTH,
	STARRED_MEASUREMENTS_STORAGE_KEY,
	clamp,
	type VitalsRowModel,
} from './bloodwork/_bloodwork';
import { useTRPC } from '../utils/trpc';

export const Route = createFileRoute('/bloodwork')({
	component: BloodworkPage,
});

function BloodworkPage() {
	const viewport = useViewport();
	const isMobileViewport = viewport.width < 900;
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const { token } = antdTheme.useToken();
	const [messageApi, messageContextHolder] = message.useMessage();
	const importInputRef = useRef<HTMLInputElement | null>(null);

	const [measurementFilter, setMeasurementFilter] = useState('');
	const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>(() =>
		readStoredSelectedRowKeys(),
	);
	const [starredMeasurementKeys, setStarredMeasurementKeys] = useState<string[]>(() =>
		readStoredStarredMeasurementKeys(),
	);
	const [dateRangeStart, setDateRangeStart] = useState('');
	const [dateRangeEnd, setDateRangeEnd] = useState('');
	const [groupByCategory, setGroupByCategory] = useState(() => readStoredGroupByCategory());
	const [outOfRangeSourceFilterIds, setOutOfRangeSourceFilterIds] = useState<string[]>(() =>
		readStoredOutOfRangeSourceFilterIds(),
	);

	const deferredMeasurementFilter = useDeferredValue(measurementFilter);
	const starredMeasurementSet = useMemo(
		() => new Set(starredMeasurementKeys),
		[starredMeasurementKeys],
	);

	const dashboardQuery = useQuery({
		...trpc.bloodwork.getDashboard.queryOptions(),
		refetchInterval: 3_000,
	});
	const documentsQuery = useQuery({
		...trpc.bloodwork.listDocuments.queryOptions(),
		refetchInterval: 3_000,
	});
	const uploadDocumentsMutation = useMutation({
		...trpc.bloodwork.uploadDocuments.mutationOptions(),
		onSuccess: async data => {
			await queryClient.invalidateQueries();
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
	const dashboard = dashboardQuery.data;
	const documents = dashboard?.documents ?? [];
	const measurements = dashboard?.measurements ?? [];
	const results = dashboard?.results ?? [];
	const importDocuments = documentsQuery.data ?? [];

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

	useEffect(() => {
		if (!dateBounds.min || !dateBounds.max) {
			setDateRangeStart('');
			setDateRangeEnd('');
			return;
		}

		setDateRangeStart(previous => {
			if (!previous) return dateBounds.min;
			if (previous < dateBounds.min) return dateBounds.min;
			if (previous > dateBounds.max) return dateBounds.max;
			return previous;
		});

		setDateRangeEnd(previous => {
			if (!previous) return dateBounds.max;
			if (previous < dateBounds.min) return dateBounds.min;
			if (previous > dateBounds.max) return dateBounds.max;
			return previous;
		});
	}, [dateBounds.max, dateBounds.min]);

	useEffect(() => {
		if (availableDates.length === 0) {
			return;
		}
		if (!availableDates.includes(dateRangeStart)) {
			setDateRangeStart(availableDates[0] ?? '');
		}
		if (!availableDates.includes(dateRangeEnd)) {
			setDateRangeEnd(availableDates[availableDates.length - 1] ?? '');
		}
	}, [availableDates, dateRangeEnd, dateRangeStart]);

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
				orderedDocuments,
				measurements,
				results,
				sourceCount: sources.length,
			}),
		[measurements, orderedDocuments, results, sources.length],
	);

	useEffect(() => {
		if (typeof window === 'undefined') return;
		window.localStorage.setItem(
			STARRED_MEASUREMENTS_STORAGE_KEY,
			JSON.stringify(starredMeasurementKeys),
		);
	}, [starredMeasurementKeys]);

	useEffect(() => {
		if (typeof window === 'undefined') return;
		window.localStorage.setItem(GROUP_BY_CATEGORY_STORAGE_KEY, String(groupByCategory));
	}, [groupByCategory]);

	useEffect(() => {
		if (typeof window === 'undefined') return;
		window.localStorage.setItem(SELECTED_ROWS_STORAGE_KEY, JSON.stringify(selectedRowKeys));
	}, [selectedRowKeys]);

	useEffect(() => {
		if (typeof window === 'undefined') return;
		window.localStorage.setItem(
			OUT_OF_RANGE_SOURCE_FILTERS_STORAGE_KEY,
			JSON.stringify(outOfRangeSourceFilterIds),
		);
	}, [outOfRangeSourceFilterIds]);

	useEffect(() => {
		if (allMeasurementRows.length === 0) {
			return;
		}
		const availableRowIds = new Set(allMeasurementRows.map(item => item.key));
		setStarredMeasurementKeys(previous => {
			const next = previous.filter(item => availableRowIds.has(item));
			return next.length === previous.length ? previous : next;
		});
	}, [allMeasurementRows]);

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

	useEffect(() => {
		const availableSourceIds = new Set(baseTableSources.map(source => source.id));
		setOutOfRangeSourceFilterIds(previous => {
			const next = previous.filter(sourceId => availableSourceIds.has(sourceId));
			return next.length === previous.length ? previous : next;
		});
	}, [baseTableSources]);

	const outOfRangeSourceFilterIdSet = useMemo(
		() => new Set(outOfRangeSourceFilterIds),
		[outOfRangeSourceFilterIds],
	);

	const tableMeasurementRows = useMemo(
		() =>
			getRowsMatchingOutOfRangeSources({
				filteredMeasurementRows: baseTableMeasurementRows,
				tableSources: baseTableSources,
				outOfRangeSourceIdSet: outOfRangeSourceFilterIdSet,
			}),
		[baseTableMeasurementRows, baseTableSources, outOfRangeSourceFilterIdSet],
	);

	useEffect(() => {
		if (allMeasurementRows.length === 0) {
			return;
		}
		setSelectedRowKeys(previous => {
			const next = getPrunedSelectedRowKeys({
				selectedRowKeys: previous,
				filteredMeasurementRows: tableMeasurementRows,
			});
			return next.length === previous.length ? previous : next;
		});
	}, [allMeasurementRows.length, tableMeasurementRows]);

	const selectedRowKeySet = useMemo(() => new Set(selectedRowKeys), [selectedRowKeys]);

	const tableSources = baseTableSources;

	const outOfRangeMeasurementCountBySourceId = useMemo(
		() =>
			getOutOfRangeMeasurementCountBySourceId({
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

	const measurementRangesTooltipByKey = useMemo(
		() =>
			getMeasurementRangesTooltipByKey({
				filteredMeasurementRows: tableMeasurementRows,
				sources,
			}),
		[sources, tableMeasurementRows],
	);

	const measurementOverviewByKey = useMemo(
		() =>
			getMeasurementOverviewByKey({
				filteredMeasurementRows: tableMeasurementRows,
				tableSources,
			}),
		[tableMeasurementRows, tableSources],
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

	const onToggleOutOfRangeSourceFilter = useCallback((sourceId: string) => {
		setOutOfRangeSourceFilterIds(previous =>
			previous.includes(sourceId)
				? previous.filter(item => item !== sourceId)
				: [...previous, sourceId],
		);
	}, []);

	const hasAnyData = documents.length > 0;
	const hasSelectedRows = selectedRows.length > 0;
	const showSplitLayout = hasSelectedRows && !isMobileViewport;

	const tableScrollX = useMemo(
		() =>
			SELECTION_COLUMN_WIDTH +
			MEASUREMENT_COLUMN_WIDTH +
			OVERVIEW_COLUMN_WIDTH +
			tableSources.length * SOURCE_COLUMN_WIDTH,
		[tableSources.length],
	);

	const tableScrollY = useMemo(
		() => (showSplitLayout ? Math.max(viewport.height - 390, 320) : Math.max(viewport.height, 420)),
		[showSplitLayout, viewport.height],
	);

	const csvMeasurementRows = useMemo(
		() => tableRows.filter((row): row is VitalsRowModel => row.rowType === 'measurement'),
		[tableRows],
	);

	const isDownloadCsvDisabled = csvMeasurementRows.length === 0 || tableSources.length === 0;

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
			...tableSources.map(source => source.prettyDate),
		];

		const rows = csvMeasurementRows.map(row => {
			const overview = measurementOverviewByKey.get(row.key) ?? { inRange: 0, outOfRange: 0 };
			return [
				row.measurement,
				row.category,
				overview.inRange,
				overview.outOfRange,
				...tableSources.map(source => {
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
	}, [csvMeasurementRows, isDownloadCsvDisabled, measurementOverviewByKey, tableSources]);

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

	const importPanel = (
		<Card
			size='small'
			title='Imports'
			extra={
				<>
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
						icon={<UploadSimple size={16} />}
						onClick={onOpenImportPicker}
						loading={uploadDocumentsMutation.isPending}
					>
						Import File
					</Button>
				</>
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

						return (
							<div
								key={item.id}
								style={{
									padding: '12px 16px',
									borderTop: index === 0 ? 'none' : `1px solid ${token.colorBorderSecondary}`,
								}}
							>
								<Flex vertical style={{ width: '100%' }} gap={4}>
									<Flex justify='space-between' align='center' gap={12} wrap>
										<Flex align='center' gap={8} wrap>
											<Tag color={statusColor}>{item.status}</Tag>
											<Typography.Text strong>{item.fileName}</Typography.Text>
										</Flex>
										<Typography.Text type='secondary'>
											{item.date ?? item.queuedAt.slice(0, 10)}
											{item.labName ? ` · ${item.labName}` : ''}
										</Typography.Text>
									</Flex>
									{item.lastError ? (
										<Typography.Text type='danger'>{item.lastError}</Typography.Text>
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
			{importPanel}
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
				tableSources={tableSources}
				outOfRangeSourceFilterIdSet={outOfRangeSourceFilterIdSet}
				outOfRangeMeasurementCountBySourceId={outOfRangeMeasurementCountBySourceId}
				selectedRowKeySet={selectedRowKeySet}
				categorySelectionByName={categorySelectionByName}
				starredMeasurementSet={starredMeasurementSet}
				measurementOverviewByKey={measurementOverviewByKey}
				measurementRangesTooltipByKey={measurementRangesTooltipByKey}
				tableScrollX={tableScrollX}
				tableScrollY={tableScrollY}
				onToggleRow={onToggleRow}
				onToggleAllRows={onToggleAllRows}
				onToggleCategory={onToggleCategory}
				onToggleStar={onToggleStar}
				onToggleOutOfRangeSourceFilter={onToggleOutOfRangeSourceFilter}
			/>
		</div>
	);
	const emptyStatePanel = (
		<Flex vertical gap={16}>
			{importPanel}
			<Card styles={{ body: { padding: 24 } }}>
				<Flex justify='center' align='center' style={{ minHeight: '40vh' }}>
					<Empty description='No bloodwork data found yet.' />
				</Flex>
			</Card>
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
					message='Unable to load bloodwork data'
					description={dashboardQuery.error.message}
				/>
			) : !hasAnyData ? (
				emptyStatePanel
			) : showSplitLayout ? (
				<Splitter
					style={{ height: Math.max(viewport.height - 32, 680) }}
					styles={{
						root: { height: Math.max(viewport.height - 32, 680) },
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
			)}
		</main>
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
