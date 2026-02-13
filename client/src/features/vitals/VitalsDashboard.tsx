import {
    useCallback,
    useDeferredValue,
    useEffect,
    useLayoutEffect,
    useMemo,
    useRef,
    useState,
    type ChangeEvent,
} from 'react';

import { ChartLineUp } from '@phosphor-icons/react';
import { Alert, Empty, Spin } from 'antd';

import { VitalsControls } from './components/VitalsControls';
import { TrendChart } from './components/TrendChart';
import { VitalsTable } from './components/VitalsTable';
import {
    getAllMeasurementRows,
    getCategorySelectionByName,
    getChartSeries,
    getChartSources,
    getDateBounds,
    getFilteredMeasurementRows,
    getMeasurementKeysByCategory,
    getMeasurementOverviewByKey,
    getMeasurementRangesTooltipByKey,
    getOrderedLabs,
    getPrunedSelectedRowKeys,
    getRowsWithVisibleData,
    getSelectedRows,
    getSources,
    getTableRows,
    getTableSources,
    getVisibleSources,
} from './model';
import type {
    ApiResponse,
    BloodworkLab,
    VitalsRowModel,
} from './types';
import {
    clamp,
    GROUP_BY_CATEGORY_STORAGE_KEY,
    MEASUREMENT_COLUMN_WIDTH,
    MIN_CHART_PANE_WIDTH,
    OVERVIEW_COLUMN_WIDTH,
    readStoredGroupByCategory,
    readStoredSelectedRowKeys,
    readStoredStarredMeasurementKeys,
    RESIZER_WIDTH,
    SELECTED_ROWS_STORAGE_KEY,
    SELECTION_COLUMN_WIDTH,
    SOURCE_COLUMN_WIDTH,
    STARRED_MEASUREMENTS_STORAGE_KEY,
} from './utils';

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

export function VitalsDashboard() {
    const viewport = useViewport();
    const isMobileViewport = viewport.width < 900;

    const [labs, setLabs] = useState<BloodworkLab[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [loadError, setLoadError] = useState<string | null>(null);
    const [measurementFilter, setMeasurementFilter] = useState('');
    const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>(() => readStoredSelectedRowKeys());
    const [starredMeasurementKeys, setStarredMeasurementKeys] = useState<string[]>(() => readStoredStarredMeasurementKeys());
    const [dateRangeStart, setDateRangeStart] = useState('');
    const [dateRangeEnd, setDateRangeEnd] = useState('');
    const [groupByCategory, setGroupByCategory] = useState(() => readStoredGroupByCategory());
    const [tablePaneWidth, setTablePaneWidth] = useState(0);
    const [isResizing, setIsResizing] = useState(false);

    const workspaceRef = useRef<HTMLDivElement | null>(null);
    const deferredMeasurementFilter = useDeferredValue(measurementFilter);
    const starredMeasurementSet = useMemo(() => new Set(starredMeasurementKeys), [starredMeasurementKeys]);

    useEffect(() => {
        const controller = new AbortController();

        async function loadLabs() {
            try {
                setIsLoading(true);
                setLoadError(null);
                const response = await fetch('/api/bloodwork', { signal: controller.signal });
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                const payload = await response.json() as ApiResponse;
                setLabs(payload.items ?? []);
            } catch (error) {
                if (controller.signal.aborted) return;
                setLoadError(String(error));
            } finally {
                if (!controller.signal.aborted) {
                    setIsLoading(false);
                }
            }
        }

        loadLabs();
        return () => controller.abort();
    }, []);

    const orderedLabs = useMemo(() => getOrderedLabs(labs), [labs]);
    const sources = useMemo(() => getSources(orderedLabs), [orderedLabs]);
    const availableDates = useMemo(
        () => Array.from(new Set(sources.map(source => source.date))).sort((left, right) => left.localeCompare(right)),
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

    const visibleSources = useMemo(() => getVisibleSources({
        sources,
        dateRangeStart,
        dateRangeEnd,
    }), [dateRangeEnd, dateRangeStart, sources]);

    const allMeasurementRows = useMemo(() => getAllMeasurementRows({
        orderedLabs,
        sourceCount: sources.length,
    }), [orderedLabs, sources.length]);

    useEffect(() => {
        if (typeof window === 'undefined') return;
        window.localStorage.setItem(STARRED_MEASUREMENTS_STORAGE_KEY, JSON.stringify(starredMeasurementKeys));
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
        if (allMeasurementRows.length === 0) {
            return;
        }
        const availableRowIds = new Set(allMeasurementRows.map(item => item.key));
        setStarredMeasurementKeys(previous => {
            const next = previous.filter(item => availableRowIds.has(item));
            return next.length === previous.length ? previous : next;
        });
    }, [allMeasurementRows]);

    const filteredMeasurementRows = useMemo(() => getFilteredMeasurementRows({
        allMeasurementRows,
        measurementFilter: deferredMeasurementFilter,
        starredMeasurementSet,
    }), [allMeasurementRows, deferredMeasurementFilter, starredMeasurementSet]);

    const rowsWithVisibleData = useMemo(() => getRowsWithVisibleData({
        filteredMeasurementRows,
        visibleSources,
    }), [filteredMeasurementRows, visibleSources]);

    useEffect(() => {
        if (allMeasurementRows.length === 0) {
            return;
        }
        setSelectedRowKeys(previous => {
            const next = getPrunedSelectedRowKeys({
                selectedRowKeys: previous,
                filteredMeasurementRows: rowsWithVisibleData,
            });
            return next.length === previous.length ? previous : next;
        });
    }, [rowsWithVisibleData]);

    const selectedRowKeySet = useMemo(
        () => new Set(selectedRowKeys),
        [selectedRowKeys],
    );

    const tableSources = useMemo(() => getTableSources({
        filteredMeasurementRows: rowsWithVisibleData,
        visibleSources,
    }), [rowsWithVisibleData, visibleSources]);

    const tableMeasurementRows = useMemo(() => getRowsWithVisibleData({
        filteredMeasurementRows: rowsWithVisibleData,
        visibleSources: tableSources,
    }), [rowsWithVisibleData, tableSources]);

    const tableRows = useMemo(() => getTableRows({
        filteredMeasurementRows: tableMeasurementRows,
        groupByCategory,
        starredMeasurementSet,
    }), [groupByCategory, starredMeasurementSet, tableMeasurementRows]);

    const measurementKeysByCategory = useMemo(
        () => getMeasurementKeysByCategory({
            filteredMeasurementRows: tableMeasurementRows,
            groupByCategory,
            starredMeasurementSet,
        }),
        [groupByCategory, starredMeasurementSet, tableMeasurementRows],
    );

    const categorySelectionByName = useMemo(() => getCategorySelectionByName({
        measurementKeysByCategory,
        selectedRowKeySet,
    }), [measurementKeysByCategory, selectedRowKeySet]);

    const selectedRows = useMemo(() => getSelectedRows({
        filteredMeasurementRows: tableMeasurementRows,
        selectedRowKeySet,
    }), [selectedRowKeySet, tableMeasurementRows]);

    const chartSources = useMemo(() => getChartSources({
        visibleSources,
        selectedRows,
    }), [selectedRows, visibleSources]);

    const measurementRangesTooltipByKey = useMemo(() => getMeasurementRangesTooltipByKey({
        filteredMeasurementRows: tableMeasurementRows,
        sources,
    }), [sources, tableMeasurementRows]);

    const measurementOverviewByKey = useMemo(() => getMeasurementOverviewByKey({
        filteredMeasurementRows: tableMeasurementRows,
        tableSources,
    }), [tableMeasurementRows, tableSources]);

    const chartSeries = useMemo(() => getChartSeries({
        selectedRows,
        chartSources,
    }), [chartSources, selectedRows]);

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

    const onDateRangeSliderChange = useCallback((nextRange: [number, number]) => {
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
    }, [availableDates]);

    const onGroupByCategoryChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
        setGroupByCategory(event.target.checked);
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

    const onToggleAllRows = useCallback((checked: boolean) => {
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
    }, [tableMeasurementRows]);

    const onToggleCategory = useCallback((category: string, shouldSelect: boolean) => {
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
    }, [measurementKeysByCategory]);

    const onToggleStar = useCallback((measurementKey: string) => {
        setStarredMeasurementKeys(previous => (
            previous.includes(measurementKey)
                ? previous.filter(item => item !== measurementKey)
                : [...previous, measurementKey]
        ));
    }, []);

    const hasAnyData = labs.length > 0;
    const hasSelectedRows = selectedRows.length > 0;
    const showSplitLayout = hasSelectedRows && !isMobileViewport;

    const clampTablePaneWidth = useCallback((nextWidth: number) => {
        const workspace = workspaceRef.current;
        if (!workspace) return nextWidth;

        const totalWidth = workspace.getBoundingClientRect().width;
        const minTablePaneWidth = Math.max(340, Math.min(560, totalWidth * 0.4));
        const maxTablePaneWidth = Math.max(minTablePaneWidth, totalWidth - MIN_CHART_PANE_WIDTH - RESIZER_WIDTH);

        return clamp(nextWidth, minTablePaneWidth, maxTablePaneWidth);
    }, []);

    useLayoutEffect(() => {
        if (!showSplitLayout) return;
        const workspace = workspaceRef.current;
        if (!workspace) return;

        const totalWidth = workspace.getBoundingClientRect().width;
        const preferredWidth = totalWidth * 0.66;

        setTablePaneWidth(previous => clampTablePaneWidth(previous > 0 ? previous : preferredWidth));
    }, [clampTablePaneWidth, showSplitLayout, viewport.width]);

    useEffect(() => {
        if (!showSplitLayout || !isResizing) return;

        const onMouseMove = (event: MouseEvent) => {
            const workspace = workspaceRef.current;
            if (!workspace) return;
            const bounds = workspace.getBoundingClientRect();
            setTablePaneWidth(clampTablePaneWidth(event.clientX - bounds.left));
        };

        const onMouseUp = () => setIsResizing(false);

        window.addEventListener('mousemove', onMouseMove);
        window.addEventListener('mouseup', onMouseUp);

        return () => {
            window.removeEventListener('mousemove', onMouseMove);
            window.removeEventListener('mouseup', onMouseUp);
        };
    }, [clampTablePaneWidth, isResizing, showSplitLayout]);

    useEffect(() => {
        if (!isResizing) return;

        const previousCursor = document.body.style.cursor;
        const previousUserSelect = document.body.style.userSelect;
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';

        return () => {
            document.body.style.cursor = previousCursor;
            document.body.style.userSelect = previousUserSelect;
        };
    }, [isResizing]);

    const tableScrollX = useMemo(
        () => (
            SELECTION_COLUMN_WIDTH +
            MEASUREMENT_COLUMN_WIDTH +
            OVERVIEW_COLUMN_WIDTH +
            (tableSources.length * SOURCE_COLUMN_WIDTH)
        ),
        [tableSources.length],
    );

    const resolvedTablePaneWidth = useMemo(() => {
        if (!showSplitLayout) {
            return 0;
        }
        const workspaceWidth = workspaceRef.current?.getBoundingClientRect().width ?? viewport.width;
        const preferredWidth = tablePaneWidth > 0 ? tablePaneWidth : workspaceWidth * 0.66;
        return clampTablePaneWidth(preferredWidth);
    }, [clampTablePaneWidth, showSplitLayout, tablePaneWidth, viewport.width]);

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

    const chartContent = useMemo(() => (
        chartSeries.length > 0
            ? <TrendChart series={chartSeries} orderedSources={chartSources} />
            : <div className='grid h-full place-items-center p-4'><Empty description='No numeric values in the selected rows for this date range.' /></div>
    ), [chartSeries, chartSources]);

    return (
        <main className='vitals-page'>
            {isLoading ? (
                <section className='grid h-full w-full place-items-center border border-slate-300 bg-white'>
                    <Spin size='large' />
                </section>
            ) : loadError ? (
                <Alert
                    type='error'
                    showIcon
                    title='Unable to load bloodwork data'
                    description={loadError}
                />
            ) : !hasAnyData ? (
                <section className='grid h-full w-full place-items-center border border-slate-300 bg-white'>
                    <Empty description='No bloodwork data found yet.' />
                </section>
            ) : (
                <>
                    <section
                        ref={workspaceRef}
                        className='vitals-workspace'
                        style={{
                            gridTemplateColumns: showSplitLayout
                                ? `${Math.round(resolvedTablePaneWidth)}px ${RESIZER_WIDTH}px minmax(${MIN_CHART_PANE_WIDTH}px, 1fr)`
                                : '1fr',
                        }}
                    >
                        <section className='flex min-h-0 min-w-0 flex-col border border-slate-300 bg-white'>
                            <VitalsControls
                                isMobile={isMobileViewport}
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
                                selectedRowKeySet={selectedRowKeySet}
                                categorySelectionByName={categorySelectionByName}
                                starredMeasurementSet={starredMeasurementSet}
                                measurementOverviewByKey={measurementOverviewByKey}
                                measurementRangesTooltipByKey={measurementRangesTooltipByKey}
                                tableScrollX={tableScrollX}
                                onToggleRow={onToggleRow}
                                onToggleAllRows={onToggleAllRows}
                                onToggleCategory={onToggleCategory}
                                onToggleStar={onToggleStar}
                            />
                        </section>

                        {showSplitLayout && (
                            <div
                                role='separator'
                                aria-label='Resize table and chart panels'
                                aria-orientation='vertical'
                                onMouseDown={event => {
                                    event.preventDefault();
                                    setIsResizing(true);
                                }}
                                className='relative cursor-col-resize bg-slate-300 hover:bg-slate-400'
                            >
                                <span className='absolute bottom-0 left-1/2 top-0 w-[2px] -translate-x-1/2 bg-slate-900/30' />
                            </div>
                        )}

                        {showSplitLayout && (
                            <section className='flex min-h-0 min-w-0 flex-col border border-l-0 border-slate-300 bg-white'>
                                <div className='inline-flex items-center gap-2 border-b border-slate-300 px-3 py-2.5'>
                                    <ChartLineUp size={18} weight='duotone' />
                                    <strong>Trend view</strong>
                                </div>
                                {chartContent}
                            </section>
                        )}
                    </section>

                    {hasSelectedRows && isMobileViewport && (
                        <section className='flex min-h-0 min-w-0 flex-col border border-slate-300 bg-white'>
                            <div className='inline-flex items-center gap-2 border-b border-slate-300 px-3 py-2.5'>
                                <ChartLineUp size={18} weight='duotone' />
                                <strong>Trend view</strong>
                            </div>
                            {chartContent}
                        </section>
                    )}
                </>
            )}
        </main>
    );
}
