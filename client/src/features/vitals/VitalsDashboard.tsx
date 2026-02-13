import {
    useCallback,
    useDeferredValue,
    useEffect,
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
    getSelectedRows,
    getSources,
    getTableRows,
    getTableSources,
    getVisibleSources,
} from './model';
import type {
    ApiResponse,
    BloodworkLab,
} from './types';
import {
    clamp,
    GROUP_BY_CATEGORY_STORAGE_KEY,
    MEASUREMENT_COLUMN_WIDTH,
    MIN_CHART_PANE_WIDTH,
    OVERVIEW_COLUMN_WIDTH,
    readStoredGroupByCategory,
    readStoredStarredMeasurementKeys,
    RESIZER_WIDTH,
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
    const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]);
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

    const visibleSources = useMemo(() => getVisibleSources({
        sources,
        dateRangeStart,
        dateRangeEnd,
    }), [dateRangeEnd, dateRangeStart, sources]);

    const chartSources = useMemo(() => getChartSources(visibleSources), [visibleSources]);

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

    useEffect(() => {
        setSelectedRowKeys(previous => {
            const next = getPrunedSelectedRowKeys({
                selectedRowKeys: previous,
                filteredMeasurementRows,
            });
            return next.length === previous.length ? previous : next;
        });
    }, [filteredMeasurementRows]);

    const selectedRowKeySet = useMemo(
        () => new Set(selectedRowKeys),
        [selectedRowKeys],
    );

    const tableSources = useMemo(() => getTableSources({
        filteredMeasurementRows,
        visibleSources,
        measurementFilter: deferredMeasurementFilter,
    }), [deferredMeasurementFilter, filteredMeasurementRows, visibleSources]);

    const tableRows = useMemo(() => getTableRows({
        filteredMeasurementRows,
        groupByCategory,
    }), [filteredMeasurementRows, groupByCategory]);

    const measurementKeysByCategory = useMemo(
        () => getMeasurementKeysByCategory(filteredMeasurementRows),
        [filteredMeasurementRows],
    );

    const categorySelectionByName = useMemo(() => getCategorySelectionByName({
        measurementKeysByCategory,
        selectedRowKeySet,
    }), [measurementKeysByCategory, selectedRowKeySet]);

    const selectedRows = useMemo(() => getSelectedRows({
        filteredMeasurementRows,
        selectedRowKeySet,
    }), [filteredMeasurementRows, selectedRowKeySet]);

    const measurementRangesTooltipByKey = useMemo(() => getMeasurementRangesTooltipByKey({
        filteredMeasurementRows,
        sources,
    }), [filteredMeasurementRows, sources]);

    const measurementOverviewByKey = useMemo(() => getMeasurementOverviewByKey({
        filteredMeasurementRows,
        tableSources,
    }), [filteredMeasurementRows, tableSources]);

    const chartSeries = useMemo(() => getChartSeries({
        selectedRows,
        chartSources,
    }), [chartSources, selectedRows]);

    const onMeasurementFilterChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
        setMeasurementFilter(event.target.value);
    }, []);

    const onDateRangeStartChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
        const nextStart = event.target.value;
        setDateRangeStart(nextStart);
        if (dateRangeEnd && nextStart && nextStart > dateRangeEnd) {
            setDateRangeEnd(nextStart);
        }
    }, [dateRangeEnd]);

    const onDateRangeEndChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
        const nextEnd = event.target.value;
        setDateRangeEnd(nextEnd);
        if (dateRangeStart && nextEnd && nextEnd < dateRangeStart) {
            setDateRangeStart(nextEnd);
        }
    }, [dateRangeStart]);

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
            const visibleMeasurementKeys = filteredMeasurementRows.map(row => row.key);
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
    }, [filteredMeasurementRows]);

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
    const hasSelectedRows = selectedRowKeys.length > 0;
    const showSplitLayout = hasSelectedRows && !isMobileViewport;

    const clampTablePaneWidth = useCallback((nextWidth: number) => {
        const workspace = workspaceRef.current;
        if (!workspace) return nextWidth;

        const totalWidth = workspace.getBoundingClientRect().width;
        const minTablePaneWidth = Math.max(340, Math.min(560, totalWidth * 0.4));
        const maxTablePaneWidth = Math.max(minTablePaneWidth, totalWidth - MIN_CHART_PANE_WIDTH - RESIZER_WIDTH);

        return clamp(nextWidth, minTablePaneWidth, maxTablePaneWidth);
    }, []);

    useEffect(() => {
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

    const tableScrollY = useMemo(
        () => Math.max(240, isMobileViewport ? viewport.height - 178 : viewport.height - 132),
        [isMobileViewport, viewport.height],
    );

    const tableScrollX = useMemo(
        () => (
            SELECTION_COLUMN_WIDTH +
            MEASUREMENT_COLUMN_WIDTH +
            OVERVIEW_COLUMN_WIDTH +
            (tableSources.length * SOURCE_COLUMN_WIDTH)
        ),
        [tableSources.length],
    );

    const onResetRange = useCallback(() => {
        setDateRangeStart(dateBounds.min);
        setDateRangeEnd(dateBounds.max);
    }, [dateBounds.max, dateBounds.min]);

    const isRangeResetDisabled = useMemo(
        () => (
            !dateBounds.min ||
            !dateBounds.max ||
            (dateRangeStart === dateBounds.min && dateRangeEnd === dateBounds.max)
        ),
        [dateBounds.max, dateBounds.min, dateRangeEnd, dateRangeStart],
    );

    const chartContent = useMemo(() => (
        chartSeries.length > 0
            ? <TrendChart series={chartSeries} orderedSources={chartSources} />
            : <div className='grid h-full place-items-center p-4'><Empty description='No numeric values in the selected rows for this date range.' /></div>
    ), [chartSeries, chartSources]);

    return (
        <main className='vitals-page gap-2 p-2 max-[899px]:gap-0 max-[899px]:p-0'>
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
                                ? `${Math.round(tablePaneWidth)}px ${RESIZER_WIDTH}px minmax(${MIN_CHART_PANE_WIDTH}px, 1fr)`
                                : '1fr',
                        }}
                    >
                        <section className='flex min-h-0 min-w-0 flex-col border border-slate-300 bg-white'>
                            <VitalsControls
                                isMobile={isMobileViewport}
                                measurementFilter={measurementFilter}
                                onMeasurementFilterChange={onMeasurementFilterChange}
                                dateRangeStart={dateRangeStart}
                                dateRangeEnd={dateRangeEnd}
                                dateBounds={dateBounds}
                                onDateRangeStartChange={onDateRangeStartChange}
                                onDateRangeEndChange={onDateRangeEndChange}
                                onResetRange={onResetRange}
                                isRangeResetDisabled={isRangeResetDisabled}
                                groupByCategory={groupByCategory}
                                onGroupByCategoryChange={onGroupByCategoryChange}
                            />

                            <VitalsTable
                                rows={tableRows}
                                tableSources={tableSources}
                                selectedRowKeySet={selectedRowKeySet}
                                categorySelectionByName={categorySelectionByName}
                                starredMeasurementSet={starredMeasurementSet}
                                measurementOverviewByKey={measurementOverviewByKey}
                                measurementRangesTooltipByKey={measurementRangesTooltipByKey}
                                tableScrollY={tableScrollY}
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
