import { useCallback, useEffect, useMemo, useRef, useState, type Key } from 'react';

import styled from '@emotion/styled';
import { ChartLineUp, Drop, Flask, Star } from '@phosphor-icons/react';
import { Alert, Button, Empty, Input, Space, Spin, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';

type BloodworkMeasurement = {
    name: string;
    originalName?: string;
    category?: string;
    value?: number | string;
    unit?: string;
    referenceRange?: {
        min?: number;
        max?: number;
    };
    flag?: 'low' | 'high' | 'normal' | 'abnormal' | 'critical' | 'unknown';
    note?: string;
};

type BloodworkLab = {
    date: string;
    labName: string;
    location?: string;
    importLocation?: string;
    weightKg?: number;
    measurements: BloodworkMeasurement[];
    notes?: string;
};

type ApiResponse = {
    items: BloodworkLab[];
};

type SourceColumn = {
    id: string;
    date: string;
    prettyDate: string;
};

type MeasurementCell = {
    display: string;
    numericValue: number | null;
    rangeMin: number | null;
    rangeMax: number | null;
    unit?: string;
    flag?: BloodworkMeasurement['flag'];
    note?: string;
};

type MeasurementRow = {
    key: string;
    measurement: string;
    valuesBySource: Record<string, MeasurementCell | undefined>;
};

type ChartSeries = {
    id: string;
    label: string;
    points: Array<{ sourceId: string; value: number }>;
};

const { Text } = Typography;

const DATE_FORMATTER = new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
});
const RESIZER_WIDTH = 10;
const MIN_CHART_PANE_WIDTH = 300;
const STARRED_MEASUREMENTS_STORAGE_KEY = 'vitals.starred.measurements';

function clamp(value: number, min: number, max: number): number {
    return Math.min(max, Math.max(min, value));
}

function normalizeStarredMeasurementKeys(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    const unique = new Set<string>();

    value.forEach(item => {
        if (typeof item !== 'string') return;
        const normalized = item.trim().toLowerCase();
        if (!normalized) return;
        unique.add(normalized);
    });

    return Array.from(unique);
}

function readStoredStarredMeasurementKeys(): string[] {
    if (typeof window === 'undefined') return [];
    try {
        const raw = window.localStorage.getItem(STARRED_MEASUREMENTS_STORAGE_KEY);
        if (!raw) return [];
        return normalizeStarredMeasurementKeys(JSON.parse(raw));
    } catch {
        return [];
    }
}

function formatNumericLabel(value: number): string {
    const absolute = Math.abs(value);
    const decimals = absolute >= 100 ? 0 : absolute >= 10 ? 1 : 2;
    return value.toFixed(decimals).replace(/\.?0+$/, '');
}

function formatRangeCaption(cell: MeasurementCell): string {
    const unitSuffix = cell.unit ? ` ${cell.unit}` : '';
    const { rangeMin, rangeMax } = cell;
    if (rangeMin !== null && rangeMax !== null) {
        const low = Math.min(rangeMin, rangeMax);
        const high = Math.max(rangeMin, rangeMax);
        return low === high
            ? `ref ${formatNumericLabel(low)}${unitSuffix}`
            : `ref ${formatNumericLabel(low)} - ${formatNumericLabel(high)}${unitSuffix}`;
    }
    if (rangeMin !== null) {
        return `ref >= ${formatNumericLabel(rangeMin)}${unitSuffix}`;
    }
    if (rangeMax !== null) {
        return `ref <= ${formatNumericLabel(rangeMax)}${unitSuffix}`;
    }
    return '';
}

function getRangeVisualization(cell: MeasurementCell): {
    minPosition: number | null;
    maxPosition: number | null;
    valuePosition: number;
} | null {
    if (cell.numericValue === null) return null;
    if (cell.rangeMin === null && cell.rangeMax === null) return null;

    const anchors = [cell.numericValue];
    if (cell.rangeMin !== null) anchors.push(cell.rangeMin);
    if (cell.rangeMax !== null) anchors.push(cell.rangeMax);

    const minAnchor = Math.min(...anchors);
    const maxAnchor = Math.max(...anchors);
    const anchorSpread = maxAnchor - minAnchor;
    const anchorScale = Math.max(...anchors.map(Math.abs), 1);
    const padding = Math.max(anchorSpread * 0.22, anchorScale * 0.06, 0.5);
    const domainMin = minAnchor - padding;
    const domainMax = maxAnchor + padding;
    const domainRange = domainMax - domainMin || 1;
    const toPosition = (value: number) => clamp(((value - domainMin) / domainRange) * 100, 0, 100);

    return {
        minPosition: cell.rangeMin === null ? null : toPosition(cell.rangeMin),
        maxPosition: cell.rangeMax === null ? null : toPosition(cell.rangeMax),
        valuePosition: toPosition(cell.numericValue),
    };
}

function formatPrettyDate(value: string): string {
    const parsed = Date.parse(value);
    if (!Number.isFinite(parsed)) return value;
    return DATE_FORMATTER.format(new Date(parsed));
}

function parseNumericValue(value: number | string | undefined): number | null {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value !== 'string') return null;

    const normalized = value.replace(',', '.').replace(/[^0-9.+-]/g, '').trim();
    if (!normalized) return null;
    const parsed = Number.parseFloat(normalized);
    return Number.isFinite(parsed) ? parsed : null;
}

function formatCell(measurement: BloodworkMeasurement): MeasurementCell {
    const valueText = measurement.value === undefined ? '' : String(measurement.value).trim();
    const unitText = measurement.unit?.trim();
    const display = [valueText, unitText].filter(Boolean).join(' ').trim() || '—';
    const rangeMin = parseNumericValue(measurement.referenceRange?.min);
    const rangeMax = parseNumericValue(measurement.referenceRange?.max);

    return {
        display,
        numericValue: parseNumericValue(measurement.value),
        rangeMin,
        rangeMax,
        unit: unitText || undefined,
        flag: measurement.flag,
        note: measurement.note?.trim() || undefined,
    };
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

function TrendChart({
    series,
    orderedSources,
}: {
    series: ChartSeries[];
    orderedSources: SourceColumn[];
}) {
    const chartWidth = Math.max(780, orderedSources.length * 112);
    const chartHeight = 370;
    const margin = { top: 34, right: 20, bottom: 86, left: 64 };
    const innerWidth = chartWidth - margin.left - margin.right;
    const innerHeight = chartHeight - margin.top - margin.bottom;

    const allValues = series.flatMap(item => item.points.map(point => point.value));
    if (allValues.length === 0) {
        return <Empty description='No numeric values for current selection.' />;
    }

    const minValue = Math.min(...allValues);
    const maxValue = Math.max(...allValues);
    const range = maxValue - minValue || 1;
    const paddedMin = minValue - range * 0.08;
    const paddedMax = maxValue + range * 0.08;

    const sourceIds = orderedSources.map(item => item.id);
    const xStep = sourceIds.length > 1 ? innerWidth / (sourceIds.length - 1) : 0;

    const getX = (sourceId: string) => {
        const index = sourceIds.indexOf(sourceId);
        return margin.left + (index < 0 ? 0 : index) * xStep;
    };
    const getY = (value: number) =>
        margin.top + innerHeight - ((value - paddedMin) / (paddedMax - paddedMin)) * innerHeight;

    const palette = ['#0f172a', '#2563eb', '#b91c1c', '#15803d', '#9333ea', '#ca8a04'];
    const yTicks = 5;
    const yTickValues = Array.from({ length: yTicks + 1 }, (_, index) => {
        const ratio = index / yTicks;
        return paddedMax - ratio * (paddedMax - paddedMin);
    });

    return (
        <ChartShell>
            <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} role='img' aria-label='Selected vitals trends'>
                {yTickValues.map(tick => (
                    <g key={`y-${tick}`}>
                        <line
                            x1={margin.left}
                            x2={margin.left + innerWidth}
                            y1={getY(tick)}
                            y2={getY(tick)}
                            stroke='rgba(15, 23, 42, 0.14)'
                            strokeWidth={1}
                        />
                        <text
                            x={margin.left - 10}
                            y={getY(tick) + 4}
                            textAnchor='end'
                            fontSize='12'
                            fill='rgba(15, 23, 42, 0.75)'
                        >
                            {tick.toFixed(1)}
                        </text>
                    </g>
                ))}

                {orderedSources.map(item => (
                    <g key={`x-${item.id}`}>
                        <line
                            x1={getX(item.id)}
                            x2={getX(item.id)}
                            y1={margin.top}
                            y2={margin.top + innerHeight}
                            stroke='rgba(15, 23, 42, 0.08)'
                            strokeWidth={1}
                        />
                        <text
                            x={getX(item.id)}
                            y={margin.top + innerHeight + 24}
                            textAnchor='end'
                            transform={`rotate(-26 ${getX(item.id)} ${margin.top + innerHeight + 24})`}
                            fontSize='11'
                            fill='rgba(15, 23, 42, 0.8)'
                        >
                            {item.prettyDate}
                        </text>
                    </g>
                ))}

                {series.map((item, index) => {
                    const color = palette[index % palette.length];
                    const points = item.points.map(point => `${getX(point.sourceId)},${getY(point.value)}`).join(' ');

                    return (
                        <g key={item.id}>
                            <polyline
                                points={points}
                                fill='none'
                                stroke={color}
                                strokeWidth={2.8}
                                strokeLinecap='round'
                                strokeLinejoin='round'
                            />
                            {item.points.map(point => (
                                <circle
                                    key={`${item.id}-${point.sourceId}`}
                                    cx={getX(point.sourceId)}
                                    cy={getY(point.value)}
                                    r={4.4}
                                    fill={color}
                                    stroke='#f8fafc'
                                    strokeWidth={1.8}
                                />
                            ))}
                        </g>
                    );
                })}
            </svg>

            <LegendGrid>
                {series.map((item, index) => (
                    <LegendItem key={item.id}>
                        <LegendDot style={{ background: palette[index % palette.length] }} />
                        <Text>{item.label}</Text>
                    </LegendItem>
                ))}
            </LegendGrid>
        </ChartShell>
    );
}

export default function App() {
    const viewport = useViewport();
    const isMobileViewport = viewport.width < 900;

    const [labs, setLabs] = useState<BloodworkLab[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [loadError, setLoadError] = useState<string | null>(null);
    const [measurementFilter, setMeasurementFilter] = useState('');
    const [selectedRowKeys, setSelectedRowKeys] = useState<Key[]>([]);
    const [starredMeasurementKeys, setStarredMeasurementKeys] = useState<string[]>(() => readStoredStarredMeasurementKeys());
    const [dateRangeStart, setDateRangeStart] = useState('');
    const [dateRangeEnd, setDateRangeEnd] = useState('');
    const [tablePaneWidth, setTablePaneWidth] = useState(0);
    const [isResizing, setIsResizing] = useState(false);

    const workspaceRef = useRef<HTMLDivElement | null>(null);
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

    const orderedLabs = useMemo(
        () => [...labs].sort((left, right) => right.date.localeCompare(left.date)),
        [labs],
    );

    const sources = useMemo<SourceColumn[]>(
        () =>
            orderedLabs.map((lab, index) => ({
                id: `${lab.date}__${lab.labName}__${index}`,
                date: lab.date,
                prettyDate: formatPrettyDate(lab.date),
            })),
        [orderedLabs],
    );

    const dateBounds = useMemo(() => {
        if (sources.length === 0) return { min: '', max: '' };
        const sortedDates = sources.map(item => item.date).sort((left, right) => left.localeCompare(right));
        return {
            min: sortedDates[0] ?? '',
            max: sortedDates[sortedDates.length - 1] ?? '',
        };
    }, [sources]);

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

    const visibleSources = useMemo(
        () =>
            sources.filter(item => {
                if (dateRangeStart && item.date < dateRangeStart) return false;
                if (dateRangeEnd && item.date > dateRangeEnd) return false;
                return true;
            }),
        [dateRangeEnd, dateRangeStart, sources],
    );

    const allRows = useMemo<MeasurementRow[]>(() => {
        const grouped = new Map<string, MeasurementRow>();

        orderedLabs.forEach((lab, labIndex) => {
            const sourceId = sources[labIndex]?.id;
            if (!sourceId) return;

            lab.measurements.forEach(measurement => {
                const key = measurement.name.trim().toLowerCase();
                const existing = grouped.get(key);
                if (existing) {
                    existing.valuesBySource[sourceId] = formatCell(measurement);
                    return;
                }

                grouped.set(key, {
                    key,
                    measurement: measurement.name,
                    valuesBySource: {
                        [sourceId]: formatCell(measurement),
                    },
                });
            });
        });

        return Array.from(grouped.values()).sort((left, right) => left.measurement.localeCompare(right.measurement));
    }, [orderedLabs, sources]);

    useEffect(() => {
        if (typeof window === 'undefined') return;
        window.localStorage.setItem(STARRED_MEASUREMENTS_STORAGE_KEY, JSON.stringify(starredMeasurementKeys));
    }, [starredMeasurementKeys]);

    useEffect(() => {
        const availableRowIds = new Set(allRows.map(item => item.key));
        setStarredMeasurementKeys(previous => {
            const next = previous.filter(item => availableRowIds.has(item));
            return next.length === previous.length ? previous : next;
        });
    }, [allRows]);

    const toggleMeasurementStar = useCallback((measurementKey: string) => {
        setStarredMeasurementKeys(previous =>
            previous.includes(measurementKey)
                ? previous.filter(item => item !== measurementKey)
                : [...previous, measurementKey],
        );
    }, []);

    const filteredRows = useMemo(() => {
        const normalizedFilter = measurementFilter.trim().toLowerCase();
        const candidateRows = normalizedFilter
            ? allRows.filter(row => row.measurement.toLowerCase().includes(normalizedFilter))
            : allRows;
        return [...candidateRows].sort((left, right) => {
            const leftIsStarred = starredMeasurementSet.has(left.key);
            const rightIsStarred = starredMeasurementSet.has(right.key);
            if (leftIsStarred !== rightIsStarred) return leftIsStarred ? -1 : 1;
            return left.measurement.localeCompare(right.measurement);
        });
    }, [allRows, measurementFilter, starredMeasurementSet]);

    useEffect(() => {
        const availableRowIds = new Set(filteredRows.map(item => item.key));
        setSelectedRowKeys(previous => previous.filter(item => availableRowIds.has(String(item))));
    }, [filteredRows]);

    const selectedRows = filteredRows.filter(row => selectedRowKeys.includes(row.key));
    const chartSeries = useMemo<ChartSeries[]>(
        () =>
            selectedRows
                .map(row => ({
                    id: row.key,
                    label: row.measurement,
                    points: visibleSources
                        .map(source => {
                            const cell = row.valuesBySource[source.id];
                            if (!cell || cell.numericValue === null) return null;
                            return {
                                sourceId: source.id,
                                value: cell.numericValue,
                            };
                        })
                        .filter((point): point is { sourceId: string; value: number } => !!point),
                }))
                .filter(series => series.points.length > 0),
        [selectedRows, visibleSources],
    );

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

    const tableColumns = useMemo<ColumnsType<MeasurementRow>>(() => {
        const measurementColumn: ColumnsType<MeasurementRow>[number] = {
            title: (
                <Space size={6}>
                    <Flask size={16} weight='duotone' />
                    <span>Measurement</span>
                </Space>
            ),
            dataIndex: 'measurement',
            key: 'measurement',
            width: 250,
            fixed: isMobileViewport ? false : 'left',
            render: (value, row) => {
                const isStarred = starredMeasurementSet.has(row.key);
                return (
                    <MeasurementNameCell>
                        <StarToggle
                            type='button'
                            active={isStarred}
                            aria-pressed={isStarred}
                            aria-label={isStarred ? `Unstar ${value}` : `Star ${value}`}
                            onClick={event => {
                                event.preventDefault();
                                event.stopPropagation();
                                toggleMeasurementStar(row.key);
                            }}
                        >
                            <Star size={14} weight={isStarred ? 'fill' : 'regular'} />
                        </StarToggle>
                        <Text strong={isStarred}>{value}</Text>
                    </MeasurementNameCell>
                );
            },
        };

        const sourceColumns: ColumnsType<MeasurementRow> = visibleSources.map(source => ({
            title: <Text>{source.prettyDate}</Text>,
            key: source.id,
            width: 164,
            render: (_, row) => {
                const cell = row.valuesBySource[source.id];
                if (!cell) return <Text type='secondary'>—</Text>;

                const rangeVisualization = getRangeVisualization(cell);
                const rangeCaption = formatRangeCaption(cell);
                const hasBand =
                    rangeVisualization?.minPosition !== null &&
                    rangeVisualization?.maxPosition !== null;
                const bandLeft = hasBand
                    ? Math.min(
                        rangeVisualization?.minPosition ?? 0,
                        rangeVisualization?.maxPosition ?? 0,
                    )
                    : 0;
                const bandWidth = hasBand
                    ? Math.abs(
                        (rangeVisualization?.maxPosition ?? 0) -
                        (rangeVisualization?.minPosition ?? 0),
                    )
                    : 0;

                return (
                    <CellValueStack>
                        <CellPrimaryRow>
                            <Text>{cell.display}</Text>
                        </CellPrimaryRow>
                        {rangeVisualization && rangeCaption && (
                            <RangeVisualizationShell>
                                <RangeTrack>
                                    {hasBand && (
                                        <RangeBand
                                            style={{
                                                left: `${bandLeft}%`,
                                                width: `${bandWidth}%`,
                                            }}
                                        />
                                    )}
                                    {rangeVisualization.minPosition !== null && (
                                        <RangeMarker style={{ left: `${rangeVisualization.minPosition}%` }} />
                                    )}
                                    {rangeVisualization.maxPosition !== null && (
                                        <RangeMarker style={{ left: `${rangeVisualization.maxPosition}%` }} />
                                    )}
                                    <ValueMarker style={{ left: `${rangeVisualization.valuePosition}%` }} />
                                </RangeTrack>
                                <RangeCaption>{rangeCaption}</RangeCaption>
                            </RangeVisualizationShell>
                        )}
                        {cell.flag && cell.flag !== 'normal' && (
                            <Tag color={cell.flag === 'high' || cell.flag === 'critical' ? 'red' : 'orange'}>
                                {cell.flag}
                            </Tag>
                        )}
                    </CellValueStack>
                );
            },
        }));

        return [measurementColumn, ...sourceColumns];
    }, [isMobileViewport, starredMeasurementSet, toggleMeasurementStar, visibleSources]);

    const tableScrollY = isMobileViewport
        ? 'calc(100dvh - 178px)'
        : 'calc(100dvh - 132px)';

    const resetRange = () => {
        setDateRangeStart(dateBounds.min);
        setDateRangeEnd(dateBounds.max);
    };

    const isRangeResetDisabled =
        !dateBounds.min ||
        !dateBounds.max ||
        (dateRangeStart === dateBounds.min && dateRangeEnd === dateBounds.max);

    const chartContent = chartSeries.length > 0
        ? <TrendChart series={chartSeries} orderedSources={visibleSources} />
        : <Empty description='No numeric values in the selected rows for this date range.' />;

    return (
        <Page mobile={isMobileViewport}>
            {isLoading ? (
                <StatusShell>
                    <Spin size='large' />
                </StatusShell>
            ) : loadError ? (
                <Alert
                    type='error'
                    showIcon
                    message='Unable to load bloodwork data'
                    description={loadError}
                />
            ) : !hasAnyData ? (
                <StatusShell>
                    <Empty description='No bloodwork data found yet.' />
                </StatusShell>
            ) : (
                <>
                    <Workspace
                        ref={workspaceRef}
                        split={showSplitLayout}
                        tablePaneWidth={showSplitLayout ? tablePaneWidth : 0}
                    >
                        <TablePane>
                            <ControlsRow mobile={isMobileViewport}>
                                <Input
                                    prefix={<Drop size={16} />}
                                    placeholder='Filter measurements'
                                    value={measurementFilter}
                                    onChange={event => setMeasurementFilter(event.target.value)}
                                    allowClear
                                />
                                <DateRangeInputs>
                                    <DateInput
                                        type='date'
                                        value={dateRangeStart}
                                        min={dateBounds.min || undefined}
                                        max={dateRangeEnd || dateBounds.max || undefined}
                                        onChange={event => {
                                            const nextStart = event.target.value;
                                            setDateRangeStart(nextStart);
                                            if (dateRangeEnd && nextStart && nextStart > dateRangeEnd) {
                                                setDateRangeEnd(nextStart);
                                            }
                                        }}
                                    />
                                    <RangeDivider>to</RangeDivider>
                                    <DateInput
                                        type='date'
                                        value={dateRangeEnd}
                                        min={dateRangeStart || dateBounds.min || undefined}
                                        max={dateBounds.max || undefined}
                                        onChange={event => {
                                            const nextEnd = event.target.value;
                                            setDateRangeEnd(nextEnd);
                                            if (dateRangeStart && nextEnd && nextEnd < dateRangeStart) {
                                                setDateRangeStart(nextEnd);
                                            }
                                        }}
                                    />
                                </DateRangeInputs>
                                <Button onClick={resetRange} disabled={isRangeResetDisabled}>
                                    All dates
                                </Button>
                            </ControlsRow>

                            <TableShell>
                                <Table<MeasurementRow>
                                    bordered
                                    size='small'
                                    rowKey='key'
                                    dataSource={filteredRows}
                                    columns={tableColumns}
                                    pagination={false}
                                    scroll={{ x: 'max-content', y: tableScrollY }}
                                    rowSelection={{
                                        selectedRowKeys,
                                        onChange: keys => setSelectedRowKeys(keys),
                                    }}
                                />
                            </TableShell>
                        </TablePane>

                        {showSplitLayout && (
                            <ResizeHandle
                                role='separator'
                                aria-label='Resize table and chart panels'
                                aria-orientation='vertical'
                                onMouseDown={event => {
                                    event.preventDefault();
                                    setIsResizing(true);
                                }}
                            />
                        )}

                        {showSplitLayout && (
                            <ChartPane>
                                <ChartHeader>
                                    <ChartLineUp size={18} weight='duotone' />
                                    <Text strong>Trend view</Text>
                                </ChartHeader>
                                {chartContent}
                            </ChartPane>
                        )}
                    </Workspace>

                    {hasSelectedRows && isMobileViewport && (
                        <MobileChartPane>
                            <ChartHeader>
                                <ChartLineUp size={18} weight='duotone' />
                                <Text strong>Trend view</Text>
                            </ChartHeader>
                            {chartContent}
                        </MobileChartPane>
                    )}
                </>
            )}
        </Page>
    );
}

const Page = styled.main<{ mobile: boolean }>`
    width: 100%;
    height: 100dvh;
    display: flex;
    flex-direction: column;
    gap: ${({ mobile }) => (mobile ? '0' : '8px')};
    padding: ${({ mobile }) => (mobile ? '0' : '8px')};
    background: #eef2f7;
    color: #111827;
    overflow: hidden;
`;

const StatusShell = styled.section`
    width: 100%;
    height: 100%;
    display: grid;
    place-items: center;
    border: 1px solid #d7dde7;
    background: #ffffff;
`;

const Workspace = styled.section<{ split: boolean; tablePaneWidth: number }>`
    width: 100%;
    height: 100%;
    min-height: 0;
    display: grid;
    grid-template-columns: ${({ split, tablePaneWidth }) =>
        split
            ? `${Math.round(tablePaneWidth)}px ${RESIZER_WIDTH}px minmax(${MIN_CHART_PANE_WIDTH}px, 1fr)`
            : '1fr'};
`;

const TablePane = styled.section`
    min-width: 0;
    min-height: 0;
    display: flex;
    flex-direction: column;
    border: 1px solid #d0d8e3;
    background: #ffffff;
`;

const ControlsRow = styled.div<{ mobile: boolean }>`
    display: grid;
    grid-template-columns: ${({ mobile }) => (mobile ? '1fr' : 'minmax(260px, 1fr) auto auto')};
    gap: 8px;
    align-items: center;
    padding: 8px;
    border-bottom: 1px solid #d0d8e3;
`;

const DateRangeInputs = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 6px;
`;

const DateInput = styled.input`
    height: 32px;
    border: 1px solid #c8d0dc;
    border-radius: 2px;
    padding: 0 8px;
    font: inherit;
    color: #0f172a;
    background: #ffffff;
`;

const RangeDivider = styled.span`
    font-size: 12px;
    color: #475569;
    text-transform: uppercase;
    letter-spacing: 0.04em;
`;

const TableShell = styled.div`
    flex: 1;
    min-height: 0;

    .ant-table-wrapper,
    .ant-spin-nested-loading,
    .ant-spin-container,
    .ant-table,
    .ant-table-container {
        height: 100%;
    }

    .ant-table-thead > tr > th {
        padding: 8px;
        background: #f8fafc;
        border-bottom: 1px solid #d7dde7;
    }

    .ant-table-tbody > tr > td {
        padding: 7px 8px;
    }

    .ant-table,
    .ant-table-container {
        border-radius: 0;
    }
`;

const MeasurementNameCell = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 7px;
    min-width: 0;
`;

const StarToggle = styled.button<{ active: boolean }>`
    width: 20px;
    height: 20px;
    display: grid;
    place-items: center;
    border: 1px solid transparent;
    border-radius: 2px;
    padding: 0;
    color: ${({ active }) => (active ? '#d97706' : '#94a3b8')};
    background: transparent;
    cursor: pointer;

    &:hover {
        color: ${({ active }) => (active ? '#b45309' : '#334155')};
        border-color: #cbd5e1;
        background: #f8fafc;
    }

    svg {
        display: block;
    }
`;

const CellValueStack = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const CellPrimaryRow = styled.div`
    min-height: 18px;
`;

const RangeVisualizationShell = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

const RangeTrack = styled.div`
    position: relative;
    height: 10px;
    border: 1px solid #cbd5e1;
    background: #e5e7eb;
`;

const RangeBand = styled.div`
    position: absolute;
    top: 1px;
    bottom: 1px;
    background: #c7d2fe;
`;

const RangeMarker = styled.span`
    position: absolute;
    top: -3px;
    bottom: -3px;
    width: 2px;
    background: #334155;
    transform: translateX(-50%);
`;

const ValueMarker = styled.span`
    position: absolute;
    top: 50%;
    width: 8px;
    height: 8px;
    border: 1px solid #f8fafc;
    background: #b91c1c;
    transform: translate(-50%, -50%);
`;

const RangeCaption = styled.span`
    font-size: 10px;
    line-height: 1.2;
    color: #475569;
`;

const ResizeHandle = styled.div`
    cursor: col-resize;
    background: #d4dae4;
    position: relative;

    &:hover {
        background: #94a3b8;
    }

    &::after {
        content: '';
        position: absolute;
        top: 0;
        bottom: 0;
        left: 50%;
        width: 2px;
        transform: translateX(-50%);
        background: rgba(15, 23, 42, 0.28);
    }
`;

const ChartPane = styled.section`
    min-width: 0;
    min-height: 0;
    display: flex;
    flex-direction: column;
    border: 1px solid #d0d8e3;
    border-left: none;
    background: #ffffff;
`;

const MobileChartPane = styled.section`
    min-width: 0;
    min-height: 0;
    display: flex;
    flex-direction: column;
    border: 1px solid #d0d8e3;
    background: #ffffff;
`;

const ChartHeader = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 10px 12px;
    border-bottom: 1px solid #d0d8e3;
`;

const ChartShell = styled.div`
    width: 100%;
    overflow-x: auto;
    padding: 12px;
`;

const LegendGrid = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    padding: 0 6px 4px;
`;

const LegendItem = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 3px 7px;
    border: 1px solid #d4dbe6;
`;

const LegendDot = styled.span`
    width: 9px;
    height: 9px;
`;
