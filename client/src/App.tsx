import { memo, startTransition, useCallback, useDeferredValue, useEffect, useMemo, useRef, useState, type Key } from 'react';

import styled from '@emotion/styled';
import { ChartLineUp, Drop, Flask, Star } from '@phosphor-icons/react';
import { Alert, Button, Checkbox, Empty, Input, Space, Spin, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import {
    CartesianGrid,
    Legend,
    Line,
    LineChart,
    ReferenceLine,
    ResponsiveContainer,
    Tooltip,
    XAxis,
    YAxis,
} from 'recharts';

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
    rangeCaption: string;
    rangeVisualization: {
        minPosition: number | null;
        maxPosition: number | null;
        valuePosition: number;
    } | null;
    rangeBandLeft: number;
    rangeBandWidth: number;
    unit?: string;
    flag?: BloodworkMeasurement['flag'];
    note?: string;
};

type MeasurementDataRow = {
    key: string;
    rowType: 'measurement';
    measurement: string;
    category: string;
    valuesBySource: Record<string, MeasurementCell | undefined>;
};

type CategoryRow = {
    key: string;
    rowType: 'category';
    measurement: string;
    category: string;
    categoryCount: number;
    valuesBySource: Record<string, MeasurementCell | undefined>;
};

type TableRow = MeasurementDataRow | CategoryRow;

type ChartSeries = {
    id: string;
    chartKey: string;
    label: string;
    color: string;
    valuesBySource: Record<string, MeasurementCell | undefined>;
    normalizedValuesBySource: Record<string, number | null>;
    outOfRangeBySource: Record<string, boolean>;
    unitLabel?: string;
};

type TrendChartDatum = {
    sourceId: string;
    prettyDate: string;
    [key: string]: string | number | boolean | null;
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
const UNCATEGORIZED_CATEGORY_LABEL = 'Uncategorized';
const CHART_PALETTE = ['#0f172a', '#2563eb', '#b91c1c', '#15803d', '#9333ea', '#ca8a04'];

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

function formatRangeCaption({
    rangeMin,
    rangeMax,
    unit,
}: {
    rangeMin: number | null;
    rangeMax: number | null;
    unit?: string;
}): string {
    const unitSuffix = unit ? ` ${unit}` : '';
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

function getRangeVisualization({
    numericValue,
    rangeMin,
    rangeMax,
}: {
    numericValue: number | null;
    rangeMin: number | null;
    rangeMax: number | null;
}): {
    minPosition: number | null;
    maxPosition: number | null;
    valuePosition: number;
} | null {
    if (numericValue === null) return null;
    if (rangeMin === null && rangeMax === null) return null;

    const anchors = [numericValue];
    if (rangeMin !== null) anchors.push(rangeMin);
    if (rangeMax !== null) anchors.push(rangeMax);

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
        minPosition: rangeMin === null ? null : toPosition(rangeMin),
        maxPosition: rangeMax === null ? null : toPosition(rangeMax),
        valuePosition: toPosition(numericValue),
    };
}

function formatPrettyDate(value: string): string {
    const parsed = Date.parse(value);
    if (!Number.isFinite(parsed)) return value;
    return DATE_FORMATTER.format(new Date(parsed));
}

function normalizeCategoryLabel(value: string | undefined): string {
    const trimmed = value?.trim();
    return trimmed || UNCATEGORIZED_CATEGORY_LABEL;
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
    const numericValue = parseNumericValue(measurement.value);
    const rangeVisualization = getRangeVisualization({
        numericValue,
        rangeMin,
        rangeMax,
    });
    const hasBand =
        rangeVisualization?.minPosition !== null &&
        rangeVisualization?.maxPosition !== null;
    const rangeBandLeft = hasBand
        ? Math.min(
            rangeVisualization?.minPosition ?? 0,
            rangeVisualization?.maxPosition ?? 0,
        )
        : 0;
    const rangeBandWidth = hasBand
        ? Math.abs(
            (rangeVisualization?.maxPosition ?? 0) -
            (rangeVisualization?.minPosition ?? 0),
        )
        : 0;
    const rangeCaption = formatRangeCaption({
        rangeMin,
        rangeMax,
        unit: unitText || undefined,
    });

    return {
        display,
        numericValue,
        rangeMin,
        rangeMax,
        rangeCaption,
        rangeVisualization,
        rangeBandLeft,
        rangeBandWidth,
        unit: unitText || undefined,
        flag: measurement.flag,
        note: measurement.note?.trim() || undefined,
    };
}

const MeasurementValueCell = memo(function MeasurementValueCell({ cell }: { cell: MeasurementCell }) {
    const rangeVisualization = cell.rangeVisualization;
    const rangeCaption = cell.rangeCaption;
    const hasBand =
        rangeVisualization?.minPosition !== null &&
        rangeVisualization?.maxPosition !== null;

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
                                    left: `${cell.rangeBandLeft}%`,
                                    width: `${cell.rangeBandWidth}%`,
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
});

function resolveSeriesUnitLabel(cells: Array<MeasurementCell | undefined>): string | undefined {
    const normalizedUnits = new Set(
        cells
            .map(cell => cell?.unit?.trim())
            .filter((unit): unit is string => Boolean(unit)),
    );
    if (normalizedUnits.size === 0) {
        return undefined;
    }
    if (normalizedUnits.size === 1) {
        return Array.from(normalizedUnits)[0];
    }
    return 'mixed units';
}

function getRowDefaultRange(cells: Array<MeasurementCell | undefined>): { min: number; max: number } | null {
    for (const cell of cells) {
        if (!cell) continue;
        if (cell.rangeMin === null || cell.rangeMax === null) continue;
        if (cell.rangeMax <= cell.rangeMin) continue;
        return {
            min: cell.rangeMin,
            max: cell.rangeMax,
        };
    }
    return null;
}

function getRowObservedBounds(cells: Array<MeasurementCell | undefined>): { min: number; max: number } | null {
    const values = cells
        .map(cell => cell?.numericValue)
        .filter((value): value is number => value !== null && Number.isFinite(value));
    if (values.length === 0) {
        return null;
    }
    return {
        min: Math.min(...values),
        max: Math.max(...values),
    };
}

function normalizeCellForChart({
    cell,
    defaultRange,
    observedBounds,
}: {
    cell: MeasurementCell | undefined;
    defaultRange: { min: number; max: number } | null;
    observedBounds: { min: number; max: number } | null;
}): number | null {
    if (!cell || cell.numericValue === null) {
        return null;
    }

    let rangeMin = cell.rangeMin;
    let rangeMax = cell.rangeMax;
    if (
        (rangeMin === null || rangeMax === null || rangeMax <= rangeMin) &&
        defaultRange
    ) {
        rangeMin = defaultRange.min;
        rangeMax = defaultRange.max;
    }

    if (rangeMin !== null && rangeMax !== null && rangeMax > rangeMin) {
        return (cell.numericValue - rangeMin) / (rangeMax - rangeMin);
    }

    if (!observedBounds) {
        return 0.5;
    }

    const spread = observedBounds.max - observedBounds.min;
    if (spread === 0) {
        return 0.5;
    }
    return (cell.numericValue - observedBounds.min) / spread;
}

function isCellOutsideReferenceRange(cell: MeasurementCell | undefined): boolean {
    if (!cell || cell.numericValue === null) {
        return false;
    }
    if (cell.rangeMin !== null && cell.numericValue < cell.rangeMin) {
        return true;
    }
    if (cell.rangeMax !== null && cell.numericValue > cell.rangeMax) {
        return true;
    }
    return false;
}

function formatNormalizedYAxisTick(value: number): string {
    if (Math.abs(value) < 0.001) {
        return 'Low';
    }
    if (Math.abs(value - 1) < 0.001) {
        return 'High';
    }
    return value.toFixed(2);
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
    const chartData = useMemo<TrendChartDatum[]>(
        () =>
            orderedSources.map(source => {
                const datum: TrendChartDatum = {
                    sourceId: source.id,
                    prettyDate: source.prettyDate,
                };
                for (const item of series) {
                    datum[item.chartKey] = item.normalizedValuesBySource[source.id];
                    datum[`${item.chartKey}__out`] = item.outOfRangeBySource[source.id];
                }
                return datum;
            }),
        [orderedSources, series],
    );

    const normalizedValues = useMemo(
        () =>
            series
                .flatMap(item => Object.values(item.normalizedValuesBySource))
                .filter((value): value is number => value !== null && Number.isFinite(value)),
        [series],
    );

    const hasNumericData = normalizedValues.length > 0;
    const yDomain = useMemo<[number, number]>(() => {
        if (!hasNumericData) {
            return [-0.2, 1.2];
        }
        const minValue = Math.min(0, ...normalizedValues);
        const maxValue = Math.max(1, ...normalizedValues);
        const spread = maxValue - minValue || 1;
        const padding = Math.max(0.08 * spread, 0.12);
        return [minValue - padding, maxValue + padding];
    }, [hasNumericData, normalizedValues]);

    const sourceById = useMemo(
        () => new Map(orderedSources.map(source => [source.id, source])),
        [orderedSources],
    );

    return (
        <ChartShell>
            <ChartCanvas>
                {hasNumericData ? (
                    <ResponsiveContainer width='100%' height='100%'>
                        <LineChart
                            data={chartData}
                            margin={{ top: 18, right: 20, left: 12, bottom: 10 }}
                        >
                            <CartesianGrid strokeDasharray='3 3' stroke='rgba(15, 23, 42, 0.16)' />
                            <XAxis
                                dataKey='sourceId'
                                tickFormatter={sourceId => sourceById.get(String(sourceId))?.prettyDate ?? String(sourceId)}
                                tick={{ fontSize: 11, fill: '#334155' }}
                                minTickGap={22}
                            />
                            <YAxis
                                domain={yDomain}
                                tickFormatter={formatNormalizedYAxisTick}
                                tick={{ fontSize: 11, fill: '#334155' }}
                                width={56}
                            />
                            <ReferenceLine
                                y={0}
                                stroke='#64748b'
                                strokeDasharray='4 4'
                                label={{ value: 'Low', position: 'insideLeft', fill: '#64748b', fontSize: 11 }}
                            />
                            <ReferenceLine
                                y={1}
                                stroke='#64748b'
                                strokeDasharray='4 4'
                                label={{ value: 'High', position: 'insideLeft', fill: '#64748b', fontSize: 11 }}
                            />
                            <Tooltip
                                content={({ active, label }) => {
                                    if (!active || typeof label !== 'string') {
                                        return null;
                                    }
                                    const source = sourceById.get(label);
                                    if (!source) {
                                        return null;
                                    }

                                    return (
                                        <TooltipCard>
                                            <TooltipTitle>{source.prettyDate}</TooltipTitle>
                                            {series.map(item => {
                                                const cell = item.valuesBySource[label];
                                                const displayLabel = item.unitLabel
                                                    ? `${item.label} (${item.unitLabel})`
                                                    : item.label;
                                                return (
                                                    <TooltipRow key={`${label}-${item.id}`}>
                                                        <TooltipLegendDot style={{ background: item.color }} />
                                                        <TooltipLabel>{displayLabel}</TooltipLabel>
                                                        <TooltipValue>{cell?.display ?? '—'}</TooltipValue>
                                                    </TooltipRow>
                                                );
                                            })}
                                        </TooltipCard>
                                    );
                                }}
                            />
                            <Legend
                                verticalAlign='top'
                                align='left'
                                wrapperStyle={{ paddingBottom: 8 }}
                            />
                            {series.map(item => (
                                <Line
                                    key={item.id}
                                    type='monotone'
                                    dataKey={item.chartKey}
                                    name={item.unitLabel ? `${item.label} (${item.unitLabel})` : item.label}
                                    stroke={item.color}
                                    strokeWidth={2.2}
                                    connectNulls={false}
                                    isAnimationActive={false}
                                    dot={props => {
                                        const { cx, cy, payload } = props as {
                                            cx?: number;
                                            cy?: number;
                                            payload?: TrendChartDatum;
                                        };
                                        if (!Number.isFinite(cx) || !Number.isFinite(cy)) {
                                            return null;
                                        }
                                        const isOutOfRange = Boolean(payload?.[`${item.chartKey}__out`]);
                                        return (
                                            <circle
                                                cx={cx}
                                                cy={cy}
                                                r={4}
                                                fill={isOutOfRange ? '#dc2626' : item.color}
                                                stroke='#ffffff'
                                                strokeWidth={1.6}
                                            />
                                        );
                                    }}
                                    activeDot={props => {
                                        const { cx, cy, payload } = props as {
                                            cx?: number;
                                            cy?: number;
                                            payload?: TrendChartDatum;
                                        };
                                        if (!Number.isFinite(cx) || !Number.isFinite(cy)) {
                                            return null;
                                        }
                                        const isOutOfRange = Boolean(payload?.[`${item.chartKey}__out`]);
                                        return (
                                            <circle
                                                cx={cx}
                                                cy={cy}
                                                r={5}
                                                fill={isOutOfRange ? '#dc2626' : item.color}
                                                stroke='#0f172a'
                                                strokeWidth={1.6}
                                            />
                                        );
                                    }}
                                />
                            ))}
                        </LineChart>
                    </ResponsiveContainer>
                ) : (
                    <Empty description='No numeric values in the selected rows for this date range.' />
                )}
            </ChartCanvas>

            <SelectedValuesShell>
                <SelectedValuesTitle>Selected values</SelectedValuesTitle>
                <SelectedValuesScroll>
                    <SelectedValuesTable>
                        <thead>
                            <tr>
                                <th>Date</th>
                                {series.map(item => (
                                    <th key={`selected-values-heading-${item.id}`}>
                                        {item.unitLabel ? `${item.label} (${item.unitLabel})` : item.label}
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {orderedSources.map(source => (
                                <tr key={`selected-values-row-${source.id}`}>
                                    <td>{source.prettyDate}</td>
                                    {series.map(item => {
                                        const cell = item.valuesBySource[source.id];
                                        return (
                                            <td key={`selected-values-${source.id}-${item.id}`}>
                                                {cell?.display ?? '—'}
                                            </td>
                                        );
                                    })}
                                </tr>
                            ))}
                        </tbody>
                    </SelectedValuesTable>
                </SelectedValuesScroll>
            </SelectedValuesShell>
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
    const [groupByCategory, setGroupByCategory] = useState(false);
    const [tablePaneWidth, setTablePaneWidth] = useState(0);
    const [isResizing, setIsResizing] = useState(false);

    const workspaceRef = useRef<HTMLDivElement | null>(null);
    const starredMeasurementSet = useMemo(() => new Set(starredMeasurementKeys), [starredMeasurementKeys]);
    const deferredMeasurementFilter = useDeferredValue(measurementFilter);

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

    const allMeasurementRows = useMemo<MeasurementDataRow[]>(() => {
        const grouped = new Map<string, MeasurementDataRow>();

        orderedLabs.forEach((lab, labIndex) => {
            const sourceId = sources[labIndex]?.id;
            if (!sourceId) return;

            lab.measurements.forEach(measurement => {
                const key = measurement.name.trim().toLowerCase();
                const category = normalizeCategoryLabel(measurement.category);
                const existing = grouped.get(key);
                if (existing) {
                    existing.valuesBySource[sourceId] = formatCell(measurement);
                    if (
                        existing.category === UNCATEGORIZED_CATEGORY_LABEL &&
                        category !== UNCATEGORIZED_CATEGORY_LABEL
                    ) {
                        existing.category = category;
                    }
                    return;
                }

                grouped.set(key, {
                    key,
                    rowType: 'measurement',
                    measurement: measurement.name,
                    category,
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
        const availableRowIds = new Set(allMeasurementRows.map(item => item.key));
        setStarredMeasurementKeys(previous => {
            const next = previous.filter(item => availableRowIds.has(item));
            return next.length === previous.length ? previous : next;
        });
    }, [allMeasurementRows]);

    const toggleMeasurementStar = useCallback((measurementKey: string) => {
        startTransition(() => {
            setStarredMeasurementKeys(previous =>
                previous.includes(measurementKey)
                    ? previous.filter(item => item !== measurementKey)
                    : [...previous, measurementKey],
            );
        });
    }, []);

    const filteredMeasurementRows = useMemo(() => {
        const normalizedFilter = deferredMeasurementFilter.trim().toLowerCase();
        const candidateRows = normalizedFilter
            ? allMeasurementRows.filter(row =>
                row.measurement.toLowerCase().includes(normalizedFilter) ||
                row.category.toLowerCase().includes(normalizedFilter),
            )
            : allMeasurementRows;
        return [...candidateRows].sort((left, right) => {
            const leftIsStarred = starredMeasurementSet.has(left.key);
            const rightIsStarred = starredMeasurementSet.has(right.key);
            if (leftIsStarred !== rightIsStarred) return leftIsStarred ? -1 : 1;
            return left.measurement.localeCompare(right.measurement);
        });
    }, [allMeasurementRows, deferredMeasurementFilter, starredMeasurementSet]);

    const tableSources = useMemo(() => {
        const normalizedFilter = deferredMeasurementFilter.trim();
        if (!normalizedFilter) {
            return visibleSources;
        }

        return visibleSources.filter(source =>
            filteredMeasurementRows.some(row => {
                const cell = row.valuesBySource[source.id];
                return Boolean(cell && cell.display !== '—');
            }),
        );
    }, [deferredMeasurementFilter, filteredMeasurementRows, visibleSources]);

    const tableRows = useMemo<TableRow[]>(() => {
        if (!groupByCategory) {
            return filteredMeasurementRows;
        }

        const grouped = new Map<string, MeasurementDataRow[]>();
        filteredMeasurementRows.forEach(row => {
            const key = row.category;
            const existing = grouped.get(key);
            if (existing) {
                existing.push(row);
                return;
            }
            grouped.set(key, [row]);
        });

        const categories = Array.from(grouped.keys()).sort((left, right) => left.localeCompare(right));
        return categories.flatMap(category => {
            const items = grouped.get(category) ?? [];
            const header: CategoryRow = {
                key: `category:${category.toLowerCase()}`,
                rowType: 'category',
                measurement: '',
                category,
                categoryCount: items.length,
                valuesBySource: {},
            };
            return [header, ...items];
        });
    }, [filteredMeasurementRows, groupByCategory]);

    useEffect(() => {
        const availableRowIds = new Set(filteredMeasurementRows.map(item => item.key));
        setSelectedRowKeys(previous => previous.filter(item => availableRowIds.has(String(item))));
    }, [filteredMeasurementRows]);

    const selectedRowKeySet = useMemo(
        () => new Set(selectedRowKeys.map(key => String(key))),
        [selectedRowKeys],
    );
    const selectedRows = useMemo(
        () => filteredMeasurementRows.filter(row => selectedRowKeySet.has(row.key)),
        [filteredMeasurementRows, selectedRowKeySet],
    );
    const chartSeries = useMemo<ChartSeries[]>(
        () =>
            selectedRows
                .map((row, index) => {
                    const cells = visibleSources.map(source => row.valuesBySource[source.id]);
                    const defaultRange = getRowDefaultRange(cells);
                    const observedBounds = getRowObservedBounds(cells);
                    const normalizedValuesBySource: Record<string, number | null> = {};
                    const outOfRangeBySource: Record<string, boolean> = {};

                    for (const source of visibleSources) {
                        const cell = row.valuesBySource[source.id];
                        normalizedValuesBySource[source.id] = normalizeCellForChart({
                            cell,
                            defaultRange,
                            observedBounds,
                        });
                        outOfRangeBySource[source.id] = isCellOutsideReferenceRange(cell);
                    }

                    return {
                        id: row.key,
                        chartKey: `series_${index}`,
                        label: row.measurement,
                        color: CHART_PALETTE[index % CHART_PALETTE.length],
                        valuesBySource: row.valuesBySource,
                        normalizedValuesBySource,
                        outOfRangeBySource,
                        unitLabel: resolveSeriesUnitLabel(cells),
                    };
                }),
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

    const tableColumns = useMemo<ColumnsType<TableRow>>(() => {
        const measurementColumn: ColumnsType<TableRow>[number] = {
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
                if (row.rowType === 'category') {
                    return (
                        <CategoryHeadingCell>
                            <Text strong>{row.category}</Text>
                            <CategoryCountText>{row.categoryCount}</CategoryCountText>
                        </CategoryHeadingCell>
                    );
                }
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

        const sourceColumns: ColumnsType<TableRow> = tableSources.map(source => ({
            title: <Text>{source.prettyDate}</Text>,
            key: source.id,
            width: 164,
            shouldCellUpdate: (record, previousRecord) => {
                if (record.rowType !== previousRecord.rowType) {
                    return true;
                }
                if (record.rowType === 'category' || previousRecord.rowType === 'category') {
                    return record !== previousRecord;
                }
                return record.valuesBySource[source.id] !== previousRecord.valuesBySource[source.id];
            },
            render: (_, row) => {
                if (row.rowType === 'category') {
                    return null;
                }
                const cell = row.valuesBySource[source.id];
                if (!cell) return <Text type='secondary'>—</Text>;
                return <MeasurementValueCell cell={cell} />;
            },
        }));

        return [measurementColumn, ...sourceColumns];
    }, [isMobileViewport, starredMeasurementSet, tableSources, toggleMeasurementStar]);

    const tableScrollY = useMemo(
        () => Math.max(240, isMobileViewport ? viewport.height - 178 : viewport.height - 132),
        [isMobileViewport, viewport.height],
    );

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
                                <Checkbox
                                    checked={groupByCategory}
                                    onChange={event => setGroupByCategory(event.target.checked)}
                                >
                                    Group by category
                                </Checkbox>
                            </ControlsRow>

                            <TableShell>
                                <Table<TableRow>
                                    bordered
                                    size='small'
                                    rowKey='key'
                                    dataSource={tableRows}
                                    columns={tableColumns}
                                    pagination={false}
                                    virtual
                                    scroll={{ x: 'max-content', y: tableScrollY }}
                                    rowClassName={row => (row.rowType === 'category' ? 'category-row' : '')}
                                    rowSelection={{
                                        selectedRowKeys,
                                        onChange: keys => {
                                            startTransition(() => {
                                                setSelectedRowKeys(keys);
                                            });
                                        },
                                        getCheckboxProps: row => ({
                                            disabled: row.rowType === 'category',
                                        }),
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
    grid-template-columns: ${({ mobile }) => (mobile ? '1fr' : 'minmax(260px, 1fr) auto auto auto')};
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

    .ant-table-tbody > tr.category-row > td {
        background: #f1f5f9;
        border-top: 1px solid #d5dce7;
        border-bottom: 1px solid #d5dce7;
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

const CategoryHeadingCell = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 8px;
`;

const CategoryCountText = styled.span`
    font-size: 11px;
    color: #64748b;
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
    display: flex;
    flex-direction: column;
    gap: 12px;
    padding: 12px;
`;

const ChartCanvas = styled.div`
    width: 100%;
    height: 380px;
    min-height: 320px;
`;

const SelectedValuesShell = styled.section`
    display: flex;
    flex-direction: column;
    gap: 6px;
`;

const SelectedValuesTitle = styled.h3`
    margin: 0;
    font-size: 12px;
    font-weight: 600;
    color: #334155;
    text-transform: uppercase;
    letter-spacing: 0.04em;
`;

const SelectedValuesScroll = styled.div`
    overflow-x: auto;
    border: 1px solid #d7dde7;
`;

const SelectedValuesTable = styled.table`
    width: max-content;
    min-width: 100%;
    border-collapse: collapse;
    font-size: 12px;

    th,
    td {
        border: 1px solid #e2e8f0;
        padding: 6px 8px;
        white-space: nowrap;
        text-align: left;
    }

    thead th {
        background: #f8fafc;
        color: #334155;
        position: sticky;
        top: 0;
        z-index: 1;
    }

    tbody tr:nth-of-type(odd) {
        background: #fdfefe;
    }
`;

const TooltipCard = styled.div`
    min-width: 260px;
    max-width: 440px;
    padding: 10px 12px;
    border: 1px solid #d7dde7;
    border-radius: 4px;
    background: #ffffff;
    box-shadow: 0 10px 28px rgba(15, 23, 42, 0.18);
`;

const TooltipTitle = styled.div`
    margin-bottom: 8px;
    font-size: 12px;
    font-weight: 600;
    color: #0f172a;
`;

const TooltipRow = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 2px 0;
`;

const TooltipLegendDot = styled.span`
    width: 10px;
    height: 10px;
    border-radius: 999px;
    flex: 0 0 auto;
`;

const TooltipLabel = styled.span`
    flex: 1;
    min-width: 0;
    color: #334155;
    font-size: 12px;
`;

const TooltipValue = styled.span`
    margin-left: auto;
    color: #0f172a;
    font-weight: 600;
    font-size: 12px;
`;
