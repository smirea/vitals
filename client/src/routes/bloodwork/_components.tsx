import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, type ChangeEvent } from 'react';

import { CheckCircle, DownloadSimple, Drop, Flag, Star, WarningCircle } from '@phosphor-icons/react';
import { Empty, Slider, theme as antdTheme } from 'antd';
import { CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip as RechartsTooltip, XAxis, YAxis } from 'recharts';

import type { CategoryOverviewItem, CategorySelectionState, ChartSeriesModel, MeaningfulChangeItem, MeasurementCell, MeasurementOverviewTally, SourceColumn, TrendChartDatum, VitalsCategoryRow, VitalsDisplayRow, VitalsRowModel } from './_bloodwork';
import { formatNormalizedYAxisTick, formatPrettyDate, MEASUREMENT_COLUMN_WIDTH, OVERVIEW_COLUMN_WIDTH, SELECTION_COLUMN_WIDTH, SOURCE_COLUMN_WIDTH } from './_bloodwork';

type CategoriesOverviewProps = {
    items: CategoryOverviewItem[];
};

function toSharePercent(value: number, maxTotal: number): number {
    if (value <= 0 || maxTotal <= 0) {
        return 0;
    }
    return (value / maxTotal) * 100;
}

export function CategoriesOverview({ items }: CategoriesOverviewProps) {
    if (items.length === 0) {
        return null;
    }

    const maxTotal = Math.max(...items.map(item => item.total), 1);

    return (
        <section className='vitals-category-overview'>
            <div className='vitals-category-overview-grid'>
                {items.map(item => {
                    const statuses = [
                        {
                            key: 'in-range',
                            count: item.inRange,
                            className: 'vitals-category-overview-pill-in-range',
                            sharePercent: toSharePercent(item.inRange, maxTotal),
                        },
                        {
                            key: 'out-of-range',
                            count: item.outOfRange,
                            className: 'vitals-category-overview-pill-out-of-range',
                            sharePercent: toSharePercent(item.outOfRange, maxTotal),
                        },
                        {
                            key: 'unclassified',
                            count: item.unclassified,
                            className: 'vitals-category-overview-pill-unclassified',
                            sharePercent: toSharePercent(item.unclassified, maxTotal),
                        },
                    ].filter(status => status.count > 0);

                    return (
                        <article key={item.category} className='vitals-category-overview-item'>
                            <div className='vitals-category-overview-row'>
                                <h3>{item.category}</h3>
                                <span>{item.total}</span>
                            </div>

                            <div className='vitals-category-overview-pills' role='presentation'>
                                {statuses.map(status => (
                                    <span
                                        key={`${item.category}-${status.key}`}
                                        className='vitals-category-overview-pill-group'
                                        style={{ width: `calc(var(--vitals-overview-section-min-width) + ${status.sharePercent}%)` }}
                                    >
                                        <span
                                            className={`vitals-category-overview-pill ${status.className}`}
                                        />
                                        <span className='vitals-category-overview-pill-count'>{status.count}</span>
                                    </span>
                                ))}
                            </div>
                        </article>
                    );
                })}
            </div>
        </section>
    );
}


type MeaningfulChangesProps = {
    items: MeaningfulChangeItem[];
};

function formatDelta(value: number | null): string | null {
    if (value === null || !Number.isFinite(value)) {
        return null;
    }
    if (value >= 1000) {
        return `${Math.round(value)}%`;
    }
    if (value >= 100) {
        return `${value.toFixed(0)}%`;
    }
    return `${value.toFixed(1)}%`;
}

export function MeaningfulChanges({ items }: MeaningfulChangesProps) {
    if (items.length === 0) {
        return null;
    }

    const improvedCount = items.filter(item => item.direction === 'improved').length;
    const worsenedCount = items.filter(item => item.direction === 'worsened').length;

    return (
        <section className='vitals-meaningful-changes'>
            <div className='vitals-meaningful-changes-header'>
                <h2>Last 6 months changes</h2>
                <p>{improvedCount} improved · {worsenedCount} worsened · {items.length} meaningful</p>
            </div>

            <div className='vitals-meaningful-changes-list'>
                {items.map(item => {
                    const relativeDelta = formatDelta(item.relativeDeltaPercent);
                    const rangeDelta = formatDelta(item.normalizedRangeDeltaPercent);
                    const directionLabel = item.direction === 'improved'
                        ? 'Improved'
                        : item.direction === 'worsened'
                            ? 'Worsened'
                            : 'Changed';

                    return (
                        <article key={item.key} className='vitals-meaningful-change-item'>
                            <div className='vitals-meaningful-change-top'>
                                <div className='vitals-meaningful-change-titles'>
                                    <h3>{item.measurement}</h3>
                                    <span>{item.category}</span>
                                </div>
                                <span className={`vitals-meaningful-change-direction vitals-meaningful-change-direction-${item.direction}`}>
                                    {directionLabel}
                                </span>
                            </div>

                            <div className='vitals-meaningful-change-comparison'>
                                <span className='vitals-meaningful-change-value'>
                                    <strong>{item.previous.display}</strong>
                                    <small>{item.previous.prettyDate}</small>
                                </span>
                                <span className='vitals-meaningful-change-arrow' aria-hidden>→</span>
                                <span className='vitals-meaningful-change-value'>
                                    <strong>{item.latest.display}</strong>
                                    <small>{item.latest.prettyDate}</small>
                                </span>
                            </div>

                            <div className='vitals-meaningful-change-meta'>
                                {item.reasons.map(reason => (
                                    <span key={`${item.key}-${reason}`} className='vitals-meaningful-change-reason'>{reason}</span>
                                ))}
                                {relativeDelta && <span className='vitals-meaningful-change-delta'>Value Δ {relativeDelta}</span>}
                                {rangeDelta && <span className='vitals-meaningful-change-delta'>Range drift {rangeDelta}</span>}
                            </div>
                        </article>
                    );
                })}
            </div>
        </section>
    );
}


type TrendChartProps = {
    series: ChartSeriesModel[];
    orderedSources: SourceColumn[];
    isMobile: boolean;
};

export const TrendChart = memo(function TrendChart({
    series,
    orderedSources,
    isMobile,
}: TrendChartProps) {
    const { token } = antdTheme.useToken();
    const visibleSeries = useMemo(
        () =>
            series.filter(item => orderedSources.some(source => {
                const cell = item.valuesBySourceIndex[source.index];
                if (!cell) return false;
                return cell.display !== '—' && cell.display !== '--' && cell.display.trim() !== '';
            })),
        [orderedSources, series],
    );

    const tableSources = useMemo(
        () =>
            orderedSources.filter(source => visibleSeries.some(item => {
                const cell = item.valuesBySourceIndex[source.index];
                if (!cell) return false;
                return cell.display !== '—' && cell.display !== '--' && cell.display.trim() !== '';
            })),
        [orderedSources, visibleSeries],
    );

    const chartData = useMemo<TrendChartDatum[]>(
        () =>
            orderedSources.map(source => {
                const datum: TrendChartDatum = {
                    sourceId: source.id,
                    prettyDate: source.prettyDate,
                };
                for (const item of visibleSeries) {
                    datum[item.chartKey] = item.normalizedValuesBySourceIndex[source.index] ?? null;
                    datum[`${item.chartKey}__out`] = item.outOfRangeBySourceIndex[source.index] ?? false;
                }
                return datum;
            }),
        [orderedSources, visibleSeries],
    );

    const normalizedValues = useMemo(
        () =>
            visibleSeries
                .flatMap(item => orderedSources.map(source => item.normalizedValuesBySourceIndex[source.index] ?? null))
                .filter((value): value is number => value !== null && Number.isFinite(value)),
        [orderedSources, visibleSeries],
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
        <div style={{ display: 'flex', width: '100%', flexDirection: 'column', gap: 12, padding: 12 }}>
            <div style={{ width: '100%', height: isMobile ? 260 : 380, minHeight: isMobile ? 220 : 320 }}>
                {hasNumericData ? (
                    <ResponsiveContainer width='100%' height='100%'>
                        <LineChart
                            data={chartData}
                            margin={isMobile
                                ? { top: 12, right: 8, left: 0, bottom: 8 }
                                : { top: 18, right: 20, left: 12, bottom: 10 }}
                        >
                            <CartesianGrid strokeDasharray='3 3' stroke='rgba(15, 23, 42, 0.16)' />
                            <XAxis
                                dataKey='sourceId'
                                tickFormatter={sourceId => sourceById.get(String(sourceId))?.prettyDate ?? String(sourceId)}
                                tick={{ fontSize: 11, fill: token.colorTextSecondary }}
                                minTickGap={isMobile ? 40 : 22}
                                interval='preserveStartEnd'
                            />
                            <YAxis
                                domain={yDomain}
                                tickFormatter={formatNormalizedYAxisTick}
                                tick={{ fontSize: 11, fill: token.colorTextSecondary }}
                                width={isMobile ? 44 : 56}
                            />
                            <ReferenceLine
                                y={0}
                                stroke={token.colorTextTertiary}
                                strokeDasharray='4 4'
                                label={{ value: 'Low', position: 'insideLeft', fill: token.colorTextTertiary, fontSize: 11 }}
                            />
                            <ReferenceLine
                                y={1}
                                stroke={token.colorTextTertiary}
                                strokeDasharray='4 4'
                                label={{ value: 'High', position: 'insideLeft', fill: token.colorTextTertiary, fontSize: 11 }}
                            />
                            <RechartsTooltip
                                content={({ active, label }) => {
                                    if (!active || typeof label !== 'string') {
                                        return null;
                                    }
                                    const source = sourceById.get(label);
                                    if (!source) {
                                        return null;
                                    }

                                    return (
                                            <div
                                                style={{
                                                    minWidth: 260,
                                                    maxWidth: 440,
                                                    padding: '8px 12px',
                                                    borderRadius: 8,
                                                    borderStyle: 'solid',
                                                    borderWidth: 1,
                                                    borderColor: token.colorBorder,
                                                    background: token.colorBgContainer,
                                                    boxShadow: token.boxShadowSecondary,
                                                }}
                                            >
                                                <div style={{ marginBottom: 8, color: token.colorText, fontSize: 12, fontWeight: 600 }}>
                                                    {source.prettyDate}
                                                </div>
                                            {visibleSeries.map(item => {
                                                const cell = item.valuesBySourceIndex[source.index];
                                                const displayLabel = item.unitLabel
                                                    ? `${item.label} (${item.unitLabel})`
                                                    : item.label;
                                                const displayValue = cell?.display ?? '--';
                                                const rangeLabel = cell?.rangeCaption?.trim();
                                                const valueWithRange = rangeLabel
                                                    ? `${displayValue} (${rangeLabel})`
                                                    : displayValue;
                                                return (
                                                    <div key={`${label}-${item.id}`} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '2px 0' }}>
                                                        <span style={{ width: 10, height: 10, borderRadius: '999px', background: item.color }} />
                                                        <span style={{ minWidth: 0, flex: 1, color: token.colorTextSecondary, fontSize: 12 }}>
                                                            {displayLabel}
                                                        </span>
                                                        <span style={{ marginLeft: 'auto', color: token.colorText, fontSize: 12, fontWeight: 600 }}>
                                                            {valueWithRange}
                                                        </span>
                                                    </div>
                                                );
                                            })}
                                        </div>
                                    );
                                }}
                            />
                            <Legend
                                verticalAlign='bottom'
                                align='left'
                                iconSize={8}
                                formatter={value => (
                                    <span style={{ color: token.colorTextSecondary, fontSize: 11, lineHeight: 1.2 }}>
                                        {value}
                                    </span>
                                )}
                                wrapperStyle={{ paddingTop: 8, maxHeight: isMobile ? 68 : undefined, overflowY: isMobile ? 'auto' : undefined }}
                            />
                            {visibleSeries.map(item => (
                                <Line
                                    key={item.id}
                                    type='linear'
                                    dataKey={item.chartKey}
                                    name={item.unitLabel ? `${item.label} (${item.unitLabel})` : item.label}
                                    stroke={item.color}
                                    strokeWidth={2.2}
                                    connectNulls
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
                                                fill={isOutOfRange ? token.colorError : item.color}
                                                stroke={token.colorBgContainer}
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
                                                fill={isOutOfRange ? token.colorError : item.color}
                                                stroke={token.colorText}
                                                strokeWidth={1.6}
                                            />
                                        );
                                    }}
                                />
                            ))}
                        </LineChart>
                    </ResponsiveContainer>
                ) : (
                    <div style={{ display: 'grid', height: '100%', placeItems: 'center' }}>
                        <Empty description='No numeric values in the selected rows for this date range.' />
                    </div>
                )}
            </div>

            <section style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                <h3 style={{ margin: 0, color: token.colorTextSecondary, fontSize: 12, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                    Selected values
                </h3>
                <div className='trend-chart-table-shell' style={{ borderColor: token.colorBorder }}>
                    <table className='trend-chart-table'>
                        <thead>
                            <tr>
                                <th
                                    className='trend-chart-table-head'
                                    style={{
                                        borderColor: token.colorBorderSecondary,
                                        background: token.colorFillAlter,
                                        color: token.colorTextSecondary,
                                    }}
                                >
                                    Date
                                </th>
                                {visibleSeries.map(item => (
                                    <th
                                        key={`selected-values-heading-${item.id}`}
                                        className='trend-chart-table-head'
                                        style={{
                                            borderColor: token.colorBorderSecondary,
                                            background: token.colorFillAlter,
                                            color: token.colorTextSecondary,
                                        }}
                                    >
                                        {item.unitLabel ? `${item.label} (${item.unitLabel})` : item.label}
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {tableSources.map((source, index) => (
                                <tr
                                    key={`selected-values-row-${source.id}`}
                                    style={{ background: index % 2 === 0 ? token.colorFillQuaternary : undefined }}
                                >
                                    <td className='trend-chart-table-cell' style={{ borderColor: token.colorBorderSecondary }}>
                                        {source.prettyDate}
                                    </td>
                                    {visibleSeries.map(item => {
                                        const cell = item.valuesBySourceIndex[source.index];
                                        return (
                                            <td
                                                key={`selected-values-${source.id}-${item.id}`}
                                                className='trend-chart-table-cell'
                                                style={{ borderColor: token.colorBorderSecondary }}
                                            >
                                                {cell?.display ?? '--'}
                                            </td>
                                        );
                                    })}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </section>
        </div>
    );
});


type VitalsControlsProps = {
    measurementFilter: string;
    onMeasurementFilterChange: (event: ChangeEvent<HTMLInputElement>) => void;
    availableDates: string[];
    dateRangeValue: [number, number];
    onDateRangeSliderChange: (nextRange: [number, number]) => void;
    groupByCategory: boolean;
    onGroupByCategoryChange: (event: ChangeEvent<HTMLInputElement>) => void;
    onDownloadCsv: () => void;
    isDownloadCsvDisabled: boolean;
};

export function VitalsControls({
    measurementFilter,
    onMeasurementFilterChange,
    availableDates,
    dateRangeValue,
    onDateRangeSliderChange,
    groupByCategory,
    onGroupByCategoryChange,
    onDownloadCsv,
    isDownloadCsvDisabled,
}: VitalsControlsProps) {
    const { token } = antdTheme.useToken();
    const sliderDates = [...availableDates].reverse();
    const maxIndex = Math.max(availableDates.length - 1, 0);
    const startIndex = Math.min(maxIndex, Math.max(0, dateRangeValue[0] ?? 0));
    const endIndex = Math.min(maxIndex, Math.max(0, dateRangeValue[1] ?? 0));
    const startDateLabel = sliderDates[startIndex] ? formatPrettyDate(sliderDates[startIndex]) : '--';
    const endDateLabel = sliderDates[endIndex] ? formatPrettyDate(sliderDates[endIndex]) : '--';
    const startHandlePercent = maxIndex > 0 ? (startIndex / maxIndex) * 100 : 0;
    const endHandlePercent = maxIndex > 0 ? (endIndex / maxIndex) * 100 : 0;
    const shouldStackLabels = Math.abs(startHandlePercent - endHandlePercent) < 10;

    return (
        <div
            style={{ borderColor: token.colorBorder }}
        >
            <label>
                <Drop
                    size={16}
                    style={{ color: token.colorTextSecondary }}
                />
                <input
                    value={measurementFilter}
                    onChange={onMeasurementFilterChange}
                    placeholder='Filter measurements'
                    style={{
                        borderColor: token.colorBorder,
                        background: token.colorBgContainer,
                        color: token.colorText,
                    }}
                />
            </label>

            <div>
                <span
                    style={{
                        color: token.colorTextSecondary,
                        left: `${startHandlePercent}%`,
                        top: shouldStackLabels ? '30px' : '24px',
                        transform: 'translateX(0)',
                    }}
                >
                    {startDateLabel}
                </span>
                <span
                    style={{
                        color: token.colorTextSecondary,
                        left: `${endHandlePercent}%`,
                        top: '24px',
                        transform: 'translateX(-100%)',
                    }}
                >
                    {endDateLabel}
                </span>
                <Slider
                    range
                    min={0}
                    max={maxIndex}
                    step={1}
                    value={dateRangeValue}
                    disabled={availableDates.length <= 1}
                    onChange={value => {
                        if (!Array.isArray(value) || value.length !== 2) return;
                        onDateRangeSliderChange([value[0], value[1]]);
                    }}
                    tooltip={{ formatter: value => (value === undefined ? '' : formatPrettyDate(sliderDates[value] ?? '')) }}
                    style={{ margin: 0, width: '100%' }}
                    styles={{
                        rail: { background: token.colorBorderSecondary },
                        track: { background: token.colorText },
                        handle: { borderColor: token.colorText, background: token.colorText },
                    }}
                />
            </div>

            <label style={{ color: token.colorText }}>
                <input
                    type='checkbox'
                    checked={groupByCategory}
                    onChange={onGroupByCategoryChange}
                    style={{ accentColor: token.colorPrimary, borderColor: token.colorBorder }}
                />
                Group by category
            </label>

            <button
                type='button'
                onClick={onDownloadCsv}
                disabled={isDownloadCsvDisabled}
                style={{
                    borderColor: token.colorBorder,
                    background: token.colorBgContainer,
                    color: token.colorText,
                }}
            >
                <DownloadSimple size={14} />
                CSV
            </button>
        </div>
    );
}


type VitalsTableProps = {
    rows: VitalsDisplayRow[];
    tableSources: SourceColumn[];
    outOfRangeSourceFilterIdSet: Set<string>;
    outOfRangeMeasurementCountBySourceId: Map<string, number>;
    selectedRowKeySet: Set<string>;
    categorySelectionByName: Map<string, CategorySelectionState>;
    starredMeasurementSet: Set<string>;
    measurementOverviewByKey: Map<string, MeasurementOverviewTally>;
    measurementRangesTooltipByKey: Map<string, string>;
    tableScrollX: number;
    onToggleRow: (key: string, checked: boolean) => void;
    onToggleAllRows: (checked: boolean) => void;
    onToggleCategory: (category: string, checked: boolean) => void;
    onToggleStar: (measurementKey: string) => void;
    onToggleOutOfRangeSourceFilter: (sourceId: string) => void;
};

type SelectionCheckboxProps = {
    checked: boolean;
    indeterminate?: boolean;
    disabled?: boolean;
    onChange: (checked: boolean) => void;
    ariaLabel?: string;
};

const SelectionCheckbox = memo(function SelectionCheckbox({
    checked,
    indeterminate = false,
    disabled = false,
    onChange,
    ariaLabel,
}: SelectionCheckboxProps) {
    const { token } = antdTheme.useToken();
    const ref = useRef<HTMLInputElement | null>(null);

    useEffect(() => {
        if (!ref.current) return;
        ref.current.indeterminate = indeterminate;
    }, [indeterminate]);

    return (
        <input
            ref={ref}
            type='checkbox'
            checked={checked}
            disabled={disabled}
            aria-label={ariaLabel}
            onChange={event => onChange(event.target.checked)}
            style={{ accentColor: token.colorPrimary, borderColor: token.colorBorder }}
        />
    );
});

const MeasurementValueCell = memo(function MeasurementValueCell({ cell }: { cell: MeasurementCell | undefined }) {
    const { token } = antdTheme.useToken();

    if (!cell) {
        return <span style={{ color: token.colorTextTertiary }}>--</span>;
    }

    const rangeVisualization = cell.rangeVisualization;
    const hasBand =
        rangeVisualization?.minPosition !== null &&
        rangeVisualization?.maxPosition !== null;

    return (
        <div className='vitals-cell-value'>
            <div style={{ minHeight: 18 }}>
                <span>{cell.display}</span>
            </div>
            {rangeVisualization && cell.rangeCaption && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                    <div className='vitals-range-track'>
                        {hasBand && (
                            <span
                                className='vitals-range-band'
                                style={{
                                    left: `${cell.rangeBandLeft}%`,
                                    width: `${cell.rangeBandWidth}%`,
                                }}
                            />
                        )}
                        {rangeVisualization.minPosition !== null && (
                            <span className='vitals-range-marker' style={{ left: `${rangeVisualization.minPosition}%` }} />
                        )}
                        {rangeVisualization.maxPosition !== null && (
                            <span className='vitals-range-marker' style={{ left: `${rangeVisualization.maxPosition}%` }} />
                        )}
                        <span className='vitals-value-marker' style={{ left: `${rangeVisualization.valuePosition}%` }} />
                    </div>
                    <span className='vitals-range-caption'>{cell.rangeCaption}</span>
                </div>
            )}
            {cell.flag && cell.flag !== 'normal' && (
                <span className={`vitals-flag ${cell.flag === 'high' || cell.flag === 'critical' ? 'vitals-flag-danger' : 'vitals-flag-warning'}`}>
                    {cell.flag}
                </span>
            )}
        </div>
    );
}, (prev, next) => prev.cell === next.cell);

type MeasurementRowProps = {
    row: VitalsRowModel;
    tableSources: SourceColumn[];
    highlightedSourceIdSet: Set<string>;
    selected: boolean;
    starred: boolean;
    tooltip: string;
    overview: MeasurementOverviewTally;
    onToggleRow: (key: string, checked: boolean) => void;
    onToggleStar: (measurementKey: string) => void;
};

const MeasurementRow = memo(function MeasurementRow({
    row,
    tableSources,
    highlightedSourceIdSet,
    selected,
    starred,
    tooltip,
    overview,
    onToggleRow,
    onToggleStar,
}: MeasurementRowProps) {
    const { token } = antdTheme.useToken();
    const hasAnyCounter = overview.inRange > 0 || overview.outOfRange > 0;

    return (
        <tr className={selected ? 'vitals-row-selected' : ''}>
            <td className='vitals-cell vitals-col-select'>
                <SelectionCheckbox
                    checked={selected}
                    onChange={checked => onToggleRow(row.key, checked)}
                    ariaLabel={`Select ${row.measurement}`}
                />
            </td>
            <td className='vitals-cell vitals-col-measurement'>
                <div style={{ display: 'flex', minWidth: 0, alignItems: 'flex-start', gap: 6 }} title={tooltip}>
                    <button
                        type='button'
                        aria-pressed={starred}
                        aria-label={starred ? `Unstar ${row.measurement}` : `Star ${row.measurement}`}
                        onMouseDown={event => event.preventDefault()}
                        onClick={() => onToggleStar(row.key)}
                        style={{
                            borderColor: 'transparent',
                            background: 'transparent',
                            color: starred ? token.colorWarning : token.colorTextTertiary,
                        }}
                    >
                        <Star size={14} weight={starred ? 'fill' : 'regular'} />
                    </button>
                    <span
                        style={{
                            minWidth: 0,
                            lineHeight: 1.35,
                            overflowWrap: 'break-word',
                            whiteSpace: 'normal',
                            fontWeight: starred ? 600 : undefined,
                        }}
                    >
                        {row.measurement}
                    </span>
                </div>
            </td>
            <td className='vitals-cell vitals-col-overview'>
                <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}>
                    {hasAnyCounter ? (
                        <>
                            {overview.inRange > 0 && (
                                <span
                                    style={{ display: 'inline-flex', alignItems: 'center', gap: 4, color: token.colorSuccess, fontSize: 11, lineHeight: 1 }}
                                    title={`${overview.inRange} in range`}
                                >
                                    <CheckCircle size={13} weight='fill' />
                                    <span style={{ color: token.colorText, fontWeight: 600 }}>{overview.inRange}</span>
                                </span>
                            )}
                            {overview.outOfRange > 0 && (
                                <span
                                    style={{ display: 'inline-flex', alignItems: 'center', gap: 4, color: token.colorError, fontSize: 11, lineHeight: 1 }}
                                    title={`${overview.outOfRange} out of range`}
                                >
                                    <WarningCircle size={13} weight='fill' />
                                    <span style={{ color: token.colorText, fontWeight: 600 }}>{overview.outOfRange}</span>
                                </span>
                            )}
                        </>
                    ) : (
                        <span style={{ color: token.colorTextTertiary }}>--</span>
                    )}
                </div>
            </td>
            {tableSources.map(source => (
                <td
                    key={`${row.key}-${source.id}`}
                    className={`vitals-cell ${highlightedSourceIdSet.has(source.id) ? 'vitals-source-filter-active' : ''}`}
                >
                    <MeasurementValueCell cell={row.valuesBySourceIndex[source.index]} />
                </td>
            ))}
        </tr>
    );
}, (prev, next) => (
    prev.row === next.row &&
    prev.tableSources === next.tableSources &&
    prev.highlightedSourceIdSet === next.highlightedSourceIdSet &&
    prev.selected === next.selected &&
    prev.starred === next.starred &&
    prev.tooltip === next.tooltip &&
    prev.overview === next.overview
));

type CategoryRowProps = {
    row: VitalsCategoryRow;
    tableSources: SourceColumn[];
    highlightedSourceIdSet: Set<string>;
    selection: CategorySelectionState;
    onToggleCategory: (category: string, checked: boolean) => void;
};

const CategoryRow = memo(function CategoryRow({
    row,
    tableSources,
    highlightedSourceIdSet,
    selection,
    onToggleCategory,
}: CategoryRowProps) {
    const { token } = antdTheme.useToken();

    return (
        <tr className='vitals-category-row'>
            <td className='vitals-cell vitals-col-select'>
                <SelectionCheckbox
                    checked={selection.checked}
                    indeterminate={selection.indeterminate}
                    disabled={selection.disabled}
                    onChange={checked => onToggleCategory(row.category, checked)}
                    ariaLabel={`Select category ${row.category}`}
                />
            </td>
            <td className='vitals-cell vitals-col-measurement'>
                <div style={{ display: 'inline-flex', minHeight: 22, alignItems: 'center', gap: 8 }}>
                    <strong>{row.category}</strong>
                    <span style={{ color: token.colorTextTertiary, fontSize: 11 }}>{row.categoryCount}</span>
                </div>
            </td>
            <td className='vitals-cell vitals-col-overview'>
                <div style={{ minHeight: 18 }} />
            </td>
            {tableSources.map(source => (
                <td
                    key={`${row.key}-${source.id}`}
                    className={`vitals-cell ${highlightedSourceIdSet.has(source.id) ? 'vitals-source-filter-active' : ''}`}
                >
                    <div style={{ minHeight: 18 }} />
                </td>
            ))}
        </tr>
    );
}, (prev, next) => (
    prev.row === next.row &&
    prev.tableSources === next.tableSources &&
    prev.highlightedSourceIdSet === next.highlightedSourceIdSet &&
    prev.selection.checked === next.selection.checked &&
    prev.selection.indeterminate === next.selection.indeterminate &&
    prev.selection.disabled === next.selection.disabled
));

export const VitalsTable = memo(function VitalsTable({
    rows,
    tableSources,
    outOfRangeSourceFilterIdSet,
    outOfRangeMeasurementCountBySourceId,
    selectedRowKeySet,
    categorySelectionByName,
    starredMeasurementSet,
    measurementOverviewByKey,
    measurementRangesTooltipByKey,
    tableScrollX,
    onToggleRow,
    onToggleAllRows,
    onToggleCategory,
    onToggleStar,
    onToggleOutOfRangeSourceFilter,
}: VitalsTableProps) {
    const tableShellRef = useRef<HTMLDivElement | null>(null);
    const pendingScrollTopRef = useRef<number | null>(null);

    const selectableRowKeys = useMemo(
        () => rows.filter((row): row is VitalsRowModel => row.rowType === 'measurement').map(row => row.key),
        [rows],
    );

    const selectedCount = useMemo(
        () => selectableRowKeys.reduce((count, key) => (selectedRowKeySet.has(key) ? count + 1 : count), 0),
        [selectableRowKeys, selectedRowKeySet],
    );

    const allChecked = selectableRowKeys.length > 0 && selectedCount === selectableRowKeys.length;
    const someChecked = selectedCount > 0 && selectedCount < selectableRowKeys.length;

    const onToggleStarWithScrollLock = useCallback((measurementKey: string) => {
        pendingScrollTopRef.current = tableShellRef.current?.scrollTop ?? null;
        onToggleStar(measurementKey);
    }, [onToggleStar]);

    useLayoutEffect(() => {
        if (pendingScrollTopRef.current === null || !tableShellRef.current) {
            return;
        }
        tableShellRef.current.scrollTop = pendingScrollTopRef.current;
        pendingScrollTopRef.current = null;
    }, [rows]);

    return (
        <div style={{ display: 'flex', minHeight: 0, flex: 1 }}>
            <div ref={tableShellRef} className='vitals-table-shell'>
                <table
                    className='vitals-table'
                    style={{
                        minWidth: tableScrollX,
                        ['--selection-col-width' as string]: `${SELECTION_COLUMN_WIDTH}px`,
                        ['--measurement-col-width' as string]: `${MEASUREMENT_COLUMN_WIDTH}px`,
                        ['--overview-col-width' as string]: `${OVERVIEW_COLUMN_WIDTH}px`,
                        ['--source-col-width' as string]: `${SOURCE_COLUMN_WIDTH}px`,
                    }}
                >
                    <thead>
                        <tr>
                            <th className='vitals-head vitals-col-select'>
                                <SelectionCheckbox
                                    checked={allChecked}
                                    indeterminate={someChecked}
                                    disabled={selectableRowKeys.length === 0}
                                    onChange={onToggleAllRows}
                                    ariaLabel='Select all'
                                />
                            </th>
                            <th className='vitals-head vitals-col-measurement'>Measurement</th>
                            <th className='vitals-head vitals-col-overview'>Overview</th>
                            {tableSources.map(source => {
                                const isFiltered = outOfRangeSourceFilterIdSet.has(source.id);
                                const outOfRangeCount = outOfRangeMeasurementCountBySourceId.get(source.id) ?? 0;

                                return (
                                    <th key={source.id} className={`vitals-head ${isFiltered ? 'vitals-source-filter-active' : ''}`}>
                                        <div className='vitals-source-head'>
                                            <span>{source.prettyDate}</span>
                                            <button
                                                type='button'
                                                aria-label={`Filter measurements out of range in ${source.prettyDate}`}
                                                aria-pressed={isFiltered}
                                                onClick={() => onToggleOutOfRangeSourceFilter(source.id)}
                                                className={`vitals-source-filter-toggle ${isFiltered ? 'vitals-source-filter-toggle-active' : ''}`}
                                            >
                                                <Flag size={12} weight={isFiltered ? 'fill' : 'regular'} />
                                                <span>{outOfRangeCount}</span>
                                            </button>
                                        </div>
                                    </th>
                                );
                            })}
                        </tr>
                    </thead>
                    <tbody>
                        {rows.map(row => {
                            if (row.rowType === 'category') {
                                return (
                                    <CategoryRow
                                        key={row.key}
                                        row={row}
                                        tableSources={tableSources}
                                        highlightedSourceIdSet={outOfRangeSourceFilterIdSet}
                                        selection={categorySelectionByName.get(row.category) ?? {
                                            checked: false,
                                            indeterminate: false,
                                            disabled: true,
                                        }}
                                        onToggleCategory={onToggleCategory}
                                    />
                                );
                            }

                            return (
                                <MeasurementRow
                                    key={row.key}
                                    row={row}
                                    tableSources={tableSources}
                                    highlightedSourceIdSet={outOfRangeSourceFilterIdSet}
                                    selected={selectedRowKeySet.has(row.key)}
                                    starred={starredMeasurementSet.has(row.key)}
                                    tooltip={measurementRangesTooltipByKey.get(row.key) ?? row.measurement}
                                    overview={measurementOverviewByKey.get(row.key) ?? { inRange: 0, outOfRange: 0 }}
                                    onToggleRow={onToggleRow}
                                    onToggleStar={onToggleStarWithScrollLock}
                                />
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
});
