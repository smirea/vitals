import {
    memo,
    useMemo,
} from 'react';

import { Empty, theme as antdTheme } from 'antd';
import {
    CartesianGrid,
    Legend,
    Line,
    LineChart,
    ReferenceLine,
    ResponsiveContainer,
    Tooltip as RechartsTooltip,
    XAxis,
    YAxis,
} from 'recharts';

import type { ChartSeriesModel, SourceColumn, TrendChartDatum } from '../types';
import { formatNormalizedYAxisTick } from '../utils';

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
