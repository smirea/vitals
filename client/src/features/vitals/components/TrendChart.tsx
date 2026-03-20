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
        <div className='flex w-full flex-col gap-3 p-3'>
            <div className={`w-full ${isMobile ? 'h-[260px] min-h-[220px]' : 'h-[380px] min-h-[320px]'}`}>
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
                                                className='min-w-[260px] max-w-[440px] rounded border px-3 py-2'
                                                style={{
                                                    borderColor: token.colorBorder,
                                                    background: token.colorBgContainer,
                                                    boxShadow: token.boxShadowSecondary,
                                                }}
                                            >
                                                <div className='mb-2 text-xs font-semibold' style={{ color: token.colorText }}>
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
                                                    <div key={`${label}-${item.id}`} className='flex items-center gap-2 py-[2px]'>
                                                        <span className='h-[10px] w-[10px] rounded-full' style={{ background: item.color }} />
                                                        <span className='min-w-0 flex-1 text-xs' style={{ color: token.colorTextSecondary }}>
                                                            {displayLabel}
                                                        </span>
                                                        <span className='ml-auto text-xs font-semibold' style={{ color: token.colorText }}>
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
                                    <span className='text-[11px] leading-tight' style={{ color: token.colorTextSecondary }}>
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
                    <div className='grid h-full place-items-center'>
                        <Empty description='No numeric values in the selected rows for this date range.' />
                    </div>
                )}
            </div>

            <section className='flex flex-col gap-1.5'>
                <h3 className='m-0 text-xs font-semibold uppercase tracking-[0.04em]' style={{ color: token.colorTextSecondary }}>
                    Selected values
                </h3>
                <div className='overflow-x-auto border' style={{ borderColor: token.colorBorder }}>
                    <table className='min-w-full w-max border-collapse text-xs'>
                        <thead>
                            <tr>
                                <th
                                    className='sticky top-0 z-[1] border px-2 py-1.5 text-left'
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
                                        className='sticky top-0 z-[1] border px-2 py-1.5 text-left'
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
                                    <td className='border px-2 py-1.5' style={{ borderColor: token.colorBorderSecondary }}>
                                        {source.prettyDate}
                                    </td>
                                    {visibleSeries.map(item => {
                                        const cell = item.valuesBySourceIndex[source.index];
                                        return (
                                            <td
                                                key={`selected-values-${source.id}-${item.id}`}
                                                className='border px-2 py-1.5'
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
