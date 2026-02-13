import {
    memo,
    useMemo,
} from 'react';

import { Empty } from 'antd';
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
};

export const TrendChart = memo(function TrendChart({
    series,
    orderedSources,
}: TrendChartProps) {
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
            <div className='h-[380px] min-h-[320px] w-full'>
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
                                            <div className='min-w-[260px] max-w-[440px] rounded border border-slate-300 bg-white px-3 py-2 shadow-[0_10px_28px_rgba(15,23,42,0.18)]'>
                                                <div className='mb-2 text-xs font-semibold text-slate-900'>{source.prettyDate}</div>
                                            {visibleSeries.map(item => {
                                                const cell = item.valuesBySourceIndex[source.index];
                                                const displayLabel = item.unitLabel
                                                    ? `${item.label} (${item.unitLabel})`
                                                    : item.label;
                                                return (
                                                    <div key={`${label}-${item.id}`} className='flex items-center gap-2 py-[2px]'>
                                                        <span className='h-[10px] w-[10px] rounded-full' style={{ background: item.color }} />
                                                        <span className='min-w-0 flex-1 text-xs text-slate-700'>{displayLabel}</span>
                                                        <span className='ml-auto text-xs font-semibold text-slate-900'>{cell?.display ?? '--'}</span>
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
                                formatter={value => <span className='text-[11px] leading-tight text-slate-700'>{value}</span>}
                                wrapperStyle={{ paddingTop: 8 }}
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
                    <div className='grid h-full place-items-center'>
                        <Empty description='No numeric values in the selected rows for this date range.' />
                    </div>
                )}
            </div>

            <section className='flex flex-col gap-1.5'>
                <h3 className='m-0 text-xs font-semibold uppercase tracking-[0.04em] text-slate-700'>Selected values</h3>
                <div className='overflow-x-auto border border-slate-300'>
                    <table className='min-w-full w-max border-collapse text-xs'>
                        <thead>
                            <tr>
                                <th className='sticky top-0 z-[1] border border-slate-200 bg-slate-50 px-2 py-1.5 text-left text-slate-700'>Date</th>
                                {visibleSeries.map(item => (
                                    <th
                                        key={`selected-values-heading-${item.id}`}
                                        className='sticky top-0 z-[1] border border-slate-200 bg-slate-50 px-2 py-1.5 text-left text-slate-700'
                                    >
                                        {item.unitLabel ? `${item.label} (${item.unitLabel})` : item.label}
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {tableSources.map(source => (
                                <tr key={`selected-values-row-${source.id}`} className='odd:bg-slate-50/30'>
                                    <td className='border border-slate-200 px-2 py-1.5'>{source.prettyDate}</td>
                                    {visibleSeries.map(item => {
                                        const cell = item.valuesBySourceIndex[source.index];
                                        return (
                                            <td key={`selected-values-${source.id}-${item.id}`} className='border border-slate-200 px-2 py-1.5'>
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
