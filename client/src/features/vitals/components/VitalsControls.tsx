import type { ChangeEvent } from 'react';

import { DownloadSimple, Drop } from '@phosphor-icons/react';
import { Slider } from 'antd';

import { formatPrettyDate } from '../utils';

type VitalsControlsProps = {
    isMobile: boolean;
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
    isMobile,
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
        <div className={`grid items-center gap-y-2 gap-x-4 border-b border-slate-300 p-2 ${isMobile ? 'grid-cols-1' : 'grid-cols-[minmax(240px,1fr)_minmax(320px,1.35fr)_auto_auto]'}`}>
            <label className='relative flex items-center'>
                <Drop size={16} className='pointer-events-none absolute left-2 text-slate-600' />
                <input
                    value={measurementFilter}
                    onChange={onMeasurementFilterChange}
                    placeholder='Filter measurements'
                    className='h-8 w-full rounded-sm border border-slate-300 bg-white pl-8 pr-2 text-sm text-slate-900 outline-none transition focus:border-slate-500'
                />
            </label>

            <div className='relative flex h-8 min-w-0 items-center'>
                <span
                    className='pointer-events-none absolute whitespace-nowrap text-left text-[11px] uppercase tracking-[0.04em] text-slate-600'
                    style={{
                        left: `${startHandlePercent}%`,
                        top: shouldStackLabels ? '30px' : '24px',
                        transform: 'translateX(0)',
                    }}
                >
                    {startDateLabel}
                </span>
                <span
                    className='pointer-events-none absolute whitespace-nowrap text-right text-[11px] uppercase tracking-[0.04em] text-slate-600'
                    style={{
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
                        rail: { background: '#cbd5e1' },
                        track: { background: '#334155' },
                        handle: { borderColor: '#0f172a', background: '#0f172a' },
                    }}
                />
            </div>

            <label className='inline-flex items-center gap-2 text-sm text-slate-800'>
                <input
                    type='checkbox'
                    checked={groupByCategory}
                    onChange={onGroupByCategoryChange}
                    className='h-4 w-4 rounded border-slate-300 text-slate-900 accent-slate-800'
                />
                Group by category
            </label>

            <button
                type='button'
                onClick={onDownloadCsv}
                disabled={isDownloadCsvDisabled}
                className='inline-flex h-8 items-center gap-1.5 rounded-sm border border-slate-300 bg-white px-3 text-sm text-slate-900 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50'
            >
                <DownloadSimple size={14} />
                CSV
            </button>
        </div>
    );
}
