import type { ChangeEvent } from 'react';

import { Drop } from '@phosphor-icons/react';

type DateBounds = {
    min: string;
    max: string;
};

type VitalsControlsProps = {
    isMobile: boolean;
    measurementFilter: string;
    onMeasurementFilterChange: (event: ChangeEvent<HTMLInputElement>) => void;
    dateRangeStart: string;
    dateRangeEnd: string;
    dateBounds: DateBounds;
    onDateRangeStartChange: (event: ChangeEvent<HTMLInputElement>) => void;
    onDateRangeEndChange: (event: ChangeEvent<HTMLInputElement>) => void;
    onResetRange: () => void;
    isRangeResetDisabled: boolean;
    groupByCategory: boolean;
    onGroupByCategoryChange: (event: ChangeEvent<HTMLInputElement>) => void;
};

export function VitalsControls({
    isMobile,
    measurementFilter,
    onMeasurementFilterChange,
    dateRangeStart,
    dateRangeEnd,
    dateBounds,
    onDateRangeStartChange,
    onDateRangeEndChange,
    onResetRange,
    isRangeResetDisabled,
    groupByCategory,
    onGroupByCategoryChange,
}: VitalsControlsProps) {
    return (
        <div className={`grid items-center gap-2 border-b border-slate-300 p-2 ${isMobile ? 'grid-cols-1' : 'grid-cols-[minmax(260px,1fr)_auto_auto_auto]'}`}>
            <label className='relative flex items-center'>
                <Drop size={16} className='pointer-events-none absolute left-2 text-slate-600' />
                <input
                    value={measurementFilter}
                    onChange={onMeasurementFilterChange}
                    placeholder='Filter measurements'
                    className='h-8 w-full rounded-sm border border-slate-300 bg-white pl-8 pr-2 text-sm text-slate-900 outline-none transition focus:border-slate-500'
                />
            </label>

            <div className='inline-flex items-center gap-1.5'>
                <input
                    type='date'
                    value={dateRangeStart}
                    min={dateBounds.min || undefined}
                    max={dateRangeEnd || dateBounds.max || undefined}
                    onChange={onDateRangeStartChange}
                    className='h-8 rounded-sm border border-slate-300 bg-white px-2 text-sm text-slate-900 outline-none transition focus:border-slate-500'
                />
                <span className='text-xs uppercase tracking-[0.04em] text-slate-600'>to</span>
                <input
                    type='date'
                    value={dateRangeEnd}
                    min={dateRangeStart || dateBounds.min || undefined}
                    max={dateBounds.max || undefined}
                    onChange={onDateRangeEndChange}
                    className='h-8 rounded-sm border border-slate-300 bg-white px-2 text-sm text-slate-900 outline-none transition focus:border-slate-500'
                />
            </div>

            <button
                type='button'
                onClick={onResetRange}
                disabled={isRangeResetDisabled}
                className='h-8 rounded-sm border border-slate-300 bg-white px-3 text-sm text-slate-900 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50'
            >
                All dates
            </button>

            <label className='inline-flex items-center gap-2 text-sm text-slate-800'>
                <input
                    type='checkbox'
                    checked={groupByCategory}
                    onChange={onGroupByCategoryChange}
                    className='h-4 w-4 rounded border-slate-300 text-slate-900 accent-slate-800'
                />
                Group by category
            </label>
        </div>
    );
}
