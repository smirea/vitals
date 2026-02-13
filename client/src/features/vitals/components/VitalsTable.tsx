import { memo, useEffect, useMemo, useRef } from 'react';

import { CheckCircle, Star, WarningCircle } from '@phosphor-icons/react';

import type {
    CategorySelectionState,
    MeasurementCell,
    MeasurementOverviewTally,
    SourceColumn,
    VitalsCategoryRow,
    VitalsDisplayRow,
    VitalsRowModel,
} from '../types';
import {
    MEASUREMENT_COLUMN_WIDTH,
    OVERVIEW_COLUMN_WIDTH,
    SELECTION_COLUMN_WIDTH,
    SOURCE_COLUMN_WIDTH,
} from '../utils';

type VitalsTableProps = {
    rows: VitalsDisplayRow[];
    tableSources: SourceColumn[];
    selectedRowKeySet: Set<string>;
    categorySelectionByName: Map<string, CategorySelectionState>;
    starredMeasurementSet: Set<string>;
    measurementOverviewByKey: Map<string, MeasurementOverviewTally>;
    measurementRangesTooltipByKey: Map<string, string>;
    tableScrollY: number;
    tableScrollX: number;
    onToggleRow: (key: string, checked: boolean) => void;
    onToggleAllRows: (checked: boolean) => void;
    onToggleCategory: (category: string, checked: boolean) => void;
    onToggleStar: (measurementKey: string) => void;
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
            className='h-4 w-4 rounded border-slate-300 accent-slate-800 disabled:cursor-not-allowed'
        />
    );
});

const MeasurementValueCell = memo(function MeasurementValueCell({ cell }: { cell: MeasurementCell | undefined }) {
    if (!cell) {
        return <span className='text-slate-500'>--</span>;
    }

    const rangeVisualization = cell.rangeVisualization;
    const hasBand =
        rangeVisualization?.minPosition !== null &&
        rangeVisualization?.maxPosition !== null;

    return (
        <div className='vitals-cell-value'>
            <div className='min-h-[18px]'>
                <span>{cell.display}</span>
            </div>
            {rangeVisualization && cell.rangeCaption && (
                <div className='flex flex-col gap-0.5'>
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
    selected,
    starred,
    tooltip,
    overview,
    onToggleRow,
    onToggleStar,
}: MeasurementRowProps) {
    const hasAnyCounter = overview.inRange > 0 || overview.outOfRange > 0;

    return (
        <tr className={selected ? 'bg-slate-100/60' : ''}>
            <td className='vitals-cell vitals-col-select'>
                <SelectionCheckbox
                    checked={selected}
                    onChange={checked => onToggleRow(row.key, checked)}
                    ariaLabel={`Select ${row.measurement}`}
                />
            </td>
            <td className='vitals-cell vitals-col-measurement'>
                <div className='inline-flex min-w-0 items-center gap-1.5' title={tooltip}>
                    <button
                        type='button'
                        aria-pressed={starred}
                        aria-label={starred ? `Unstar ${row.measurement}` : `Star ${row.measurement}`}
                        onClick={() => onToggleStar(row.key)}
                        className={`grid h-5 w-5 place-items-center rounded-sm border border-transparent p-0 ${starred ? 'text-amber-600 hover:text-amber-700' : 'text-slate-400 hover:text-slate-700'} hover:border-slate-300 hover:bg-slate-50`}
                    >
                        <Star size={14} weight={starred ? 'fill' : 'regular'} />
                    </button>
                    <span className={`truncate ${starred ? 'font-semibold' : ''}`}>{row.measurement}</span>
                </div>
            </td>
            <td className='vitals-cell vitals-col-overview'>
                <div className='inline-flex items-center gap-1.5 whitespace-nowrap'>
                    {hasAnyCounter ? (
                        <>
                            {overview.inRange > 0 && (
                                <span className='inline-flex items-center gap-1 text-[11px] leading-none text-green-700' title={`${overview.inRange} in range`}>
                                    <CheckCircle size={13} weight='fill' />
                                    <span className='font-semibold text-slate-900'>{overview.inRange}</span>
                                </span>
                            )}
                            {overview.outOfRange > 0 && (
                                <span className='inline-flex items-center gap-1 text-[11px] leading-none text-red-700' title={`${overview.outOfRange} out of range`}>
                                    <WarningCircle size={13} weight='fill' />
                                    <span className='font-semibold text-slate-900'>{overview.outOfRange}</span>
                                </span>
                            )}
                        </>
                    ) : (
                        <span className='text-slate-500'>--</span>
                    )}
                </div>
            </td>
            {tableSources.map(source => (
                <td key={`${row.key}-${source.id}`} className='vitals-cell'>
                    <MeasurementValueCell cell={row.valuesBySourceIndex[source.index]} />
                </td>
            ))}
        </tr>
    );
}, (prev, next) => (
    prev.row === next.row &&
    prev.tableSources === next.tableSources &&
    prev.selected === next.selected &&
    prev.starred === next.starred &&
    prev.tooltip === next.tooltip &&
    prev.overview === next.overview
));

type CategoryRowProps = {
    row: VitalsCategoryRow;
    tableSources: SourceColumn[];
    selection: CategorySelectionState;
    onToggleCategory: (category: string, checked: boolean) => void;
};

const CategoryRow = memo(function CategoryRow({
    row,
    tableSources,
    selection,
    onToggleCategory,
}: CategoryRowProps) {
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
                <div className='inline-flex min-h-[22px] items-center gap-2'>
                    <strong>{row.category}</strong>
                    <span className='text-[11px] text-slate-500'>{row.categoryCount}</span>
                </div>
            </td>
            <td className='vitals-cell vitals-col-overview'>
                <div className='min-h-[18px]' />
            </td>
            {tableSources.map(source => (
                <td key={`${row.key}-${source.id}`} className='vitals-cell'>
                    <div className='min-h-[18px]' />
                </td>
            ))}
        </tr>
    );
}, (prev, next) => (
    prev.row === next.row &&
    prev.tableSources === next.tableSources &&
    prev.selection.checked === next.selection.checked &&
    prev.selection.indeterminate === next.selection.indeterminate &&
    prev.selection.disabled === next.selection.disabled
));

export const VitalsTable = memo(function VitalsTable({
    rows,
    tableSources,
    selectedRowKeySet,
    categorySelectionByName,
    starredMeasurementSet,
    measurementOverviewByKey,
    measurementRangesTooltipByKey,
    tableScrollY,
    tableScrollX,
    onToggleRow,
    onToggleAllRows,
    onToggleCategory,
    onToggleStar,
}: VitalsTableProps) {
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

    return (
        <div className='flex min-h-0 flex-1'>
            <div className='vitals-table-shell' style={{ maxHeight: tableScrollY }}>
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
                            {tableSources.map(source => (
                                <th key={source.id} className='vitals-head'>
                                    {source.prettyDate}
                                </th>
                            ))}
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
                                    selected={selectedRowKeySet.has(row.key)}
                                    starred={starredMeasurementSet.has(row.key)}
                                    tooltip={measurementRangesTooltipByKey.get(row.key) ?? row.measurement}
                                    overview={measurementOverviewByKey.get(row.key) ?? { inRange: 0, outOfRange: 0 }}
                                    onToggleRow={onToggleRow}
                                    onToggleStar={onToggleStar}
                                />
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
});
