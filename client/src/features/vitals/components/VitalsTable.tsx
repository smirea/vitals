import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef } from 'react';

import { CheckCircle, Flag, Star, WarningCircle } from '@phosphor-icons/react';
import { theme as antdTheme } from 'antd';

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
            className='vitals-toggle-checkbox'
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
                        className='vitals-star-button'
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
