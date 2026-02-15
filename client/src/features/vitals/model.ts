import type {
    BloodworkLab,
    CategorySelectionState,
    ChartSeriesModel,
    MeasurementOverviewTally,
    SourceColumn,
    VitalsCategoryRow,
    VitalsDisplayRow,
    VitalsRowModel,
} from './types';
import {
    CHART_PALETTE,
    formatCell,
    formatPrettyDate,
    hasCellDisplayValue,
    hasCellNumericValue,
    getRowDefaultRange,
    getRowObservedBounds,
    isCellOutsideReferenceRange,
    normalizeCategoryLabel,
    normalizeCellForChart,
    resolveSeriesUnitLabel,
    UNCATEGORIZED_CATEGORY_LABEL,
} from './utils';

const FAVORITES_CATEGORY_LABEL = 'Favorites';

export function getOrderedLabs(labs: BloodworkLab[]): BloodworkLab[] {
    return [...labs].sort((left, right) => right.date.localeCompare(left.date));
}

export function getSources(orderedLabs: BloodworkLab[]): SourceColumn[] {
    return orderedLabs.map((lab, index) => ({
        id: `${lab.date}__${lab.labName}__${index}`,
        date: lab.date,
        prettyDate: formatPrettyDate(lab.date),
        index,
    }));
}

export function getDateBounds(sources: SourceColumn[]): { min: string; max: string } {
    if (sources.length === 0) return { min: '', max: '' };
    const sortedDates = sources.map(item => item.date).sort((left, right) => left.localeCompare(right));
    return {
        min: sortedDates[0] ?? '',
        max: sortedDates[sortedDates.length - 1] ?? '',
    };
}

export function getVisibleSources({
    sources,
    dateRangeStart,
    dateRangeEnd,
}: {
    sources: SourceColumn[];
    dateRangeStart: string;
    dateRangeEnd: string;
}): SourceColumn[] {
    return sources.filter(source => {
        if (dateRangeStart && source.date < dateRangeStart) return false;
        if (dateRangeEnd && source.date > dateRangeEnd) return false;
        return true;
    });
}

export function getChartSources({
    visibleSources,
    selectedRows,
}: {
    visibleSources: SourceColumn[];
    selectedRows: VitalsRowModel[];
}): SourceColumn[] {
    if (selectedRows.length === 0) {
        return [];
    }

    return visibleSources
        .filter(source => selectedRows.some(row => hasCellNumericValue(row.valuesBySourceIndex[source.index])))
        .sort((left, right) => {
            const byDate = left.date.localeCompare(right.date);
            if (byDate !== 0) {
                return byDate;
            }
            return left.index - right.index;
        });
}

export function getAllMeasurementRows({
    orderedLabs,
    sourceCount,
}: {
    orderedLabs: BloodworkLab[];
    sourceCount: number;
}): VitalsRowModel[] {
    const grouped = new Map<string, VitalsRowModel>();

    orderedLabs.forEach((lab, sourceIndex) => {
        lab.measurements.forEach(measurement => {
            const key = measurement.name.trim().toLowerCase();
            if (!key) return;
            const category = normalizeCategoryLabel(measurement.category);
            const existing = grouped.get(key);
            if (existing) {
                existing.valuesBySourceIndex[sourceIndex] = formatCell(measurement);
                if (
                    existing.category === UNCATEGORIZED_CATEGORY_LABEL &&
                    category !== UNCATEGORIZED_CATEGORY_LABEL
                ) {
                    existing.category = category;
                    existing.categorySearchText = category.toLowerCase();
                }
                return;
            }

            const valuesBySourceIndex = Array.from({ length: sourceCount }, () => undefined) as Array<ReturnType<typeof formatCell> | undefined>;
            valuesBySourceIndex[sourceIndex] = formatCell(measurement);
            grouped.set(key, {
                key,
                rowType: 'measurement',
                measurement: measurement.name,
                category,
                measurementSearchText: measurement.name.trim().toLowerCase(),
                categorySearchText: category.toLowerCase(),
                valuesBySourceIndex,
            });
        });
    });

    return Array.from(grouped.values()).sort((left, right) => left.measurement.localeCompare(right.measurement));
}

export function getFilteredMeasurementRows({
    allMeasurementRows,
    measurementFilter,
    starredMeasurementSet,
}: {
    allMeasurementRows: VitalsRowModel[];
    measurementFilter: string;
    starredMeasurementSet: Set<string>;
}): VitalsRowModel[] {
    const normalizedFilter = measurementFilter.trim().toLowerCase();
    const candidateRows = normalizedFilter
        ? allMeasurementRows.filter(row => (
            row.measurementSearchText.includes(normalizedFilter) ||
            row.categorySearchText.includes(normalizedFilter)
        ))
        : allMeasurementRows;

    return [...candidateRows].sort((left, right) => {
        const leftIsStarred = starredMeasurementSet.has(left.key);
        const rightIsStarred = starredMeasurementSet.has(right.key);
        if (leftIsStarred !== rightIsStarred) return leftIsStarred ? -1 : 1;
        return left.measurement.localeCompare(right.measurement);
    });
}

export function getRowsWithVisibleData({
    filteredMeasurementRows,
    visibleSources,
}: {
    filteredMeasurementRows: VitalsRowModel[];
    visibleSources: SourceColumn[];
}): VitalsRowModel[] {
    if (visibleSources.length === 0) {
        return [];
    }

    return filteredMeasurementRows.filter(row => (
        visibleSources.some(source => hasCellDisplayValue(row.valuesBySourceIndex[source.index]))
    ));
}

function getVisibleSourceIndicesWithData({
    filteredMeasurementRows,
    visibleSources,
}: {
    filteredMeasurementRows: VitalsRowModel[];
    visibleSources: SourceColumn[];
}): Set<number> {
    const sourceIndices = new Set<number>();
    for (const row of filteredMeasurementRows) {
        for (const source of visibleSources) {
            const cell = row.valuesBySourceIndex[source.index];
            if (hasCellDisplayValue(cell)) {
                sourceIndices.add(source.index);
            }
        }
    }
    return sourceIndices;
}

export function getTableSources({
    filteredMeasurementRows,
    visibleSources,
}: {
    filteredMeasurementRows: VitalsRowModel[];
    visibleSources: SourceColumn[];
}): SourceColumn[] {
    const visibleSourceIndicesWithData = getVisibleSourceIndicesWithData({
        filteredMeasurementRows,
        visibleSources,
    });

    return visibleSources.filter(source => visibleSourceIndicesWithData.has(source.index));
}

export function getTableRows({
    filteredMeasurementRows,
    groupByCategory,
    starredMeasurementSet,
}: {
    filteredMeasurementRows: VitalsRowModel[];
    groupByCategory: boolean;
    starredMeasurementSet: Set<string>;
}): VitalsDisplayRow[] {
    if (!groupByCategory) {
        return filteredMeasurementRows;
    }

    const favoriteRows = filteredMeasurementRows.filter(row => starredMeasurementSet.has(row.key));
    const nonFavoriteRows = filteredMeasurementRows.filter(row => !starredMeasurementSet.has(row.key));

    const grouped = new Map<string, VitalsRowModel[]>();
    nonFavoriteRows.forEach(row => {
        const existing = grouped.get(row.category);
        if (existing) {
            existing.push(row);
            return;
        }
        grouped.set(row.category, [row]);
    });

    const categories = Array.from(grouped.keys()).sort((left, right) => left.localeCompare(right));
    const rows: VitalsDisplayRow[] = [];

    if (favoriteRows.length > 0) {
        const favoriteHeader: VitalsCategoryRow = {
            key: `category:${FAVORITES_CATEGORY_LABEL.toLowerCase()}`,
            rowType: 'category',
            category: FAVORITES_CATEGORY_LABEL,
            categoryCount: favoriteRows.length,
        };
        rows.push(favoriteHeader, ...favoriteRows);
    }

    categories.forEach(category => {
        const items = grouped.get(category) ?? [];
        const header: VitalsCategoryRow = {
            key: `category:${category.toLowerCase()}`,
            rowType: 'category',
            category,
            categoryCount: items.length,
        };
        rows.push(header, ...items);
    });

    return rows;
}

export function getMeasurementKeysByCategory({
    filteredMeasurementRows,
    groupByCategory,
    starredMeasurementSet,
}: {
    filteredMeasurementRows: VitalsRowModel[];
    groupByCategory: boolean;
    starredMeasurementSet: Set<string>;
}): Map<string, string[]> {
    const grouped = new Map<string, string[]>();

    if (groupByCategory) {
        const favoriteKeys = filteredMeasurementRows
            .filter(row => starredMeasurementSet.has(row.key))
            .map(row => row.key);

        if (favoriteKeys.length > 0) {
            grouped.set(FAVORITES_CATEGORY_LABEL, favoriteKeys);
        }
    }

    filteredMeasurementRows.forEach(row => {
        if (groupByCategory && starredMeasurementSet.has(row.key)) {
            return;
        }
        const existing = grouped.get(row.category);
        if (existing) {
            existing.push(row.key);
            return;
        }
        grouped.set(row.category, [row.key]);
    });
    return grouped;
}

export function getPrunedSelectedRowKeys({
    selectedRowKeys,
    filteredMeasurementRows,
}: {
    selectedRowKeys: string[];
    filteredMeasurementRows: VitalsRowModel[];
}): string[] {
    const availableRowIds = new Set(filteredMeasurementRows.map(item => item.key));
    return selectedRowKeys.filter(item => availableRowIds.has(item));
}

export function getCategorySelectionByName({
    measurementKeysByCategory,
    selectedRowKeySet,
}: {
    measurementKeysByCategory: Map<string, string[]>;
    selectedRowKeySet: Set<string>;
}): Map<string, CategorySelectionState> {
    const stateByCategory = new Map<string, CategorySelectionState>();
    measurementKeysByCategory.forEach((measurementKeys, category) => {
        const selectedCount = measurementKeys.reduce(
            (count, key) => (selectedRowKeySet.has(key) ? count + 1 : count),
            0,
        );
        const total = measurementKeys.length;
        stateByCategory.set(category, {
            checked: total > 0 && selectedCount === total,
            indeterminate: selectedCount > 0 && selectedCount < total,
            disabled: total === 0,
        });
    });
    return stateByCategory;
}

export function getSelectedRows({
    filteredMeasurementRows,
    selectedRowKeySet,
}: {
    filteredMeasurementRows: VitalsRowModel[];
    selectedRowKeySet: Set<string>;
}): VitalsRowModel[] {
    return filteredMeasurementRows.filter(row => selectedRowKeySet.has(row.key));
}

export function getMeasurementRangesTooltipByKey({
    filteredMeasurementRows,
    sources,
}: {
    filteredMeasurementRows: VitalsRowModel[];
    sources: SourceColumn[];
}): Map<string, string> {
    const tooltipByKey = new Map<string, string>();
    filteredMeasurementRows.forEach(row => {
        const rangeLines = sources
            .map(source => {
                const rangeCaption = row.valuesBySourceIndex[source.index]?.rangeCaption;
                if (!rangeCaption) return null;
                return `${source.prettyDate}: ${rangeCaption}`;
            })
            .filter((entry): entry is string => Boolean(entry));

        const lines = [
            row.measurement,
            ...(rangeLines.length > 0 ? rangeLines : ['No recorded reference ranges.']),
        ];
        tooltipByKey.set(row.key, lines.join('\n'));
    });
    return tooltipByKey;
}

export function getMeasurementOverviewByKey({
    filteredMeasurementRows,
    tableSources,
}: {
    filteredMeasurementRows: VitalsRowModel[];
    tableSources: SourceColumn[];
}): Map<string, MeasurementOverviewTally> {
    const overviewByKey = new Map<string, MeasurementOverviewTally>();
    filteredMeasurementRows.forEach(row => {
        let inRange = 0;
        let outOfRange = 0;

        tableSources.forEach(source => {
            const cell = row.valuesBySourceIndex[source.index];
            if (!cell) {
                return;
            }
            if (isCellOutsideReferenceRange(cell)) {
                outOfRange += 1;
                return;
            }
            inRange += 1;
        });

        overviewByKey.set(row.key, {
            inRange,
            outOfRange,
        });
    });
    return overviewByKey;
}

export function getOutOfRangeMeasurementCountBySourceId({
    filteredMeasurementRows,
    tableSources,
}: {
    filteredMeasurementRows: VitalsRowModel[];
    tableSources: SourceColumn[];
}): Map<string, number> {
    const countBySourceId = new Map<string, number>();

    tableSources.forEach(source => {
        let count = 0;
        filteredMeasurementRows.forEach(row => {
            if (isCellOutsideReferenceRange(row.valuesBySourceIndex[source.index])) {
                count += 1;
            }
        });
        countBySourceId.set(source.id, count);
    });

    return countBySourceId;
}

export function getRowsMatchingOutOfRangeSources({
    filteredMeasurementRows,
    tableSources,
    outOfRangeSourceIdSet,
}: {
    filteredMeasurementRows: VitalsRowModel[];
    tableSources: SourceColumn[];
    outOfRangeSourceIdSet: Set<string>;
}): VitalsRowModel[] {
    if (outOfRangeSourceIdSet.size === 0) {
        return filteredMeasurementRows;
    }

    const selectedSources = tableSources.filter(source => outOfRangeSourceIdSet.has(source.id));
    if (selectedSources.length === 0) {
        return filteredMeasurementRows;
    }

    return filteredMeasurementRows.filter(row => (
        selectedSources.some(source => isCellOutsideReferenceRange(row.valuesBySourceIndex[source.index]))
    ));
}

export function getChartSeries({
    selectedRows,
    chartSources,
}: {
    selectedRows: VitalsRowModel[];
    chartSources: SourceColumn[];
}): ChartSeriesModel[] {
    const result: ChartSeriesModel[] = [];

    selectedRows.forEach(row => {
        const cells = chartSources.map(source => row.valuesBySourceIndex[source.index]);
        const hasAnyNumericData = cells.some(hasCellNumericValue);
        if (!hasAnyNumericData) {
            return;
        }
        const defaultRange = getRowDefaultRange(cells);
        const observedBounds = getRowObservedBounds(cells);
        const normalizedValuesBySourceIndex = Array.from({ length: row.valuesBySourceIndex.length }, () => null as number | null);
        const outOfRangeBySourceIndex = Array.from({ length: row.valuesBySourceIndex.length }, () => false);

        for (const source of chartSources) {
            const cell = row.valuesBySourceIndex[source.index];
            const normalizedValue = normalizeCellForChart({
                cell,
                defaultRange,
                observedBounds,
            });
            normalizedValuesBySourceIndex[source.index] = normalizedValue;
            outOfRangeBySourceIndex[source.index] = isCellOutsideReferenceRange(cell) || (
                normalizedValue !== null && (normalizedValue < 0 || normalizedValue > 1)
            );
        }

        result.push({
            id: row.key,
            chartKey: `series_${result.length}`,
            label: row.measurement,
            color: CHART_PALETTE[result.length % CHART_PALETTE.length],
            valuesBySourceIndex: row.valuesBySourceIndex,
            normalizedValuesBySourceIndex,
            outOfRangeBySourceIndex,
            unitLabel: resolveSeriesUnitLabel(cells),
        });
    });

    return result;
}
