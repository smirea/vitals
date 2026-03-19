import type {
    BloodworkDashboardMarker,
    BloodworkDashboardReport,
    BloodworkDashboardResult,
} from './api';
import type {
    CategoryOverviewItem,
    CategorySelectionState,
    ChartSeriesModel,
    MeasurementCell,
    MeasurementOverviewTally,
    MeasurementRangeStatus,
    MeaningfulChangeDirection,
    MeaningfulChangeItem,
    SourceColumn,
    VitalsCategoryRow,
    VitalsDisplayRow,
    VitalsRowModel,
} from './types';
import {
    CHART_PALETTE,
    formatCell,
    formatPrettyDate,
    getRowDefaultRange,
    getRowObservedBounds,
    hasCellDisplayValue,
    hasCellNumericValue,
    isCellOutsideReferenceRange,
    normalizeCategoryLabel,
    normalizeCellForChart,
    resolveSeriesUnitLabel,
    UNCATEGORIZED_CATEGORY_LABEL,
} from './utils';

const FAVORITES_CATEGORY_LABEL = 'Favorites';
const SIX_MONTHS = 6;

export function getOrderedReports(reports: BloodworkDashboardReport[]): BloodworkDashboardReport[] {
    return [...reports].sort((left, right) => {
        if (left.date !== right.date) {
            return right.date.localeCompare(left.date);
        }
        return right.id - left.id;
    });
}

export function getSources(orderedReports: BloodworkDashboardReport[]): SourceColumn[] {
    return orderedReports.map((report, index) => ({
        id: String(report.id),
        reportId: report.id,
        date: report.date,
        prettyDate: formatPrettyDate(report.date),
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
    orderedReports,
    markers,
    results,
    sourceCount,
}: {
    orderedReports: BloodworkDashboardReport[];
    markers: BloodworkDashboardMarker[];
    results: BloodworkDashboardResult[];
    sourceCount: number;
}): VitalsRowModel[] {
    const grouped = new Map<string, VitalsRowModel>();
    const markerById = new Map(markers.map(marker => [marker.id, marker]));
    const sourceIndexByReportId = new Map(orderedReports.map((report, index) => [report.id, index]));
    const resultsByReportId = new Map<number, BloodworkDashboardResult[]>();

    results.forEach(result => {
        const existing = resultsByReportId.get(result.reportId);
        if (existing) {
            existing.push(result);
            return;
        }
        resultsByReportId.set(result.reportId, [result]);
    });

    orderedReports.forEach(report => {
        const sourceIndex = sourceIndexByReportId.get(report.id);
        if (sourceIndex === undefined) {
            return;
        }

        const reportResults = resultsByReportId.get(report.id) ?? [];
        reportResults.forEach(result => {
            const marker = markerById.get(result.markerId);
            if (!marker) {
                return;
            }

            const key = marker.key.trim().toLowerCase();
            if (!key) {
                return;
            }

            const category = normalizeCategoryLabel(result.category);
            const existing = grouped.get(key);
            const measurement = {
                key,
                name: marker.name,
                category: result.category,
                valueText: result.valueText,
                valueNumeric: result.valueNumeric,
                unit: result.unit,
                referenceRangeMin: result.referenceRangeMin,
                referenceRangeMax: result.referenceRangeMax,
                flag: result.flag,
                note: result.note,
            };

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
                measurement: marker.name,
                category,
                measurementSearchText: marker.name.trim().toLowerCase(),
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

export function getCategoryOverviewByLatestAcrossAllLabs({
    allMeasurementRows,
    sources,
}: {
    allMeasurementRows: VitalsRowModel[];
    sources: SourceColumn[];
}): CategoryOverviewItem[] {
    if (sources.length === 0 || allMeasurementRows.length === 0) {
        return [];
    }

    const latestSources = sources
        .map(source => {
            const timestamp = Date.parse(source.date);
            if (!Number.isFinite(timestamp)) {
                return null;
            }
            return { source, timestamp };
        })
        .filter((item): item is { source: SourceColumn; timestamp: number } => item !== null);

    latestSources.sort((left, right) => {
        if (left.timestamp !== right.timestamp) {
            return right.timestamp - left.timestamp;
        }
        return left.source.index - right.source.index;
    });

    if (latestSources.length === 0) {
        return [];
    }

    const overviewByCategory = new Map<string, CategoryOverviewItem>();

    allMeasurementRows.forEach(row => {
        const latestSourceEntry = latestSources.find(source => hasCellDisplayValue(row.valuesBySourceIndex[source.source.index]));
        if (!latestSourceEntry) {
            return;
        }

        const latestCell = row.valuesBySourceIndex[latestSourceEntry.source.index];
        if (!latestCell) {
            return;
        }

        const existing = overviewByCategory.get(row.category) ?? {
            category: row.category,
            inRange: 0,
            outOfRange: 0,
            unclassified: 0,
            total: 0,
        };

        if (isCellOutsideReferenceRange(latestCell)) {
            existing.outOfRange += 1;
        } else if (latestCell.numericValue !== null && (latestCell.rangeMin !== null || latestCell.rangeMax !== null)) {
            existing.inRange += 1;
        } else {
            existing.unclassified += 1;
        }

        existing.total += 1;
        overviewByCategory.set(row.category, existing);
    });

    return Array.from(overviewByCategory.values())
        .filter(item => item.total > 0)
        .sort((left, right) => left.category.localeCompare(right.category));
}

function getRangeStatus(cell: MeasurementCell | undefined): MeasurementRangeStatus {
    if (!cell || cell.numericValue === null) {
        return 'unclassified';
    }

    if (cell.rangeMin === null && cell.rangeMax === null) {
        return 'unclassified';
    }

    return isCellOutsideReferenceRange(cell) ? 'out-of-range' : 'in-range';
}

function getNormalizedRangePosition(cell: MeasurementCell | undefined): number | null {
    if (!cell || cell.numericValue === null || cell.rangeMin === null || cell.rangeMax === null) {
        return null;
    }

    const low = Math.min(cell.rangeMin, cell.rangeMax);
    const high = Math.max(cell.rangeMin, cell.rangeMax);
    if (!(high > low)) {
        return null;
    }

    return (cell.numericValue - low) / (high - low);
}

function getDistanceOutsideRange(cell: MeasurementCell | undefined): number | null {
    if (!cell || cell.numericValue === null) {
        return null;
    }

    const value = cell.numericValue;
    const min = cell.rangeMin;
    const max = cell.rangeMax;

    if (min !== null && value < min) {
        return min - value;
    }
    if (max !== null && value > max) {
        return value - max;
    }

    return 0;
}

function resolveMeaningfulChangeDirection({
    previousStatus,
    latestStatus,
    previousCell,
    latestCell,
    previousNormalizedPosition,
    latestNormalizedPosition,
}: {
    previousStatus: MeasurementRangeStatus;
    latestStatus: MeasurementRangeStatus;
    previousCell: MeasurementCell | undefined;
    latestCell: MeasurementCell | undefined;
    previousNormalizedPosition: number | null;
    latestNormalizedPosition: number | null;
}): MeaningfulChangeDirection {
    if (previousStatus === 'out-of-range' && latestStatus === 'in-range') {
        return 'improved';
    }
    if (previousStatus === 'in-range' && latestStatus === 'out-of-range') {
        return 'worsened';
    }

    if (
        previousStatus === 'in-range' &&
        latestStatus === 'in-range' &&
        previousNormalizedPosition !== null &&
        latestNormalizedPosition !== null
    ) {
        const previousDistanceFromCenter = Math.abs(previousNormalizedPosition - 0.5);
        const latestDistanceFromCenter = Math.abs(latestNormalizedPosition - 0.5);
        if (latestDistanceFromCenter < previousDistanceFromCenter) {
            return 'improved';
        }
        if (latestDistanceFromCenter > previousDistanceFromCenter) {
            return 'worsened';
        }
    }

    const previousOutsideDistance = getDistanceOutsideRange(previousCell);
    const latestOutsideDistance = getDistanceOutsideRange(latestCell);
    if (previousOutsideDistance !== null && latestOutsideDistance !== null) {
        if (latestOutsideDistance < previousOutsideDistance) {
            return 'improved';
        }
        if (latestOutsideDistance > previousOutsideDistance) {
            return 'worsened';
        }
    }

    if (
        previousCell?.numericValue !== null &&
        previousCell?.numericValue !== undefined &&
        latestCell?.numericValue !== null &&
        latestCell?.numericValue !== undefined
    ) {
        if (latestCell.numericValue < previousCell.numericValue) {
            return 'improved';
        }
        if (latestCell.numericValue > previousCell.numericValue) {
            return 'worsened';
        }
    }

    return 'changed';
}

function getMeaningfulChangeScore({
    direction,
    relativeDeltaPercent,
    normalizedRangeDeltaPercent,
    latestStatus,
    statusTransition,
}: {
    direction: MeaningfulChangeDirection;
    relativeDeltaPercent: number | null;
    normalizedRangeDeltaPercent: number | null;
    latestStatus: MeasurementRangeStatus;
    statusTransition: boolean;
}): number {
    let score = 0;

    if (statusTransition) {
        score += 1000;
    }
    if (latestStatus === 'out-of-range') {
        score += 220;
    }
    if (direction === 'worsened') {
        score += 120;
    }
    if (direction === 'improved') {
        score += 80;
    }
    if (relativeDeltaPercent !== null) {
        score += Math.min(relativeDeltaPercent, 500);
    }
    if (normalizedRangeDeltaPercent !== null) {
        score += normalizedRangeDeltaPercent * 1.4;
    }

    return score;
}

export function getSixMonthMeaningfulChanges({
    allMeasurementRows,
    sources,
}: {
    allMeasurementRows: VitalsRowModel[];
    sources: SourceColumn[];
}): MeaningfulChangeItem[] {
    if (allMeasurementRows.length === 0 || sources.length < 2) {
        return [];
    }

    const datedSources = sources
        .map(source => {
            const timestamp = Date.parse(source.date);
            if (!Number.isFinite(timestamp)) {
                return null;
            }
            return { source, timestamp };
        })
        .filter((entry): entry is { source: SourceColumn; timestamp: number } => entry !== null)
        .sort((left, right) => {
            if (left.timestamp !== right.timestamp) {
                return right.timestamp - left.timestamp;
            }
            return left.source.index - right.source.index;
        });

    if (datedSources.length < 2) {
        return [];
    }

    const latestTimestamp = datedSources[0].timestamp;
    const sixMonthCutoff = new Date(latestTimestamp);
    sixMonthCutoff.setUTCMonth(sixMonthCutoff.getUTCMonth() - SIX_MONTHS);
    const sixMonthCutoffTimestamp = sixMonthCutoff.getTime();
    const meaningfulChanges: MeaningfulChangeItem[] = [];

    allMeasurementRows.forEach(row => {
        let latestInWindowIndex = -1;
        for (let index = 0; index < datedSources.length; index += 1) {
            const entry = datedSources[index];
            if (entry.timestamp < sixMonthCutoffTimestamp) {
                break;
            }
            if (hasCellDisplayValue(row.valuesBySourceIndex[entry.source.index])) {
                latestInWindowIndex = index;
                break;
            }
        }

        if (latestInWindowIndex < 0) {
            return;
        }

        let previousIndex = -1;
        for (let index = latestInWindowIndex + 1; index < datedSources.length; index += 1) {
            const entry = datedSources[index];
            if (hasCellDisplayValue(row.valuesBySourceIndex[entry.source.index])) {
                previousIndex = index;
                break;
            }
        }

        if (previousIndex < 0) {
            return;
        }

        const latestEntry = datedSources[latestInWindowIndex];
        const previousEntry = datedSources[previousIndex];
        const latestCell = row.valuesBySourceIndex[latestEntry.source.index];
        const previousCell = row.valuesBySourceIndex[previousEntry.source.index];

        if (!latestCell || !previousCell) {
            return;
        }

        const latestStatus = getRangeStatus(latestCell);
        const previousStatus = getRangeStatus(previousCell);

        const latestNumeric = latestCell.numericValue;
        const previousNumeric = previousCell.numericValue;
        const relativeDeltaPercent = (
            latestNumeric !== null &&
            previousNumeric !== null
        )
            ? (Math.abs(latestNumeric - previousNumeric) / Math.max(Math.abs(previousNumeric), 1e-6)) * 100
            : null;

        const latestNormalizedPosition = getNormalizedRangePosition(latestCell);
        const previousNormalizedPosition = getNormalizedRangePosition(previousCell);
        const normalizedRangeDeltaPercent = (
            latestNormalizedPosition !== null &&
            previousNormalizedPosition !== null
        )
            ? Math.abs(latestNormalizedPosition - previousNormalizedPosition) * 100
            : null;

        const movedOutToIn = previousStatus === 'out-of-range' && latestStatus === 'in-range';
        const movedInToOut = previousStatus === 'in-range' && latestStatus === 'out-of-range';
        const largeRelativeChange = relativeDeltaPercent !== null && relativeDeltaPercent >= 30;
        const largeWithinRangeDrift = (
            previousStatus === 'in-range' &&
            latestStatus === 'in-range' &&
            normalizedRangeDeltaPercent !== null &&
            normalizedRangeDeltaPercent >= 30
        );

        if (!movedOutToIn && !movedInToOut && !largeRelativeChange && !largeWithinRangeDrift) {
            return;
        }

        const reasons: string[] = [];
        if (movedOutToIn) {
            reasons.push('Out of range → in range');
        }
        if (movedInToOut) {
            reasons.push('In range → out of range');
        }
        if (largeWithinRangeDrift) {
            reasons.push('Within-range drift ≥ 30%');
        }
        if (largeRelativeChange) {
            reasons.push('Value changed ≥ 30%');
        }

        const direction = resolveMeaningfulChangeDirection({
            previousStatus,
            latestStatus,
            previousCell,
            latestCell,
            previousNormalizedPosition,
            latestNormalizedPosition,
        });

        const score = getMeaningfulChangeScore({
            direction,
            relativeDeltaPercent,
            normalizedRangeDeltaPercent,
            latestStatus,
            statusTransition: movedOutToIn || movedInToOut,
        });

        meaningfulChanges.push({
            key: row.key,
            measurement: row.measurement,
            category: row.category,
            direction,
            score,
            reasons,
            relativeDeltaPercent,
            normalizedRangeDeltaPercent,
            latest: {
                date: latestEntry.source.date,
                prettyDate: latestEntry.source.prettyDate,
                display: latestCell.display,
                status: latestStatus,
            },
            previous: {
                date: previousEntry.source.date,
                prettyDate: previousEntry.source.prettyDate,
                display: previousCell.display,
                status: previousStatus,
            },
        });
    });

    return meaningfulChanges.sort((left, right) => {
        if (right.score !== left.score) {
            return right.score - left.score;
        }
        if (right.latest.date !== left.latest.date) {
            return right.latest.date.localeCompare(left.latest.date);
        }
        return left.measurement.localeCompare(right.measurement);
    });
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
