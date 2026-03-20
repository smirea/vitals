import type {
    BloodworkDashboardMarker,
    BloodworkDashboardReport,
    BloodworkDashboardResult,
} from '../../utils/api';

export type MeasurementFlag = 'low' | 'high' | 'normal' | 'abnormal' | 'critical' | 'unknown';

export type BloodworkMeasurementRecord = {
    key: string;
    name: string;
    category: string | null;
    valueText: string | null;
    valueNumeric: number | null;
    unit: string | null;
    referenceRangeMin: number | null;
    referenceRangeMax: number | null;
    flag: MeasurementFlag | null;
    note: string | null;
};

export type SourceColumn = {
    id: string;
    reportId: number;
    date: string;
    prettyDate: string;
    index: number;
};

export type MeasurementCell = {
    display: string;
    numericValue: number | null;
    rangeMin: number | null;
    rangeMax: number | null;
    rangeCaption: string;
    rangeVisualization: {
        minPosition: number | null;
        maxPosition: number | null;
        valuePosition: number;
    } | null;
    rangeBandLeft: number;
    rangeBandWidth: number;
    unit?: string;
    flag?: MeasurementFlag;
    note?: string;
};

export type VitalsRowModel = {
    rowType: 'measurement';
    key: string;
    measurement: string;
    category: string;
    measurementSearchText: string;
    categorySearchText: string;
    valuesBySourceIndex: Array<MeasurementCell | undefined>;
};

export type VitalsCategoryRow = {
    rowType: 'category';
    key: string;
    category: string;
    categoryCount: number;
};

export type VitalsDisplayRow = VitalsRowModel | VitalsCategoryRow;

export type CategorySelectionState = {
    checked: boolean;
    indeterminate: boolean;
    disabled: boolean;
};

export type MeasurementOverviewTally = {
    inRange: number;
    outOfRange: number;
};

export type CategoryOverviewItem = {
    category: string;
    inRange: number;
    outOfRange: number;
    unclassified: number;
    total: number;
};

export type MeasurementRangeStatus = 'in-range' | 'out-of-range' | 'unclassified';

export type MeaningfulChangeDirection = 'improved' | 'worsened' | 'changed';

export type MeaningfulChangeItem = {
    key: string;
    measurement: string;
    category: string;
    direction: MeaningfulChangeDirection;
    score: number;
    reasons: string[];
    relativeDeltaPercent: number | null;
    normalizedRangeDeltaPercent: number | null;
    latest: {
        date: string;
        prettyDate: string;
        display: string;
        status: MeasurementRangeStatus;
    };
    previous: {
        date: string;
        prettyDate: string;
        display: string;
        status: MeasurementRangeStatus;
    };
};

export type ChartSeriesModel = {
    id: string;
    chartKey: string;
    label: string;
    color: string;
    valuesBySourceIndex: Array<MeasurementCell | undefined>;
    normalizedValuesBySourceIndex: Array<number | null>;
    outOfRangeBySourceIndex: boolean[];
    unitLabel?: string;
};

export type TrendChartDatum = {
    sourceId: string;
    prettyDate: string;
    [key: string]: string | number | boolean | null;
};

export type SelectionState = {
    selectedRowKeys: string[];
    selectedRowKeySet: Set<string>;
};

export type VitalsViewModel = {
    sources: SourceColumn[];
    visibleSources: SourceColumn[];
    chartSources: SourceColumn[];
    tableSources: SourceColumn[];
    allMeasurementRows: VitalsRowModel[];
    filteredMeasurementRows: VitalsRowModel[];
    tableRows: VitalsDisplayRow[];
    measurementKeysByCategory: Map<string, string[]>;
    categorySelectionByName: Map<string, CategorySelectionState>;
    selectedRows: VitalsRowModel[];
    measurementOverviewByKey: Map<string, MeasurementOverviewTally>;
    measurementRangesTooltipByKey: Map<string, string>;
    chartSeries: ChartSeriesModel[];
};

export const DATE_FORMATTER = new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
});

export const RESIZER_WIDTH = 10;
export const MIN_CHART_PANE_WIDTH = 300;
export const SELECTION_COLUMN_WIDTH = 52;
export const MEASUREMENT_COLUMN_WIDTH = 250;
export const OVERVIEW_COLUMN_WIDTH = 94;
export const SOURCE_COLUMN_WIDTH = 164;
export const STARRED_MEASUREMENTS_STORAGE_KEY = 'vitals.starred.measurements';
export const SELECTED_ROWS_STORAGE_KEY = 'vitals.selected.rows';
export const GROUP_BY_CATEGORY_STORAGE_KEY = 'vitals.group-by-category';
export const OUT_OF_RANGE_SOURCE_FILTERS_STORAGE_KEY = 'vitals.out-of-range.source-filters';
export const UNCATEGORIZED_CATEGORY_LABEL = 'Uncategorized';
export const CHART_PALETTE = ['#0f172a', '#2563eb', '#0f766e', '#15803d', '#7c3aed', '#ca8a04'];

export function clamp(value: number, min: number, max: number): number {
    return Math.min(max, Math.max(min, value));
}

export function normalizeStarredMeasurementKeys(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    const unique = new Set<string>();

    value.forEach(item => {
        if (typeof item !== 'string') return;
        const normalized = item.trim().toLowerCase();
        if (!normalized) return;
        unique.add(normalized);
    });

    return Array.from(unique);
}

export function readStoredStarredMeasurementKeys(): string[] {
    if (typeof window === 'undefined') return [];
    try {
        const raw = window.localStorage.getItem(STARRED_MEASUREMENTS_STORAGE_KEY);
        if (!raw) return [];
        return normalizeStarredMeasurementKeys(JSON.parse(raw));
    } catch {
        return [];
    }
}

export function normalizeSelectedRowKeys(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    const unique = new Set<string>();

    value.forEach(item => {
        if (typeof item !== 'string') return;
        const normalized = item.trim().toLowerCase();
        if (!normalized) return;
        unique.add(normalized);
    });

    return Array.from(unique);
}

export function readStoredSelectedRowKeys(): string[] {
    if (typeof window === 'undefined') return [];
    try {
        const raw = window.localStorage.getItem(SELECTED_ROWS_STORAGE_KEY);
        if (!raw) return [];
        return normalizeSelectedRowKeys(JSON.parse(raw));
    } catch {
        return [];
    }
}

export function readStoredGroupByCategory(): boolean {
    if (typeof window === 'undefined') return true;
    try {
        const raw = window.localStorage.getItem(GROUP_BY_CATEGORY_STORAGE_KEY);
        if (raw === null) return true;
        return raw === 'true';
    } catch {
        return true;
    }
}

export function normalizeOutOfRangeSourceFilterIds(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    const unique = new Set<string>();

    value.forEach(item => {
        if (typeof item !== 'string') return;
        const normalized = item.trim();
        if (!normalized) return;
        unique.add(normalized);
    });

    return Array.from(unique);
}

export function readStoredOutOfRangeSourceFilterIds(): string[] {
    if (typeof window === 'undefined') return [];
    try {
        const raw = window.localStorage.getItem(OUT_OF_RANGE_SOURCE_FILTERS_STORAGE_KEY);
        if (!raw) return [];
        return normalizeOutOfRangeSourceFilterIds(JSON.parse(raw));
    } catch {
        return [];
    }
}

export function parseNumericValue(value: number | string | null | undefined): number | null {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value !== 'string') return null;

    const normalized = value.replace(',', '.').replace(/[^0-9.+-]/g, '').trim();
    if (!normalized) return null;
    const parsed = Number.parseFloat(normalized);
    return Number.isFinite(parsed) ? parsed : null;
}

function formatNumericLabel(value: number): string {
    const absolute = Math.abs(value);
    const decimals = absolute >= 100 ? 0 : absolute >= 10 ? 1 : 2;
    return value.toFixed(decimals).replace(/\.?0+$/, '');
}

function formatRangeCaption({
    rangeMin,
    rangeMax,
    unit,
}: {
    rangeMin: number | null;
    rangeMax: number | null;
    unit?: string;
}): string {
    const unitSuffix = unit ? ` ${unit}` : '';
    if (rangeMin !== null && rangeMax !== null) {
        const low = Math.min(rangeMin, rangeMax);
        const high = Math.max(rangeMin, rangeMax);
        return low === high
            ? `ref ${formatNumericLabel(low)}${unitSuffix}`
            : `ref ${formatNumericLabel(low)} - ${formatNumericLabel(high)}${unitSuffix}`;
    }
    if (rangeMin !== null) {
        return `ref >= ${formatNumericLabel(rangeMin)}${unitSuffix}`;
    }
    if (rangeMax !== null) {
        return `ref <= ${formatNumericLabel(rangeMax)}${unitSuffix}`;
    }
    return '';
}

function getRangeVisualization({
    numericValue,
    rangeMin,
    rangeMax,
}: {
    numericValue: number | null;
    rangeMin: number | null;
    rangeMax: number | null;
}): {
    minPosition: number | null;
    maxPosition: number | null;
    valuePosition: number;
} | null {
    if (numericValue === null) return null;
    if (rangeMin === null && rangeMax === null) return null;

    const anchors = [numericValue];
    if (rangeMin !== null) anchors.push(rangeMin);
    if (rangeMax !== null) anchors.push(rangeMax);

    const minAnchor = Math.min(...anchors);
    const maxAnchor = Math.max(...anchors);
    const anchorSpread = maxAnchor - minAnchor;
    const anchorScale = Math.max(...anchors.map(Math.abs), 1);
    const padding = Math.max(anchorSpread * 0.22, anchorScale * 0.06, 0.5);
    const domainMin = minAnchor - padding;
    const domainMax = maxAnchor + padding;
    const domainRange = domainMax - domainMin || 1;
    const toPosition = (value: number) => clamp(((value - domainMin) / domainRange) * 100, 0, 100);

    return {
        minPosition: rangeMin === null ? null : toPosition(rangeMin),
        maxPosition: rangeMax === null ? null : toPosition(rangeMax),
        valuePosition: toPosition(numericValue),
    };
}

export function formatPrettyDate(value: string): string {
    const parsed = Date.parse(value);
    if (!Number.isFinite(parsed)) return value;
    return DATE_FORMATTER.format(new Date(parsed));
}

export function normalizeCategoryLabel(value: string | null | undefined): string {
    const trimmed = value?.trim();
    return trimmed || UNCATEGORIZED_CATEGORY_LABEL;
}

export function formatCell(measurement: BloodworkMeasurementRecord): MeasurementCell {
    const valueText = measurement.valueText?.trim() ?? '';
    const unitText = measurement.unit?.trim() ?? '';
    const display = [valueText, unitText].filter(Boolean).join(' ').trim() || '—';
    const rangeMin = measurement.referenceRangeMin;
    const rangeMax = measurement.referenceRangeMax;
    const numericValue = measurement.valueNumeric ?? parseNumericValue(valueText);
    const rangeVisualization = getRangeVisualization({
        numericValue,
        rangeMin,
        rangeMax,
    });
    const hasBand =
        rangeVisualization?.minPosition !== null &&
        rangeVisualization?.maxPosition !== null;
    const rangeBandLeft = hasBand
        ? Math.min(
            rangeVisualization?.minPosition ?? 0,
            rangeVisualization?.maxPosition ?? 0,
        )
        : 0;
    const rangeBandWidth = hasBand
        ? Math.abs(
            (rangeVisualization?.maxPosition ?? 0) -
            (rangeVisualization?.minPosition ?? 0),
        )
        : 0;
    const rangeCaption = formatRangeCaption({
        rangeMin,
        rangeMax,
        unit: unitText || undefined,
    });

    return {
        display,
        numericValue,
        rangeMin,
        rangeMax,
        rangeCaption,
        rangeVisualization,
        rangeBandLeft,
        rangeBandWidth,
        unit: unitText || undefined,
        flag: measurement.flag ?? undefined,
        note: measurement.note?.trim() || undefined,
    };
}

export function hasCellDisplayValue(cell: MeasurementCell | undefined): boolean {
    if (!cell) {
        return false;
    }
    const value = cell.display.trim();
    return value !== '' && value !== '—' && value !== '--';
}

export function hasCellNumericValue(cell: MeasurementCell | undefined): boolean {
    if (!cell || cell.numericValue === null) {
        return false;
    }
    return Number.isFinite(cell.numericValue);
}

export function resolveSeriesUnitLabel(cells: Array<MeasurementCell | undefined>): string | undefined {
    const normalizedUnits = new Set(
        cells
            .map(cell => cell?.unit?.trim())
            .filter((unit): unit is string => Boolean(unit)),
    );
    if (normalizedUnits.size === 0) {
        return undefined;
    }
    if (normalizedUnits.size === 1) {
        return Array.from(normalizedUnits)[0];
    }
    return 'mixed units';
}

export function getRowDefaultRange(cells: Array<MeasurementCell | undefined>): { min: number; max: number } | null {
    for (const cell of cells) {
        if (!cell) continue;
        if (cell.rangeMin === null || cell.rangeMax === null) continue;
        if (cell.rangeMax <= cell.rangeMin) continue;
        return {
            min: cell.rangeMin,
            max: cell.rangeMax,
        };
    }
    return null;
}

export function getRowObservedBounds(cells: Array<MeasurementCell | undefined>): { min: number; max: number } | null {
    const values = cells
        .map(cell => cell?.numericValue)
        .filter((value): value is number => value !== null && Number.isFinite(value));
    if (values.length === 0) {
        return null;
    }
    return {
        min: Math.min(...values),
        max: Math.max(...values),
    };
}

export function normalizeCellForChart({
    cell,
    defaultRange,
    observedBounds,
}: {
    cell: MeasurementCell | undefined;
    defaultRange: { min: number; max: number } | null;
    observedBounds: { min: number; max: number } | null;
}): number | null {
    if (!cell || cell.numericValue === null) {
        return null;
    }

    let rangeMin = cell.rangeMin;
    let rangeMax = cell.rangeMax;
    if (
        (rangeMin === null || rangeMax === null || rangeMax <= rangeMin) &&
        defaultRange
    ) {
        rangeMin = defaultRange.min;
        rangeMax = defaultRange.max;
    }

    if (rangeMin !== null && rangeMax !== null && rangeMax > rangeMin) {
        return (cell.numericValue - rangeMin) / (rangeMax - rangeMin);
    }

    if (!observedBounds) {
        return 0.5;
    }

    const spread = observedBounds.max - observedBounds.min;
    if (spread === 0) {
        return 0.5;
    }
    return (cell.numericValue - observedBounds.min) / spread;
}

export function isCellOutsideReferenceRange(cell: MeasurementCell | undefined): boolean {
    if (!cell || cell.numericValue === null) {
        return false;
    }
    if (cell.rangeMin !== null && cell.numericValue < cell.rangeMin) {
        return true;
    }
    if (cell.rangeMax !== null && cell.numericValue > cell.rangeMax) {
        return true;
    }
    return false;
}

export function formatNormalizedYAxisTick(value: number): string {
    if (Math.abs(value) < 0.001) {
        return 'Low';
    }
    if (Math.abs(value - 1) < 0.001) {
        return 'High';
    }
    return value.toFixed(2);
}

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
