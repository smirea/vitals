import type { MeasurementCell, BloodworkMeasurement } from './types';

export const DATE_FORMATTER = new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
});

export const RESIZER_WIDTH = 10;
export const MIN_CHART_PANE_WIDTH = 300;
export const SELECTION_COLUMN_WIDTH = 52;
export const MEASUREMENT_COLUMN_WIDTH = 250;
export const OVERVIEW_COLUMN_WIDTH = 94;
export const SOURCE_COLUMN_WIDTH = 164;
export const STARRED_MEASUREMENTS_STORAGE_KEY = 'vitals.starred.measurements';
export const GROUP_BY_CATEGORY_STORAGE_KEY = 'vitals.group-by-category';
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

export function parseNumericValue(value: number | string | undefined): number | null {
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

export function normalizeCategoryLabel(value: string | undefined): string {
    const trimmed = value?.trim();
    return trimmed || UNCATEGORIZED_CATEGORY_LABEL;
}

export function formatCell(measurement: BloodworkMeasurement): MeasurementCell {
    const valueText = measurement.value === undefined ? '' : String(measurement.value).trim();
    const unitText = measurement.unit?.trim();
    const display = [valueText, unitText].filter(Boolean).join(' ').trim() || '—';
    const rangeMin = parseNumericValue(measurement.referenceRange?.min);
    const rangeMax = parseNumericValue(measurement.referenceRange?.max);
    const numericValue = parseNumericValue(measurement.value);
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
        flag: measurement.flag,
        note: measurement.note?.trim() || undefined,
    };
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
