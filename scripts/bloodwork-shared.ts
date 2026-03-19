import type {
    BloodworkMeasurement,
    BloodworkMeasurementDuplicateValue,
} from 'scripts/bloodwork-schema.ts';

export function buildMeasurementNameKey(value: string): string {
    return value
        .normalize('NFKD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .replace(/[^a-z0-9]+/g, ' ')
        .trim();
}

export function cloneReferenceRange(
    referenceRange: BloodworkMeasurement['referenceRange'],
): BloodworkMeasurement['referenceRange'] {
    if (!referenceRange) {
        return undefined;
    }

    const nextRange: NonNullable<BloodworkMeasurement['referenceRange']> = {};
    if (referenceRange.min !== undefined) {
        nextRange.min = referenceRange.min;
    }
    if (referenceRange.max !== undefined) {
        nextRange.max = referenceRange.max;
    }
    if (nextRange.min === undefined && nextRange.max === undefined) {
        return undefined;
    }

    return nextRange;
}

function cloneMeasurementOriginal(
    original: BloodworkMeasurement['original'],
): BloodworkMeasurement['original'] {
    if (!original) {
        return undefined;
    }

    const nextOriginal: NonNullable<BloodworkMeasurement['original']> = {};
    if (original.value !== undefined) {
        nextOriginal.value = original.value;
    }
    if (original.unit !== undefined) {
        nextOriginal.unit = original.unit;
    }

    const originalRange = cloneReferenceRange(original.referenceRange);
    if (originalRange) {
        nextOriginal.referenceRange = originalRange;
    }
    if (
        nextOriginal.value === undefined &&
        nextOriginal.unit === undefined &&
        nextOriginal.referenceRange === undefined
    ) {
        return undefined;
    }

    return nextOriginal;
}

export function cloneDuplicateValue(
    value: BloodworkMeasurementDuplicateValue,
): BloodworkMeasurementDuplicateValue {
    const cloned: BloodworkMeasurementDuplicateValue = {
        date: value.date,
    };

    if (value.value !== undefined) {
        cloned.value = value.value;
    }
    if (value.unit !== undefined) {
        cloned.unit = value.unit;
    }

    const range = cloneReferenceRange(value.referenceRange);
    if (range) {
        cloned.referenceRange = range;
    }
    if (value.flag !== undefined) {
        cloned.flag = value.flag;
    }
    if (value.note !== undefined) {
        cloned.note = value.note;
    }
    if (value.sourceFile !== undefined) {
        cloned.sourceFile = value.sourceFile;
    }
    if (value.sourceLabName !== undefined) {
        cloned.sourceLabName = value.sourceLabName;
    }
    if (value.importLocation !== undefined) {
        cloned.importLocation = value.importLocation;
    }

    return cloned;
}

export function cloneMeasurement(measurement: BloodworkMeasurement): BloodworkMeasurement {
    const cloned: BloodworkMeasurement = {
        name: measurement.name,
    };

    if (measurement.originalName !== undefined) {
        cloned.originalName = measurement.originalName;
    }
    if (measurement.category !== undefined) {
        cloned.category = measurement.category;
    }
    if (measurement.value !== undefined) {
        cloned.value = measurement.value;
    }
    if (measurement.unit !== undefined) {
        cloned.unit = measurement.unit;
    }

    const range = cloneReferenceRange(measurement.referenceRange);
    if (range) {
        cloned.referenceRange = range;
    }

    const original = cloneMeasurementOriginal(measurement.original);
    if (original) {
        cloned.original = original;
    }
    if (measurement.flag !== undefined) {
        cloned.flag = measurement.flag;
    }
    if (measurement.note !== undefined) {
        cloned.note = measurement.note;
    }
    if (measurement.notes !== undefined) {
        cloned.notes = measurement.notes;
    }
    if (measurement.reviewStatus !== undefined) {
        cloned.reviewStatus = measurement.reviewStatus;
    }
    if (measurement.confidence !== undefined) {
        cloned.confidence = measurement.confidence;
    }
    if (measurement.conflict !== undefined) {
        cloned.conflict = {
            reason: measurement.conflict.reason,
            candidateCount: measurement.conflict.candidateCount,
        };
    }
    if (measurement.duplicateValues?.length) {
        cloned.duplicateValues = measurement.duplicateValues.map(cloneDuplicateValue);
    }
    if (measurement.provenance?.length) {
        cloned.provenance = measurement.provenance.map(entry => ({ ...entry }));
    }

    return cloned;
}

export function buildDuplicateValueKey(value: BloodworkMeasurementDuplicateValue): string {
    const rawValue = value.value;
    const valuePart =
        rawValue === undefined || rawValue === null
            ? ''
            : typeof rawValue === 'number'
                ? rawValue.toString()
                : rawValue.trim().toLowerCase();
    const rangePart = value.referenceRange
        ? [
            value.referenceRange.min?.toString() ?? '',
            value.referenceRange.max?.toString() ?? '',
        ].join('|')
        : '';

    return [
        value.date,
        valuePart,
        value.unit?.trim().toLowerCase() ?? '',
        rangePart,
        value.flag ?? '',
        value.note?.trim().toLowerCase() ?? '',
        value.sourceFile?.trim().toLowerCase() ?? '',
        value.sourceLabName?.trim().toLowerCase() ?? '',
        value.importLocation?.trim().toLowerCase() ?? '',
    ].join('|');
}

export function dedupeDuplicateValues(values: BloodworkMeasurementDuplicateValue[]): BloodworkMeasurementDuplicateValue[] {
    const deduped = new Map<string, BloodworkMeasurementDuplicateValue>();

    for (const value of values) {
        const key = buildDuplicateValueKey(value);
        if (!deduped.has(key)) {
            deduped.set(key, cloneDuplicateValue(value));
        }
    }

    return Array.from(deduped.values()).sort((left, right) => {
        const dateCompare = left.date.localeCompare(right.date);
        if (dateCompare !== 0) {
            return dateCompare;
        }

        const sourceCompare = (left.sourceFile ?? '').localeCompare(right.sourceFile ?? '');
        if (sourceCompare !== 0) {
            return sourceCompare;
        }

        return buildDuplicateValueKey(left).localeCompare(buildDuplicateValueKey(right));
    });
}
