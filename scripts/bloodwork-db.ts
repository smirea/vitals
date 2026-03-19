import path from 'path';

import { eq, inArray } from 'drizzle-orm';

import {
    bloodworkLabSchema,
    normalizeIsoDate,
    slugifyForPath,
    type BloodworkLab,
    type BloodworkMeasurement,
    type BloodworkMeasurementDuplicateValue,
    type BloodworkMeasurementProvenance,
    type BloodworkMergedSource,
} from 'scripts/bloodwork-schema.ts';
import {
    buildMeasurementNameKey,
    cloneDuplicateValue,
    cloneMeasurement,
    cloneReferenceRange,
    dedupeDuplicateValues,
} from 'scripts/bloodwork-shared.ts';
import { getDatabase, type VitalsDatabase } from 'server/db/client.ts';
import {
    bloodworkImportReviews,
    bloodworkMarkers,
    bloodworkMergedSources,
    bloodworkReports,
    bloodworkResultDuplicates,
    bloodworkResultProvenance,
    bloodworkResults,
} from 'server/db/schema.ts';

export type StoredBloodworkReport = {
    reportId: number;
    sourceKey: string;
    lab: BloodworkLab;
};

export type BloodworkImportReview = {
    id: number;
    createdAt: string;
    sourceKey: string;
    sourcePdfPath: string;
    unresolvedCount: number;
    status: 'pending' | 'applied';
    payload: unknown;
    appliedAt: string | null;
};

export type ConsolidationGroupSummary = {
    targetSourceKey: string;
    latestDate: string;
    sourceKeys: string[];
    sourceDates: string[];
};

export type ConsolidationSummary = {
    groupsProcessed: number;
    mergedGroups: number;
    reportsBefore: number;
    reportsAfter: number;
    writtenSourceKeys: string[];
    removedSourceKeys: string[];
    groups: ConsolidationGroupSummary[];
};

type BloodworkWriteDb = Pick<VitalsDatabase, 'delete' | 'insert' | 'select' | 'update'>;

function toOptionalText(value: number | string | undefined): string | null {
    if (value === undefined) {
        return null;
    }

    if (typeof value === 'number') {
        return Number.isFinite(value) ? String(value) : null;
    }

    const trimmed = value.trim();
    return trimmed ? trimmed : null;
}

function toOptionalNumber(value: number | string | undefined): number | null {
    if (typeof value === 'number') {
        return Number.isFinite(value) ? value : null;
    }

    if (typeof value !== 'string') {
        return null;
    }

    const normalized = value.replace(',', '.').replace(/[^0-9.+-]/g, '').trim();
    if (!normalized) {
        return null;
    }

    const parsed = Number.parseFloat(normalized);
    return Number.isFinite(parsed) ? parsed : null;
}

function toOptionalRangeMin(
    value: BloodworkMeasurement['referenceRange'] | BloodworkMeasurementDuplicateValue['referenceRange'],
) {
    return value?.min ?? null;
}

function toOptionalRangeMax(
    value: BloodworkMeasurement['referenceRange'] | BloodworkMeasurementDuplicateValue['referenceRange'],
) {
    return value?.max ?? null;
}

function toMeasurementValue(valueNumeric: number | null, valueText: string | null): number | string | undefined {
    if (valueNumeric !== null) {
        return valueNumeric;
    }

    const trimmed = valueText?.trim();
    return trimmed ? trimmed : undefined;
}

function toReferenceRange(min: number | null, max: number | null): { min?: number; max?: number } | undefined {
    if (min === null && max === null) {
        return undefined;
    }

    const range: { min?: number; max?: number } = {};
    if (min !== null) {
        range.min = min;
    }
    if (max !== null) {
        range.max = max;
    }
    return range;
}

function buildBloodworkSourceKey(input: Pick<BloodworkLab, 'date' | 'labName'>): string {
    return `bloodwork_${normalizeIsoDate(input.date)}_${slugifyForPath(input.labName)}`;
}

function getLegacySourceKey(sourceKey: string): string {
    return `${sourceKey}.json`;
}

function normalizeSourceReference(sourceKey: string): string {
    return sourceKey.replace(/\.json$/i, '');
}

function assertUniqueMeasurementsInReport(sourceKey: string, lab: BloodworkLab) {
    const seen = new Set<string>();

    lab.measurements.forEach(measurement => {
        const key = buildMeasurementNameKey(measurement.name);
        if (!key) {
            return;
        }
        if (seen.has(key)) {
            throw new Error(`Duplicate canonical measurement "${measurement.name}" in ${sourceKey}`);
        }
        seen.add(key);
    });
}

function collectMarkerDefinitions(labs: Array<{ sourceKey: string; lab: BloodworkLab }>) {
    const markers = new Map<string, string>();

    labs.forEach(({ lab }) => {
        lab.measurements.forEach(measurement => {
            const key = buildMeasurementNameKey(measurement.name);
            if (!key) {
                return;
            }
            markers.set(key, measurement.name.trim());
        });
    });

    return Array.from(markers.entries())
        .sort((left, right) => left[0].localeCompare(right[0]))
        .map(([key, name]) => ({ key, name }));
}

function insertDuplicateValues(args: {
    db: BloodworkWriteDb;
    resultId: number;
    duplicateValues: BloodworkMeasurementDuplicateValue[] | undefined;
}) {
    const { db, resultId, duplicateValues } = args;
    if (!duplicateValues?.length) {
        return 0;
    }

    db.insert(bloodworkResultDuplicates).values(
        duplicateValues.map((item, index) => ({
            resultId,
            sortOrder: index,
            date: item.date,
            valueText: toOptionalText(item.value),
            valueNumeric: toOptionalNumber(item.value),
            unit: item.unit ?? null,
            referenceRangeMin: toOptionalRangeMin(item.referenceRange),
            referenceRangeMax: toOptionalRangeMax(item.referenceRange),
            flag: item.flag ?? null,
            note: item.note ?? null,
            sourceFile: item.sourceFile ?? null,
            sourceLabName: item.sourceLabName ?? null,
            importLocation: item.importLocation ?? null,
        })),
    ).run();

    return duplicateValues.length;
}

function insertProvenance(args: {
    db: BloodworkWriteDb;
    resultId: number;
    provenance: BloodworkMeasurementProvenance[] | undefined;
}) {
    const { db, resultId, provenance } = args;
    if (!provenance?.length) {
        return 0;
    }

    db.insert(bloodworkResultProvenance).values(
        provenance.map((item, index) => ({
            resultId,
            sortOrder: index,
            extractor: item.extractor,
            page: item.page,
            rawName: item.rawName ?? null,
            rawValue: item.rawValue ?? null,
            rawUnit: item.rawUnit ?? null,
            rawRange: item.rawRange ?? null,
            confidence: item.confidence ?? null,
        })),
    ).run();

    return provenance.length;
}

function pruneUnusedMarkers(db: VitalsDatabase): void {
    db.$client.exec(`
        DELETE FROM bloodwork_markers
        WHERE id NOT IN (
            SELECT DISTINCT marker_id FROM bloodwork_results
        )
    `);
}

function insertBloodworkReport(args: {
    db: BloodworkWriteDb;
    sourceKey: string;
    lab: BloodworkLab;
    markerIdByKey: Map<string, number>;
}) {
    const { db, sourceKey, lab, markerIdByKey } = args;

    const reportRow = db.insert(bloodworkReports).values({
        sourceFileName: sourceKey,
        date: lab.date,
        collectionDate: lab.collectionDate ?? null,
        reportedDate: lab.reportedDate ?? null,
        receivedDate: lab.receivedDate ?? null,
        labName: lab.labName,
        location: lab.location ?? null,
        importLocation: lab.importLocation ?? null,
        importLocationIsInferred: lab.importLocationIsInferred ?? false,
        weightKg: lab.weightKg ?? null,
        notes: lab.notes ?? null,
        reviewUnresolvedCount: lab.reviewSummary?.unresolvedCount ?? 0,
        reviewReportFile: lab.reviewSummary?.reportFile ?? null,
    }).returning({
        id: bloodworkReports.id,
    }).get();

    if (lab.mergedFrom?.length) {
        db.insert(bloodworkMergedSources).values(
            lab.mergedFrom.map((item, index) => ({
                reportId: reportRow.id,
                sortOrder: index,
                fileName: item.fileName,
                date: item.date,
                labName: item.labName,
                importLocation: item.importLocation ?? null,
                measurementCount: item.measurementCount ?? null,
            })),
        ).run();
    }

    lab.measurements.forEach((measurement, sortOrder) => {
        const markerKey = buildMeasurementNameKey(measurement.name);
        const markerId = markerIdByKey.get(markerKey);
        if (!markerId) {
            throw new Error(`Unknown marker key "${markerKey}" while importing ${sourceKey}`);
        }

        const resultRow = db.insert(bloodworkResults).values({
            reportId: reportRow.id,
            markerId,
            sortOrder,
            category: measurement.category ?? null,
            originalName: measurement.originalName ?? null,
            valueText: toOptionalText(measurement.value),
            valueNumeric: toOptionalNumber(measurement.value),
            unit: measurement.unit ?? null,
            referenceRangeMin: toOptionalRangeMin(measurement.referenceRange),
            referenceRangeMax: toOptionalRangeMax(measurement.referenceRange),
            flag: measurement.flag ?? null,
            note: measurement.note ?? null,
            notes: measurement.notes ?? null,
            originalValueText: toOptionalText(measurement.original?.value),
            originalValueNumeric: toOptionalNumber(measurement.original?.value),
            originalUnit: measurement.original?.unit ?? null,
            originalRangeMin: toOptionalRangeMin(measurement.original?.referenceRange),
            originalRangeMax: toOptionalRangeMax(measurement.original?.referenceRange),
            reviewStatus: measurement.reviewStatus ?? null,
            confidence: measurement.confidence ?? null,
            conflictReason: measurement.conflict?.reason ?? null,
            conflictCandidateCount: measurement.conflict?.candidateCount ?? null,
        }).returning({
            id: bloodworkResults.id,
        }).get();

        insertDuplicateValues({
            db,
            resultId: resultRow.id,
            duplicateValues: measurement.duplicateValues,
        });

        insertProvenance({
            db,
            resultId: resultRow.id,
            provenance: measurement.provenance,
        });
    });

    return reportRow.id;
}

function buildDuplicateValueFromMeasurement(args: {
    measurement: BloodworkMeasurement;
    source: StoredBloodworkReport;
}): BloodworkMeasurementDuplicateValue {
    const { measurement, source } = args;
    const duplicateValue: BloodworkMeasurementDuplicateValue = {
        date: source.lab.date,
    };

    if (measurement.value !== undefined) {
        duplicateValue.value = measurement.value;
    }
    if (measurement.unit !== undefined) {
        duplicateValue.unit = measurement.unit;
    }

    const range = cloneReferenceRange(measurement.referenceRange);
    if (range) {
        duplicateValue.referenceRange = range;
    }

    if (measurement.flag !== undefined) {
        duplicateValue.flag = measurement.flag;
    }

    const measurementNote = measurement.note?.trim() || measurement.notes?.trim();
    if (measurementNote) {
        duplicateValue.note = measurementNote;
    }

    duplicateValue.sourceFile = source.sourceKey;
    duplicateValue.sourceLabName = source.lab.labName;
    if (source.lab.importLocation) {
        duplicateValue.importLocation = source.lab.importLocation;
    }

    return duplicateValue;
}

function buildMergedFromKey(entry: BloodworkMergedSource): string {
    return [
        entry.fileName.trim().toLowerCase(),
        entry.date,
        entry.labName.trim().toLowerCase(),
        entry.importLocation?.trim().toLowerCase() ?? '',
    ].join('|');
}

function dedupeMergedFromEntries(entries: BloodworkMergedSource[]): BloodworkMergedSource[] {
    const deduped = new Map<string, BloodworkMergedSource>();

    for (const entry of entries) {
        const key = buildMergedFromKey(entry);
        if (!deduped.has(key)) {
            deduped.set(key, {
                fileName: entry.fileName,
                date: entry.date,
                labName: entry.labName,
                importLocation: entry.importLocation,
                measurementCount: entry.measurementCount,
            });
        }
    }

    return Array.from(deduped.values()).sort((left, right) => {
        const dateCompare = left.date.localeCompare(right.date);
        if (dateCompare !== 0) {
            return dateCompare;
        }

        return left.fileName.localeCompare(right.fileName);
    });
}

function buildMergedSourceFromReport(source: StoredBloodworkReport): BloodworkMergedSource {
    return {
        fileName: normalizeSourceReference(source.sourceKey),
        date: source.lab.date,
        labName: source.lab.labName,
        importLocation: source.lab.importLocation,
        measurementCount: source.lab.measurements.length,
    };
}

function collectMergedFromEntries(group: StoredBloodworkReport[]): BloodworkMergedSource[] {
    const entries: BloodworkMergedSource[] = [];

    for (const source of group) {
        if (source.lab.mergedFrom?.length) {
            entries.push(...source.lab.mergedFrom.map(entry => ({
                fileName: normalizeSourceReference(entry.fileName),
                date: entry.date,
                labName: entry.labName,
                importLocation: entry.importLocation,
                measurementCount: entry.measurementCount,
            })));
            continue;
        }

        entries.push(buildMergedSourceFromReport(source));
    }

    return dedupeMergedFromEntries(entries);
}

function compareSourceFreshness(left: StoredBloodworkReport, right: StoredBloodworkReport): number {
    const dateCompare = left.lab.date.localeCompare(right.lab.date);
    if (dateCompare !== 0) {
        return dateCompare;
    }

    const measurementCountCompare = left.lab.measurements.length - right.lab.measurements.length;
    if (measurementCountCompare !== 0) {
        return measurementCountCompare;
    }

    return left.sourceKey.localeCompare(right.sourceKey);
}

function pickLatestDefinedText(values: Array<string | undefined>): string | undefined {
    for (let index = values.length - 1; index >= 0; index--) {
        const value = values[index]?.trim();
        if (value) {
            return value;
        }
    }

    return undefined;
}

function pickLatestDefinedNumber(values: Array<number | undefined>): number | undefined {
    for (let index = values.length - 1; index >= 0; index--) {
        const value = values[index];
        if (value !== undefined) {
            return value;
        }
    }

    return undefined;
}

function pickLatestDefinedBoolean(values: Array<boolean | undefined>): boolean | undefined {
    for (let index = values.length - 1; index >= 0; index--) {
        const value = values[index];
        if (value !== undefined) {
            return value;
        }
    }

    return undefined;
}

function mergeNotes(group: StoredBloodworkReport[]): string | undefined {
    const byNormalizedValue = new Map<string, string>();

    for (const source of group) {
        const note = source.lab.notes?.trim();
        if (!note) {
            continue;
        }

        const key = note.toLowerCase();
        if (!byNormalizedValue.has(key)) {
            byNormalizedValue.set(key, note);
        }
    }

    if (byNormalizedValue.size === 0) {
        return undefined;
    }

    return Array.from(byNormalizedValue.values()).join('\n\n');
}

function scoreConsolidationMeasurement(measurement: BloodworkMeasurement): number {
    let score = measurement.confidence ?? 0;

    if (measurement.referenceRange) {
        score += 0.08;
    }
    if (measurement.unit?.trim()) {
        score += 0.08;
    }
    if (measurement.reviewStatus === 'accepted') {
        score += 0.08;
    }
    if (measurement.reviewStatus === 'needs_review') {
        score -= 0.12;
    }
    if (measurement.conflict) {
        score -= 0.1;
    }
    if (typeof measurement.value === 'number' && Number.isFinite(measurement.value)) {
        score += 0.05;
    }

    return score;
}

function buildMeasurementKey(measurement: BloodworkMeasurement): string {
    const rawValue = measurement.value;
    const valuePart =
        rawValue === undefined || rawValue === null
            ? ''
            : typeof rawValue === 'number'
                ? rawValue.toString()
                : rawValue.trim().toLowerCase();

    const rangePart = measurement.referenceRange
        ? [
            measurement.referenceRange.min?.toString() ?? '',
            measurement.referenceRange.max?.toString() ?? '',
        ].join('|')
        : '';

    return [
        measurement.name.trim().toLowerCase(),
        measurement.unit?.trim().toLowerCase() ?? '',
        valuePart,
        rangePart,
    ].join('|');
}

function mergeUniqueMeasurements(measurements: BloodworkMeasurement[]): BloodworkMeasurement[] {
    const unique = new Map<string, BloodworkMeasurement>();

    for (const measurement of measurements) {
        const key = buildMeasurementKey(measurement);
        if (!unique.has(key)) {
            unique.set(key, measurement);
        }
    }

    return Array.from(unique.values());
}

function mergeMeasurementsForGroup(group: StoredBloodworkReport[]): BloodworkMeasurement[] {
    const selected = new Map<string, {
        measurement: BloodworkMeasurement;
        source: StoredBloodworkReport;
        duplicateValues: BloodworkMeasurementDuplicateValue[];
    }>();

    for (const source of group) {
        for (const sourceMeasurement of source.lab.measurements) {
            const measurement = cloneMeasurement(sourceMeasurement);
            const measurementKey = buildMeasurementNameKey(measurement.name);
            if (!measurementKey) {
                continue;
            }

            const incomingDuplicateValues = measurement.duplicateValues?.map(cloneDuplicateValue) ?? [];
            delete measurement.duplicateValues;

            const existing = selected.get(measurementKey);
            if (!existing) {
                selected.set(measurementKey, {
                    measurement,
                    source,
                    duplicateValues: dedupeDuplicateValues(incomingDuplicateValues),
                });
                continue;
            }

            const existingScore = scoreConsolidationMeasurement(existing.measurement);
            const incomingScore = scoreConsolidationMeasurement(measurement);
            const shouldReplace =
                incomingScore > existingScore ||
                (Math.abs(incomingScore - existingScore) <= 1e-9 && compareSourceFreshness(source, existing.source) > 0);

            const mergedDuplicateValues = dedupeDuplicateValues([
                ...existing.duplicateValues,
                buildDuplicateValueFromMeasurement(shouldReplace
                    ? { measurement: existing.measurement, source: existing.source }
                    : { measurement, source }),
                ...incomingDuplicateValues,
            ]);

            selected.set(measurementKey, {
                measurement: shouldReplace ? measurement : existing.measurement,
                source: shouldReplace ? source : existing.source,
                duplicateValues: mergedDuplicateValues,
            });
        }
    }

    const mergedMeasurements = Array.from(selected.values())
        .map(({ measurement, duplicateValues }) => (
            duplicateValues.length === 0
                ? measurement
                : {
                    ...measurement,
                    duplicateValues,
                }
        ))
        .sort((left, right) => left.name.localeCompare(right.name));

    return mergeUniqueMeasurements(mergedMeasurements);
}

function dateDifferenceInDays(leftDate: string, rightDate: string): number {
    const [leftYear, leftMonth, leftDay] = leftDate.split('-').map(part => Number.parseInt(part, 10));
    const [rightYear, rightMonth, rightDay] = rightDate.split('-').map(part => Number.parseInt(part, 10));
    const leftTimestamp = Date.UTC(leftYear!, (leftMonth ?? 1) - 1, leftDay!);
    const rightTimestamp = Date.UTC(rightYear!, (rightMonth ?? 1) - 1, rightDay!);

    return Math.abs(Math.round((leftTimestamp - rightTimestamp) / (24 * 60 * 60 * 1000)));
}

export function groupBloodworkReportsByDateWindow(reports: StoredBloodworkReport[]): StoredBloodworkReport[][] {
    if (reports.length === 0) {
        return [];
    }

    const sorted = [...reports].sort((left, right) => {
        const dateCompare = right.lab.date.localeCompare(left.lab.date);
        if (dateCompare !== 0) {
            return dateCompare;
        }

        return right.sourceKey.localeCompare(left.sourceKey);
    });

    const groups: StoredBloodworkReport[][] = [];
    let currentGroup: StoredBloodworkReport[] = [];
    let currentGroupLatestDate: string | null = null;

    for (const report of sorted) {
        if (!currentGroupLatestDate) {
            currentGroup = [report];
            currentGroupLatestDate = report.lab.date;
            continue;
        }

        if (dateDifferenceInDays(currentGroupLatestDate, report.lab.date) <= 14) {
            currentGroup.push(report);
            continue;
        }

        groups.push(currentGroup);
        currentGroup = [report];
        currentGroupLatestDate = report.lab.date;
    }

    if (currentGroup.length > 0) {
        groups.push(currentGroup);
    }

    return groups.map(group =>
        group.sort((left, right) => {
            const dateCompare = left.lab.date.localeCompare(right.lab.date);
            if (dateCompare !== 0) {
                return dateCompare;
            }

            return left.sourceKey.localeCompare(right.sourceKey);
        }),
    );
}

export function mergeBloodworkReportGroup(group: StoredBloodworkReport[]): {
    targetSourceKey: string;
    lab: BloodworkLab;
} {
    if (group.length === 0) {
        throw new Error('Cannot merge an empty bloodwork group');
    }

    const orderedGroup = [...group].sort((left, right) => {
        const dateCompare = left.lab.date.localeCompare(right.lab.date);
        if (dateCompare !== 0) {
            return dateCompare;
        }

        return left.sourceKey.localeCompare(right.sourceKey);
    });

    let primary = orderedGroup[0]!;
    for (const source of orderedGroup.slice(1)) {
        if (compareSourceFreshness(source, primary) > 0) {
            primary = source;
        }
    }

    const mergedFrom = collectMergedFromEntries(orderedGroup);
    const mergedLab = bloodworkLabSchema.parse({
        date: primary.lab.date,
        collectionDate: primary.lab.collectionDate ?? pickLatestDefinedText(orderedGroup.map(item => item.lab.collectionDate)),
        reportedDate: primary.lab.reportedDate ?? pickLatestDefinedText(orderedGroup.map(item => item.lab.reportedDate)),
        receivedDate: primary.lab.receivedDate ?? pickLatestDefinedText(orderedGroup.map(item => item.lab.receivedDate)),
        labName: primary.lab.labName,
        location: primary.lab.location ?? pickLatestDefinedText(orderedGroup.map(item => item.lab.location)),
        importLocation: primary.lab.importLocation ?? pickLatestDefinedText(orderedGroup.map(item => item.lab.importLocation)),
        importLocationIsInferred:
            primary.lab.importLocationIsInferred
                ?? pickLatestDefinedBoolean(orderedGroup.map(item => item.lab.importLocationIsInferred)),
        weightKg: primary.lab.weightKg ?? pickLatestDefinedNumber(orderedGroup.map(item => item.lab.weightKg)),
        measurements: mergeMeasurementsForGroup(orderedGroup),
        mergedFrom: mergedFrom.length > 1 ? mergedFrom : undefined,
        notes: mergeNotes(orderedGroup),
    });

    return {
        targetSourceKey: normalizeSourceReference(primary.sourceKey),
        lab: mergedLab,
    };
}

export async function listBloodworkReports(args: {
    db?: VitalsDatabase;
} = {}): Promise<StoredBloodworkReport[]> {
    const db = args.db ?? getDatabase();
    const reportRows = db.select().from(bloodworkReports).all();
    if (reportRows.length === 0) {
        return [];
    }

    const reportIds = reportRows.map(row => row.id);

    const mergedSourceRows = db
        .select()
        .from(bloodworkMergedSources)
        .where(inArray(bloodworkMergedSources.reportId, reportIds))
        .all();

    const resultRows = db.select({
        id: bloodworkResults.id,
        reportId: bloodworkResults.reportId,
        sortOrder: bloodworkResults.sortOrder,
        markerName: bloodworkMarkers.name,
        category: bloodworkResults.category,
        originalName: bloodworkResults.originalName,
        valueText: bloodworkResults.valueText,
        valueNumeric: bloodworkResults.valueNumeric,
        unit: bloodworkResults.unit,
        referenceRangeMin: bloodworkResults.referenceRangeMin,
        referenceRangeMax: bloodworkResults.referenceRangeMax,
        flag: bloodworkResults.flag,
        note: bloodworkResults.note,
        notes: bloodworkResults.notes,
        originalValueText: bloodworkResults.originalValueText,
        originalValueNumeric: bloodworkResults.originalValueNumeric,
        originalUnit: bloodworkResults.originalUnit,
        originalRangeMin: bloodworkResults.originalRangeMin,
        originalRangeMax: bloodworkResults.originalRangeMax,
        reviewStatus: bloodworkResults.reviewStatus,
        confidence: bloodworkResults.confidence,
        conflictReason: bloodworkResults.conflictReason,
        conflictCandidateCount: bloodworkResults.conflictCandidateCount,
    })
        .from(bloodworkResults)
        .innerJoin(bloodworkMarkers, eq(bloodworkResults.markerId, bloodworkMarkers.id))
        .where(inArray(bloodworkResults.reportId, reportIds))
        .all();

    const resultIds = resultRows.map(row => row.id);

    const duplicateRows = resultIds.length === 0
        ? []
        : db.select()
            .from(bloodworkResultDuplicates)
            .where(inArray(bloodworkResultDuplicates.resultId, resultIds))
            .all();

    const provenanceRows = resultIds.length === 0
        ? []
        : db.select()
            .from(bloodworkResultProvenance)
            .where(inArray(bloodworkResultProvenance.resultId, resultIds))
            .all();

    const mergedSourcesByReportId = new Map<number, typeof mergedSourceRows>();
    for (const row of mergedSourceRows) {
        const bucket = mergedSourcesByReportId.get(row.reportId) ?? [];
        bucket.push(row);
        mergedSourcesByReportId.set(row.reportId, bucket);
    }

    const duplicatesByResultId = new Map<number, typeof duplicateRows>();
    for (const row of duplicateRows) {
        const bucket = duplicatesByResultId.get(row.resultId) ?? [];
        bucket.push(row);
        duplicatesByResultId.set(row.resultId, bucket);
    }

    const provenanceByResultId = new Map<number, typeof provenanceRows>();
    for (const row of provenanceRows) {
        const bucket = provenanceByResultId.get(row.resultId) ?? [];
        bucket.push(row);
        provenanceByResultId.set(row.resultId, bucket);
    }

    const resultsByReportId = new Map<number, typeof resultRows>();
    for (const row of resultRows) {
        const bucket = resultsByReportId.get(row.reportId) ?? [];
        bucket.push(row);
        resultsByReportId.set(row.reportId, bucket);
    }

    return reportRows
        .map(reportRow => {
            const measurements = (resultsByReportId.get(reportRow.id) ?? [])
                .sort((left, right) => left.sortOrder - right.sortOrder)
                .map(resultRow => {
                    const measurement: BloodworkMeasurement = {
                        name: resultRow.markerName,
                    };

                    if (resultRow.category) {
                        measurement.category = resultRow.category;
                    }
                    if (resultRow.originalName) {
                        measurement.originalName = resultRow.originalName;
                    }

                    const value = toMeasurementValue(resultRow.valueNumeric, resultRow.valueText);
                    if (value !== undefined) {
                        measurement.value = value;
                    }

                    if (resultRow.unit) {
                        measurement.unit = resultRow.unit;
                    }

                    const referenceRange = toReferenceRange(resultRow.referenceRangeMin, resultRow.referenceRangeMax);
                    if (referenceRange) {
                        measurement.referenceRange = referenceRange;
                    }

                    const originalValue = toMeasurementValue(resultRow.originalValueNumeric, resultRow.originalValueText);
                    const originalRange = toReferenceRange(resultRow.originalRangeMin, resultRow.originalRangeMax);
                    if (originalValue !== undefined || resultRow.originalUnit || originalRange) {
                        measurement.original = {};
                        if (originalValue !== undefined) {
                            measurement.original.value = originalValue;
                        }
                        if (resultRow.originalUnit) {
                            measurement.original.unit = resultRow.originalUnit;
                        }
                        if (originalRange) {
                            measurement.original.referenceRange = originalRange;
                        }
                    }

                    if (resultRow.flag) {
                        measurement.flag = resultRow.flag;
                    }
                    if (resultRow.note) {
                        measurement.note = resultRow.note;
                    }
                    if (resultRow.notes) {
                        measurement.notes = resultRow.notes;
                    }
                    if (resultRow.reviewStatus) {
                        measurement.reviewStatus = resultRow.reviewStatus;
                    }
                    if (resultRow.confidence !== null) {
                        measurement.confidence = resultRow.confidence;
                    }
                    if (resultRow.conflictReason) {
                        measurement.conflict = {
                            reason: resultRow.conflictReason,
                            candidateCount: resultRow.conflictCandidateCount ?? 0,
                        };
                    }

                    const duplicates = (duplicatesByResultId.get(resultRow.id) ?? [])
                        .sort((left, right) => left.sortOrder - right.sortOrder)
                        .map(row => {
                            const duplicateValue: BloodworkMeasurementDuplicateValue = {
                                date: row.date,
                            };

                            const duplicateMeasurementValue = toMeasurementValue(row.valueNumeric, row.valueText);
                            if (duplicateMeasurementValue !== undefined) {
                                duplicateValue.value = duplicateMeasurementValue;
                            }
                            if (row.unit) {
                                duplicateValue.unit = row.unit;
                            }

                            const duplicateRange = toReferenceRange(row.referenceRangeMin, row.referenceRangeMax);
                            if (duplicateRange) {
                                duplicateValue.referenceRange = duplicateRange;
                            }

                            if (row.flag) {
                                duplicateValue.flag = row.flag;
                            }
                            if (row.note) {
                                duplicateValue.note = row.note;
                            }
                            if (row.sourceFile) {
                                duplicateValue.sourceFile = normalizeSourceReference(row.sourceFile);
                            }
                            if (row.sourceLabName) {
                                duplicateValue.sourceLabName = row.sourceLabName;
                            }
                            if (row.importLocation) {
                                duplicateValue.importLocation = row.importLocation;
                            }

                            return duplicateValue;
                        });

                    if (duplicates.length > 0) {
                        measurement.duplicateValues = duplicates;
                    }

                    const provenance = (provenanceByResultId.get(resultRow.id) ?? [])
                        .sort((left, right) => left.sortOrder - right.sortOrder)
                        .map(row => {
                            const entry: BloodworkMeasurementProvenance = {
                                extractor: row.extractor,
                                page: row.page,
                            };
                            if (row.rawName) {
                                entry.rawName = row.rawName;
                            }
                            if (row.rawValue) {
                                entry.rawValue = row.rawValue;
                            }
                            if (row.rawUnit) {
                                entry.rawUnit = row.rawUnit;
                            }
                            if (row.rawRange) {
                                entry.rawRange = row.rawRange;
                            }
                            if (row.confidence !== null) {
                                entry.confidence = row.confidence;
                            }

                            return entry;
                        });

                    if (provenance.length > 0) {
                        measurement.provenance = provenance;
                    }

                    return measurement;
                });

            const mergedFrom = (mergedSourcesByReportId.get(reportRow.id) ?? [])
                .sort((left, right) => left.sortOrder - right.sortOrder)
                .map(row => ({
                    fileName: normalizeSourceReference(row.fileName),
                    date: row.date,
                    labName: row.labName,
                    importLocation: row.importLocation ?? undefined,
                    measurementCount: row.measurementCount ?? undefined,
                }));

            const lab = bloodworkLabSchema.parse({
                date: reportRow.date,
                collectionDate: reportRow.collectionDate ?? undefined,
                reportedDate: reportRow.reportedDate ?? undefined,
                receivedDate: reportRow.receivedDate ?? undefined,
                labName: reportRow.labName,
                location: reportRow.location ?? undefined,
                importLocation: reportRow.importLocation ?? undefined,
                importLocationIsInferred: reportRow.importLocationIsInferred || undefined,
                weightKg: reportRow.weightKg ?? undefined,
                measurements,
                mergedFrom: mergedFrom.length > 0 ? mergedFrom : undefined,
                notes: reportRow.notes ?? undefined,
                reviewSummary:
                    reportRow.reviewUnresolvedCount > 0 || reportRow.reviewReportFile
                        ? {
                            unresolvedCount: reportRow.reviewUnresolvedCount,
                            reportFile: reportRow.reviewReportFile ?? undefined,
                        }
                        : undefined,
            });

            return {
                reportId: reportRow.id,
                sourceKey: normalizeSourceReference(reportRow.sourceFileName),
                lab,
            };
        })
        .sort((left, right) => {
            const dateCompare = left.lab.date.localeCompare(right.lab.date);
            if (dateCompare !== 0) {
                return dateCompare;
            }

            return left.sourceKey.localeCompare(right.sourceKey);
        });
}

export async function resolveBloodworkSourceKey(args: {
    lab: BloodworkLab;
    sourcePath: string;
    db?: VitalsDatabase;
}): Promise<string> {
    const db = args.db ?? getDatabase();
    const baseSourceKey = buildBloodworkSourceKey(args.lab);
    const existingReports = db.select({
        sourceKey: bloodworkReports.sourceFileName,
        importLocation: bloodworkReports.importLocation,
    }).from(bloodworkReports).all();
    const existingBySourceKey = new Map(existingReports.map(row => [row.sourceKey, row.importLocation ?? null]));

    const existingImportLocation = existingBySourceKey.get(baseSourceKey) ?? existingBySourceKey.get(getLegacySourceKey(baseSourceKey));
    if (existingImportLocation === undefined || existingImportLocation === args.sourcePath) {
        return baseSourceKey;
    }

    const sourceSlug = slugifyForPath(path.basename(args.sourcePath, path.extname(args.sourcePath)));
    const scopedBaseKey = `${baseSourceKey}_${sourceSlug}`;
    const scopedImportLocation = existingBySourceKey.get(scopedBaseKey)
        ?? existingBySourceKey.get(getLegacySourceKey(scopedBaseKey));
    if (scopedImportLocation === undefined || scopedImportLocation === args.sourcePath) {
        return scopedBaseKey;
    }

    let suffix = 2;
    while (true) {
        const candidate = `${scopedBaseKey}_${suffix}`;
        const candidateImportLocation = existingBySourceKey.get(candidate)
            ?? existingBySourceKey.get(getLegacySourceKey(candidate));
        if (candidateImportLocation === undefined || candidateImportLocation === args.sourcePath) {
            return candidate;
        }
        suffix += 1;
    }
}

export async function upsertBloodworkReport(args: {
    sourceKey: string;
    lab: BloodworkLab;
    db?: VitalsDatabase;
}): Promise<number> {
    const db = args.db ?? getDatabase();
    assertUniqueMeasurementsInReport(args.sourceKey, args.lab);
    const markerDefinitions = collectMarkerDefinitions([{ sourceKey: args.sourceKey, lab: args.lab }]);

    const reportId = db.transaction(tx => {
        const existingReports = tx.select({
            id: bloodworkReports.id,
        }).from(bloodworkReports).where(inArray(
            bloodworkReports.sourceFileName,
            [args.sourceKey, getLegacySourceKey(args.sourceKey)],
        )).all();

        if (existingReports.length > 0) {
            tx.delete(bloodworkReports).where(inArray(
                bloodworkReports.id,
                existingReports.map(report => report.id),
            )).run();
        }

        if (markerDefinitions.length > 0) {
            markerDefinitions.forEach(marker => {
                tx.insert(bloodworkMarkers).values(marker).onConflictDoUpdate({
                    target: bloodworkMarkers.key,
                    set: {
                        name: marker.name,
                    },
                }).run();
            });
        }

        const markerRows = markerDefinitions.length === 0
            ? []
            : tx.select({
                id: bloodworkMarkers.id,
                key: bloodworkMarkers.key,
            }).from(bloodworkMarkers).where(inArray(
                bloodworkMarkers.key,
                markerDefinitions.map(marker => marker.key),
            )).all();

        return insertBloodworkReport({
            db: tx,
            sourceKey: args.sourceKey,
            lab: args.lab,
            markerIdByKey: new Map(markerRows.map(row => [row.key, row.id])),
        });
    });

    pruneUnusedMarkers(db);
    return reportId;
}

export async function deleteBloodworkReportsBySourceKeys(args: {
    sourceKeys: string[];
    db?: VitalsDatabase;
}): Promise<number> {
    const uniqueSourceKeys = Array.from(new Set(args.sourceKeys.map(value => value.trim()).filter(Boolean)));
    if (uniqueSourceKeys.length === 0) {
        return 0;
    }

    const db = args.db ?? getDatabase();
    const rows = db.select({
        id: bloodworkReports.id,
    }).from(bloodworkReports).where(inArray(bloodworkReports.sourceFileName, uniqueSourceKeys)).all();

    if (rows.length === 0) {
        return 0;
    }

    db.delete(bloodworkReports).where(inArray(
        bloodworkReports.id,
        rows.map(row => row.id),
    )).run();

    pruneUnusedMarkers(db);
    return rows.length;
}

export async function createBloodworkImportReview(args: {
    sourceKey: string;
    sourcePdfPath: string;
    unresolvedCount: number;
    payload: unknown;
    db?: VitalsDatabase;
}): Promise<BloodworkImportReview> {
    const db = args.db ?? getDatabase();
    const row = db.insert(bloodworkImportReviews).values({
        createdAt: new Date().toISOString(),
        sourceKey: args.sourceKey,
        sourcePdfPath: args.sourcePdfPath,
        unresolvedCount: args.unresolvedCount,
        status: 'pending',
        payload: JSON.stringify(args.payload),
        appliedAt: null,
    }).returning().get();

    return {
        id: row.id,
        createdAt: row.createdAt,
        sourceKey: row.sourceKey,
        sourcePdfPath: row.sourcePdfPath,
        unresolvedCount: row.unresolvedCount,
        status: row.status,
        payload: JSON.parse(row.payload) as unknown,
        appliedAt: row.appliedAt,
    };
}

export async function getBloodworkImportReview(args: {
    id: number;
    db?: VitalsDatabase;
}): Promise<BloodworkImportReview | null> {
    const db = args.db ?? getDatabase();
    const row = db.select().from(bloodworkImportReviews).where(eq(bloodworkImportReviews.id, args.id)).get();
    if (!row) {
        return null;
    }

    return {
        id: row.id,
        createdAt: row.createdAt,
        sourceKey: row.sourceKey,
        sourcePdfPath: row.sourcePdfPath,
        unresolvedCount: row.unresolvedCount,
        status: row.status,
        payload: JSON.parse(row.payload) as unknown,
        appliedAt: row.appliedAt,
    };
}

export async function markBloodworkImportReviewApplied(args: {
    id: number;
    db?: VitalsDatabase;
}): Promise<void> {
    const db = args.db ?? getDatabase();
    db.update(bloodworkImportReviews).set({
        status: 'applied',
        appliedAt: new Date().toISOString(),
    }).where(eq(bloodworkImportReviews.id, args.id)).run();
}

export async function consolidateBloodworkReports(args: {
    selectedSourceKeys?: string[];
    db?: VitalsDatabase;
} = {}): Promise<ConsolidationSummary> {
    const db = args.db ?? getDatabase();
    const sourceReports = await listBloodworkReports({ db });
    let groups = groupBloodworkReportsByDateWindow(sourceReports);

    if (args.selectedSourceKeys?.length) {
        const selectedSourceKeys = Array.from(new Set(
            args.selectedSourceKeys.map(value => value.trim()).filter(Boolean),
        ));
        const bySourceKey = new Map(sourceReports.map(report => [report.sourceKey, report]));
        const missing = selectedSourceKeys.filter(sourceKey => !bySourceKey.has(sourceKey));
        if (missing.length > 0) {
            throw new Error(`Selected bloodwork reports not found: ${missing.join(', ')}`);
        }

        const selectedReports = selectedSourceKeys.map(sourceKey => bySourceKey.get(sourceKey)!);
        groups = groupBloodworkReportsByDateWindow(selectedReports);
    }

    const summary: ConsolidationSummary = {
        groupsProcessed: groups.length,
        mergedGroups: 0,
        reportsBefore: sourceReports.length,
        reportsAfter: sourceReports.length,
        writtenSourceKeys: [],
        removedSourceKeys: [],
        groups: [],
    };

    for (const group of groups) {
        const { targetSourceKey, lab } = mergeBloodworkReportGroup(group);

        await upsertBloodworkReport({
            sourceKey: targetSourceKey,
            lab,
            db,
        });

        summary.writtenSourceKeys.push(targetSourceKey);

        if (group.length > 1) {
            summary.mergedGroups += 1;
        }

        summary.groups.push({
            targetSourceKey,
            latestDate: lab.date,
            sourceKeys: group.map(item => item.sourceKey),
            sourceDates: group.map(item => item.lab.date),
        });

        const sourceKeysToRemove = group
            .map(item => item.sourceKey)
            .filter(sourceKey => sourceKey !== targetSourceKey);

        if (sourceKeysToRemove.length > 0) {
            await deleteBloodworkReportsBySourceKeys({
                sourceKeys: sourceKeysToRemove,
                db,
            });
            summary.removedSourceKeys.push(...sourceKeysToRemove);
        }
    }

    summary.reportsAfter = (await listBloodworkReports({ db })).length;
    return summary;
}
