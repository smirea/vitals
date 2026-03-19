import fs from 'fs';
import path from 'path';

import { PROJECT_DATA_DIR, PROJECT_ROOT } from 'scripts/project-paths.ts';
import {
    bloodworkLabSchema,
    type BloodworkLab,
    type BloodworkMeasurement,
    type BloodworkMeasurementDuplicateValue,
    type BloodworkMeasurementProvenance,
} from 'scripts/bloodwork-schema.ts';
import { getDatabase, type VitalsDatabase } from 'server/db/client.ts';
import {
    bloodworkMarkers,
    bloodworkMergedSources,
    bloodworkReports,
    bloodworkResultDuplicates,
    bloodworkResultProvenance,
    bloodworkResults,
} from 'server/db/schema.ts';

type BloodworkWriteDb = Pick<VitalsDatabase, 'delete' | 'insert' | 'select'>;

type BloodworkDataFile = {
    fileName: string;
    lab: BloodworkLab;
};

export type BloodworkDatabaseSyncSummary = {
    dataDir: string;
    scannedFileCount: number;
    reportCount: number;
    markerCount: number;
    resultCount: number;
    mergedSourceCount: number;
    duplicateCount: number;
    provenanceCount: number;
};

function normalizeMeasurementKey(value: string): string {
    return value.trim().toLowerCase();
}

function listBloodworkJsonFiles(rootDir: string): string[] {
    if (!fs.existsSync(rootDir)) {
        return [];
    }

    const stack = [rootDir];
    const files: string[] = [];

    while (stack.length > 0) {
        const current = stack.pop();
        if (!current) continue;

        for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
            const fullPath = path.join(current, entry.name);
            if (entry.isDirectory()) {
                stack.push(fullPath);
                continue;
            }
            if (entry.isFile() && /^bloodwork_.*\.json$/i.test(entry.name)) {
                files.push(fullPath);
            }
        }
    }

    return files.sort((left, right) => left.localeCompare(right));
}

function toRelativeSourceFileName(dataDir: string, filePath: string): string {
    return path.relative(dataDir, filePath).split(path.sep).join('/');
}

function loadBloodworkDataFiles(dataDir: string): BloodworkDataFile[] {
    return listBloodworkJsonFiles(dataDir).map(filePath => ({
        fileName: toRelativeSourceFileName(dataDir, filePath),
        lab: bloodworkLabSchema.parse(JSON.parse(fs.readFileSync(filePath, 'utf8')) as unknown),
    }));
}

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

function assertUniqueMeasurementsInReport(dataFile: BloodworkDataFile) {
    const seen = new Set<string>();

    dataFile.lab.measurements.forEach(measurement => {
        const key = normalizeMeasurementKey(measurement.name);
        if (!key) {
            return;
        }
        if (seen.has(key)) {
            throw new Error(`Duplicate canonical measurement "${measurement.name}" in ${dataFile.fileName}`);
        }
        seen.add(key);
    });
}

function collectMarkers(dataFiles: BloodworkDataFile[]) {
    const markers = new Map<string, string>();

    dataFiles.forEach(dataFile => {
        dataFile.lab.measurements.forEach(measurement => {
            const key = normalizeMeasurementKey(measurement.name);
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

function clearBloodworkTables(db: BloodworkWriteDb) {
    db.delete(bloodworkResultProvenance).run();
    db.delete(bloodworkResultDuplicates).run();
    db.delete(bloodworkMergedSources).run();
    db.delete(bloodworkResults).run();
    db.delete(bloodworkMarkers).run();
    db.delete(bloodworkReports).run();
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

export async function syncBloodworkDatabaseFromJson(args: {
    dataDir?: string;
    db?: VitalsDatabase;
} = {}): Promise<BloodworkDatabaseSyncSummary> {
    await Bun.$`bunx drizzle-kit push --config drizzle.config.ts --force`.cwd(PROJECT_ROOT);

    const dataDir = args.dataDir ?? PROJECT_DATA_DIR;
    const db = args.db ?? getDatabase();
    const dataFiles = loadBloodworkDataFiles(dataDir);

    dataFiles.forEach(assertUniqueMeasurementsInReport);

    const markerDefinitions = collectMarkers(dataFiles);

    return db.transaction(tx => {
        clearBloodworkTables(tx);

        if (markerDefinitions.length > 0) {
            tx.insert(bloodworkMarkers).values(markerDefinitions).run();
        }

        const markerRows = tx.select({
            id: bloodworkMarkers.id,
            key: bloodworkMarkers.key,
        }).from(bloodworkMarkers).all();
        const markerIdByKey = new Map(markerRows.map(row => [row.key, row.id]));

        let reportCount = 0;
        let resultCount = 0;
        let mergedSourceCount = 0;
        let duplicateCount = 0;
        let provenanceCount = 0;

        dataFiles.forEach(dataFile => {
            const reportRow = tx.insert(bloodworkReports).values({
                sourceFileName: dataFile.fileName,
                date: dataFile.lab.date,
                collectionDate: dataFile.lab.collectionDate ?? null,
                reportedDate: dataFile.lab.reportedDate ?? null,
                receivedDate: dataFile.lab.receivedDate ?? null,
                labName: dataFile.lab.labName,
                location: dataFile.lab.location ?? null,
                importLocation: dataFile.lab.importLocation ?? null,
                importLocationIsInferred: dataFile.lab.importLocationIsInferred ?? false,
                weightKg: dataFile.lab.weightKg ?? null,
                notes: dataFile.lab.notes ?? null,
                reviewUnresolvedCount: dataFile.lab.reviewSummary?.unresolvedCount ?? 0,
                reviewReportFile: dataFile.lab.reviewSummary?.reportFile ?? null,
            }).returning({
                id: bloodworkReports.id,
            }).get();

            reportCount += 1;

            if (dataFile.lab.mergedFrom?.length) {
                tx.insert(bloodworkMergedSources).values(
                    dataFile.lab.mergedFrom.map((item, index) => ({
                        reportId: reportRow.id,
                        sortOrder: index,
                        fileName: item.fileName,
                        date: item.date,
                        labName: item.labName,
                        importLocation: item.importLocation ?? null,
                        measurementCount: item.measurementCount ?? null,
                    })),
                ).run();
                mergedSourceCount += dataFile.lab.mergedFrom.length;
            }

            dataFile.lab.measurements.forEach((measurement, sortOrder) => {
                const markerKey = normalizeMeasurementKey(measurement.name);
                const markerId = markerIdByKey.get(markerKey);
                if (!markerId) {
                    throw new Error(`Unknown marker key "${markerKey}" while importing ${dataFile.fileName}`);
                }

                const resultRow = tx.insert(bloodworkResults).values({
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

                resultCount += 1;
                duplicateCount += insertDuplicateValues({
                    db: tx,
                    resultId: resultRow.id,
                    duplicateValues: measurement.duplicateValues,
                });
                provenanceCount += insertProvenance({
                    db: tx,
                    resultId: resultRow.id,
                    provenance: measurement.provenance,
                });
            });
        });

        return {
            dataDir,
            scannedFileCount: dataFiles.length,
            reportCount,
            markerCount: markerRows.length,
            resultCount,
            mergedSourceCount,
            duplicateCount,
            provenanceCount,
        };
    });
}
