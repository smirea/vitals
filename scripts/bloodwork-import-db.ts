import { createScript } from 'scripts/createScript.ts';
import { syncBloodworkDatabaseFromJson } from 'scripts/bloodwork-db.ts';

await createScript(async () => {
    const summary = await syncBloodworkDatabaseFromJson();
    console.info(
        'Bloodwork database imported:',
        `files=${summary.scannedFileCount}`,
        `reports=${summary.reportCount}`,
        `markers=${summary.markerCount}`,
        `results=${summary.resultCount}`,
        `duplicates=${summary.duplicateCount}`,
        `provenance=${summary.provenanceCount}`,
    );
});
