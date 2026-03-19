import path from 'path';

import { normalizeIsoDate } from './bloodwork-schema.ts';
import {
    consolidateBloodworkDataFiles,
    createS3ClientIfNeeded,
    listBloodworkDataFiles,
} from './bloodwork-import.ts';
import { syncBloodworkDatabaseFromJson } from './bloodwork-db.ts';
import { createScript } from './createScript.ts';
import { PROJECT_DATA_DIR } from './project-paths.ts';

const HELP_TEXT = [
    'Usage:',
    '  bun scripts/bloodwork-merge.ts --file <bloodwork_*.json> --file <bloodwork_*.json> [--skip-upload]',
    '  bun scripts/bloodwork-merge.ts --date <YYYY-MM-DD> --date <YYYY-MM-DD> [--skip-upload]',
    '',
    'Flags:',
    '  --file <name>         Select a bloodwork file by filename (can be repeated)',
    '  --date <iso-date>     Select a bloodwork file by lab date (can be repeated)',
    '  --skip-upload         Skip S3 upload and delete operations',
].join('\n');

type CliOptions = {
    fileNames: string[];
    dates: string[];
    skipUpload: boolean;
};

function parseCliOptions(argv: string[]): CliOptions {
    const fileNames: string[] = [];
    const dates: string[] = [];
    let skipUpload = false;

    for (let index = 0; index < argv.length; index++) {
        const token = argv[index];
        if (token === '--file') {
            const value = argv[index + 1];
            if (!value || value.startsWith('--')) {
                throw new Error(`Expected a value after --file\n\n${HELP_TEXT}`);
            }
            fileNames.push(path.basename(value));
            index += 1;
            continue;
        }
        if (token === '--date') {
            const value = argv[index + 1];
            if (!value || value.startsWith('--')) {
                throw new Error(`Expected a value after --date\n\n${HELP_TEXT}`);
            }
            dates.push(value);
            index += 1;
            continue;
        }
        if (token === '--skip-upload') {
            skipUpload = true;
            continue;
        }
        throw new Error(`Unknown argument: ${token}\n\n${HELP_TEXT}`);
    }

    if (fileNames.length === 0 && dates.length === 0) {
        throw new Error(`Provide at least two --file or --date values\n\n${HELP_TEXT}`);
    }

    return {
        fileNames,
        dates,
        skipUpload,
    };
}

function resolveSelectedFileNames({
    outputDirectory,
    fileNames,
    dates,
}: {
    outputDirectory: string;
    fileNames: string[];
    dates: string[];
}): string[] {
    const selected = new Set(
        fileNames.map(value => value.trim()).filter(value => value.length > 0),
    );

    if (dates.length > 0) {
        const availableFiles = listBloodworkDataFiles(outputDirectory);
        for (const rawDate of dates) {
            const date = normalizeIsoDate(rawDate);
            const matches = availableFiles.filter(file => file.lab.date === date);
            if (matches.length === 0) {
                throw new Error(`No bloodwork file found for date ${date}`);
            }
            if (matches.length > 1) {
                throw new Error(
                    `Multiple bloodwork files found for date ${date}: ${matches.map(file => file.fileName).join(', ')}. Use --file instead.`,
                );
            }
            selected.add(matches[0]!.fileName);
        }
    }

    const selectedFileNames = Array.from(selected).sort((left, right) => left.localeCompare(right));
    if (selectedFileNames.length < 2) {
        throw new Error('Expected at least two distinct bloodwork files to merge');
    }

    return selectedFileNames;
}

async function runBloodworkMerge(argv: string[] = process.argv.slice(2)): Promise<void> {
    const options = parseCliOptions(argv);
    const selectedFileNames = resolveSelectedFileNames({
        outputDirectory: PROJECT_DATA_DIR,
        fileNames: options.fileNames,
        dates: options.dates,
    });
    const { s3Client, s3Bucket, s3Prefix } = createS3ClientIfNeeded({
        skipUpload: options.skipUpload,
    });

    console.info(`Selected ${selectedFileNames.length} file(s): ${selectedFileNames.join(', ')}`);
    if (options.skipUpload) {
        console.info('S3 upload is disabled for this run (--skip-upload)');
    } else {
        console.info(`S3 destination: s3://${s3Bucket}/${s3Prefix}`);
    }

    const consolidation = await consolidateBloodworkDataFiles({
        outputDirectory: PROJECT_DATA_DIR,
        s3Client,
        s3Bucket,
        s3Prefix,
        selectedFileNames,
    });

    console.info(`Consolidated ${consolidation.filesBefore} file(s) into ${consolidation.filesAfter} file(s)`);
    console.info(`Merged groups: ${consolidation.mergedGroups}/${consolidation.groupsProcessed}`);
    for (const group of consolidation.groups) {
        if (group.sourceFileNames.length < 2) {
            continue;
        }
        console.info(
            [
                `Merged ${group.sourceFileNames.length} files into ${group.targetFileName}`,
                `latest date ${group.latestDate}`,
                `sources: ${group.sourceFileNames.join(', ')}`,
            ].join(' | '),
        );
    }
    if (consolidation.uploadedKeys.length > 0) {
        console.info(`Uploaded ${consolidation.uploadedKeys.length} consolidated file(s)`);
    }
    if (consolidation.deletedKeys.length > 0) {
        console.info(`Deleted ${consolidation.deletedKeys.length} stale S3 object(s)`);
    }

    const syncSummary = await syncBloodworkDatabaseFromJson();
    console.info(
        `Imported ${syncSummary.reportCount} report(s) into SQLite from ${syncSummary.scannedFileCount} JSON file(s)`,
    );
}

export {
    parseCliOptions,
    resolveSelectedFileNames,
    runBloodworkMerge,
};

if (import.meta.main) {
    createScript(async () => {
        await runBloodworkMerge();
    });
}
