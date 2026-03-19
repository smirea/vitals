import { consolidateBloodworkReports, listBloodworkReports } from 'scripts/bloodwork-db.ts';
import { normalizeIsoDate } from 'scripts/bloodwork-schema.ts';
import { createScript } from 'scripts/createScript.ts';

const HELP_TEXT = [
    'Usage:',
    '  bun scripts/bloodwork-merge.ts --source <report-key> --source <report-key>',
    '  bun scripts/bloodwork-merge.ts --file <report-key> --file <report-key>',
    '  bun scripts/bloodwork-merge.ts --date <YYYY-MM-DD> --date <YYYY-MM-DD>',
    '',
    'Flags:',
    '  --source <key>       Select a bloodwork report by SQLite source key (can be repeated)',
    '  --file <key>         Alias for --source',
    '  --date <iso-date>    Select a bloodwork report by lab date (can be repeated)',
].join('\n');

type CliOptions = {
    sourceKeys: string[];
    dates: string[];
};

function parseCliOptions(argv: string[]): CliOptions {
    const sourceKeys: string[] = [];
    const dates: string[] = [];

    for (let index = 0; index < argv.length; index++) {
        const token = argv[index];
        if (token === '--source' || token === '--file') {
            const value = argv[index + 1];
            if (!value || value.startsWith('--')) {
                throw new Error(`Expected a value after ${token}\n\n${HELP_TEXT}`);
            }
            sourceKeys.push(value.trim());
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

        throw new Error(`Unknown argument: ${token}\n\n${HELP_TEXT}`);
    }

    if (sourceKeys.length === 0 && dates.length === 0) {
        throw new Error(`Provide at least two --source/--file or --date values\n\n${HELP_TEXT}`);
    }

    return {
        sourceKeys,
        dates,
    };
}

async function resolveSelectedSourceKeys(args: {
    sourceKeys: string[];
    dates: string[];
}): Promise<string[]> {
    const selected = new Set(
        args.sourceKeys.map(value => value.trim()).filter(value => value.length > 0),
    );

    if (args.dates.length > 0) {
        const availableReports = await listBloodworkReports();
        for (const rawDate of args.dates) {
            const date = normalizeIsoDate(rawDate);
            const matches = availableReports.filter(report => report.lab.date === date);
            if (matches.length === 0) {
                throw new Error(`No bloodwork report found for date ${date}`);
            }
            if (matches.length > 1) {
                throw new Error(
                    `Multiple bloodwork reports found for date ${date}: ${matches.map(report => report.sourceKey).join(', ')}. Use --source instead.`,
                );
            }
            selected.add(matches[0]!.sourceKey);
        }
    }

    const selectedSourceKeys = Array.from(selected).sort((left, right) => left.localeCompare(right));
    if (selectedSourceKeys.length < 2) {
        throw new Error('Expected at least two distinct bloodwork reports to merge');
    }

    return selectedSourceKeys;
}

async function runBloodworkMerge(argv: string[] = process.argv.slice(2)): Promise<void> {
    const options = parseCliOptions(argv);
    const selectedSourceKeys = await resolveSelectedSourceKeys({
        sourceKeys: options.sourceKeys,
        dates: options.dates,
    });

    console.info(`Selected ${selectedSourceKeys.length} report(s): ${selectedSourceKeys.join(', ')}`);

    const consolidation = await consolidateBloodworkReports({
        selectedSourceKeys,
    });

    console.info(`Consolidated ${consolidation.reportsBefore} report(s) into ${consolidation.reportsAfter} report(s)`);
    console.info(`Merged groups: ${consolidation.mergedGroups}/${consolidation.groupsProcessed}`);

    for (const group of consolidation.groups) {
        if (group.sourceKeys.length < 2) {
            continue;
        }
        console.info(
            [
                `Merged ${group.sourceKeys.length} reports into ${group.targetSourceKey}`,
                `latest date ${group.latestDate}`,
                `sources: ${group.sourceKeys.join(', ')}`,
            ].join(' | '),
        );
    }
}

export {
    parseCliOptions,
    resolveSelectedSourceKeys,
    runBloodworkMerge,
};

if (import.meta.main) {
    createScript(async () => {
        await runBloodworkMerge();
    });
}
