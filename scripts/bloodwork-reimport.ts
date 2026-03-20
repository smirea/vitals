import fs from 'fs';
import path from 'path';

import {
	getDefaultBloodworkImportFilePaths,
	processBloodworkQueueUntilIdle,
	queueBloodworkPdfFiles,
	resetBloodworkData,
} from 'server/db/bloodwork.ts';
import { getDatabase } from 'server/db/client.ts';
import { bloodworkDocuments, bloodworkMeasurements, bloodworkResults } from 'server/db/schema.ts';
import { PROJECT_ROOT, PROJECT_TO_IMPORT_DIR } from 'scripts/project-paths.ts';

type CliOptions = {
	reset: boolean;
	all: boolean;
	filePaths: string[];
};

function parseCliOptions(argv: string[]): CliOptions {
	const reset = argv.includes('--reset');
	const all = argv.includes('--all');
	const filePaths = argv.filter(argument => !argument.startsWith('--'));

	return {
		reset,
		all,
		filePaths,
	};
}

function resolveImportFilePath(input: string) {
	const candidates = [
		path.isAbsolute(input) ? input : path.resolve(process.cwd(), input),
		path.join(PROJECT_TO_IMPORT_DIR, input),
	];

	for (const candidate of candidates) {
		if (fs.existsSync(candidate)) {
			return candidate;
		}
	}

	throw new Error(`Bloodwork file not found: ${input}`);
}

async function main() {
	const options = parseCliOptions(process.argv.slice(2));

	await Bun.$`bunx drizzle-kit push --config drizzle.config.ts --force`.cwd(PROJECT_ROOT);

	const db = getDatabase();
	if (options.reset) {
		resetBloodworkData(db);
	}

	const requestedFilePaths = options.all ? getDefaultBloodworkImportFilePaths() : [];
	const explicitFilePaths = options.filePaths.map(resolveImportFilePath);
	const filePaths = Array.from(new Set([...requestedFilePaths, ...explicitFilePaths]));

	if (filePaths.length === 0) {
		throw new Error('No bloodwork PDFs were selected. Pass --all or one or more file paths.');
	}

	const queuedDocuments = queueBloodworkPdfFiles(db, filePaths);
	const deduplicatedCount = queuedDocuments.filter(document => document.deduplicated).length;
	const queuedCount = queuedDocuments.length - deduplicatedCount;

	console.log(
		`Queued ${queuedCount} document${queuedCount === 1 ? '' : 's'} and skipped ${deduplicatedCount} duplicate${deduplicatedCount === 1 ? '' : 's'}.`,
	);

	await processBloodworkQueueUntilIdle(db);

	const queuedIds = new Set(queuedDocuments.map(document => document.id));
	const processedDocuments = db
		.select({
			id: bloodworkDocuments.id,
			fileName: bloodworkDocuments.fileName,
			status: bloodworkDocuments.status,
			date: bloodworkDocuments.date,
			labName: bloodworkDocuments.labName,
			lastError: bloodworkDocuments.lastError,
		})
		.from(bloodworkDocuments)
		.all()
		.filter(document => queuedIds.has(document.id));

	for (const document of processedDocuments) {
		const metadata = [document.date, document.labName].filter(Boolean).join(' · ');
		console.log(
			`${document.status.toUpperCase()} ${document.fileName}${metadata ? ` (${metadata})` : ''}`,
		);
		if (document.lastError) {
			console.log(`  ${document.lastError}`);
		}
	}

	const failedDocuments = processedDocuments.filter(document => document.status === 'failed');
	const measurementCount = db
		.select({ id: bloodworkMeasurements.id })
		.from(bloodworkMeasurements)
		.all().length;
	const resultCount = db.select({ id: bloodworkResults.id }).from(bloodworkResults).all().length;
	const completedCount = processedDocuments.filter(
		document => document.status === 'completed',
	).length;

	console.log(
		`Completed ${completedCount}/${processedDocuments.length} selected documents. ${measurementCount} canonical measurements and ${resultCount} results are stored.`,
	);

	if (failedDocuments.length > 0) {
		throw new Error(`Bloodwork import failed for ${failedDocuments.length} document(s).`);
	}
}

await main();
