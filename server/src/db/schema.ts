import { relations, sql } from 'drizzle-orm';
import { index, integer, real, sqliteTable, text, uniqueIndex } from 'drizzle-orm/sqlite-core';

const labDocumentStatusValues = ['pending', 'processing', 'completed', 'failed'] as const;
const pillTimingValues = ['morning', 'afternoon', 'evening'] as const;
const pillWeekdayValues = [
	'monday',
	'tuesday',
	'wednesday',
	'thursday',
	'friday',
	'saturday',
	'sunday',
] as const;
const voiceMemoStatusValues = [
	'uploaded',
	'transcribing',
	'summarizing',
	'completed',
	'failed',
] as const;
const diaryMemoKindValues = ['audio', 'video'] as const;

export const labDocuments = sqliteTable(
	'lab_documents',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		fileName: text('file_name').notNull(),
		mimeType: text('mime_type').notNull(),
		s3Path: text('s3_path').notNull(),
		sizeBytes: integer('size_bytes').notNull(),
		sha256: text('sha256').notNull(),
		status: text('status', { enum: labDocumentStatusValues }).notNull().default('pending'),
		statusText: text('status_text').notNull().default('Queued for import'),
		statusUpdatedAt: text('status_updated_at'),
		group: text('group'),
		queuedAt: text('queued_at').notNull(),
		startedAt: text('started_at'),
		completedAt: text('completed_at'),
		failedAt: text('failed_at'),
		lastError: text('last_error'),
		date: text('date'),
		collectionDate: text('collection_date'),
		reportedDate: text('reported_date'),
		receivedDate: text('received_date'),
		labName: text('lab_name'),
		location: text('location'),
		language: text('language'),
		country: text('country'),
		notes: text('notes'),
		rawMarkdown: text('raw_markdown'),
		retryCount: integer('retry_count').notNull().default(0),
	},
	table => [
		uniqueIndex('lab_documents_sha256_idx').on(table.sha256),
		index('lab_documents_status_idx').on(table.status, table.id),
		index('lab_documents_date_idx').on(table.date, table.id),
	],
);

export const labMeasurements = sqliteTable(
	'lab_measurements',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		key: text('key').notNull(),
		name: text('name').notNull(),
		category: text('category'),
		aliasesJson: text('aliases_json', { mode: 'json' })
			.$type<string[]>()
			.notNull()
			.default(sql`'[]'`),
		unit: text('unit'),
		range: text('range_text'),
		rangeMin: real('range_min'),
		rangeMax: real('range_max'),
		createdAt: text('created_at').notNull(),
		updatedAt: text('updated_at').notNull(),
	},
	table => [
		uniqueIndex('lab_measurements_key_idx').on(table.key),
		index('lab_measurements_name_idx').on(table.name, table.id),
	],
);

export const labResults = sqliteTable(
	'lab_results',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		documentId: integer('document_id')
			.notNull()
			.references(() => labDocuments.id, { onDelete: 'cascade' }),
		measurementId: integer('measurement_id')
			.notNull()
			.references(() => labMeasurements.id, { onDelete: 'cascade' }),
		sortOrder: integer('sort_order').notNull(),
		originalName: text('original_name'),
		originalValueText: text('original_value_text'),
		originalValueNumeric: real('original_value_numeric'),
		originalUnit: text('original_unit'),
		originalRangeText: text('original_range_text'),
		originalRangeMin: real('original_range_min'),
		originalRangeMax: real('original_range_max'),
		valueText: text('value_text'),
		valueNumeric: real('value_numeric'),
		unit: text('unit'),
		note: text('note'),
		confidence: real('confidence'),
		sourcePage: integer('source_page'),
		evidenceJson: text('evidence_json', { mode: 'json' })
			.$type<string[]>()
			.notNull()
			.default(sql`'[]'`),
	},
	table => [
		uniqueIndex('lab_results_document_measurement_idx').on(table.documentId, table.measurementId),
		index('lab_results_document_sort_idx').on(table.documentId, table.sortOrder, table.id),
		index('lab_results_measurement_idx').on(table.measurementId, table.id),
	],
);

export const pills = sqliteTable(
	'pills',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		name: text('name').notNull(),
		value: text('value'),
		unit: text('unit'),
		url: text('url'),
		note: text('note'),
	},
	table => [
		uniqueIndex('pills_name_idx').on(table.name),
		index('pills_name_sort_idx').on(table.name, table.id),
	],
);

export const pillComponents = sqliteTable(
	'pill_components',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		pillId: integer('pill_id')
			.notNull()
			.references(() => pills.id, { onDelete: 'cascade' }),
		sortOrder: integer('sort_order').notNull(),
		name: text('name').notNull(),
		value: text('value'),
		unit: text('unit'),
	},
	table => [index('pill_components_pill_sort_idx').on(table.pillId, table.sortOrder)],
);

export const pillImages = sqliteTable(
	'pill_images',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		pillId: integer('pill_id')
			.notNull()
			.references(() => pills.id, { onDelete: 'cascade' }),
		sortOrder: integer('sort_order').notNull(),
		fileName: text('file_name').notNull(),
		mimeType: text('mime_type').notNull(),
		s3Path: text('s3_path').notNull(),
		sizeBytes: integer('size_bytes').notNull(),
	},
	table => [index('pill_images_pill_sort_idx').on(table.pillId, table.sortOrder)],
);

export const pillPeriods = sqliteTable(
	'pill_periods',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		pillId: integer('pill_id')
			.notNull()
			.references(() => pills.id, { onDelete: 'cascade' }),
		startDate: text('start_date').notNull(),
		endDate: text('end_date'),
		count: real('count').notNull().default(1),
		timing: text('timing', { enum: pillTimingValues }),
		daysOfWeekJson: text('days_of_week_json', { mode: 'json' })
			.$type<(typeof pillWeekdayValues)[number][]>()
			.notNull()
			.default(sql`'[]'`),
	},
	table => [index('pill_periods_pill_start_idx').on(table.pillId, table.startDate, table.id)],
);

export const tags = sqliteTable(
	'tags',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		name: text('name').notNull(),
		color: text('color').notNull(),
		note: text('note'),
		createdDate: text('created_date').notNull(),
	},
	table => [
		uniqueIndex('tags_name_idx').on(table.name),
		index('tags_created_date_idx').on(table.createdDate),
	],
);

export const locations = sqliteTable(
	'locations',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		name: text('name'),
		city: text('city'),
		country: text('country'),
		countryCode: text('country_code'),
		geocodedAt: text('geocoded_at'),
		capturedAt: text('captured_at').notNull(),
		latitude: real('latitude').notNull(),
		longitude: real('longitude').notNull(),
		accuracy: real('accuracy'),
		altitude: real('altitude'),
		altitudeAccuracy: real('altitude_accuracy'),
		heading: real('heading'),
		speed: real('speed'),
	},
	table => [index('locations_captured_at_idx').on(table.capturedAt, table.id)],
);

export const diaryEntries = sqliteTable(
	'diary_entries',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		createdAt: text('created_at').notNull(),
		notes: text('notes').notNull().default(''),
		summary: text('summary'),
		locationId: integer('location_id')
			.notNull()
			.references(() => locations.id, { onDelete: 'cascade' }),
	},
	table => [
		index('diary_entries_created_at_idx').on(table.createdAt, table.id),
		index('diary_entries_location_idx').on(table.locationId, table.id),
	],
);

export const diaryVoiceMemos = sqliteTable(
	'diary_voice_memos',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		entryId: integer('entry_id')
			.notNull()
			.references(() => diaryEntries.id, { onDelete: 'cascade' }),
		createdAt: text('created_at').notNull(),
		mediaKind: text('media_kind', { enum: diaryMemoKindValues }).notNull().default('audio'),
		fileName: text('file_name').notNull(),
		mimeType: text('mime_type').notNull(),
		audioS3Path: text('audio_s3_path').notNull(),
		audioSizeBytes: integer('audio_size_bytes').notNull(),
		videoFileName: text('video_file_name'),
		videoMimeType: text('video_mime_type'),
		videoS3Path: text('video_s3_path'),
		videoSizeBytes: integer('video_size_bytes').notNull().default(0),
		durationSeconds: real('duration_seconds'),
		transcriptionStatus: text('transcription_status', { enum: voiceMemoStatusValues })
			.notNull()
			.default('uploaded'),
		transcript: text('transcript'),
		transcriptLanguage: text('transcript_language'),
		transcriptionDurationSeconds: real('transcription_duration_seconds'),
		transcriptionError: text('transcription_error'),
		processedAt: text('processed_at'),
	},
	table => [
		index('diary_voice_memos_entry_idx').on(table.entryId, table.createdAt, table.id),
		index('diary_voice_memos_status_idx').on(table.transcriptionStatus, table.id),
	],
);

export const diaryEntryTags = sqliteTable(
	'diary_entry_tags',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		entryId: integer('entry_id')
			.notNull()
			.references(() => diaryEntries.id, { onDelete: 'cascade' }),
		tagId: integer('tag_id')
			.notNull()
			.references(() => tags.id, { onDelete: 'cascade' }),
	},
	table => [
		uniqueIndex('diary_entry_tags_entry_tag_idx').on(table.entryId, table.tagId),
		index('diary_entry_tags_entry_idx').on(table.entryId, table.id),
		index('diary_entry_tags_tag_idx').on(table.tagId, table.id),
	],
);

export const pillTags = sqliteTable(
	'pill_tags',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		pillId: integer('pill_id')
			.notNull()
			.references(() => pills.id, { onDelete: 'cascade' }),
		tagId: integer('tag_id')
			.notNull()
			.references(() => tags.id, { onDelete: 'cascade' }),
	},
	table => [
		uniqueIndex('pill_tags_pill_tag_idx').on(table.pillId, table.tagId),
		index('pill_tags_pill_idx').on(table.pillId, table.id),
		index('pill_tags_tag_idx').on(table.tagId, table.id),
	],
);

export const pillPeriodTags = sqliteTable(
	'pill_period_tags',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		pillPeriodId: integer('pill_period_id')
			.notNull()
			.references(() => pillPeriods.id, { onDelete: 'cascade' }),
		tagId: integer('tag_id')
			.notNull()
			.references(() => tags.id, { onDelete: 'cascade' }),
	},
	table => [
		uniqueIndex('pill_period_tags_period_tag_idx').on(table.pillPeriodId, table.tagId),
		index('pill_period_tags_period_idx').on(table.pillPeriodId, table.id),
		index('pill_period_tags_tag_idx').on(table.tagId, table.id),
	],
);

export const labDocumentsRelations = relations(labDocuments, ({ many }) => ({
	results: many(labResults),
}));

export const labMeasurementsRelations = relations(labMeasurements, ({ many }) => ({
	results: many(labResults),
}));

export const labResultsRelations = relations(labResults, ({ one }) => ({
	document: one(labDocuments, {
		fields: [labResults.documentId],
		references: [labDocuments.id],
	}),
	measurement: one(labMeasurements, {
		fields: [labResults.measurementId],
		references: [labMeasurements.id],
	}),
}));

export const pillsRelations = relations(pills, ({ many }) => ({
	components: many(pillComponents),
	images: many(pillImages),
	periods: many(pillPeriods),
	tagLinks: many(pillTags),
}));

export const pillComponentsRelations = relations(pillComponents, ({ one }) => ({
	pill: one(pills, {
		fields: [pillComponents.pillId],
		references: [pills.id],
	}),
}));

export const pillImagesRelations = relations(pillImages, ({ one }) => ({
	pill: one(pills, {
		fields: [pillImages.pillId],
		references: [pills.id],
	}),
}));

export const pillPeriodsRelations = relations(pillPeriods, ({ one, many }) => ({
	pill: one(pills, {
		fields: [pillPeriods.pillId],
		references: [pills.id],
	}),
	tagLinks: many(pillPeriodTags),
}));

export const pillTagsRelations = relations(pillTags, ({ one }) => ({
	pill: one(pills, {
		fields: [pillTags.pillId],
		references: [pills.id],
	}),
	tag: one(tags, {
		fields: [pillTags.tagId],
		references: [tags.id],
	}),
}));

export const tagsRelations = relations(tags, ({ many }) => ({
	pillLinks: many(pillTags),
	pillPeriodLinks: many(pillPeriodTags),
	diaryEntryLinks: many(diaryEntryTags),
}));

export const pillPeriodTagsRelations = relations(pillPeriodTags, ({ one }) => ({
	pillPeriod: one(pillPeriods, {
		fields: [pillPeriodTags.pillPeriodId],
		references: [pillPeriods.id],
	}),
	tag: one(tags, {
		fields: [pillPeriodTags.tagId],
		references: [tags.id],
	}),
}));

export const locationsRelations = relations(locations, ({ many }) => ({
	diaryEntries: many(diaryEntries),
}));

export const diaryEntriesRelations = relations(diaryEntries, ({ one, many }) => ({
	location: one(locations, {
		fields: [diaryEntries.locationId],
		references: [locations.id],
	}),
	voiceMemos: many(diaryVoiceMemos),
	tagLinks: many(diaryEntryTags),
}));

export const diaryVoiceMemosRelations = relations(diaryVoiceMemos, ({ one }) => ({
	entry: one(diaryEntries, {
		fields: [diaryVoiceMemos.entryId],
		references: [diaryEntries.id],
	}),
}));

export const diaryEntryTagsRelations = relations(diaryEntryTags, ({ one }) => ({
	entry: one(diaryEntries, {
		fields: [diaryEntryTags.entryId],
		references: [diaryEntries.id],
	}),
	tag: one(tags, {
		fields: [diaryEntryTags.tagId],
		references: [tags.id],
	}),
}));

export const labTables = {
	labDocuments,
	labMeasurements,
	labResults,
} as const;

export const appTables = {
	...labTables,
	pills,
	pillComponents,
	pillImages,
	pillPeriods,
	pillTags,
	tags,
	pillPeriodTags,
	locations,
	diaryEntries,
	diaryVoiceMemos,
	diaryEntryTags,
} as const;

export const schema = {
	...appTables,
	labDocumentsRelations,
	labMeasurementsRelations,
	labResultsRelations,
	pillsRelations,
	pillComponentsRelations,
	pillImagesRelations,
	pillPeriodsRelations,
	pillTagsRelations,
	tagsRelations,
	pillPeriodTagsRelations,
	locationsRelations,
	diaryEntriesRelations,
	diaryVoiceMemosRelations,
	diaryEntryTagsRelations,
};

export type LabDocumentRow = typeof labDocuments.$inferSelect;
export type LabMeasurementRow = typeof labMeasurements.$inferSelect;
export type LabResultRow = typeof labResults.$inferSelect;
export type PillRow = typeof pills.$inferSelect;
export type PillComponentRow = typeof pillComponents.$inferSelect;
export type PillImageRow = typeof pillImages.$inferSelect;
export type PillPeriodRow = typeof pillPeriods.$inferSelect;
export type PillTagRow = typeof pillTags.$inferSelect;
export type TagRow = typeof tags.$inferSelect;
export type PillPeriodTagRow = typeof pillPeriodTags.$inferSelect;
export type LocationRow = typeof locations.$inferSelect;
export type DiaryEntryRow = typeof diaryEntries.$inferSelect;
export type DiaryVoiceMemoRow = typeof diaryVoiceMemos.$inferSelect;
export type DiaryEntryTagRow = typeof diaryEntryTags.$inferSelect;
