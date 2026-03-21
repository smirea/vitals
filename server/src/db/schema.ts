import { relations } from 'drizzle-orm';
import {
	blob,
	index,
	integer,
	real,
	sqliteTable,
	text,
	uniqueIndex,
} from 'drizzle-orm/sqlite-core';

const bloodworkDocumentStatusValues = ['pending', 'processing', 'completed', 'failed'] as const;
const pillTimingValues = ['morning', 'afternoon', 'evening'] as const;

export const bloodworkDocuments = sqliteTable(
	'bloodwork_documents',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		fileName: text('file_name').notNull(),
		mimeType: text('mime_type').notNull(),
		pdfData: blob('pdf_data', { mode: 'buffer' }).notNull(),
		sha256: text('sha256').notNull(),
		status: text('status', { enum: bloodworkDocumentStatusValues }).notNull().default('pending'),
		statusText: text('status_text').notNull().default('Queued for import'),
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
	},
	table => [
		uniqueIndex('bloodwork_documents_sha256_idx').on(table.sha256),
		index('bloodwork_documents_status_idx').on(table.status, table.id),
		index('bloodwork_documents_date_idx').on(table.date, table.id),
	],
);

export const bloodworkMeasurements = sqliteTable(
	'bloodwork_measurements',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		key: text('key').notNull(),
		name: text('name').notNull(),
		category: text('category'),
		aliasesJson: text('aliases_json').notNull().default('[]'),
		canonicalUnit: text('canonical_unit'),
		knownUnitsJson: text('known_units_json').notNull().default('[]'),
		canonicalRangeMin: real('canonical_range_min'),
		canonicalRangeMax: real('canonical_range_max'),
		canonicalRangeText: text('canonical_range_text'),
		rangeEvidenceJson: text('range_evidence_json').notNull().default('[]'),
		createdAt: text('created_at').notNull(),
		updatedAt: text('updated_at').notNull(),
	},
	table => [
		uniqueIndex('bloodwork_measurements_key_idx').on(table.key),
		index('bloodwork_measurements_name_idx').on(table.name, table.id),
	],
);

export const bloodworkResults = sqliteTable(
	'bloodwork_results',
	{
		id: integer('id').primaryKey({ autoIncrement: true }),
		documentId: integer('document_id')
			.notNull()
			.references(() => bloodworkDocuments.id, { onDelete: 'cascade' }),
		measurementId: integer('measurement_id')
			.notNull()
			.references(() => bloodworkMeasurements.id, { onDelete: 'cascade' }),
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
		evidenceJson: text('evidence_json').notNull().default('[]'),
	},
	table => [
		uniqueIndex('bloodwork_results_document_measurement_idx').on(
			table.documentId,
			table.measurementId,
		),
		index('bloodwork_results_document_sort_idx').on(table.documentId, table.sortOrder, table.id),
		index('bloodwork_results_measurement_idx').on(table.measurementId, table.id),
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
		dataUrl: text('data_url').notNull(),
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

export const bloodworkDocumentsRelations = relations(bloodworkDocuments, ({ many }) => ({
	results: many(bloodworkResults),
}));

export const bloodworkMeasurementsRelations = relations(bloodworkMeasurements, ({ many }) => ({
	results: many(bloodworkResults),
}));

export const bloodworkResultsRelations = relations(bloodworkResults, ({ one }) => ({
	document: one(bloodworkDocuments, {
		fields: [bloodworkResults.documentId],
		references: [bloodworkDocuments.id],
	}),
	measurement: one(bloodworkMeasurements, {
		fields: [bloodworkResults.measurementId],
		references: [bloodworkMeasurements.id],
	}),
}));

export const pillsRelations = relations(pills, ({ many }) => ({
	components: many(pillComponents),
	images: many(pillImages),
	periods: many(pillPeriods),
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

export const tagsRelations = relations(tags, ({ many }) => ({
	pillPeriodLinks: many(pillPeriodTags),
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

export const bloodworkTables = {
	bloodworkDocuments,
	bloodworkMeasurements,
	bloodworkResults,
} as const;

export const appTables = {
	...bloodworkTables,
	pills,
	pillComponents,
	pillImages,
	pillPeriods,
	tags,
	pillPeriodTags,
} as const;

export const schema = {
	...appTables,
	bloodworkDocumentsRelations,
	bloodworkMeasurementsRelations,
	bloodworkResultsRelations,
	pillsRelations,
	pillComponentsRelations,
	pillImagesRelations,
	pillPeriodsRelations,
	tagsRelations,
	pillPeriodTagsRelations,
};

export type BloodworkDocumentRow = typeof bloodworkDocuments.$inferSelect;
export type BloodworkMeasurementRow = typeof bloodworkMeasurements.$inferSelect;
export type BloodworkResultRow = typeof bloodworkResults.$inferSelect;
export type PillRow = typeof pills.$inferSelect;
export type PillComponentRow = typeof pillComponents.$inferSelect;
export type PillImageRow = typeof pillImages.$inferSelect;
export type PillPeriodRow = typeof pillPeriods.$inferSelect;
export type TagRow = typeof tags.$inferSelect;
export type PillPeriodTagRow = typeof pillPeriodTags.$inferSelect;
