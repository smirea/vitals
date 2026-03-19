import { relations } from 'drizzle-orm';
import {
    index,
    integer,
    real,
    sqliteTable,
    text,
    uniqueIndex,
} from 'drizzle-orm/sqlite-core';

const measurementFlagValues = ['low', 'high', 'normal', 'abnormal', 'critical', 'unknown'] as const;
const reviewStatusValues = ['accepted', 'needs_review'] as const;
const provenanceExtractorValues = ['layout_text', 'textract', 'llm_normalizer'] as const;
const importReviewStatusValues = ['pending', 'applied'] as const;
const pillTimingValues = ['morning', 'afternoon', 'evening', 'random'] as const;

export const bloodworkReports = sqliteTable('bloodwork_reports', {
    id: integer('id').primaryKey({ autoIncrement: true }),
    sourceFileName: text('source_file_name').notNull(),
    date: text('date').notNull(),
    collectionDate: text('collection_date'),
    reportedDate: text('reported_date'),
    receivedDate: text('received_date'),
    labName: text('lab_name').notNull(),
    location: text('location'),
    importLocation: text('import_location'),
    importLocationIsInferred: integer('import_location_is_inferred', { mode: 'boolean' }).notNull().default(false),
    weightKg: real('weight_kg'),
    notes: text('notes'),
    reviewUnresolvedCount: integer('review_unresolved_count').notNull().default(0),
    reviewReportFile: text('review_report_file'),
}, table => [
    uniqueIndex('bloodwork_reports_source_file_name_idx').on(table.sourceFileName),
    index('bloodwork_reports_date_idx').on(table.date),
]);

export const bloodworkMarkers = sqliteTable('bloodwork_markers', {
    id: integer('id').primaryKey({ autoIncrement: true }),
    key: text('key').notNull(),
    name: text('name').notNull(),
}, table => [
    uniqueIndex('bloodwork_markers_key_idx').on(table.key),
    index('bloodwork_markers_name_idx').on(table.name),
]);

export const bloodworkResults = sqliteTable('bloodwork_results', {
    id: integer('id').primaryKey({ autoIncrement: true }),
    reportId: integer('report_id').notNull().references(() => bloodworkReports.id, { onDelete: 'cascade' }),
    markerId: integer('marker_id').notNull().references(() => bloodworkMarkers.id, { onDelete: 'cascade' }),
    sortOrder: integer('sort_order').notNull(),
    category: text('category'),
    originalName: text('original_name'),
    valueText: text('value_text'),
    valueNumeric: real('value_numeric'),
    unit: text('unit'),
    referenceRangeMin: real('reference_range_min'),
    referenceRangeMax: real('reference_range_max'),
    flag: text('flag', { enum: measurementFlagValues }),
    note: text('note'),
    notes: text('notes'),
    originalValueText: text('original_value_text'),
    originalValueNumeric: real('original_value_numeric'),
    originalUnit: text('original_unit'),
    originalRangeMin: real('original_range_min'),
    originalRangeMax: real('original_range_max'),
    reviewStatus: text('review_status', { enum: reviewStatusValues }),
    confidence: real('confidence'),
    conflictReason: text('conflict_reason'),
    conflictCandidateCount: integer('conflict_candidate_count'),
}, table => [
    uniqueIndex('bloodwork_results_report_marker_idx').on(table.reportId, table.markerId),
    index('bloodwork_results_report_sort_idx').on(table.reportId, table.sortOrder),
    index('bloodwork_results_marker_idx').on(table.markerId),
]);

export const bloodworkMergedSources = sqliteTable('bloodwork_merged_sources', {
    id: integer('id').primaryKey({ autoIncrement: true }),
    reportId: integer('report_id').notNull().references(() => bloodworkReports.id, { onDelete: 'cascade' }),
    sortOrder: integer('sort_order').notNull(),
    fileName: text('file_name').notNull(),
    date: text('date').notNull(),
    labName: text('lab_name').notNull(),
    importLocation: text('import_location'),
    measurementCount: integer('measurement_count'),
}, table => [
    index('bloodwork_merged_sources_report_sort_idx').on(table.reportId, table.sortOrder),
]);

export const bloodworkResultDuplicates = sqliteTable('bloodwork_result_duplicates', {
    id: integer('id').primaryKey({ autoIncrement: true }),
    resultId: integer('result_id').notNull().references(() => bloodworkResults.id, { onDelete: 'cascade' }),
    sortOrder: integer('sort_order').notNull(),
    date: text('date').notNull(),
    valueText: text('value_text'),
    valueNumeric: real('value_numeric'),
    unit: text('unit'),
    referenceRangeMin: real('reference_range_min'),
    referenceRangeMax: real('reference_range_max'),
    flag: text('flag', { enum: measurementFlagValues }),
    note: text('note'),
    sourceFile: text('source_file'),
    sourceLabName: text('source_lab_name'),
    importLocation: text('import_location'),
}, table => [
    index('bloodwork_result_duplicates_result_sort_idx').on(table.resultId, table.sortOrder),
]);

export const bloodworkResultProvenance = sqliteTable('bloodwork_result_provenance', {
    id: integer('id').primaryKey({ autoIncrement: true }),
    resultId: integer('result_id').notNull().references(() => bloodworkResults.id, { onDelete: 'cascade' }),
    sortOrder: integer('sort_order').notNull(),
    extractor: text('extractor', { enum: provenanceExtractorValues }).notNull(),
    page: integer('page').notNull(),
    rawName: text('raw_name'),
    rawValue: text('raw_value'),
    rawUnit: text('raw_unit'),
    rawRange: text('raw_range'),
    confidence: real('confidence'),
}, table => [
    index('bloodwork_result_provenance_result_sort_idx').on(table.resultId, table.sortOrder),
]);

export const bloodworkImportReviews = sqliteTable('bloodwork_import_reviews', {
    id: integer('id').primaryKey({ autoIncrement: true }),
    createdAt: text('created_at').notNull(),
    sourceKey: text('source_key').notNull(),
    sourcePdfPath: text('source_pdf_path').notNull(),
    unresolvedCount: integer('unresolved_count').notNull(),
    status: text('status', { enum: importReviewStatusValues }).notNull().default('pending'),
    payload: text('payload').notNull(),
    appliedAt: text('applied_at'),
}, table => [
    index('bloodwork_import_reviews_source_key_idx').on(table.sourceKey),
    index('bloodwork_import_reviews_status_idx').on(table.status),
]);

export const pills = sqliteTable('pills', {
    id: integer('id').primaryKey({ autoIncrement: true }),
    name: text('name').notNull(),
    value: text('value'),
    unit: text('unit'),
    note: text('note'),
}, table => [
    uniqueIndex('pills_name_idx').on(table.name),
    index('pills_name_sort_idx').on(table.name, table.id),
]);

export const pillComponents = sqliteTable('pill_components', {
    id: integer('id').primaryKey({ autoIncrement: true }),
    pillId: integer('pill_id').notNull().references(() => pills.id, { onDelete: 'cascade' }),
    sortOrder: integer('sort_order').notNull(),
    name: text('name').notNull(),
    value: text('value'),
    unit: text('unit'),
}, table => [
    index('pill_components_pill_sort_idx').on(table.pillId, table.sortOrder),
]);

export const pillImages = sqliteTable('pill_images', {
    id: integer('id').primaryKey({ autoIncrement: true }),
    pillId: integer('pill_id').notNull().references(() => pills.id, { onDelete: 'cascade' }),
    sortOrder: integer('sort_order').notNull(),
    fileName: text('file_name').notNull(),
    dataUrl: text('data_url').notNull(),
}, table => [
    index('pill_images_pill_sort_idx').on(table.pillId, table.sortOrder),
]);

export const pillPeriods = sqliteTable('pill_periods', {
    id: integer('id').primaryKey({ autoIncrement: true }),
    pillId: integer('pill_id').notNull().references(() => pills.id, { onDelete: 'cascade' }),
    startDate: text('start_date').notNull(),
    endDate: text('end_date'),
    valueOverride: text('value_override'),
    unitOverride: text('unit_override'),
    timing: text('timing', { enum: pillTimingValues }).notNull().default('random'),
}, table => [
    index('pill_periods_pill_start_idx').on(table.pillId, table.startDate, table.id),
]);

export const bloodworkReportsRelations = relations(bloodworkReports, ({ many }) => ({
    results: many(bloodworkResults),
    mergedSources: many(bloodworkMergedSources),
}));

export const bloodworkMarkersRelations = relations(bloodworkMarkers, ({ many }) => ({
    results: many(bloodworkResults),
}));

export const bloodworkResultsRelations = relations(bloodworkResults, ({ one, many }) => ({
    report: one(bloodworkReports, {
        fields: [bloodworkResults.reportId],
        references: [bloodworkReports.id],
    }),
    marker: one(bloodworkMarkers, {
        fields: [bloodworkResults.markerId],
        references: [bloodworkMarkers.id],
    }),
    duplicates: many(bloodworkResultDuplicates),
    provenance: many(bloodworkResultProvenance),
}));

export const bloodworkMergedSourcesRelations = relations(bloodworkMergedSources, ({ one }) => ({
    report: one(bloodworkReports, {
        fields: [bloodworkMergedSources.reportId],
        references: [bloodworkReports.id],
    }),
}));

export const bloodworkResultDuplicatesRelations = relations(bloodworkResultDuplicates, ({ one }) => ({
    result: one(bloodworkResults, {
        fields: [bloodworkResultDuplicates.resultId],
        references: [bloodworkResults.id],
    }),
}));

export const bloodworkResultProvenanceRelations = relations(bloodworkResultProvenance, ({ one }) => ({
    result: one(bloodworkResults, {
        fields: [bloodworkResultProvenance.resultId],
        references: [bloodworkResults.id],
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

export const pillPeriodsRelations = relations(pillPeriods, ({ one }) => ({
    pill: one(pills, {
        fields: [pillPeriods.pillId],
        references: [pills.id],
    }),
}));

export const bloodworkTables = {
    bloodworkReports,
    bloodworkMarkers,
    bloodworkResults,
    bloodworkMergedSources,
    bloodworkResultDuplicates,
    bloodworkResultProvenance,
} as const;

export const appTables = {
    ...bloodworkTables,
    bloodworkImportReviews,
    pills,
    pillComponents,
    pillImages,
    pillPeriods,
} as const;

export const schema = {
    ...appTables,
    bloodworkReportsRelations,
    bloodworkMarkersRelations,
    bloodworkResultsRelations,
    bloodworkMergedSourcesRelations,
    bloodworkResultDuplicatesRelations,
    bloodworkResultProvenanceRelations,
    pillsRelations,
    pillComponentsRelations,
    pillImagesRelations,
    pillPeriodsRelations,
};

export type BloodworkReportRow = typeof bloodworkReports.$inferSelect;
export type BloodworkMarkerRow = typeof bloodworkMarkers.$inferSelect;
export type BloodworkResultRow = typeof bloodworkResults.$inferSelect;
export type BloodworkMergedSourceRow = typeof bloodworkMergedSources.$inferSelect;
export type BloodworkResultDuplicateRow = typeof bloodworkResultDuplicates.$inferSelect;
export type BloodworkResultProvenanceRow = typeof bloodworkResultProvenance.$inferSelect;
export type BloodworkImportReviewRow = typeof bloodworkImportReviews.$inferSelect;
export type PillRow = typeof pills.$inferSelect;
export type PillComponentRow = typeof pillComponents.$inferSelect;
export type PillImageRow = typeof pillImages.$inferSelect;
export type PillPeriodRow = typeof pillPeriods.$inferSelect;
