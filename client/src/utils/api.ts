import type { inferRouterOutputs } from '@trpc/server';

import type { AppRouter } from 'server/trpc/index.ts';

type RouterOutput = inferRouterOutputs<AppRouter>;

export type LabDashboardPayload = RouterOutput['labs']['getDashboard'];
export type LabDashboardDocument = LabDashboardPayload['documents'][number];
export type LabDashboardMeasurement = LabDashboardPayload['measurements'][number];
export type LabDashboardResult = LabDashboardPayload['results'][number];
export type LabImportDocument = RouterOutput['labs']['listDocuments'][number];

export type PillsDashboardPayload = RouterOutput['pills']['getDashboard'];
export type PillRecord = PillsDashboardPayload['pills'][number];
export type PillComponent = PillRecord['components'][number];
export type PillImage = PillRecord['images'][number];
export type PillPeriod = PillRecord['periods'][number];
export type PillTag = PillPeriod['tags'][number];
export type PillSearchResult = RouterOutput['pills']['search'][number];
export type PillExtractionResult = RouterOutput['pills']['extractFromImages'];
export type TagRecord = RouterOutput['tags']['list'][number];
