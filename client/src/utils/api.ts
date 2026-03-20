import type { inferRouterOutputs } from '@trpc/server';

import type { AppRouter } from 'server/trpc/index.ts';

type RouterOutput = inferRouterOutputs<AppRouter>;

export type BloodworkDashboardPayload = RouterOutput['bloodwork']['getDashboard'];
export type BloodworkDashboardDocument = BloodworkDashboardPayload['documents'][number];
export type BloodworkDashboardMeasurement = BloodworkDashboardPayload['measurements'][number];
export type BloodworkDashboardResult = BloodworkDashboardPayload['results'][number];
export type BloodworkImportDocument = RouterOutput['bloodwork']['listDocuments'][number];

export type PillsDashboardPayload = RouterOutput['pills']['getDashboard'];
export type PillRecord = PillsDashboardPayload['pills'][number];
export type PillComponent = PillRecord['components'][number];
export type PillImage = PillRecord['images'][number];
export type PillPeriod = PillRecord['periods'][number];
export type PillTag = PillPeriod['tags'][number];
export type PillSearchResult = RouterOutput['pills']['search'][number];
export type PillExtractionResult = RouterOutput['pills']['extractFromImages'];
export type TagRecord = RouterOutput['tags']['list'][number];
