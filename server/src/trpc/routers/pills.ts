import {
    extractPillFromImages,
    getPillsDashboard,
    pillImageExtractionInputSchema,
    pillSearchInputSchema,
    pillUpsertInputSchema,
    searchPills,
    upsertPill,
} from 'server/db/pills.ts';
import { createRouter, publicProcedure } from 'server/trpc/shared.ts';

export const pillsRouter = createRouter({
    getDashboard: publicProcedure.query(({ ctx }) => getPillsDashboard(ctx.db)),
    search: publicProcedure
        .input(pillSearchInputSchema.optional())
        .query(({ ctx, input }) => searchPills(ctx.db, input ?? { query: '', limit: 12 })),
    upsert: publicProcedure
        .input(pillUpsertInputSchema)
        .mutation(({ ctx, input }) => upsertPill(ctx.db, input)),
    extractFromImages: publicProcedure
        .input(pillImageExtractionInputSchema)
        .mutation(({ input }) => extractPillFromImages(input)),
});
