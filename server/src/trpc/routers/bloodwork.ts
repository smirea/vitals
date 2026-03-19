import { getBloodworkDashboard } from 'server/db/bloodwork.ts';
import { createRouter, publicProcedure } from 'server/trpc/shared.ts';

export const bloodworkRouter = createRouter({
    getDashboard: publicProcedure.query(({ ctx }) => getBloodworkDashboard(ctx.db)),
});
