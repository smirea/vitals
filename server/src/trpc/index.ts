import { bloodworkRouter } from './routers/bloodwork.ts';
import { tableRouter } from './routers/table.ts';
import { createRouter } from './shared.ts';

export { createTrpcContext, publicProcedure } from './shared.ts';

export const appRouter = createRouter({
    bloodwork: bloodworkRouter,
    table: tableRouter,
});

export type AppRouter = typeof appRouter;
