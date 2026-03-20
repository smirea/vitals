import { bloodworkRouter } from './routers/bloodwork.ts';
import { pillsRouter } from './routers/pills.ts';
import { tagsRouter } from './routers/tags.ts';
import { tableRouter } from './routers/table.ts';
import { createRouter } from './shared.ts';

export { createTrpcContext, publicProcedure } from './shared.ts';

export const appRouter = createRouter({
    bloodwork: bloodworkRouter,
    pills: pillsRouter,
    tags: tagsRouter,
    table: tableRouter,
});

export type AppRouter = typeof appRouter;
