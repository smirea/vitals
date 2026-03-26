import { labsRouter } from './routers/labs.ts';
import { pillsRouter } from './routers/pills.ts';
import { tagsRouter } from './routers/tags.ts';
import { tableRouter } from './routers/table.ts';
import { createRouter } from './shared.ts';

export { createTrpcContext, publicProcedure } from './shared.ts';

export const appRouter = createRouter({
	labs: labsRouter,
	pills: pillsRouter,
	tags: tagsRouter,
	table: tableRouter,
});

export type AppRouter = typeof appRouter;
