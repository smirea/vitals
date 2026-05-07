import { diaryRouter } from './routers/diary.ts';
import { labsRouter } from './routers/labs.ts';
import { pillsRouter } from './routers/pills.ts';
import { sensorsRouter } from './routers/sensors.ts';
import { tagsRouter } from './routers/tags.ts';
import { tableRouter } from './routers/table.ts';
import { createRouter } from './shared.ts';

export { createTrpcContext, publicProcedure } from './shared.ts';

export const appRouter = createRouter({
	diary: diaryRouter,
	labs: labsRouter,
	pills: pillsRouter,
	sensors: sensorsRouter,
	tags: tagsRouter,
	table: tableRouter,
});

export type AppRouter = typeof appRouter;
