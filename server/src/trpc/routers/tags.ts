import {
	createTag,
	listTags,
	tagCreateInputSchema,
	tagUpdateInputSchema,
	updateTag,
} from 'server/db/tags.ts';
import { createRouter, publicProcedure } from 'server/trpc/shared.ts';

export const tagsRouter = createRouter({
	list: publicProcedure.query(({ ctx }) => listTags(ctx.db)),
	create: publicProcedure
		.input(tagCreateInputSchema)
		.mutation(({ ctx, input }) => createTag(ctx.db, input)),
	update: publicProcedure
		.input(tagUpdateInputSchema)
		.mutation(({ ctx, input }) => updateTag(ctx.db, input)),
});
