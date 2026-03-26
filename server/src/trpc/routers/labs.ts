import {
	labReprocessDocumentInputSchema,
	labRetryDocumentInputSchema,
	labUploadDocumentsInputSchema,
	getLabDashboard,
	listLabDocuments,
	reprocessLabDocument,
	retryLabDocument,
	uploadLabDocuments,
} from 'server/db/labs.ts';
import { createRouter, publicProcedure } from 'server/trpc/shared.ts';

export const labsRouter = createRouter({
	getDashboard: publicProcedure.query(({ ctx }) => getLabDashboard(ctx.db)),
	listDocuments: publicProcedure.query(({ ctx }) => listLabDocuments(ctx.db)),
	uploadDocuments: publicProcedure
		.input(labUploadDocumentsInputSchema)
		.mutation(({ ctx, input }) => uploadLabDocuments(ctx.db, input)),
	retryDocument: publicProcedure
		.input(labRetryDocumentInputSchema)
		.mutation(({ ctx, input }) => retryLabDocument(ctx.db, input)),
	reprocessDocument: publicProcedure
		.input(labReprocessDocumentInputSchema)
		.mutation(({ ctx, input }) => reprocessLabDocument(ctx.db, input)),
});
