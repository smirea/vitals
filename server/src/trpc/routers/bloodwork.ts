import {
	bloodworkReprocessDocumentInputSchema,
	bloodworkRetryDocumentInputSchema,
	bloodworkUploadDocumentsInputSchema,
	getBloodworkDashboard,
	listBloodworkDocuments,
	reprocessBloodworkDocument,
	retryBloodworkDocument,
	uploadBloodworkDocuments,
} from 'server/db/bloodwork.ts';
import { createRouter, publicProcedure } from 'server/trpc/shared.ts';

export const bloodworkRouter = createRouter({
	getDashboard: publicProcedure.query(({ ctx }) => getBloodworkDashboard(ctx.db)),
	listDocuments: publicProcedure.query(({ ctx }) => listBloodworkDocuments(ctx.db)),
	uploadDocuments: publicProcedure
		.input(bloodworkUploadDocumentsInputSchema)
		.mutation(({ ctx, input }) => uploadBloodworkDocuments(ctx.db, input)),
	retryDocument: publicProcedure
		.input(bloodworkRetryDocumentInputSchema)
		.mutation(({ ctx, input }) => retryBloodworkDocument(ctx.db, input)),
	reprocessDocument: publicProcedure
		.input(bloodworkReprocessDocumentInputSchema)
		.mutation(({ ctx, input }) => reprocessBloodworkDocument(ctx.db, input)),
});
