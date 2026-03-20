import {
	bloodworkUploadDocumentsInputSchema,
	getBloodworkDashboard,
	listBloodworkDocuments,
	uploadBloodworkDocuments,
} from 'server/db/bloodwork.ts';
import { createRouter, publicProcedure } from 'server/trpc/shared.ts';

export const bloodworkRouter = createRouter({
	getDashboard: publicProcedure.query(({ ctx }) => getBloodworkDashboard(ctx.db)),
	listDocuments: publicProcedure.query(({ ctx }) => listBloodworkDocuments(ctx.db)),
	uploadDocuments: publicProcedure
		.input(bloodworkUploadDocumentsInputSchema)
		.mutation(({ ctx, input }) => uploadBloodworkDocuments(ctx.db, input)),
});
