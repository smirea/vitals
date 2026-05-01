import {
	diaryAddVoiceMemoTagsInputSchema,
	diaryCreateEntryInputSchema,
	diaryDeleteVoiceMemoInputSchema,
	diaryFailVoiceMemoInputSchema,
	diaryProcessVoiceMemoInputSchema,
	diaryUploadVoiceMemoInputSchema,
	addTagsToDiaryVoiceMemo,
	createDiaryEntry,
	deleteDiaryVoiceMemo,
	failDiaryVoiceMemo,
	getDiaryVoiceMemoAudio,
	listDiaryEntries,
	listPendingDiaryVoiceMemos,
	processSavedDiaryVoiceMemo,
	saveDiaryVoiceMemo,
	uploadDiaryVoiceMemo,
} from 'server/db/diary.ts';
import { createRouter, publicProcedure } from 'server/trpc/shared.ts';

export { getDiaryVoiceMemoAudio };

export const diaryRouter = createRouter({
	list: publicProcedure.query(({ ctx }) => listDiaryEntries(ctx.db)),
	listPendingVoiceMemos: publicProcedure.query(({ ctx }) => listPendingDiaryVoiceMemos(ctx.db)),
	createEntry: publicProcedure
		.input(diaryCreateEntryInputSchema)
		.mutation(({ ctx, input }) => createDiaryEntry(ctx.db, input)),
	saveVoiceMemo: publicProcedure
		.input(diaryUploadVoiceMemoInputSchema)
		.mutation(({ ctx, input }) => saveDiaryVoiceMemo(ctx.db, input)),
	processVoiceMemo: publicProcedure
		.input(diaryProcessVoiceMemoInputSchema)
		.mutation(({ ctx, input }) => processSavedDiaryVoiceMemo(ctx.db, input)),
	failVoiceMemo: publicProcedure
		.input(diaryFailVoiceMemoInputSchema)
		.mutation(({ ctx, input }) => failDiaryVoiceMemo(ctx.db, input)),
	deleteVoiceMemo: publicProcedure
		.input(diaryDeleteVoiceMemoInputSchema)
		.mutation(({ ctx, input }) => deleteDiaryVoiceMemo(ctx.db, input)),
	addVoiceMemoTags: publicProcedure
		.input(diaryAddVoiceMemoTagsInputSchema)
		.mutation(({ ctx, input }) => addTagsToDiaryVoiceMemo(ctx.db, input)),
	uploadVoiceMemo: publicProcedure
		.input(diaryUploadVoiceMemoInputSchema)
		.mutation(({ ctx, input }) => uploadDiaryVoiceMemo(ctx.db, input)),
});
