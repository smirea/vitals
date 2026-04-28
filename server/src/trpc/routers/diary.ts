import {
	diaryCreateEntryInputSchema,
	diaryUploadVoiceMemoInputSchema,
	createDiaryEntry,
	getDiaryVoiceMemoAudio,
	listDiaryEntries,
	uploadDiaryVoiceMemo,
} from 'server/db/diary.ts';
import { createRouter, publicProcedure } from 'server/trpc/shared.ts';

export { getDiaryVoiceMemoAudio };

export const diaryRouter = createRouter({
	list: publicProcedure.query(({ ctx }) => listDiaryEntries(ctx.db)),
	createEntry: publicProcedure
		.input(diaryCreateEntryInputSchema)
		.mutation(({ ctx, input }) => createDiaryEntry(ctx.db, input)),
	uploadVoiceMemo: publicProcedure
		.input(diaryUploadVoiceMemoInputSchema)
		.mutation(({ ctx, input }) => uploadDiaryVoiceMemo(ctx.db, input)),
});
