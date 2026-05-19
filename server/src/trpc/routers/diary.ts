import {
	diaryAddEntryTagsInputSchema,
	diaryAddVoiceMemoTagsInputSchema,
	diaryAppendVoiceMemoDraftInputSchema,
	diaryCreateEntryInputSchema,
	diaryDeleteVoiceMemoInputSchema,
	diaryDeleteVoiceMemoRecoveryInputSchema,
	diaryFailVoiceMemoInputSchema,
	diaryFinishVoiceMemoDraftInputSchema,
	diaryProcessVoiceMemoInputSchema,
	diaryProcessVoiceMemoRecoveryInputSchema,
	diaryResetVoiceMemoDraftInputSchema,
	diarySetEntryTagsInputSchema,
	diarySetVoiceMemoTagsInputSchema,
	diarySetVoiceMemoDraftVideoInputSchema,
	diaryStartVoiceMemoDraftInputSchema,
	diaryUploadVoiceMemoInputSchema,
	addTagsToDiaryEntry,
	addTagsToDiaryVoiceMemo,
	appendDiaryVoiceMemoDraft,
	createDiaryEntry,
	deleteDiaryVoiceMemo,
	deleteDiaryVoiceMemoRecovery,
	failDiaryVoiceMemo,
	finishDiaryVoiceMemoDraft,
	getDiaryVoiceMemoAudio,
	getDiaryVoiceMemoVideo,
	listDiaryEntries,
	listPendingDiaryVoiceMemoRecoveries,
	listPendingDiaryVoiceMemos,
	processDiaryVoiceMemoRecovery,
	processSavedDiaryVoiceMemo,
	resetDiaryVoiceMemoDraft,
	saveDiaryVoiceMemo,
	setDiaryEntryTags,
	setDiaryVoiceMemoDraftVideo,
	setDiaryVoiceMemoTags,
	startDiaryVoiceMemoDraft,
	uploadDiaryVoiceMemo,
} from 'server/db/diary.ts';
import { createRouter, publicProcedure } from 'server/trpc/shared.ts';

export { getDiaryVoiceMemoAudio, getDiaryVoiceMemoVideo };

export const diaryRouter = createRouter({
	list: publicProcedure.query(({ ctx }) => listDiaryEntries(ctx.db)),
	listPendingVoiceMemos: publicProcedure.query(({ ctx }) => listPendingDiaryVoiceMemos(ctx.db)),
	listPendingVoiceMemoRecoveries: publicProcedure.query(({ ctx }) =>
		listPendingDiaryVoiceMemoRecoveries(ctx.db),
	),
	createEntry: publicProcedure
		.input(diaryCreateEntryInputSchema)
		.mutation(({ ctx, input }) => createDiaryEntry(ctx.db, input)),
	saveVoiceMemo: publicProcedure
		.input(diaryUploadVoiceMemoInputSchema)
		.mutation(({ ctx, input }) => saveDiaryVoiceMemo(ctx.db, input)),
	startVoiceMemoDraft: publicProcedure
		.input(diaryStartVoiceMemoDraftInputSchema)
		.mutation(({ ctx, input }) => startDiaryVoiceMemoDraft(ctx.db, input)),
	appendVoiceMemoDraft: publicProcedure
		.input(diaryAppendVoiceMemoDraftInputSchema)
		.mutation(({ ctx, input }) => appendDiaryVoiceMemoDraft(ctx.db, input)),
	setVoiceMemoDraftVideo: publicProcedure
		.input(diarySetVoiceMemoDraftVideoInputSchema)
		.mutation(({ ctx, input }) => setDiaryVoiceMemoDraftVideo(ctx.db, input)),
	resetVoiceMemoDraft: publicProcedure
		.input(diaryResetVoiceMemoDraftInputSchema)
		.mutation(({ ctx, input }) => resetDiaryVoiceMemoDraft(ctx.db, input)),
	finishVoiceMemoDraft: publicProcedure
		.input(diaryFinishVoiceMemoDraftInputSchema)
		.mutation(({ ctx, input }) => finishDiaryVoiceMemoDraft(ctx.db, input)),
	processVoiceMemo: publicProcedure
		.input(diaryProcessVoiceMemoInputSchema)
		.mutation(({ ctx, input }) => processSavedDiaryVoiceMemo(ctx.db, input)),
	processVoiceMemoRecovery: publicProcedure
		.input(diaryProcessVoiceMemoRecoveryInputSchema)
		.mutation(({ ctx, input }) => processDiaryVoiceMemoRecovery(ctx.db, input)),
	deleteVoiceMemoRecovery: publicProcedure
		.input(diaryDeleteVoiceMemoRecoveryInputSchema)
		.mutation(({ ctx, input }) => deleteDiaryVoiceMemoRecovery(ctx.db, input)),
	failVoiceMemo: publicProcedure
		.input(diaryFailVoiceMemoInputSchema)
		.mutation(({ ctx, input }) => failDiaryVoiceMemo(ctx.db, input)),
	deleteVoiceMemo: publicProcedure
		.input(diaryDeleteVoiceMemoInputSchema)
		.mutation(({ ctx, input }) => deleteDiaryVoiceMemo(ctx.db, input)),
	addEntryTags: publicProcedure
		.input(diaryAddEntryTagsInputSchema)
		.mutation(({ ctx, input }) => addTagsToDiaryEntry(ctx.db, input)),
	addVoiceMemoTags: publicProcedure
		.input(diaryAddVoiceMemoTagsInputSchema)
		.mutation(({ ctx, input }) => addTagsToDiaryVoiceMemo(ctx.db, input)),
	setEntryTags: publicProcedure
		.input(diarySetEntryTagsInputSchema)
		.mutation(({ ctx, input }) => setDiaryEntryTags(ctx.db, input)),
	setVoiceMemoTags: publicProcedure
		.input(diarySetVoiceMemoTagsInputSchema)
		.mutation(({ ctx, input }) => setDiaryVoiceMemoTags(ctx.db, input)),
	uploadVoiceMemo: publicProcedure
		.input(diaryUploadVoiceMemoInputSchema)
		.mutation(({ ctx, input }) => uploadDiaryVoiceMemo(ctx.db, input)),
});
