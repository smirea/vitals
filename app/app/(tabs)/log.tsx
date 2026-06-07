import type { inferRouterOutputs } from '@trpc/server';
import type { AppRouter } from 'server/trpc/index.ts';
import { withNativeAuthToken } from '@/src/api/auth';
import { API_BASE_URL, useTRPC } from '@/src/api/trpc';
import { ActivityIndicator, Modal, Steps, Tag, Toast } from '@ant-design/react-native';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { CameraView, useCameraPermissions } from 'expo-camera';
import * as FileSystem from 'expo-file-system/legacy';
import { activateKeepAwakeAsync, deactivateKeepAwake } from 'expo-keep-awake';
import * as Location from 'expo-location';
import {
	RecordingPresets,
	requestRecordingPermissionsAsync,
	setAudioModeAsync,
	useAudioRecorder as useExpoAudioRecorder,
	useAudioRecorderState,
} from 'expo-audio';
import { useVideoPlayer, VideoView } from 'expo-video';
import { useEffect, useMemo, useRef, useState } from 'react';
import {
	Animated,
	AppState,
	Easing,
	Linking,
	Modal as NativeModal,
	Pressable,
	ScrollView,
	Text,
	TextInput,
	View,
	useColorScheme,
} from 'react-native';
import { Button, FloatingActionButton } from '@/src/components/button';
import { BottomSheet } from '@/src/components/mobile-ui';
import { MarkdownText } from '@/src/components/markdown-text';
import { formatTagNames, TagChips, TagSelector } from '@/src/components/tag-selector';
import { pageStyles } from '@/src/theme/page-styles';

type RouterOutput = inferRouterOutputs<AppRouter>;

type DiaryEntry = RouterOutput['diary']['list'][number];
type DiaryVoiceMemo = DiaryEntry['voiceMemos'][number];
type DiaryPendingVoiceMemo = RouterOutput['diary']['listPendingVoiceMemos'][number];
type DiaryPendingVoiceMemoRecovery =
	RouterOutput['diary']['listPendingVoiceMemoRecoveries'][number];
type TagRecord = RouterOutput['tags']['list'][number];
type PlayableVideoMemo = {
	id: number;
	fileName: string;
};
type LocalRecordingDraftStatus =
	| 'local_saved'
	| 'uploading'
	| 'processing'
	| 'server_saved'
	| 'failed';
type LocalRecordingDraftBase = {
	id: string;
	createdAt: string;
	updatedAt: string;
	status: LocalRecordingDraftStatus;
	error: string | null;
	notes: string;
	tagNames: string[];
	location: DiaryLocationInput;
	durationSeconds: number;
	serverRecoveryId: string | null;
	serverVoiceMemoId: number | null;
};
type LocalAudioDraft = LocalRecordingDraftBase & {
	mediaKind: 'audio';
	audioUri: string;
	audioFileName: string;
	audioMimeType: string;
	audioBytes: number;
};
type LocalVideoDraft = LocalRecordingDraftBase & {
	mediaKind: 'video';
	videoUri: string;
	videoFileName: string;
	videoMimeType: string;
	videoBytes: number;
};
type LocalRecordingDraft = LocalAudioDraft | LocalVideoDraft;
type RecordingSession = {
	localId: string;
	startedAt: string;
};
type VideoProgressStep = 'upload' | 'transcribe' | 'summarize' | 'done';

type DiaryLocationInput = {
	capturedAt: string;
	latitude: number;
	longitude: number;
	accuracy: number | null;
	altitude: number | null;
	altitudeAccuracy: number | null;
	heading: number | null;
	speed: number | null;
};

function parseTagText(value: string) {
	const seen = new Set<string>();
	const tags: string[] = [];

	for (const rawTag of value.split(',')) {
		const tag = rawTag.trim();
		const key = tag.toLocaleLowerCase();
		if (!tag || seen.has(key)) continue;
		seen.add(key);
		tags.push(tag);
	}

	return tags;
}

function getEntryTranscriptText(entry: DiaryEntry) {
	const voiceTranscripts = entry.voiceMemos
		.map(memo => memo.transcript?.trim() ?? '')
		.filter(Boolean);

	if (voiceTranscripts.length > 0) {
		return voiceTranscripts.join('\n\n');
	}

	return entry.notes.trim();
}

function formatDiaryDate(value: string) {
	return new Date(value).toLocaleDateString('en-US', {
		month: 'short',
		day: 'numeric',
		year: 'numeric',
	});
}

function formatDiaryTime(value: string) {
	return new Date(value).toLocaleTimeString('en-US', {
		hour: 'numeric',
		minute: '2-digit',
	});
}

function formatDiaryTimestamp(value: string) {
	return `${formatDiaryDate(value)} at ${formatDiaryTime(value)}`;
}

function formatDuration(value: number | null) {
	if (!Number.isFinite(value)) {
		return 'unknown duration';
	}

	const minutes = Math.floor((value ?? 0) / 60);
	const seconds = Math.round((value ?? 0) % 60)
		.toString()
		.padStart(2, '0');
	return `${minutes}:${seconds}`;
}

function formatBytes(value: number) {
	if (value < 1024) {
		return `${value} B`;
	}
	if (value < 1024 * 1024) {
		return `${(value / 1024).toFixed(1)} KB`;
	}

	return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function formatLocationLabel(location: {
	name?: string | null;
	latitude: number;
	longitude: number;
}) {
	return (
		location.name?.trim() || `${location.latitude.toFixed(4)}, ${location.longitude.toFixed(4)}`
	);
}

function mapsUrl(location: { latitude: number; longitude: number }) {
	return `https://www.google.com/maps?q=${location.latitude},${location.longitude}`;
}

function voiceMemoAudioUrl(voiceMemoId: number) {
	return withNativeAuthToken(`${API_BASE_URL}/asset/diary_voice_memos/${voiceMemoId}/audio`);
}

function voiceMemoVideoUrl(voiceMemoId: number) {
	return withNativeAuthToken(`${API_BASE_URL}/asset/diary_voice_memos/${voiceMemoId}/video`);
}

function audioFileNameFromUri(uri: string) {
	const extension = audioExtensionFromUri(uri);
	return `diary-${new Date().toISOString().replace(/[:.]/g, '-')}.${extension}`;
}

function videoFileNameFromUri(uri: string) {
	const extension = videoExtensionFromUri(uri);
	return `diary-video-${new Date().toISOString().replace(/[:.]/g, '-')}.${extension}`;
}

function videoAudioFileNameFromVideoFileName(fileName: string) {
	return `${fileName.replace(/\.[^.]+$/, '') || 'diary-video-audio'}.m4a`;
}

function audioMimeTypeFromUri(uri: string) {
	const extension = audioExtensionFromUri(uri);
	if (extension === 'm4a' || extension === 'mp4') return 'audio/mp4';
	if (extension === 'wav') return 'audio/wav';
	if (extension === 'caf') return 'audio/x-caf';
	if (extension === 'webm') return 'audio/webm';
	if (extension === 'ogg') return 'audio/ogg';
	return `audio/${extension}`;
}

function videoMimeTypeFromUri(uri: string) {
	const extension = videoExtensionFromUri(uri);
	if (extension === 'mov' || extension === 'qt') return 'video/quicktime';
	if (extension === 'm4v') return 'video/x-m4v';
	if (extension === 'webm') return 'video/webm';
	return `video/${extension}`;
}

function formatRecorderDuration(milliseconds: number) {
	return formatDuration(Math.max(milliseconds, 0) / 1000);
}

function videoProgressStepIndex(step: VideoProgressStep) {
	if (step === 'transcribe') return 1;
	if (step === 'summarize') return 2;
	if (step === 'done') return 3;
	return 0;
}

function recordingProgressFromLocalDraft(draft: LocalRecordingDraft) {
	if (draft.status === 'failed') {
		return {
			step: draft.serverVoiceMemoId ? ('transcribe' as const) : ('upload' as const),
			failed: true,
		};
	}
	if (draft.status === 'processing' || draft.serverVoiceMemoId) {
		return { step: 'transcribe' as const, failed: false };
	}
	return { step: 'upload' as const, failed: false };
}

function videoProgressFromVoiceMemoStatus(status: DiaryPendingVoiceMemo['transcriptionStatus']) {
	if (status === 'summarizing') return { step: 'summarize' as const, failed: false };
	if (status === 'completed') return { step: 'done' as const, failed: false };
	if (status === 'failed') return { step: 'transcribe' as const, failed: true };
	return { step: 'transcribe' as const, failed: false };
}

function videoProgressFromRecovery(recovery: DiaryPendingVoiceMemoRecovery) {
	if (recovery.status === 'summarizing') return { step: 'summarize' as const, failed: false };
	if (recovery.status === 'completed') return { step: 'done' as const, failed: false };
	if (recovery.status === 'transcribing') return { step: 'transcribe' as const, failed: false };
	if (recovery.status === 'failed') return { step: 'upload' as const, failed: true };
	return { step: 'upload' as const, failed: false };
}

function audioExtensionFromUri(uri: string) {
	const path = uri.split('?')[0] ?? uri;
	const extension = path
		.split('.')
		.at(-1)
		?.toLowerCase()
		.replace(/[^a-z0-9]/g, '');
	if (!extension) {
		throw new Error(`Recording URI has no file extension: ${uri}`);
	}

	return extension;
}

function videoExtensionFromUri(uri: string) {
	const path = uri.split('?')[0] ?? uri;
	const extension = path
		.split('.')
		.at(-1)
		?.toLowerCase()
		.replace(/[^a-z0-9]/g, '');
	if (!extension) {
		throw new Error(`Video URI has no file extension: ${uri}`);
	}

	return extension === 'quicktime' ? 'mov' : extension;
}

async function ensureDirectory(path: string) {
	const info = await FileSystem.getInfoAsync(path);
	if (!info.exists) {
		await FileSystem.makeDirectoryAsync(path, { intermediates: true });
	}
}

function requireDocumentDirectory() {
	if (!FileSystem.documentDirectory) {
		throw new Error('Expo document directory is not available.');
	}

	return FileSystem.documentDirectory;
}

function localRecordingLogsDirectory() {
	return `${requireDocumentDirectory()}video-logs/`;
}

function localRecordingDraftManifestPath() {
	return `${localRecordingLogsDirectory()}drafts.json`;
}

function normalizeLocalRecordingDraft(value: unknown): LocalRecordingDraft {
	const draft = value as Partial<LocalRecordingDraft> & Partial<LocalVideoDraft>;
	if (!draft.mediaKind && draft.videoUri) {
		return {
			...(draft as LocalVideoDraft),
			mediaKind: 'video',
		};
	}

	return draft as LocalRecordingDraft;
}

async function readLocalRecordingDrafts() {
	const manifestPath = localRecordingDraftManifestPath();
	const info = await FileSystem.getInfoAsync(manifestPath);
	if (!info.exists) {
		return [] satisfies LocalRecordingDraft[];
	}

	const text = await FileSystem.readAsStringAsync(manifestPath);
	const parsed = JSON.parse(text) as unknown;
	if (!Array.isArray(parsed)) {
		throw new Error('Local recording draft manifest is not an array.');
	}

	return parsed.map(normalizeLocalRecordingDraft);
}

async function writeLocalRecordingDrafts(drafts: LocalRecordingDraft[]) {
	const directory = localRecordingLogsDirectory();
	await ensureDirectory(directory);
	await FileSystem.writeAsStringAsync(
		localRecordingDraftManifestPath(),
		JSON.stringify(drafts, null, 2),
	);
}

function localRecordingFileUri(draft: LocalRecordingDraft) {
	return draft.mediaKind === 'video' ? draft.videoUri : draft.audioUri;
}

function localRecordingFileName(draft: LocalRecordingDraft) {
	return draft.mediaKind === 'video' ? draft.videoFileName : draft.audioFileName;
}

function localRecordingBytes(draft: LocalRecordingDraft) {
	return draft.mediaKind === 'video' ? draft.videoBytes : draft.audioBytes;
}

async function deleteLocalRecordingFile(draft: LocalRecordingDraft) {
	await FileSystem.deleteAsync(localRecordingFileUri(draft), { idempotent: true });
}

async function cleanupExpiredLocalRecordingDrafts(drafts: LocalRecordingDraft[]) {
	const cutoff = Date.now() - LOCAL_RECORDING_RETENTION_MS;
	const retained: LocalRecordingDraft[] = [];

	for (const draft of drafts) {
		const createdAt = Date.parse(draft.createdAt);
		if (Number.isFinite(createdAt) && createdAt < cutoff) {
			await deleteLocalRecordingFile(draft);
			continue;
		}
		retained.push(draft);
	}

	return retained;
}

async function getFileSize(uri: string) {
	const info = await FileSystem.getInfoAsync(uri);
	if (!info.exists || typeof info.size !== 'number') {
		throw new Error(`File does not exist or has no size: ${uri}`);
	}

	return info.size;
}

type LogFilter = 'entries' | 'pending' | 'recoveries';
const MEDIA_UPLOAD_CHUNK_BYTES = 512 * 1024;
const VIDEO_RECORDER_MODAL_OPEN_DELAY_MS = 280;
const LOCAL_RECORDING_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
const KEEP_AWAKE_TAG = 'vitals-diary-recording';

export default function LogScreen() {
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const isDark = useColorScheme() === 'dark';
	const styles = logStyles(isDark);
	const sharedStyles = pageStyles(isDark);
	const audioRecorder = useExpoAudioRecorder(RecordingPresets.HIGH_QUALITY);
	const recorderState = useAudioRecorderState(audioRecorder);
	const cameraRef = useRef<CameraView>(null);
	const audioRecordingSessionRef = useRef<RecordingSession | null>(null);
	const videoRecordingPromiseRef = useRef<Promise<{ uri: string } | undefined> | null>(null);
	const videoRecordingSessionRef = useRef<RecordingSession | null>(null);
	const videoStopRequestedRef = useRef(false);
	const videoInterruptedAtRef = useRef<number | null>(null);
	const videoFinalizingRef = useRef(false);
	const videoResumeAfterOpenRef = useRef(false);
	const interruptedRecordingDraftIdsRef = useRef<string[]>([]);
	const audioStopInFlightRef = useRef(false);
	const audioDurationMillisRef = useRef(0);
	const audioRecorderUrlRef = useRef<string | null>(null);
	const isAudioRecordingRef = useRef(false);
	const isVideoRecordingRef = useRef(false);
	const currentLocationRef = useRef<DiaryLocationInput | null>(null);
	const locationMessageRef = useRef('Requesting location...');
	const notesRef = useRef('');
	const tagNamesRef = useRef<string[]>([]);
	const videoStartedAtRef = useRef<number | null>(null);
	const localRecordingDraftsRef = useRef<LocalRecordingDraft[]>([]);
	const openingVideoRecorderRef = useRef(false);
	const [cameraPermission, requestCameraPermission] = useCameraPermissions();
	const recordPulse = useRef(new Animated.Value(0)).current;
	const [notes, setNotes] = useState('');
	const [tagText, setTagText] = useState('');
	const [currentLocation, setCurrentLocation] = useState<DiaryLocationInput | null>(null);
	const [locationMessage, setLocationMessage] = useState('Requesting location...');
	const [notice, setNotice] = useState<string | null>(null);
	const [selectedEntry, setSelectedEntry] = useState<DiaryEntry | null>(null);
	const [selectedVideoMemo, setSelectedVideoMemo] = useState<PlayableVideoMemo | null>(null);
	const [isUploadingRecording, setIsUploadingRecording] = useState(false);
	const [videoRecorderOpen, setVideoRecorderOpen] = useState(false);
	const [videoCameraMounted, setVideoCameraMounted] = useState(false);
	const [isVideoCameraReady, setIsVideoCameraReady] = useState(false);
	const [isVideoRecording, setIsVideoRecording] = useState(false);
	const [isOpeningVideoRecorder, setIsOpeningVideoRecorder] = useState(false);
	const [isVideoStarting, setIsVideoStarting] = useState(false);
	const [isVideoSaving, setIsVideoSaving] = useState(false);
	const [videoStartedAt, setVideoStartedAt] = useState<number | null>(null);
	const [videoElapsedMs, setVideoElapsedMs] = useState(0);
	const [videoCameraKey, setVideoCameraKey] = useState(0);
	const [localRecordingDrafts, setLocalRecordingDrafts] = useState<LocalRecordingDraft[]>([]);
	const [interruptedAudioDraftId, setInterruptedAudioDraftId] = useState<string | null>(null);
	const [interruptedVideoDraftId, setInterruptedVideoDraftId] = useState<string | null>(null);
	const [composerOpen, setComposerOpen] = useState(false);
	const [activeFilter, setActiveFilter] = useState<LogFilter>('entries');

	const entriesQuery = useQuery(trpc.diary.list.queryOptions());
	const pendingVoiceMemosQuery = useQuery(trpc.diary.listPendingVoiceMemos.queryOptions());
	const pendingVoiceMemoRecoveriesQuery = useQuery(
		trpc.diary.listPendingVoiceMemoRecoveries.queryOptions(),
	);
	const tagsQuery = useQuery(trpc.tags.list.queryOptions());

	const availableTags = tagsQuery.data ?? [];
	const tagNames = useMemo(() => parseTagText(tagText), [tagText]);
	const entries = entriesQuery.data ?? [];
	const pendingVoiceMemos = pendingVoiceMemosQuery.data ?? [];
	const pendingVoiceMemoRecoveries = pendingVoiceMemoRecoveriesQuery.data ?? [];
	const visibleLocalRecordingDrafts = useMemo(
		() => localRecordingDrafts.filter(draft => draft.status !== 'server_saved'),
		[localRecordingDrafts],
	);
	const filterOptions = useMemo(
		() =>
			[
				{ key: 'entries', label: 'Entries', value: entries.length },
				{ key: 'pending', label: 'Pending', value: pendingVoiceMemos.length },
				{
					key: 'recoveries',
					label: 'Recoveries',
					value:
						pendingVoiceMemoRecoveries.length +
						visibleLocalRecordingDrafts.length +
						(isVideoSaving ? 1 : 0),
				},
			].filter(option => option.key === 'entries' || option.value > 0) as Array<{
				key: LogFilter;
				label: string;
				value: number;
			}>,
		[
			entries.length,
			isVideoSaving,
			pendingVoiceMemoRecoveries.length,
			pendingVoiceMemos.length,
			visibleLocalRecordingDrafts.length,
		],
	);

	const invalidateDiary = async () => {
		await Promise.all([
			queryClient.invalidateQueries({ queryKey: [['diary']] }),
			queryClient.invalidateQueries({ queryKey: [['tags']] }),
		]);
	};

	const createEntryMutation = useMutation({
		...trpc.diary.createEntry.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			setNotes('');
			setTagText('');
			setComposerOpen(false);
			setNotice('Entry added.');
		},
		onError: error => setNotice(error.message),
	});
	const startVoiceMemoDraftMutation = useMutation({
		...trpc.diary.startVoiceMemoDraft.mutationOptions(),
		onError: error => setNotice(error.message),
	});
	const appendVoiceMemoDraftMutation = useMutation({
		...trpc.diary.appendVoiceMemoDraft.mutationOptions(),
		onError: error => setNotice(error.message),
	});
	const setVoiceMemoDraftVideoMutation = useMutation({
		...trpc.diary.setVoiceMemoDraftVideo.mutationOptions(),
		onError: error => setNotice(error.message),
	});
	const resetVoiceMemoDraftMutation = useMutation({
		...trpc.diary.resetVoiceMemoDraft.mutationOptions(),
		onError: error => setNotice(error.message),
	});
	const finishVoiceMemoDraftMutation = useMutation({
		...trpc.diary.finishVoiceMemoDraft.mutationOptions(),
		onError: error => setNotice(error.message),
	});
	const processVoiceMemoMutation = useMutation({
		...trpc.diary.processVoiceMemo.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
		},
		onError: error => setNotice(error.message),
	});
	const processRecoveryMutation = useMutation({
		...trpc.diary.processVoiceMemoRecovery.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
		},
		onError: error => setNotice(error.message),
	});
	const deleteRecoveryMutation = useMutation({
		...trpc.diary.deleteVoiceMemoRecovery.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			setNotice('Recovery recording deleted.');
		},
		onError: error => setNotice(error.message),
	});
	const deleteVoiceMemoMutation = useMutation({
		...trpc.diary.deleteVoiceMemo.mutationOptions(),
		onSuccess: async (_data, variables) => {
			setSelectedVideoMemo(previous => (previous?.id === variables.voiceMemoId ? null : previous));
			await invalidateDiary();
			setNotice('Voice memo deleted.');
		},
		onError: error => setNotice(error.message),
	});
	const deleteEntryMutation = useMutation({
		...trpc.table.diaryEntries.deleteMany.mutationOptions(),
		onSuccess: async data => {
			await invalidateDiary();
			setSelectedEntry(null);
			setSelectedVideoMemo(null);
			setNotice(`${data.deletedCount} entr${data.deletedCount === 1 ? 'y' : 'ies'} deleted.`);
		},
		onError: error => setNotice(error.message),
	});
	const setEntryTagsMutation = useMutation({
		...trpc.diary.setEntryTags.mutationOptions(),
		onSuccess: async data => {
			await invalidateDiary();
			setSelectedEntry(previous => (previous?.id === data.id ? data : previous));
			setNotice('Tags updated.');
		},
		onError: error => setNotice(error.message),
	});
	const setVoiceMemoTagsMutation = useMutation({
		...trpc.diary.setVoiceMemoTags.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			setNotice('Tags updated.');
		},
		onError: error => setNotice(error.message),
	});

	useEffect(() => {
		void refreshLocation();
	}, []);
	useEffect(() => {
		void loadLocalRecordingDrafts();
	}, []);
	useEffect(() => {
		currentLocationRef.current = currentLocation;
	}, [currentLocation]);
	useEffect(() => {
		locationMessageRef.current = locationMessage;
	}, [locationMessage]);
	useEffect(() => {
		notesRef.current = notes;
	}, [notes]);
	useEffect(() => {
		tagNamesRef.current = tagNames;
	}, [tagNames]);
	useEffect(() => {
		audioDurationMillisRef.current = recorderState.durationMillis;
		audioRecorderUrlRef.current = recorderState.url;
		isAudioRecordingRef.current = recorderState.isRecording;
	}, [recorderState.durationMillis, recorderState.isRecording, recorderState.url]);
	useEffect(() => {
		isVideoRecordingRef.current = isVideoRecording;
	}, [isVideoRecording]);
	useEffect(() => {
		const shouldKeepAwake = recorderState.isRecording || isVideoRecording || isVideoStarting;
		if (!shouldKeepAwake) return;

		void activateKeepAwakeAsync(KEEP_AWAKE_TAG).catch(error => {
			setNotice(error instanceof Error ? error.message : String(error));
		});
		return () => {
			void deactivateKeepAwake(KEEP_AWAKE_TAG).catch(error => {
				setNotice(error instanceof Error ? error.message : String(error));
			});
		};
	}, [isVideoRecording, isVideoStarting, recorderState.isRecording]);
	useEffect(() => {
		const subscription = AppState.addEventListener('change', state => {
			if (state === 'active') return;
			if (isAudioRecordingRef.current) {
				void finishAudioRecording({ interrupted: true });
			}
			if (isVideoRecordingRef.current) {
				stopVideoRecordingForInterruption();
			}
		});
		return () => subscription.remove();
	}, []);
	useEffect(() => {
		if (!isVideoRecording) {
			recordPulse.stopAnimation();
			recordPulse.setValue(0);
			return;
		}

		const animation = Animated.loop(
			Animated.sequence([
				Animated.timing(recordPulse, {
					toValue: 1,
					duration: 700,
					easing: Easing.out(Easing.quad),
					useNativeDriver: true,
				}),
				Animated.timing(recordPulse, {
					toValue: 0,
					duration: 700,
					easing: Easing.in(Easing.quad),
					useNativeDriver: true,
				}),
			]),
		);
		animation.start();
		return () => animation.stop();
	}, [isVideoRecording, recordPulse]);
	useEffect(() => {
		if (!isVideoRecording || !videoStartedAt) {
			setVideoElapsedMs(0);
			return;
		}

		const updateElapsed = () => setVideoElapsedMs(Date.now() - videoStartedAt);
		updateElapsed();
		const interval = setInterval(updateElapsed, 250);
		return () => clearInterval(interval);
	}, [isVideoRecording, videoStartedAt]);
	useEffect(() => {
		if (!filterOptions.some(option => option.key === activeFilter)) setActiveFilter('entries');
	}, [activeFilter, filterOptions]);
	useEffect(() => {
		if (videoRecorderOpen) return;
		setVideoCameraMounted(false);
		setIsVideoCameraReady(false);
	}, [videoRecorderOpen]);
	useEffect(() => {
		if (
			!videoResumeAfterOpenRef.current ||
			!videoRecorderOpen ||
			!videoCameraMounted ||
			!isVideoCameraReady ||
			isVideoStarting ||
			isVideoRecording ||
			isVideoSaving
		) {
			return;
		}

		videoResumeAfterOpenRef.current = false;
		void startVideoRecording();
	}, [
		isVideoCameraReady,
		isVideoRecording,
		isVideoSaving,
		isVideoStarting,
		videoCameraMounted,
		videoRecorderOpen,
	]);

	const hasInterruptedRecording =
		interruptedAudioDraftId !== null || interruptedVideoDraftId !== null;
	const isBusy =
		createEntryMutation.isPending ||
		startVoiceMemoDraftMutation.isPending ||
		resetVoiceMemoDraftMutation.isPending ||
		finishVoiceMemoDraftMutation.isPending ||
		isUploadingRecording ||
		isOpeningVideoRecorder ||
		isVideoStarting ||
		isVideoSaving ||
		recorderState.isRecording;
	const canCreateEntry =
		notes.trim().length > 0 && currentLocation !== null && !isBusy && !hasInterruptedRecording;
	const canRecord =
		currentLocation !== null && !createEntryMutation.isPending && !isUploadingRecording;
	const canRecordVideo =
		currentLocation !== null &&
		!createEntryMutation.isPending &&
		!isUploadingRecording &&
		!startVoiceMemoDraftMutation.isPending &&
		!resetVoiceMemoDraftMutation.isPending &&
		!finishVoiceMemoDraftMutation.isPending &&
		!isOpeningVideoRecorder &&
		!isVideoStarting &&
		!isVideoSaving &&
		!recorderState.isRecording &&
		interruptedAudioDraftId === null;
	const error =
		entriesQuery.error ??
		pendingVoiceMemosQuery.error ??
		pendingVoiceMemoRecoveriesQuery.error ??
		tagsQuery.error;

	async function refreshLocation() {
		try {
			setLocationMessage('Requesting location...');
			const permission = await Location.requestForegroundPermissionsAsync();
			if (permission.status !== 'granted') {
				throw new Error('Location permission is required for diary entries.');
			}

			const location = await Location.getCurrentPositionAsync({
				accuracy: Location.Accuracy.Balanced,
			});
			setCurrentLocation(locationInputFromNative(location));
			setLocationMessage('Location ready.');
		} catch (error) {
			setCurrentLocation(null);
			setLocationMessage(error instanceof Error ? error.message : String(error));
		}
	}

	function getRequiredLocation() {
		const location = currentLocationRef.current;
		if (!location) {
			throw new Error(locationMessageRef.current || 'Location is not ready.');
		}

		return location;
	}

	async function loadLocalRecordingDrafts() {
		try {
			const cleanedDrafts = await cleanupExpiredLocalRecordingDrafts(
				await readLocalRecordingDrafts(),
			);
			const drafts = cleanedDrafts.sort((left, right) =>
				right.createdAt.localeCompare(left.createdAt),
			);
			localRecordingDraftsRef.current = drafts;
			setLocalRecordingDrafts(drafts);
			await writeLocalRecordingDrafts(drafts);
		} catch (error) {
			setNotice(error instanceof Error ? error.message : String(error));
		}
	}

	async function replaceLocalRecordingDrafts(
		updater: (drafts: LocalRecordingDraft[]) => LocalRecordingDraft[],
	) {
		const drafts = updater(localRecordingDraftsRef.current).sort((left, right) =>
			right.createdAt.localeCompare(left.createdAt),
		);
		localRecordingDraftsRef.current = drafts;
		setLocalRecordingDrafts(drafts);
		await writeLocalRecordingDrafts(drafts);
	}

	async function saveLocalRecordingDraft(draft: LocalRecordingDraft) {
		await replaceLocalRecordingDrafts(drafts => [
			draft,
			...drafts.filter(existingDraft => existingDraft.id !== draft.id),
		]);
	}

	async function updateLocalRecordingDraft(
		draftId: string,
		updates: Partial<Omit<LocalRecordingDraftBase, 'id' | 'createdAt'>>,
	) {
		await replaceLocalRecordingDrafts(drafts =>
			drafts.map(draft =>
				draft.id === draftId
					? {
							...draft,
							...updates,
							updatedAt: new Date().toISOString(),
						}
					: draft,
			),
		);
	}

	async function removeLocalRecordingDraft(draftId: string) {
		await replaceLocalRecordingDrafts(drafts => drafts.filter(draft => draft.id !== draftId));
	}

	async function discardLocalRecordingDraft(draft: LocalRecordingDraft) {
		await deleteLocalRecordingFile(draft);
		await removeLocalRecordingDraft(draft.id);
	}

	function rememberInterruptedRecordingDraft(draftId: string) {
		if (interruptedRecordingDraftIdsRef.current.includes(draftId)) return;
		interruptedRecordingDraftIdsRef.current = [...interruptedRecordingDraftIdsRef.current, draftId];
	}

	function forgetInterruptedRecordingDraft(draftId: string) {
		interruptedRecordingDraftIdsRef.current = interruptedRecordingDraftIdsRef.current.filter(
			id => id !== draftId,
		);
	}

	async function uploadRememberedInterruptedRecordings(
		mediaKind: LocalRecordingDraft['mediaKind'],
	) {
		const rememberedIds = new Set(interruptedRecordingDraftIdsRef.current);
		const drafts = localRecordingDraftsRef.current.filter(
			draft =>
				draft.mediaKind === mediaKind &&
				draft.status !== 'server_saved' &&
				rememberedIds.has(draft.id),
		);

		for (const draft of drafts) {
			try {
				await uploadLocalRecordingDraft(draft);
			} catch (error) {
				setNotice(error instanceof Error ? error.message : String(error));
			}
		}
	}

	async function createEntry() {
		try {
			await createEntryMutation.mutateAsync({
				notes,
				tagNames,
				location: getRequiredLocation(),
			});
		} catch {
			return;
		}
	}

	async function toggleRecording() {
		if (recorderState.isRecording) {
			await finishAudioRecording({ interrupted: false });
			return;
		}
		if (interruptedAudioDraftId) {
			await resumeInterruptedAudioRecording();
			return;
		}
		await startRecording();
	}

	async function startRecording() {
		try {
			getRequiredLocation();
			const permission = await requestRecordingPermissionsAsync();
			if (!permission.granted) {
				throw new Error('Microphone permission is required for voice memos.');
			}

			await setAudioModeAsync({
				allowsRecording: true,
				playsInSilentMode: true,
			});
			await audioRecorder.prepareToRecordAsync();
			const startedAt = new Date();
			audioRecordingSessionRef.current = {
				localId: `audio-${startedAt.toISOString().replace(/[:.]/g, '-')}`,
				startedAt: startedAt.toISOString(),
			};
			setInterruptedAudioDraftId(null);
			audioRecorder.record();
			setNotice('Recording...');
		} catch (error) {
			audioRecordingSessionRef.current = null;
			setNotice(error instanceof Error ? error.message : String(error));
		}
	}

	async function resumeInterruptedAudioRecording() {
		setInterruptedAudioDraftId(null);
		await startRecording();
	}

	async function finishAudioRecording({ interrupted }: { interrupted: boolean }) {
		if (audioStopInFlightRef.current) return;
		audioStopInFlightRef.current = true;
		const durationSeconds = Math.max(
			audioRecorder.currentTime || audioDurationMillisRef.current / 1000,
			0.1,
		);
		let savedDraft: LocalAudioDraft | null = null;
		try {
			setIsUploadingRecording(true);
			await audioRecorder.stop();
			const uri = audioRecorder.uri ?? audioRecorderUrlRef.current;
			if (!uri) {
				throw new Error('Recording finished without a file URI.');
			}

			const session = audioRecordingSessionRef.current ?? {
				localId: `audio-${new Date().toISOString().replace(/[:.]/g, '-')}`,
				startedAt: new Date().toISOString(),
			};
			const savedAudio = await persistAudioLog(uri);
			const audioBytes = await getFileSize(savedAudio.uri);
			savedDraft = {
				id: session.localId,
				mediaKind: 'audio',
				createdAt: session.startedAt,
				updatedAt: new Date().toISOString(),
				status: interrupted ? 'local_saved' : 'uploading',
				error: null,
				notes: notesRef.current,
				tagNames: tagNamesRef.current,
				location: getRequiredLocation(),
				durationSeconds,
				audioUri: savedAudio.uri,
				audioFileName: savedAudio.fileName,
				audioMimeType: audioMimeTypeFromUri(savedAudio.uri),
				audioBytes,
				serverRecoveryId: null,
				serverVoiceMemoId: null,
			};
			await saveLocalRecordingDraft(savedDraft);
			setActiveFilter('recoveries');

			if (interrupted) {
				setComposerOpen(true);
				setInterruptedAudioDraftId(savedDraft.id);
				rememberInterruptedRecordingDraft(savedDraft.id);
				setNotice(
					'Audio recording saved locally after interruption. Tap Resume audio to continue.',
				);
				return;
			}

			await uploadLocalRecordingDraft(savedDraft);
			await uploadRememberedInterruptedRecordings('audio');
			setNotes('');
			setTagText('');
			setComposerOpen(false);
			setNotice(`Voice memo saved: ${savedDraft.audioFileName}`);
		} catch (error) {
			if (savedDraft) {
				await updateLocalRecordingDraft(savedDraft.id, {
					status: 'failed',
					error: error instanceof Error ? error.message : String(error),
				});
			}
			setNotice(error instanceof Error ? error.message : String(error));
		} finally {
			setIsUploadingRecording(false);
			audioStopInFlightRef.current = false;
			audioRecordingSessionRef.current = null;
			await setAudioModeAsync({
				allowsRecording: false,
				playsInSilentMode: true,
			});
		}
	}

	async function openVideoRecorder() {
		if (openingVideoRecorderRef.current || videoRecorderOpen) return;
		openingVideoRecorderRef.current = true;
		setIsOpeningVideoRecorder(true);

		try {
			getRequiredLocation();
			const permission = cameraPermission?.granted
				? cameraPermission
				: await requestCameraPermission();
			if (!permission.granted) {
				throw new Error('Camera permission is required for video logs.');
			}

			const microphonePermission = await requestRecordingPermissionsAsync();
			if (!microphonePermission.granted) {
				throw new Error('Microphone permission is required for video logs.');
			}

			await presentVideoRecorder();
		} catch (error) {
			setNotice(error instanceof Error ? error.message : String(error));
		} finally {
			openingVideoRecorderRef.current = false;
			setIsOpeningVideoRecorder(false);
		}
	}

	async function presentVideoRecorder() {
		setIsVideoCameraReady(false);
		setVideoCameraMounted(false);
		setVideoCameraKey(key => key + 1);
		if (composerOpen) {
			setComposerOpen(false);
			await new Promise(resolve => setTimeout(resolve, VIDEO_RECORDER_MODAL_OPEN_DELAY_MS));
		}
		setVideoRecorderOpen(true);
	}

	function closeVideoRecorder() {
		if (isVideoRecording || isVideoStarting || isVideoSaving) return;
		setVideoRecorderOpen(false);
		setVideoCameraMounted(false);
		setIsVideoCameraReady(false);
	}

	async function toggleVideoRecording() {
		if (isVideoRecording) {
			await stopVideoRecording();
			return;
		}
		await startVideoRecording();
	}

	async function resumeInterruptedVideoRecording() {
		setInterruptedVideoDraftId(null);
		videoResumeAfterOpenRef.current = true;
		await openVideoRecorder();
	}

	async function startVideoRecording() {
		try {
			setIsVideoStarting(true);
			getRequiredLocation();
			if (!cameraRef.current) {
				throw new Error('Camera is not ready.');
			}
			if (!isVideoCameraReady) {
				throw new Error('Camera preview is still starting.');
			}

			const startedAt = new Date();
			videoRecordingSessionRef.current = {
				localId: `video-${startedAt.toISOString().replace(/[:.]/g, '-')}`,
				startedAt: startedAt.toISOString(),
			};
			videoStopRequestedRef.current = false;
			videoInterruptedAtRef.current = null;
			videoFinalizingRef.current = false;
			setInterruptedVideoDraftId(null);
			setVideoElapsedMs(0);
			const startedAtMs = Date.now();
			videoStartedAtRef.current = startedAtMs;
			setVideoStartedAt(startedAtMs);
			setIsVideoRecording(true);
			setIsVideoStarting(false);
			const recordingPromise = cameraRef.current.recordAsync({
				maxDuration: 10 * 60,
			});
			videoRecordingPromiseRef.current = recordingPromise;
			void recordingPromise
				.then(video => {
					if (videoStopRequestedRef.current) return;
					void finishVideoRecording({ video, interrupted: true });
				})
				.catch(error => {
					if (videoStopRequestedRef.current) return;
					setIsVideoRecording(false);
					setIsVideoSaving(false);
					setVideoStartedAt(null);
					videoStartedAtRef.current = null;
					setVideoElapsedMs(0);
					videoRecordingPromiseRef.current = null;
					videoRecordingSessionRef.current = null;
					setNotice(error instanceof Error ? error.message : String(error));
				});
		} catch (error) {
			setIsVideoRecording(false);
			setIsVideoStarting(false);
			videoStartedAtRef.current = null;
			videoRecordingPromiseRef.current = null;
			videoRecordingSessionRef.current = null;
			const message = error instanceof Error ? error.message : String(error);
			setNotice(message);
		}
	}

	async function stopVideoRecording() {
		try {
			videoStopRequestedRef.current = true;
			cameraRef.current?.stopRecording();
			const video = await videoRecordingPromiseRef.current;
			await finishVideoRecording({ video: video ?? undefined, interrupted: false });
		} catch (error) {
			setNotice(error instanceof Error ? error.message : String(error));
		}
	}

	function stopVideoRecordingForInterruption() {
		if (videoStopRequestedRef.current || videoFinalizingRef.current) return;
		videoInterruptedAtRef.current = Date.now();
		isVideoRecordingRef.current = false;
		setIsVideoRecording(false);
		setVideoRecorderOpen(false);
		setVideoCameraMounted(false);
		setIsVideoCameraReady(false);
		setComposerOpen(true);
		setActiveFilter('recoveries');
		setNotice('Video recording interrupted. Saving the current segment locally...');
		cameraRef.current?.stopRecording();
	}

	async function finishVideoRecording({
		video,
		interrupted,
	}: {
		video: { uri: string } | undefined;
		interrupted: boolean;
	}) {
		if (videoFinalizingRef.current) return;
		videoFinalizingRef.current = true;
		const stoppedAt = videoInterruptedAtRef.current ?? Date.now();
		const durationSeconds = Math.max(
			(stoppedAt - (videoStartedAtRef.current ?? stoppedAt)) / 1000,
			0.1,
		);
		let savedDraft: LocalVideoDraft | null = null;
		try {
			setIsVideoSaving(true);
			const session = videoRecordingSessionRef.current;
			if (!session) {
				throw new Error('Video recording session is missing.');
			}
			if (!video?.uri) {
				throw new Error('Video recording finished without a file URI.');
			}

			setIsVideoRecording(false);
			setVideoRecorderOpen(false);
			setVideoCameraMounted(false);
			setIsVideoCameraReady(false);
			setComposerOpen(true);
			setActiveFilter('recoveries');

			const savedVideo = await persistVideoLog(video.uri);
			const savedVideoUri = savedVideo.uri;
			const videoFileName = savedVideo.fileName;
			const videoBytes = await getFileSize(savedVideoUri);
			savedDraft = {
				id: session.localId,
				mediaKind: 'video',
				createdAt: session.startedAt,
				updatedAt: new Date().toISOString(),
				status: interrupted ? 'local_saved' : 'uploading',
				error: null,
				notes: notesRef.current,
				tagNames: tagNamesRef.current,
				location: getRequiredLocation(),
				durationSeconds,
				videoUri: savedVideoUri,
				videoFileName,
				videoMimeType: videoMimeTypeFromUri(savedVideoUri),
				videoBytes,
				serverRecoveryId: null,
				serverVoiceMemoId: null,
			};
			await saveLocalRecordingDraft(savedDraft);
			setActiveFilter('recoveries');

			if (interrupted) {
				setInterruptedVideoDraftId(savedDraft.id);
				rememberInterruptedRecordingDraft(savedDraft.id);
				setNotice(
					'Video recording saved locally after interruption. Tap Resume video to continue.',
				);
				return;
			}

			await uploadLocalRecordingDraft(savedDraft);
			await uploadRememberedInterruptedRecordings('video');
			setNotes('');
			setTagText('');
			setComposerOpen(false);
			Toast.success('Video processed.', 2, undefined, false);
		} catch (error) {
			if (savedDraft) {
				await updateLocalRecordingDraft(savedDraft.id, {
					status: 'failed',
					error: error instanceof Error ? error.message : String(error),
				});
			}
			setNotice(error instanceof Error ? error.message : String(error));
		} finally {
			setIsVideoRecording(false);
			setIsVideoSaving(false);
			setVideoStartedAt(null);
			videoStartedAtRef.current = null;
			setVideoElapsedMs(0);
			videoRecordingPromiseRef.current = null;
			videoRecordingSessionRef.current = null;
			videoStopRequestedRef.current = false;
			videoInterruptedAtRef.current = null;
			videoFinalizingRef.current = false;
		}
	}

	async function uploadLocalRecordingDraft(draft: LocalRecordingDraft) {
		await updateLocalRecordingDraft(draft.id, {
			status: 'uploading',
			error: null,
		});

		try {
			if (draft.serverVoiceMemoId) {
				await updateLocalRecordingDraft(draft.id, {
					status: 'processing',
				});
				await processVoiceMemoMutation.mutateAsync({
					voiceMemoId: draft.serverVoiceMemoId,
				});
				await updateLocalRecordingDraft(draft.id, {
					status: 'server_saved',
					error: null,
				});
				forgetInterruptedRecordingDraft(draft.id);
				await invalidateDiary();
				return;
			}

			let recoveryId = draft.serverRecoveryId;
			if (recoveryId) {
				await resetVoiceMemoDraftMutation.mutateAsync({ recoveryId });
				if (draft.mediaKind === 'video') {
					await setVoiceMemoDraftVideoMutation.mutateAsync({
						recoveryId,
						videoFileName: draft.videoFileName,
						videoMimeType: draft.videoMimeType,
					});
				}
			} else {
				const serverDraft = await startVoiceMemoDraftMutation.mutateAsync({
					mediaKind: draft.mediaKind,
					notes: draft.notes,
					transcript: '',
					fileName:
						draft.mediaKind === 'video'
							? videoAudioFileNameFromVideoFileName(draft.videoFileName)
							: draft.audioFileName,
					mimeType: draft.mediaKind === 'video' ? 'audio/mp4' : draft.audioMimeType,
					videoFileName: draft.mediaKind === 'video' ? draft.videoFileName : undefined,
					videoMimeType: draft.mediaKind === 'video' ? draft.videoMimeType : undefined,
					tagNames: draft.tagNames,
					location: draft.location,
				});
				recoveryId = serverDraft.recoveryId;
				await updateLocalRecordingDraft(draft.id, {
					serverRecoveryId: recoveryId,
				});
			}

			await uploadFileToVoiceMemoDraft(recoveryId, draft.mediaKind, localRecordingFileUri(draft));
			const saved = await finishVoiceMemoDraftMutation.mutateAsync({
				recoveryId,
				transcript: '',
				durationSeconds: draft.durationSeconds,
			});
			await updateLocalRecordingDraft(draft.id, {
				status: 'processing',
				serverVoiceMemoId: saved.voiceMemoId,
				error: null,
			});
			await processVoiceMemoMutation.mutateAsync({
				voiceMemoId: saved.voiceMemoId,
			});
			await updateLocalRecordingDraft(draft.id, {
				status: 'server_saved',
				error: null,
			});
			forgetInterruptedRecordingDraft(draft.id);
			await invalidateDiary();
		} catch (error) {
			await updateLocalRecordingDraft(draft.id, {
				status: 'failed',
				error: error instanceof Error ? error.message : String(error),
			});
			throw error;
		}
	}

	async function uploadFileToVoiceMemoDraft(
		recoveryId: string,
		mediaKind: 'audio' | 'video',
		uri: string,
	) {
		const fileBytes = await getFileSize(uri);
		for (let position = 0; position < fileBytes; position += MEDIA_UPLOAD_CHUNK_BYTES) {
			const length = Math.min(MEDIA_UPLOAD_CHUNK_BYTES, fileBytes - position);
			const dataBase64 = await FileSystem.readAsStringAsync(uri, {
				encoding: FileSystem.EncodingType.Base64,
				position,
				length,
			});
			await appendVoiceMemoDraftMutation.mutateAsync({
				recoveryId,
				mediaKind,
				dataBase64,
			});
		}
	}

	async function persistAudioLog(uri: string) {
		const directory = localRecordingLogsDirectory();
		await ensureDirectory(directory);
		const fileName = audioFileNameFromUri(uri);
		const destination = `${directory}${fileName}`;
		await FileSystem.copyAsync({ from: uri, to: destination });
		return {
			uri: destination,
			fileName,
		};
	}

	async function persistVideoLog(uri: string) {
		const directory = localRecordingLogsDirectory();
		await ensureDirectory(directory);
		const fileName = videoFileNameFromUri(uri);
		const destination = `${directory}${fileName}`;
		await FileSystem.copyAsync({ from: uri, to: destination });
		return {
			uri: destination,
			fileName,
		};
	}

	async function deleteLocalRecordingDraftFiles(draft: LocalRecordingDraft) {
		if (draft.serverVoiceMemoId) {
			await deleteVoiceMemoMutation.mutateAsync({ voiceMemoId: draft.serverVoiceMemoId });
		} else if (draft.serverRecoveryId) {
			await deleteRecoveryMutation.mutateAsync({ recoveryId: draft.serverRecoveryId });
		}
		forgetInterruptedRecordingDraft(draft.id);
		await discardLocalRecordingDraft(draft);
	}

	function deleteEntry(entry: DiaryEntry) {
		Modal.alert('Delete diary entry?', formatDiaryTimestamp(entry.createdAt), [
			{ text: 'Cancel' },
			{
				text: 'Delete',
				onPress: () => {
					deleteEntryMutation.mutate({
						where: [{ column: 'id', operator: 'eq', value: entry.id }],
					});
				},
			},
		]);
	}

	function deleteVoiceMemo(memo: DiaryPendingVoiceMemo) {
		Modal.alert('Delete pending memo?', memo.fileName, [
			{ text: 'Cancel' },
			{
				text: 'Delete',
				onPress: () => deleteVoiceMemoMutation.mutate({ voiceMemoId: memo.id }),
			},
		]);
	}

	function deleteVoiceMemoRecovery(recovery: DiaryPendingVoiceMemoRecovery) {
		Modal.alert('Delete recovery recording?', recovery.fileName, [
			{ text: 'Cancel' },
			{
				text: 'Delete',
				onPress: () => deleteRecoveryMutation.mutate({ recoveryId: recovery.id }),
			},
		]);
	}

	function reprocessLocalRecordingDraft(draft: LocalRecordingDraft) {
		setIsVideoSaving(true);
		void uploadLocalRecordingDraft(draft)
			.then(() => {
				setComposerOpen(false);
				Toast.success(
					`${draft.mediaKind === 'video' ? 'Video' : 'Audio'} processed.`,
					2,
					undefined,
					false,
				);
			})
			.catch(error => {
				setNotice(error instanceof Error ? error.message : String(error));
			})
			.finally(() => {
				setIsVideoSaving(false);
			});
	}

	function deleteLocalRecordingDraft(draft: LocalRecordingDraft) {
		Modal.alert('Delete local recording?', localRecordingFileName(draft), [
			{ text: 'Cancel' },
			{
				text: 'Delete',
				onPress: () => {
					void deleteLocalRecordingDraftFiles(draft)
						.then(() => setNotice('Local recording deleted.'))
						.catch(error => {
							setNotice(error instanceof Error ? error.message : String(error));
						});
				},
			},
		]);
	}

	function openVideoMemo(memo: Pick<DiaryVoiceMemo, 'id' | 'videoFileName' | 'fileName'>) {
		setSelectedVideoMemo({
			id: memo.id,
			fileName: memo.videoFileName ?? memo.fileName,
		});
	}

	if (entriesQuery.isLoading || pendingVoiceMemosQuery.isLoading || tagsQuery.isLoading) {
		return (
			<View style={styles.loadingScreen}>
				<ActivityIndicator animating text='Loading log...' />
			</View>
		);
	}

	if (error) {
		return (
			<ScrollView
				contentInsetAdjustmentBehavior='automatic'
				contentContainerStyle={sharedStyles.page}
			>
				<Text selectable style={sharedStyles.errorText}>
					{error.message}
				</Text>
			</ScrollView>
		);
	}

	return (
		<>
			<ScrollView
				contentInsetAdjustmentBehavior='automatic'
				contentContainerStyle={sharedStyles.page}
			>
				{notice ? (
					<Pressable onPress={() => setNotice(null)} style={styles.notice}>
						<Text selectable style={styles.noticeText}>
							{notice}
						</Text>
					</Pressable>
				) : null}

				<View style={styles.totalsRow}>
					{filterOptions.map(option => (
						<TotalCard
							key={option.key}
							label={option.label}
							value={option.value}
							active={activeFilter === option.key}
							onPress={() => setActiveFilter(option.key)}
							styles={styles}
						/>
					))}
				</View>

				{activeFilter === 'pending' ? (
					<View style={styles.listStack}>
						{pendingVoiceMemos.map(memo => (
							<PendingVoiceMemoCard
								key={memo.id}
								memo={memo}
								availableTags={availableTags}
								isSettingTags={setVoiceMemoTagsMutation.isPending}
								isProcessing={processVoiceMemoMutation.isPending}
								isDeleting={deleteVoiceMemoMutation.isPending}
								onSetTags={value =>
									setVoiceMemoTagsMutation.mutate({
										voiceMemoId: memo.id,
										tagNames: parseTagText(value),
									})
								}
								onReprocess={() => processVoiceMemoMutation.mutate({ voiceMemoId: memo.id })}
								onDelete={() => deleteVoiceMemo(memo)}
								onPlayVideo={() => openVideoMemo(memo)}
								styles={styles}
							/>
						))}
					</View>
				) : null}

				{activeFilter === 'recoveries' ? (
					<View style={styles.listStack}>
						{isVideoSaving && visibleLocalRecordingDrafts.length === 0 ? (
							<VideoProcessingCard styles={styles} />
						) : null}
						{visibleLocalRecordingDrafts.map(draft => (
							<LocalRecordingDraftCard
								key={draft.id}
								draft={draft}
								isProcessing={isVideoSaving}
								onReprocess={() => reprocessLocalRecordingDraft(draft)}
								onDelete={() => deleteLocalRecordingDraft(draft)}
								styles={styles}
							/>
						))}
						{pendingVoiceMemoRecoveries.map(recovery => (
							<RecoveryMemoCard
								key={recovery.id}
								recovery={recovery}
								isProcessing={processRecoveryMutation.isPending}
								isDeleting={deleteRecoveryMutation.isPending}
								onReprocess={() => processRecoveryMutation.mutate({ recoveryId: recovery.id })}
								onDelete={() => deleteVoiceMemoRecovery(recovery)}
								styles={styles}
							/>
						))}
					</View>
				) : null}

				{activeFilter === 'entries' ? (
					<View style={styles.listStack}>
						{entries.map(entry => (
							<EntryCard
								key={entry.id}
								entry={entry}
								onOpen={() => setSelectedEntry(entry)}
								onPlayVideo={openVideoMemo}
								styles={styles}
							/>
						))}
					</View>
				) : null}
			</ScrollView>
			<FloatingActionButton
				icon='mic.fill'
				label={
					recorderState.isRecording
						? formatRecorderDuration(recorderState.durationMillis)
						: 'Record'
				}
				onPress={() => setComposerOpen(true)}
			/>
			<BottomSheet
				visible={composerOpen}
				title='New entry'
				onClose={() => setComposerOpen(false)}
				footer={
					<View style={styles.composerFooter}>
						<View style={styles.recordActionGroup}>
							<Button
								icon='video.fill'
								label={interruptedVideoDraftId ? 'Resume video' : 'Video log'}
								disabled={!canRecordVideo}
								loading={isVideoSaving || isOpeningVideoRecorder}
								intent='video'
								size='small'
								onPress={() =>
									void (interruptedVideoDraftId
										? resumeInterruptedVideoRecording()
										: openVideoRecorder())
								}
							/>
							<Button
								icon={recorderState.isRecording ? 'stop.fill' : 'mic.fill'}
								label={
									recorderState.isRecording
										? 'Stop'
										: interruptedAudioDraftId
											? 'Resume audio'
											: 'Audio Log'
								}
								active={recorderState.isRecording || interruptedAudioDraftId !== null}
								disabled={!canRecord}
								loading={isUploadingRecording}
								intent='audio'
								size='small'
								onPress={toggleRecording}
							/>
						</View>
						<Button
							icon='plus'
							label='Add'
							disabled={!canCreateEntry}
							loading={createEntryMutation.isPending}
							intent='success'
							size='small'
							onPress={() => void createEntry()}
						/>
					</View>
				}
			>
				<View style={styles.stack}>
					<Text style={styles.muted}>{locationMessage}</Text>
					<TextInput
						value={notes}
						editable={!isBusy}
						multiline
						placeholder='What happened?'
						placeholderTextColor={styles.placeholder.color}
						style={[styles.input, styles.composerInput]}
						onChangeText={setNotes}
					/>
					<TagSelector
						value={tagText}
						availableTags={availableTags}
						onChange={setTagText}
						disabled={isBusy}
					/>
					{recorderState.isRecording ? (
						<Text style={styles.recordingText}>
							Recording {formatRecorderDuration(recorderState.durationMillis)}
						</Text>
					) : null}
					{interruptedAudioDraftId ? (
						<Text style={styles.recordingText}>
							Audio segment saved locally. Resume audio to continue.
						</Text>
					) : null}
					{interruptedVideoDraftId ? (
						<Text style={styles.recordingText}>
							Video segment saved locally. Resume video to continue.
						</Text>
					) : null}
					{pendingVoiceMemos.length > 0 ||
					pendingVoiceMemoRecoveries.length > 0 ||
					visibleLocalRecordingDrafts.length > 0 ||
					isVideoSaving ? (
						<View style={styles.stack}>
							<Text style={styles.sectionTitle}>Unprocessed recordings</Text>
							{isVideoSaving && visibleLocalRecordingDrafts.length === 0 ? (
								<VideoProcessingCard styles={styles} />
							) : null}
							{visibleLocalRecordingDrafts.slice(0, 2).map(draft => (
								<LocalRecordingDraftCard
									key={draft.id}
									draft={draft}
									isProcessing={isVideoSaving}
									onReprocess={() => reprocessLocalRecordingDraft(draft)}
									onDelete={() => deleteLocalRecordingDraft(draft)}
									styles={styles}
								/>
							))}
							{pendingVoiceMemos.slice(0, 3).map(memo => (
								<PendingVoiceMemoCard
									key={memo.id}
									memo={memo}
									availableTags={availableTags}
									isSettingTags={setVoiceMemoTagsMutation.isPending}
									isProcessing={processVoiceMemoMutation.isPending}
									isDeleting={deleteVoiceMemoMutation.isPending}
									onSetTags={value =>
										setVoiceMemoTagsMutation.mutate({
											voiceMemoId: memo.id,
											tagNames: parseTagText(value),
										})
									}
									onReprocess={() => processVoiceMemoMutation.mutate({ voiceMemoId: memo.id })}
									onDelete={() => deleteVoiceMemo(memo)}
									onPlayVideo={() => openVideoMemo(memo)}
									styles={styles}
								/>
							))}
							{pendingVoiceMemoRecoveries.slice(0, 2).map(recovery => (
								<RecoveryMemoCard
									key={recovery.id}
									recovery={recovery}
									isProcessing={processRecoveryMutation.isPending}
									isDeleting={deleteRecoveryMutation.isPending}
									onReprocess={() => processRecoveryMutation.mutate({ recoveryId: recovery.id })}
									onDelete={() => deleteVoiceMemoRecovery(recovery)}
									styles={styles}
								/>
							))}
						</View>
					) : null}
				</View>
			</BottomSheet>

			<NativeModal
				visible={videoRecorderOpen}
				animationType='slide'
				presentationStyle='fullScreen'
				onShow={() => setVideoCameraMounted(true)}
				onDismiss={() => {
					setVideoCameraMounted(false);
					setIsVideoCameraReady(false);
				}}
				onRequestClose={closeVideoRecorder}
			>
				<View style={styles.videoScreen}>
					{videoCameraMounted ? (
						<CameraView
							key={videoCameraKey}
							ref={cameraRef}
							active={videoRecorderOpen && videoCameraMounted}
							facing='front'
							mirror
							mode='video'
							onCameraReady={() => setIsVideoCameraReady(true)}
							onMountError={event => {
								setIsVideoCameraReady(false);
								setNotice(event.message);
							}}
							style={styles.cameraPreview}
							videoQuality='720p'
						/>
					) : (
						<View style={styles.cameraPreview} />
					)}
					{!isVideoCameraReady ? (
						<View pointerEvents='none' style={styles.cameraStartingOverlay}>
							<Text style={styles.cameraStartingText}>Starting camera...</Text>
						</View>
					) : null}
					<View style={styles.videoTopBar}>
						<Pressable
							disabled={isVideoRecording || isVideoStarting || isVideoSaving}
							onPress={closeVideoRecorder}
							style={styles.videoCloseButton}
						>
							<Text style={styles.videoCloseText}>Close</Text>
						</Pressable>
					</View>
					<View style={styles.videoControls}>
						<Pressable
							disabled={
								isVideoSaving || isVideoStarting || (!isVideoRecording && !isVideoCameraReady)
							}
							onPress={() => void toggleVideoRecording()}
							style={[
								styles.recordButtonShell,
								(isVideoSaving || isVideoStarting || (!isVideoRecording && !isVideoCameraReady)) &&
									styles.recordButtonShellDisabled,
							]}
						>
							{isVideoRecording ? (
								<Animated.View
									pointerEvents='none'
									style={[
										styles.recordPulseRing,
										{
											opacity: recordPulse.interpolate({
												inputRange: [0, 1],
												outputRange: [0.32, 0],
											}),
											transform: [
												{
													scale: recordPulse.interpolate({
														inputRange: [0, 1],
														outputRange: [1, 1.28],
													}),
												},
											],
										},
									]}
								/>
							) : null}
							<Animated.View
								style={[styles.recordButton, isVideoRecording && styles.recordButtonActive]}
							>
								{isVideoRecording ? <View style={styles.stopIcon} /> : null}
							</Animated.View>
						</Pressable>
						<Text style={styles.videoStatus}>
							{isVideoSaving
								? 'Saving...'
								: isVideoStarting
									? 'Starting...'
									: isVideoRecording && videoStartedAt
										? formatDuration(videoElapsedMs / 1000)
										: isVideoCameraReady
											? 'Tap to record'
											: 'Starting camera...'}
						</Text>
					</View>
				</View>
			</NativeModal>

			<EntryDetailSheet
				entry={selectedEntry}
				visible={selectedEntry !== null}
				availableTags={availableTags}
				isSettingTags={setEntryTagsMutation.isPending}
				onClose={() => setSelectedEntry(null)}
				onSetTags={value => {
					if (!selectedEntry) return;
					setEntryTagsMutation.mutate({
						entryId: selectedEntry.id,
						tagNames: parseTagText(value),
					});
				}}
				onDelete={entry => deleteEntry(entry)}
				onPlayVideo={openVideoMemo}
				styles={styles}
			/>
			<FullscreenVideoPlayer
				video={selectedVideoMemo}
				onClose={() => setSelectedVideoMemo(null)}
				styles={styles}
			/>
		</>
	);
}

function TotalCard({
	label,
	value,
	active,
	onPress,
	styles,
}: {
	label: string;
	value: number;
	active: boolean;
	onPress: () => void;
	styles: ReturnType<typeof logStyles>;
}) {
	return (
		<Pressable onPress={onPress} style={[styles.totalCard, active && styles.totalCardActive]}>
			<Text style={[styles.totalValue, active && styles.totalValueActive]}>{value}</Text>
			<Text style={active ? styles.totalLabelActive : styles.muted}>{label}</Text>
		</Pressable>
	);
}

function EntryCard({
	entry,
	onOpen,
	onPlayVideo,
	styles,
}: {
	entry: DiaryEntry;
	onOpen: () => void;
	onPlayVideo: (memo: DiaryVoiceMemo) => void;
	styles: ReturnType<typeof logStyles>;
}) {
	const transcript = getEntryTranscriptText(entry);
	const summary = entry.summary?.trim() ?? '';
	const videoMemo = entry.voiceMemos.find(memo => memo.mediaKind === 'video') ?? null;
	const content = (
		<View style={styles.videoMemoContent}>
			{summary ? (
				<MarkdownText value={summary} numberOfLines={2} compact style={styles.summaryText} />
			) : null}
			{transcript ? (
				<Text style={styles.bodyPreview} numberOfLines={2}>
					{transcript}
				</Text>
			) : null}
			<View style={styles.rowBetween}>
				<Text style={styles.muted}>{formatDiaryTimestamp(entry.createdAt)}</Text>
				<Text style={styles.muted}>#{entry.id}</Text>
			</View>
			<View style={styles.entryMetaRow}>
				<Pressable
					onPress={() => {
						void Linking.openURL(mapsUrl(entry.location));
					}}
				>
					<Text style={styles.linkText} numberOfLines={1}>
						{formatLocationLabel(entry.location)}
					</Text>
				</Pressable>
				{entry.voiceMemos.length > 0 ? (
					<Text style={styles.metaPill}>{entry.voiceMemos.length} memo</Text>
				) : null}
			</View>
			<TagList tags={entry.tags} />
		</View>
	);

	if (videoMemo) {
		return (
			<View style={[styles.entryCard, styles.videoMemoCard]}>
				<VideoMemoPreview memo={videoMemo} onPress={() => onPlayVideo(videoMemo)} styles={styles} />
				<Pressable onPress={onOpen} style={styles.videoMemoContentPressable}>
					{content}
				</Pressable>
			</View>
		);
	}

	return (
		<Pressable onPress={onOpen} style={styles.entryCard}>
			{content}
		</Pressable>
	);
}

function PendingVoiceMemoCard({
	memo,
	availableTags,
	isSettingTags,
	isProcessing,
	isDeleting,
	onSetTags,
	onReprocess,
	onDelete,
	onPlayVideo,
	styles,
}: {
	memo: DiaryPendingVoiceMemo;
	availableTags: TagRecord[];
	isSettingTags: boolean;
	isProcessing: boolean;
	isDeleting: boolean;
	onSetTags: (value: string) => void;
	onReprocess: () => void;
	onDelete: () => void;
	onPlayVideo: () => void;
	styles: ReturnType<typeof logStyles>;
}) {
	const content = (
		<View style={styles.videoMemoContent}>
			<View style={styles.rowBetween}>
				<Text style={styles.cardTitle}>{formatDiaryTimestamp(memo.createdAt)}</Text>
				{memo.mediaKind === 'audio' ? <Tag small>{memo.transcriptionStatus}</Tag> : null}
			</View>
			<Text style={styles.muted}>
				{memo.mediaKind === 'video' ? (memo.videoFileName ?? memo.fileName) : memo.fileName} -{' '}
				{formatDuration(memo.durationSeconds)} -{' '}
				{formatBytes(memo.mediaKind === 'video' ? memo.videoBytes : memo.audioBytes)}
			</Text>
			{memo.mediaKind === 'video' ? (
				<VideoProgressSteps
					progress={videoProgressFromVoiceMemoStatus(memo.transcriptionStatus)}
					styles={styles}
				/>
			) : null}
			{memo.mediaKind === 'audio' ? (
				<Pressable
					onPress={() => {
						void Linking.openURL(voiceMemoAudioUrl(memo.id));
					}}
				>
					<Text style={styles.linkText}>Open audio</Text>
				</Pressable>
			) : null}
			<Text style={styles.bodyPreview} numberOfLines={3}>
				{memo.transcript?.trim() || memo.notes.trim() || 'No transcript yet'}
			</Text>
			{memo.transcriptionError ? (
				<Text selectable style={styles.errorText} numberOfLines={4}>
					{memo.transcriptionError}
				</Text>
			) : null}
			<TagSelector
				value={formatTagNames(memo.tags.map(tag => tag.name))}
				availableTags={availableTags}
				onChange={onSetTags}
				disabled={isSettingTags}
			/>
			<View style={styles.actionRow}>
				<Button size='small' onPress={onReprocess} loading={isProcessing}>
					Reprocess
				</Button>
				<Button size='small' onPress={onDelete} loading={isDeleting}>
					Delete
				</Button>
			</View>
		</View>
	);

	if (memo.mediaKind === 'video') {
		return (
			<View style={[styles.entryCard, styles.videoMemoCard]}>
				<VideoMemoPreview memo={memo} onPress={onPlayVideo} styles={styles} />
				<View style={styles.videoMemoContentPressable}>{content}</View>
			</View>
		);
	}

	return <View style={styles.entryCard}>{content}</View>;
}

function VideoProcessingCard({ styles }: { styles: ReturnType<typeof logStyles> }) {
	return (
		<View style={styles.entryCard}>
			<View style={styles.stack}>
				<Text style={styles.cardTitle}>Video log</Text>
				<VideoProgressSteps progress={{ step: 'upload', failed: false }} styles={styles} />
			</View>
		</View>
	);
}

function VideoProgressSteps({
	progress,
	styles,
}: {
	progress: { step: VideoProgressStep; failed: boolean };
	styles: ReturnType<typeof logStyles>;
}) {
	const current = videoProgressStepIndex(progress.step);
	return (
		<View style={styles.videoProgress}>
			<Steps direction='horizontal' size='small' current={current}>
				{(['Upload', 'Transcribe', 'Summarize', 'Done'] as const).map((title, index) => (
					<Steps.Step
						key={title}
						title={title}
						status={
							progress.failed && index === current
								? 'error'
								: index < current
									? 'finish'
									: index === current
										? 'process'
										: 'wait'
						}
					/>
				))}
			</Steps>
		</View>
	);
}

function LocalRecordingDraftCard({
	draft,
	isProcessing,
	onReprocess,
	onDelete,
	styles,
}: {
	draft: LocalRecordingDraft;
	isProcessing: boolean;
	onReprocess: () => void;
	onDelete: () => void;
	styles: ReturnType<typeof logStyles>;
}) {
	return (
		<View style={[styles.entryCard, styles.videoMemoCard]}>
			{draft.mediaKind === 'video' ? (
				<LocalVideoPreview uri={draft.videoUri} styles={styles} />
			) : (
				<LocalAudioPreview styles={styles} />
			)}
			<View style={styles.videoMemoContentPressable}>
				<View style={styles.videoMemoContent}>
					<View style={styles.rowBetween}>
						<Text style={styles.cardTitle}>{formatDiaryTimestamp(draft.createdAt)}</Text>
						<Tag small>{draft.mediaKind}</Tag>
					</View>
					<Text style={styles.muted}>
						{localRecordingFileName(draft)} - {formatDuration(draft.durationSeconds)} -{' '}
						{formatBytes(localRecordingBytes(draft))}
					</Text>
					<VideoProgressSteps progress={recordingProgressFromLocalDraft(draft)} styles={styles} />
					<Text style={styles.bodyPreview} numberOfLines={3}>
						{draft.notes.trim() || `Local ${draft.mediaKind} waiting to process`}
					</Text>
					{draft.error ? (
						<Text selectable style={styles.errorText} numberOfLines={4}>
							{draft.error}
						</Text>
					) : null}
					<View style={styles.actionRow}>
						<Button size='small' onPress={onReprocess} loading={isProcessing}>
							Reprocess
						</Button>
						<Button size='small' onPress={onDelete} disabled={isProcessing}>
							Delete
						</Button>
					</View>
				</View>
			</View>
		</View>
	);
}

function RecoveryMemoCard({
	recovery,
	isProcessing,
	isDeleting,
	onReprocess,
	onDelete,
	styles,
}: {
	recovery: DiaryPendingVoiceMemoRecovery;
	isProcessing: boolean;
	isDeleting: boolean;
	onReprocess: () => void;
	onDelete: () => void;
	styles: ReturnType<typeof logStyles>;
}) {
	return (
		<View style={styles.entryCard}>
			<View style={styles.stack}>
				<View style={styles.rowBetween}>
					<Text style={styles.cardTitle}>{formatDiaryTimestamp(recovery.createdAt)}</Text>
					{recovery.mediaKind === 'audio' ? <Tag small>{recovery.status}</Tag> : null}
				</View>
				<Text style={styles.muted}>
					{recovery.mediaKind === 'video'
						? (recovery.videoFileName ?? recovery.fileName)
						: recovery.fileName}{' '}
					- {formatDuration(recovery.durationSeconds)} -{' '}
					{formatBytes(recovery.mediaKind === 'video' ? recovery.videoBytes : recovery.audioBytes)}
				</Text>
				{recovery.mediaKind === 'video' ? (
					<VideoProgressSteps progress={videoProgressFromRecovery(recovery)} styles={styles} />
				) : null}
				<Text style={styles.bodyPreview} numberOfLines={4}>
					{recovery.transcript?.trim() || 'No transcript yet'}
				</Text>
				{recovery.error ? (
					<Text selectable style={styles.errorText} numberOfLines={4}>
						{recovery.error}
					</Text>
				) : null}
				<View style={styles.actionRow}>
					<Button size='small' onPress={onReprocess} loading={isProcessing}>
						Reprocess
					</Button>
					<Button size='small' onPress={onDelete} loading={isDeleting}>
						Delete
					</Button>
				</View>
			</View>
		</View>
	);
}

function LocalVideoPreview({ uri, styles }: { uri: string; styles: ReturnType<typeof logStyles> }) {
	const player = useVideoPlayer(uri, videoPlayer => {
		videoPlayer.muted = true;
		videoPlayer.loop = false;
	});

	return (
		<View style={styles.videoThumbnail}>
			<VideoView
				player={player}
				nativeControls={false}
				contentFit='cover'
				style={styles.videoThumbnailPlayer}
			/>
			<View style={styles.videoPlayBadge}>
				<Text style={styles.videoPlayGlyph}>▶</Text>
			</View>
		</View>
	);
}

function LocalAudioPreview({ styles }: { styles: ReturnType<typeof logStyles> }) {
	return (
		<View style={styles.audioThumbnail}>
			<Text style={styles.audioThumbnailIcon}>REC</Text>
			<Text style={styles.audioThumbnailText}>Audio</Text>
		</View>
	);
}

function VoiceMemoButton({
	memo,
	onPlayVideo,
	styles,
}: {
	memo: DiaryVoiceMemo;
	onPlayVideo: (memo: DiaryVoiceMemo) => void;
	styles: ReturnType<typeof logStyles>;
}) {
	return (
		<Pressable
			onPress={() => {
				if (memo.mediaKind === 'video') {
					onPlayVideo(memo);
					return;
				}
				void Linking.openURL(voiceMemoAudioUrl(memo.id));
			}}
			style={styles.voiceMemoPill}
		>
			<Text style={styles.voiceMemoText}>
				{memo.mediaKind === 'video' ? 'Video' : 'Audio'} - {formatDuration(memo.durationSeconds)} -{' '}
				{memo.transcriptionStatus}
			</Text>
		</Pressable>
	);
}

function VideoMemoPreview({
	memo,
	onPress,
	styles,
}: {
	memo: Pick<DiaryVoiceMemo, 'id'> | Pick<DiaryPendingVoiceMemo, 'id'>;
	onPress: () => void;
	styles: ReturnType<typeof logStyles>;
}) {
	const player = useVideoPlayer(voiceMemoVideoUrl(memo.id), videoPlayer => {
		videoPlayer.muted = true;
		videoPlayer.loop = false;
	});

	return (
		<Pressable onPress={onPress} style={styles.videoThumbnail}>
			<VideoView
				player={player}
				nativeControls={false}
				contentFit='cover'
				style={styles.videoThumbnailPlayer}
			/>
			<View style={styles.videoPlayBadge}>
				<Text style={styles.videoPlayGlyph}>▶</Text>
			</View>
		</Pressable>
	);
}

function FullscreenVideoPlayer({
	video,
	onClose,
	styles,
}: {
	video: PlayableVideoMemo | null;
	onClose: () => void;
	styles: ReturnType<typeof logStyles>;
}) {
	const player = useVideoPlayer(video ? voiceMemoVideoUrl(video.id) : null, videoPlayer => {
		videoPlayer.loop = false;
		videoPlayer.muted = false;
		if (video) {
			videoPlayer.play();
		}
	});

	useEffect(() => {
		if (video) {
			player.play();
			return;
		}
		player.pause();
	}, [player, video]);

	return (
		<NativeModal visible={video !== null} animationType='fade' presentationStyle='fullScreen'>
			<View style={styles.fullscreenPlayerScreen}>
				<VideoView
					player={player}
					nativeControls
					contentFit='contain'
					allowsPictureInPicture
					style={styles.fullscreenPlayer}
				/>
				<View style={styles.videoTopBar}>
					<Pressable onPress={onClose} style={styles.videoCloseButton}>
						<Text style={styles.videoCloseText}>Close</Text>
					</Pressable>
				</View>
				{video ? (
					<View style={styles.fullscreenVideoTitle}>
						<Text style={styles.fullscreenVideoTitleText} numberOfLines={1}>
							{video.fileName}
						</Text>
					</View>
				) : null}
			</View>
		</NativeModal>
	);
}

function TagList({ tags }: { tags: Array<{ id: number; name: string; color: string }> }) {
	return <TagChips tags={tags} />;
}

function EntryDetailSheet({
	entry,
	visible,
	availableTags,
	isSettingTags,
	onClose,
	onSetTags,
	onDelete,
	onPlayVideo,
	styles,
}: {
	entry: DiaryEntry | null;
	visible: boolean;
	availableTags: TagRecord[];
	isSettingTags: boolean;
	onClose: () => void;
	onSetTags: (value: string) => void;
	onDelete: (entry: DiaryEntry) => void;
	onPlayVideo: (memo: DiaryVoiceMemo) => void;
	styles: ReturnType<typeof logStyles>;
}) {
	return (
		<BottomSheet visible={visible} title='Diary entry' onClose={onClose}>
			{entry ? (
				<View style={styles.stack}>
					<View style={styles.rowBetween}>
						<Text style={styles.cardTitle}>{formatDiaryTimestamp(entry.createdAt)}</Text>
						<Button size='small' onPress={() => onDelete(entry)}>
							Delete
						</Button>
					</View>
					<Pressable
						onPress={() => {
							void Linking.openURL(mapsUrl(entry.location));
						}}
					>
						<Text style={styles.linkText}>{formatLocationLabel(entry.location)}</Text>
					</Pressable>
					<TagSelector
						value={formatTagNames(entry.tags.map(tag => tag.name))}
						availableTags={availableTags}
						onChange={onSetTags}
						disabled={isSettingTags}
					/>
					{entry.summary ? (
						<View style={styles.stack}>
							<Text style={styles.sectionTitle}>Summary</Text>
							<MarkdownText value={entry.summary} style={styles.body} />
						</View>
					) : null}
					<View style={styles.stack}>
						<Text style={styles.sectionTitle}>Transcript</Text>
						<Text style={styles.body} numberOfLines={8}>
							{getEntryTranscriptText(entry) || entry.notes.trim() || 'No transcript'}
						</Text>
					</View>
					{entry.voiceMemos.length > 0 ? (
						<View style={styles.stack}>
							<Text style={styles.sectionTitle}>Voice memos</Text>
							{entry.voiceMemos.map(memo => (
								<VoiceMemoButton
									key={memo.id}
									memo={memo}
									onPlayVideo={onPlayVideo}
									styles={styles}
								/>
							))}
						</View>
					) : null}
				</View>
			) : null}
		</BottomSheet>
	);
}

function locationInputFromNative(location: Location.LocationObject): DiaryLocationInput {
	const { coords } = location;
	return {
		capturedAt: new Date(location.timestamp).toISOString(),
		latitude: coords.latitude,
		longitude: coords.longitude,
		accuracy: nullableCoordinate(coords.accuracy),
		altitude: nullableCoordinate(coords.altitude),
		altitudeAccuracy: nullableCoordinate(coords.altitudeAccuracy),
		heading: nullableCoordinate(coords.heading),
		speed: nullableCoordinate(coords.speed),
	};
}

function nullableCoordinate(value: number | null) {
	return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function logStyles(isDark: boolean) {
	const text = isDark ? '#f9fafb' : '#111827';
	const muted = isDark ? '#a1a1aa' : '#71717a';
	const bg = isDark ? '#0f172a' : '#f6f7f9';
	const border = isDark ? '#27272a' : '#e5e7eb';
	const surface = isDark ? '#111827' : '#fff';

	return {
		loadingScreen: {
			alignItems: 'center' as const,
			backgroundColor: bg,
			flex: 1,
			justifyContent: 'center' as const,
		},
		stack: {
			gap: 12,
		},
		listStack: {
			gap: 10,
		},
		rowBetween: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 10,
			justifyContent: 'space-between' as const,
		},
		actionRow: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 8,
			justifyContent: 'flex-end' as const,
		},
		videoProgress: {
			paddingTop: 2,
		},
		composerFooter: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 10,
			justifyContent: 'space-between' as const,
		},
		recordActionGroup: {
			alignItems: 'center' as const,
			flex: 1,
			flexDirection: 'row' as const,
			gap: 8,
			justifyContent: 'flex-start' as const,
		},
		notice: {
			backgroundColor: isDark ? '#102a43' : '#e6f4ff',
			borderColor: '#91caff',
			borderRadius: 8,
			borderWidth: 1,
			padding: 10,
		},
		noticeText: {
			color: text,
			fontSize: 14,
		},
		sectionTitle: {
			color: muted,
			fontSize: 13,
			fontWeight: '700' as const,
			textTransform: 'uppercase' as const,
		},
		cardTitle: {
			color: text,
			flex: 1,
			fontSize: 14,
			fontWeight: '700' as const,
		},
		body: {
			color: text,
			fontSize: 14,
			lineHeight: 20,
		},
		bodyPreview: {
			color: muted,
			fontSize: 14,
			lineHeight: 20,
		},
		summaryText: {
			color: text,
			fontSize: 15,
			fontWeight: '500' as const,
			lineHeight: 21,
		},
		muted: {
			color: muted,
			fontSize: 12,
		},
		errorText: {
			color: '#cf1322',
			fontSize: 13,
			lineHeight: 19,
		},
		linkText: {
			color: '#1677ff',
			fontSize: 13,
			fontWeight: '700' as const,
		},
		recordingText: {
			color: '#cf1322',
			flex: 1,
			fontSize: 13,
			fontWeight: '700' as const,
		},
		totalsRow: {
			flexDirection: 'row' as const,
			gap: 8,
		},
		totalCard: {
			alignItems: 'center' as const,
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 12,
			borderWidth: 1,
			flex: 1,
			paddingHorizontal: 10,
			paddingVertical: 9,
		},
		totalCardActive: {
			backgroundColor: '#1677ff',
			borderColor: '#1677ff',
		},
		totalValue: {
			color: text,
			fontSize: 18,
			fontWeight: '800' as const,
		},
		totalValueActive: {
			color: '#fff',
		},
		totalLabelActive: {
			color: '#fff',
			fontSize: 12,
			fontWeight: '800' as const,
		},
		entryCard: {
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 14,
			borderWidth: 1,
			gap: 8,
			padding: 12,
		},
		videoMemoCard: {
			alignItems: 'stretch' as const,
			flexDirection: 'row' as const,
			gap: 0,
			minHeight: 154,
			overflow: 'hidden' as const,
			padding: 0,
		},
		videoMemoContent: {
			flex: 1,
			gap: 8,
		},
		videoMemoContentPressable: {
			flex: 1,
			padding: 12,
		},
		videoThumbnail: {
			alignItems: 'center' as const,
			alignSelf: 'stretch' as const,
			backgroundColor: '#050505',
			justifyContent: 'center' as const,
			minHeight: 154,
			overflow: 'hidden' as const,
			width: 112,
		},
		audioThumbnail: {
			alignItems: 'center' as const,
			alignSelf: 'stretch' as const,
			backgroundColor: isDark ? '#1f2937' : '#eef2ff',
			gap: 8,
			justifyContent: 'center' as const,
			minHeight: 154,
			width: 112,
		},
		audioThumbnailIcon: {
			color: isDark ? '#bfdbfe' : '#1d4ed8',
			fontSize: 15,
			fontWeight: '900' as const,
		},
		audioThumbnailText: {
			color: isDark ? '#dbeafe' : '#1e3a8a',
			fontSize: 12,
			fontWeight: '700' as const,
		},
		videoThumbnailPlayer: {
			bottom: 0,
			left: 0,
			position: 'absolute' as const,
			right: 0,
			top: 0,
		},
		videoPlayBadge: {
			alignItems: 'center' as const,
			backgroundColor: 'rgba(0, 0, 0, 0.48)',
			borderColor: 'rgba(255, 255, 255, 0.62)',
			borderRadius: 999,
			borderWidth: 1,
			height: 38,
			justifyContent: 'center' as const,
			width: 38,
		},
		videoPlayGlyph: {
			color: '#fff',
			fontSize: 18,
			fontWeight: '900' as const,
			paddingLeft: 3,
		},
		entryMetaRow: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			flexWrap: 'wrap' as const,
			gap: 8,
		},
		metaPill: {
			backgroundColor: isDark ? '#1f2937' : '#eef6ff',
			borderColor: isDark ? '#334155' : '#bfdbfe',
			borderRadius: 999,
			borderWidth: 1,
			color: '#1677ff',
			fontSize: 11,
			fontWeight: '800' as const,
			paddingHorizontal: 8,
			paddingVertical: 4,
		},
		input: {
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 8,
			borderWidth: 1,
			color: text,
			fontSize: 15,
			paddingHorizontal: 10,
			paddingVertical: 8,
		},
		composerInput: {
			minHeight: 104,
			textAlignVertical: 'top' as const,
		},
		placeholder: {
			color: muted,
		},
		voiceMemoPill: {
			backgroundColor: isDark ? '#1f2937' : '#eef6ff',
			borderColor: isDark ? '#334155' : '#bfdbfe',
			borderRadius: 999,
			borderWidth: 1,
			paddingHorizontal: 10,
			paddingVertical: 6,
		},
		voiceMemoText: {
			color: '#1677ff',
			fontSize: 12,
			fontWeight: '700' as const,
		},
		videoScreen: {
			backgroundColor: '#000',
			flex: 1,
		},
		fullscreenPlayerScreen: {
			backgroundColor: '#000',
			flex: 1,
		},
		fullscreenPlayer: {
			flex: 1,
		},
		fullscreenVideoTitle: {
			alignItems: 'center' as const,
			bottom: 34,
			left: 18,
			position: 'absolute' as const,
			right: 18,
		},
		fullscreenVideoTitleText: {
			color: '#fff',
			fontSize: 13,
			fontWeight: '700' as const,
			textShadowColor: 'rgba(0, 0, 0, 0.65)',
			textShadowOffset: { width: 0, height: 1 },
			textShadowRadius: 8,
		},
		cameraPreview: {
			flex: 1,
		},
		cameraStartingOverlay: {
			alignItems: 'center' as const,
			bottom: 0,
			justifyContent: 'center' as const,
			left: 0,
			position: 'absolute' as const,
			right: 0,
			top: 0,
		},
		cameraStartingText: {
			backgroundColor: 'rgba(0, 0, 0, 0.42)',
			borderColor: 'rgba(255, 255, 255, 0.26)',
			borderRadius: 999,
			borderWidth: 1,
			color: '#fff',
			fontSize: 14,
			fontWeight: '800' as const,
			overflow: 'hidden' as const,
			paddingHorizontal: 14,
			paddingVertical: 8,
		},
		videoTopBar: {
			left: 0,
			paddingHorizontal: 18,
			paddingTop: 58,
			position: 'absolute' as const,
			right: 0,
			top: 0,
		},
		videoCloseButton: {
			alignSelf: 'flex-start' as const,
			backgroundColor: 'rgba(0, 0, 0, 0.42)',
			borderColor: 'rgba(255, 255, 255, 0.32)',
			borderRadius: 999,
			borderWidth: 1,
			paddingHorizontal: 14,
			paddingVertical: 8,
		},
		videoCloseText: {
			color: '#fff',
			fontSize: 14,
			fontWeight: '800' as const,
		},
		videoControls: {
			alignItems: 'center' as const,
			bottom: 36,
			gap: 12,
			left: 0,
			paddingHorizontal: 24,
			position: 'absolute' as const,
			right: 0,
		},
		recordButtonShell: {
			alignItems: 'center' as const,
			backgroundColor: 'rgba(255, 255, 255, 0.16)',
			borderColor: 'rgba(255, 255, 255, 0.42)',
			borderRadius: 999,
			borderWidth: 4,
			height: 90,
			justifyContent: 'center' as const,
			position: 'relative' as const,
			width: 90,
		},
		recordButtonShellDisabled: {
			opacity: 0.62,
		},
		recordPulseRing: {
			backgroundColor: 'rgba(239, 35, 60, 0.16)',
			borderColor: 'rgba(239, 35, 60, 0.78)',
			borderRadius: 999,
			borderWidth: 3,
			height: 90,
			position: 'absolute' as const,
			width: 90,
		},
		recordButton: {
			alignItems: 'center' as const,
			backgroundColor: '#ef233c',
			borderRadius: 999,
			height: 68,
			justifyContent: 'center' as const,
			width: 68,
		},
		recordButtonActive: {
			backgroundColor: '#d90429',
		},
		stopIcon: {
			backgroundColor: '#fff',
			borderRadius: 6,
			height: 28,
			width: 28,
		},
		videoStatus: {
			color: '#fff',
			fontSize: 13,
			fontWeight: '800' as const,
			textShadowColor: 'rgba(0, 0, 0, 0.6)',
			textShadowOffset: { width: 0, height: 1 },
			textShadowRadius: 6,
		},
	};
}
