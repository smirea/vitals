import type { inferRouterOutputs } from '@trpc/server';
import type { AppRouter } from 'server/trpc/index.ts';
import { withNativeAuthToken } from '@/src/api/auth';
import { API_BASE_URL, useTRPC } from '@/src/api/trpc';
import { ActivityIndicator, Modal, Steps, Tag, Toast } from '@ant-design/react-native';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { CameraView, useCameraPermissions } from 'expo-camera';
import * as FileSystem from 'expo-file-system/legacy';
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
type LocalVideoDraftStatus = 'local_saved' | 'uploading' | 'processing' | 'server_saved' | 'failed';
type LocalVideoDraft = {
	id: string;
	createdAt: string;
	updatedAt: string;
	status: LocalVideoDraftStatus;
	error: string | null;
	notes: string;
	tagNames: string[];
	location: DiaryLocationInput;
	durationSeconds: number;
	videoUri: string;
	videoFileName: string;
	videoMimeType: string;
	videoBytes: number;
	serverRecoveryId: string | null;
	serverVoiceMemoId: number | null;
};
type VideoRecordingSession = {
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

function videoProgressFromLocalDraft(draft: LocalVideoDraft) {
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

function localVideoLogsDirectory() {
	return `${requireDocumentDirectory()}video-logs/`;
}

function localVideoDraftManifestPath() {
	return `${localVideoLogsDirectory()}drafts.json`;
}

async function readLocalVideoDrafts() {
	const manifestPath = localVideoDraftManifestPath();
	const info = await FileSystem.getInfoAsync(manifestPath);
	if (!info.exists) {
		return [] satisfies LocalVideoDraft[];
	}

	const text = await FileSystem.readAsStringAsync(manifestPath);
	const parsed = JSON.parse(text) as unknown;
	if (!Array.isArray(parsed)) {
		throw new Error('Local video draft manifest is not an array.');
	}

	return parsed as LocalVideoDraft[];
}

async function writeLocalVideoDrafts(drafts: LocalVideoDraft[]) {
	const directory = localVideoLogsDirectory();
	await ensureDirectory(directory);
	await FileSystem.writeAsStringAsync(
		localVideoDraftManifestPath(),
		JSON.stringify(drafts, null, 2),
	);
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

export default function LogScreen() {
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const isDark = useColorScheme() === 'dark';
	const styles = logStyles(isDark);
	const sharedStyles = pageStyles(isDark);
	const audioRecorder = useExpoAudioRecorder(RecordingPresets.HIGH_QUALITY);
	const recorderState = useAudioRecorderState(audioRecorder);
	const cameraRef = useRef<CameraView>(null);
	const videoRecordingPromiseRef = useRef<Promise<{ uri: string } | undefined> | null>(null);
	const videoRecordingSessionRef = useRef<VideoRecordingSession | null>(null);
	const localVideoDraftsRef = useRef<LocalVideoDraft[]>([]);
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
	const [localVideoDrafts, setLocalVideoDrafts] = useState<LocalVideoDraft[]>([]);
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
	const filterOptions = useMemo(
		() =>
			[
				{ key: 'entries', label: 'Entries', value: entries.length },
				{ key: 'pending', label: 'Pending', value: pendingVoiceMemos.length },
				{
					key: 'recoveries',
					label: 'Recoveries',
					value:
						pendingVoiceMemoRecoveries.length + localVideoDrafts.length + (isVideoSaving ? 1 : 0),
				},
			].filter(option => option.key === 'entries' || option.value > 0) as Array<{
				key: LogFilter;
				label: string;
				value: number;
			}>,
		[
			entries.length,
			isVideoSaving,
			localVideoDrafts.length,
			pendingVoiceMemoRecoveries.length,
			pendingVoiceMemos.length,
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
		void loadLocalVideoDrafts();
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
	const canCreateEntry = notes.trim().length > 0 && currentLocation !== null && !isBusy;
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
		!recorderState.isRecording;
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
		if (!currentLocation) {
			throw new Error(locationMessage || 'Location is not ready.');
		}

		return currentLocation;
	}

	async function loadLocalVideoDrafts() {
		try {
			const drafts = (await readLocalVideoDrafts()).sort((left, right) =>
				right.createdAt.localeCompare(left.createdAt),
			);
			localVideoDraftsRef.current = drafts;
			setLocalVideoDrafts(drafts);
		} catch (error) {
			setNotice(error instanceof Error ? error.message : String(error));
		}
	}

	async function replaceLocalVideoDrafts(
		updater: (drafts: LocalVideoDraft[]) => LocalVideoDraft[],
	) {
		const drafts = updater(localVideoDraftsRef.current).sort((left, right) =>
			right.createdAt.localeCompare(left.createdAt),
		);
		localVideoDraftsRef.current = drafts;
		setLocalVideoDrafts(drafts);
		await writeLocalVideoDrafts(drafts);
	}

	async function saveLocalVideoDraft(draft: LocalVideoDraft) {
		await replaceLocalVideoDrafts(drafts => [
			draft,
			...drafts.filter(existingDraft => existingDraft.id !== draft.id),
		]);
	}

	async function updateLocalVideoDraft(
		draftId: string,
		updates: Partial<Omit<LocalVideoDraft, 'id' | 'createdAt'>>,
	) {
		await replaceLocalVideoDrafts(drafts =>
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

	async function removeLocalVideoDraft(draftId: string) {
		await replaceLocalVideoDrafts(drafts => drafts.filter(draft => draft.id !== draftId));
	}

	async function discardLocalVideoDraft(draft: Pick<LocalVideoDraft, 'id' | 'videoUri'>) {
		await FileSystem.deleteAsync(draft.videoUri, { idempotent: true });
		await removeLocalVideoDraft(draft.id);
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
			await stopRecording();
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
			audioRecorder.record();
			setNotice('Recording...');
		} catch (error) {
			setNotice(error instanceof Error ? error.message : String(error));
		}
	}

	async function stopRecording() {
		const durationSeconds = Math.max(recorderState.durationMillis / 1000, 0.1);
		try {
			setIsUploadingRecording(true);
			await audioRecorder.stop();
			const uri = audioRecorder.uri ?? recorderState.url;
			if (!uri) {
				throw new Error('Recording finished without a file URI.');
			}

			const fileName = audioFileNameFromUri(uri);
			const draft = await startVoiceMemoDraftMutation.mutateAsync({
				notes,
				transcript: '',
				fileName,
				mimeType: audioMimeTypeFromUri(uri),
				tagNames,
				location: getRequiredLocation(),
			});
			await uploadFileToVoiceMemoDraft(draft.recoveryId, 'audio', uri);
			const saved = await finishVoiceMemoDraftMutation.mutateAsync({
				recoveryId: draft.recoveryId,
				transcript: '',
				durationSeconds,
			});
			await processVoiceMemoMutation.mutateAsync({ voiceMemoId: saved.voiceMemoId });
			setNotes('');
			setTagText('');
			setComposerOpen(false);
			setNotice(`Voice memo saved: ${fileName}`);
		} catch (error) {
			setNotice(error instanceof Error ? error.message : String(error));
		} finally {
			setIsUploadingRecording(false);
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
			setVideoElapsedMs(0);
			setVideoStartedAt(Date.now());
			setIsVideoRecording(true);
			setIsVideoStarting(false);
			videoRecordingPromiseRef.current = cameraRef.current.recordAsync({
				maxDuration: 10 * 60,
			});
		} catch (error) {
			setIsVideoRecording(false);
			setIsVideoStarting(false);
			videoRecordingPromiseRef.current = null;
			videoRecordingSessionRef.current = null;
			const message = error instanceof Error ? error.message : String(error);
			setNotice(message);
		}
	}

	async function stopVideoRecording() {
		const durationSeconds = Math.max((Date.now() - (videoStartedAt ?? Date.now())) / 1000, 0.1);
		let savedDraft: LocalVideoDraft | null = null;
		try {
			setIsVideoSaving(true);
			const session = videoRecordingSessionRef.current;
			if (!session) {
				throw new Error('Video recording session is missing.');
			}
			cameraRef.current?.stopRecording();
			setIsVideoRecording(false);
			setVideoRecorderOpen(false);
			setVideoCameraMounted(false);
			setIsVideoCameraReady(false);
			setComposerOpen(true);
			setActiveFilter('recoveries');
			const video = await videoRecordingPromiseRef.current;
			if (!video?.uri) {
				throw new Error('Video recording finished without a file URI.');
			}

			const savedVideo = await persistVideoLog(video.uri);
			const savedVideoUri = savedVideo.uri;
			const videoFileName = savedVideo.fileName;
			const videoBytes = await getFileSize(savedVideoUri);
			savedDraft = {
				id: session.localId,
				createdAt: session.startedAt,
				updatedAt: new Date().toISOString(),
				status: 'uploading',
				error: null,
				notes,
				tagNames,
				location: getRequiredLocation(),
				durationSeconds,
				videoUri: savedVideoUri,
				videoFileName,
				videoMimeType: videoMimeTypeFromUri(savedVideoUri),
				videoBytes,
				serverRecoveryId: null,
				serverVoiceMemoId: null,
			};
			await saveLocalVideoDraft(savedDraft);
			setActiveFilter('recoveries');
			await uploadLocalVideoDraft(savedDraft);
			setNotes('');
			setTagText('');
			setComposerOpen(false);
			Toast.success('Video processed.', 2, undefined, false);
		} catch (error) {
			if (savedDraft) {
				await updateLocalVideoDraft(savedDraft.id, {
					status: 'failed',
					error: error instanceof Error ? error.message : String(error),
				});
			}
			setNotice(error instanceof Error ? error.message : String(error));
		} finally {
			setIsVideoRecording(false);
			setIsVideoSaving(false);
			setVideoStartedAt(null);
			setVideoElapsedMs(0);
			videoRecordingPromiseRef.current = null;
			videoRecordingSessionRef.current = null;
		}
	}

	async function uploadLocalVideoDraft(draft: LocalVideoDraft) {
		await updateLocalVideoDraft(draft.id, {
			status: 'uploading',
			error: null,
		});

		try {
			if (draft.serverVoiceMemoId) {
				await updateLocalVideoDraft(draft.id, {
					status: 'processing',
				});
				await processVoiceMemoMutation.mutateAsync({
					voiceMemoId: draft.serverVoiceMemoId,
				});
				await discardLocalVideoDraft(draft);
				return;
			}

			let recoveryId = draft.serverRecoveryId;
			if (recoveryId) {
				await resetVoiceMemoDraftMutation.mutateAsync({ recoveryId });
				await setVoiceMemoDraftVideoMutation.mutateAsync({
					recoveryId,
					videoFileName: draft.videoFileName,
					videoMimeType: draft.videoMimeType,
				});
			} else {
				const serverDraft = await startVoiceMemoDraftMutation.mutateAsync({
					mediaKind: 'video',
					notes: draft.notes,
					transcript: '',
					fileName: videoAudioFileNameFromVideoFileName(draft.videoFileName),
					mimeType: 'audio/mp4',
					videoFileName: draft.videoFileName,
					videoMimeType: draft.videoMimeType,
					tagNames: draft.tagNames,
					location: draft.location,
				});
				recoveryId = serverDraft.recoveryId;
				await updateLocalVideoDraft(draft.id, {
					serverRecoveryId: recoveryId,
				});
			}

			await uploadFileToVoiceMemoDraft(recoveryId, 'video', draft.videoUri);
			const saved = await finishVoiceMemoDraftMutation.mutateAsync({
				recoveryId,
				durationSeconds: draft.durationSeconds,
			});
			await updateLocalVideoDraft(draft.id, {
				status: 'processing',
				serverVoiceMemoId: saved.voiceMemoId,
				error: null,
			});
			await processVoiceMemoMutation.mutateAsync({
				voiceMemoId: saved.voiceMemoId,
			});
			await discardLocalVideoDraft(draft);
			await invalidateDiary();
		} catch (error) {
			await updateLocalVideoDraft(draft.id, {
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

	async function persistVideoLog(uri: string) {
		const directory = localVideoLogsDirectory();
		await ensureDirectory(directory);
		const fileName = videoFileNameFromUri(uri);
		const destination = `${directory}${fileName}`;
		await FileSystem.copyAsync({ from: uri, to: destination });
		return {
			uri: destination,
			fileName,
		};
	}

	async function deleteLocalVideoDraftFiles(draft: LocalVideoDraft) {
		if (draft.serverVoiceMemoId) {
			await deleteVoiceMemoMutation.mutateAsync({ voiceMemoId: draft.serverVoiceMemoId });
		} else if (draft.serverRecoveryId) {
			await deleteRecoveryMutation.mutateAsync({ recoveryId: draft.serverRecoveryId });
		}
		await discardLocalVideoDraft(draft);
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

	function reprocessLocalVideoDraft(draft: LocalVideoDraft) {
		setIsVideoSaving(true);
		void uploadLocalVideoDraft(draft)
			.then(() => {
				setComposerOpen(false);
				Toast.success('Video processed.', 2, undefined, false);
			})
			.catch(error => {
				setNotice(error instanceof Error ? error.message : String(error));
			})
			.finally(() => {
				setIsVideoSaving(false);
			});
	}

	function deleteLocalVideoDraft(draft: LocalVideoDraft) {
		Modal.alert('Delete local video recording?', draft.videoFileName, [
			{ text: 'Cancel' },
			{
				text: 'Delete',
				onPress: () => {
					void deleteLocalVideoDraftFiles(draft)
						.then(() => setNotice('Local video recording deleted.'))
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
						{isVideoSaving && localVideoDrafts.length === 0 ? (
							<VideoProcessingCard styles={styles} />
						) : null}
						{localVideoDrafts.map(draft => (
							<LocalVideoDraftCard
								key={draft.id}
								draft={draft}
								isProcessing={isVideoSaving}
								onReprocess={() => reprocessLocalVideoDraft(draft)}
								onDelete={() => deleteLocalVideoDraft(draft)}
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
								label='Video log'
								disabled={!canRecordVideo}
								loading={isVideoSaving || isOpeningVideoRecorder}
								intent='video'
								size='small'
								onPress={() => void openVideoRecorder()}
							/>
							<Button
								icon={recorderState.isRecording ? 'stop.fill' : 'mic.fill'}
								label={recorderState.isRecording ? 'Stop' : 'Audio Log'}
								active={recorderState.isRecording}
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
					{pendingVoiceMemos.length > 0 ||
					pendingVoiceMemoRecoveries.length > 0 ||
					localVideoDrafts.length > 0 ||
					isVideoSaving ? (
						<View style={styles.stack}>
							<Text style={styles.sectionTitle}>Unprocessed recordings</Text>
							{isVideoSaving && localVideoDrafts.length === 0 ? (
								<VideoProcessingCard styles={styles} />
							) : null}
							{localVideoDrafts.slice(0, 2).map(draft => (
								<LocalVideoDraftCard
									key={draft.id}
									draft={draft}
									isProcessing={isVideoSaving}
									onReprocess={() => reprocessLocalVideoDraft(draft)}
									onDelete={() => deleteLocalVideoDraft(draft)}
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

function LocalVideoDraftCard({
	draft,
	isProcessing,
	onReprocess,
	onDelete,
	styles,
}: {
	draft: LocalVideoDraft;
	isProcessing: boolean;
	onReprocess: () => void;
	onDelete: () => void;
	styles: ReturnType<typeof logStyles>;
}) {
	return (
		<View style={[styles.entryCard, styles.videoMemoCard]}>
			<LocalVideoPreview uri={draft.videoUri} styles={styles} />
			<View style={styles.videoMemoContentPressable}>
				<View style={styles.videoMemoContent}>
					<View style={styles.rowBetween}>
						<Text style={styles.cardTitle}>{formatDiaryTimestamp(draft.createdAt)}</Text>
					</View>
					<Text style={styles.muted}>
						{draft.videoFileName} - {formatDuration(draft.durationSeconds)} -{' '}
						{formatBytes(draft.videoBytes)}
					</Text>
					<VideoProgressSteps progress={videoProgressFromLocalDraft(draft)} styles={styles} />
					<Text style={styles.bodyPreview} numberOfLines={3}>
						{draft.notes.trim() || 'Local video waiting to process'}
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
