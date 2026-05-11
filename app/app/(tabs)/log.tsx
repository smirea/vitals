import type { inferRouterOutputs } from '@trpc/server';
import type { AppRouter } from 'server/trpc/index.ts';
import { API_BASE_URL, useTRPC } from '@/src/api/trpc';
import { ActivityIndicator, Button, Modal, Tag } from '@ant-design/react-native';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import * as FileSystem from 'expo-file-system/legacy';
import * as Location from 'expo-location';
import {
	RecordingPresets,
	requestRecordingPermissionsAsync,
	setAudioModeAsync,
	useAudioRecorder,
	useAudioRecorderState,
} from 'expo-audio';
import { useEffect, useMemo, useState } from 'react';
import {
	Linking,
	Pressable,
	ScrollView,
	Text,
	TextInput,
	View,
	useColorScheme,
} from 'react-native';
import { BottomSheet, FloatingActionButton } from '@/src/components/mobile-ui';
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
	return `${API_BASE_URL}/diary/voice-memos/${voiceMemoId}/audio`;
}

function audioFileNameFromUri(uri: string) {
	const extension = audioExtensionFromUri(uri);
	return `diary-${new Date().toISOString().replace(/[:.]/g, '-')}.${extension}`;
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

function formatRecorderDuration(milliseconds: number) {
	return formatDuration(Math.max(milliseconds, 0) / 1000);
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

type LogFilter = 'entries' | 'pending' | 'recoveries';

export default function LogScreen() {
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const isDark = useColorScheme() === 'dark';
	const styles = logStyles(isDark);
	const sharedStyles = pageStyles(isDark);
	const audioRecorder = useAudioRecorder(RecordingPresets.HIGH_QUALITY);
	const recorderState = useAudioRecorderState(audioRecorder);
	const [notes, setNotes] = useState('');
	const [tagText, setTagText] = useState('');
	const [currentLocation, setCurrentLocation] = useState<DiaryLocationInput | null>(null);
	const [locationMessage, setLocationMessage] = useState('Requesting location...');
	const [notice, setNotice] = useState<string | null>(null);
	const [selectedEntry, setSelectedEntry] = useState<DiaryEntry | null>(null);
	const [isUploadingRecording, setIsUploadingRecording] = useState(false);
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
				{ key: 'recoveries', label: 'Recoveries', value: pendingVoiceMemoRecoveries.length },
			].filter(option => option.key === 'entries' || option.value > 0) as Array<{
				key: LogFilter;
				label: string;
				value: number;
			}>,
		[entries.length, pendingVoiceMemoRecoveries.length, pendingVoiceMemos.length],
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
	const uploadVoiceMemoMutation = useMutation({
		...trpc.diary.uploadVoiceMemo.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			setNotes('');
			setTagText('');
			setComposerOpen(false);
			setNotice('Voice memo saved.');
		},
		onError: error => setNotice(error.message),
	});
	const processVoiceMemoMutation = useMutation({
		...trpc.diary.processVoiceMemo.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			setNotice('Voice memo reprocessed.');
		},
		onError: error => setNotice(error.message),
	});
	const processRecoveryMutation = useMutation({
		...trpc.diary.processVoiceMemoRecovery.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			setNotice('Recovery memo reprocessed.');
		},
		onError: error => setNotice(error.message),
	});
	const deleteVoiceMemoMutation = useMutation({
		...trpc.diary.deleteVoiceMemo.mutationOptions(),
		onSuccess: async () => {
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
		if (!filterOptions.some(option => option.key === activeFilter)) setActiveFilter('entries');
	}, [activeFilter, filterOptions]);

	const isBusy =
		createEntryMutation.isPending ||
		uploadVoiceMemoMutation.isPending ||
		isUploadingRecording ||
		recorderState.isRecording;
	const canCreateEntry = notes.trim().length > 0 && currentLocation !== null && !isBusy;
	const canRecord =
		currentLocation !== null && !createEntryMutation.isPending && !isUploadingRecording;
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

			const dataBase64 = await FileSystem.readAsStringAsync(uri, {
				encoding: FileSystem.EncodingType.Base64,
			});
			await uploadVoiceMemoMutation.mutateAsync({
				notes,
				transcript: '',
				fileName: audioFileNameFromUri(uri),
				mimeType: audioMimeTypeFromUri(uri),
				dataBase64,
				durationSeconds,
				tagNames,
				location: getRequiredLocation(),
			});
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
								styles={styles}
							/>
						))}
					</View>
				) : null}

				{activeFilter === 'recoveries' ? (
					<View style={styles.listStack}>
						{pendingVoiceMemoRecoveries.map(recovery => (
							<RecoveryMemoCard
								key={recovery.id}
								recovery={recovery}
								isProcessing={processRecoveryMutation.isPending}
								onReprocess={() => processRecoveryMutation.mutate({ recoveryId: recovery.id })}
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
					<View style={styles.actionRow}>
						<Button size='small' onPress={refreshLocation}>
							Locate
						</Button>
						<Button
							size='small'
							onPress={toggleRecording}
							disabled={!canRecord}
							loading={isUploadingRecording || uploadVoiceMemoMutation.isPending}
						>
							{recorderState.isRecording ? 'Stop' : 'Record'}
						</Button>
						<Button
							type='primary'
							size='small'
							onPress={() => void createEntry()}
							disabled={!canCreateEntry}
							loading={createEntryMutation.isPending}
						>
							Add
						</Button>
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
				</View>
			</BottomSheet>

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
	styles,
}: {
	entry: DiaryEntry;
	onOpen: () => void;
	styles: ReturnType<typeof logStyles>;
}) {
	const transcript = getEntryTranscriptText(entry);
	const summary = entry.summary?.trim() ?? '';
	return (
		<Pressable onPress={onOpen} style={styles.entryCard}>
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
	styles: ReturnType<typeof logStyles>;
}) {
	return (
		<View style={styles.entryCard}>
			<View style={styles.stack}>
				<View style={styles.rowBetween}>
					<Text style={styles.cardTitle}>{formatDiaryTimestamp(memo.createdAt)}</Text>
					<Tag small>{memo.transcriptionStatus}</Tag>
				</View>
				<Text style={styles.muted}>
					{memo.fileName} - {formatDuration(memo.durationSeconds)} - {formatBytes(memo.audioBytes)}
				</Text>
				<Pressable
					onPress={() => {
						void Linking.openURL(voiceMemoAudioUrl(memo.id));
					}}
				>
					<Text style={styles.linkText}>Open audio</Text>
				</Pressable>
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
		</View>
	);
}

function RecoveryMemoCard({
	recovery,
	isProcessing,
	onReprocess,
	styles,
}: {
	recovery: DiaryPendingVoiceMemoRecovery;
	isProcessing: boolean;
	onReprocess: () => void;
	styles: ReturnType<typeof logStyles>;
}) {
	return (
		<View style={styles.entryCard}>
			<View style={styles.stack}>
				<View style={styles.rowBetween}>
					<Text style={styles.cardTitle}>{formatDiaryTimestamp(recovery.createdAt)}</Text>
					<Tag small>{recovery.status}</Tag>
				</View>
				<Text style={styles.muted}>
					{recovery.fileName} - {formatDuration(recovery.durationSeconds)} -{' '}
					{formatBytes(recovery.audioBytes)}
				</Text>
				<Text style={styles.bodyPreview} numberOfLines={4}>
					{recovery.transcript?.trim() || 'No transcript yet'}
				</Text>
				{recovery.error ? (
					<Text selectable style={styles.errorText} numberOfLines={4}>
						{recovery.error}
					</Text>
				) : null}
				<Button size='small' onPress={onReprocess} loading={isProcessing}>
					Reprocess
				</Button>
			</View>
		</View>
	);
}

function VoiceMemoButton({
	memo,
	styles,
}: {
	memo: DiaryVoiceMemo;
	styles: ReturnType<typeof logStyles>;
}) {
	return (
		<Pressable
			onPress={() => {
				void Linking.openURL(voiceMemoAudioUrl(memo.id));
			}}
			style={styles.voiceMemoPill}
		>
			<Text style={styles.voiceMemoText}>
				{formatDuration(memo.durationSeconds)} - {memo.transcriptionStatus}
			</Text>
		</Pressable>
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
	styles,
}: {
	entry: DiaryEntry | null;
	visible: boolean;
	availableTags: TagRecord[];
	isSettingTags: boolean;
	onClose: () => void;
	onSetTags: (value: string) => void;
	onDelete: (entry: DiaryEntry) => void;
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
								<VoiceMemoButton key={memo.id} memo={memo} styles={styles} />
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
	};
}
