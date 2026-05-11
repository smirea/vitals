import { ActivityIndicator, Button, Card, Modal, Tag } from '@ant-design/react-native';
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
import { useTRPC } from '@/src/api/trpc';
import {
	appendTagText,
	audioFileNameFromUri,
	audioMimeTypeFromUri,
	formatBytes,
	formatDiaryTimestamp,
	formatDuration,
	formatLocationLabel,
	formatRecorderDuration,
	getEntryPreview,
	getEntryTranscriptText,
	mapsUrl,
	parseTagText,
	voiceMemoAudioUrl,
	type DiaryEntry,
	type DiaryLocationInput,
	type DiaryPendingVoiceMemo,
	type DiaryPendingVoiceMemoRecovery,
	type DiaryVoiceMemo,
	type TagRecord,
} from '@/src/features/log/model';
import { pageStyles } from '@/src/theme/page-styles';

type TagDraftKey = `entry-${number}` | `voice-${number}`;

export function LogScreen() {
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const isDark = useColorScheme() === 'dark';
	const styles = logStyles(isDark);
	const sharedStyles = pageStyles(isDark);
	const audioRecorder = useAudioRecorder(RecordingPresets.HIGH_QUALITY);
	const recorderState = useAudioRecorderState(audioRecorder);
	const [notes, setNotes] = useState('');
	const [tagText, setTagText] = useState('');
	const [tagDrafts, setTagDrafts] = useState<Partial<Record<TagDraftKey, string>>>({});
	const [currentLocation, setCurrentLocation] = useState<DiaryLocationInput | null>(null);
	const [locationMessage, setLocationMessage] = useState('Requesting location...');
	const [notice, setNotice] = useState<string | null>(null);
	const [selectedEntry, setSelectedEntry] = useState<DiaryEntry | null>(null);
	const [isUploadingRecording, setIsUploadingRecording] = useState(false);
	const [composerOpen, setComposerOpen] = useState(false);

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
	const addEntryTagsMutation = useMutation({
		...trpc.diary.addEntryTags.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			setNotice('Tags added.');
		},
		onError: error => setNotice(error.message),
	});
	const addVoiceMemoTagsMutation = useMutation({
		...trpc.diary.addVoiceMemoTags.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			setNotice('Tags added.');
		},
		onError: error => setNotice(error.message),
	});

	useEffect(() => {
		void refreshLocation();
	}, []);

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

	function addTagsToEntry(entryId: number) {
		const key = `entry-${entryId}` as const;
		const names = parseTagText(tagDrafts[key] ?? '');
		if (names.length === 0) return;
		addEntryTagsMutation.mutate({ entryId, tagNames: names });
		setTagDrafts(previous => ({ ...previous, [key]: '' }));
	}

	function addTagsToVoiceMemo(voiceMemoId: number) {
		const key = `voice-${voiceMemoId}` as const;
		const names = parseTagText(tagDrafts[key] ?? '');
		if (names.length === 0) return;
		addVoiceMemoTagsMutation.mutate({ voiceMemoId, tagNames: names });
		setTagDrafts(previous => ({ ...previous, [key]: '' }));
	}

	function setTagDraft(key: TagDraftKey, value: string) {
		setTagDrafts(previous => ({ ...previous, [key]: value }));
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
					<TotalCard label='Entries' value={entries.length} styles={styles} />
					<TotalCard label='Pending' value={pendingVoiceMemos.length} styles={styles} />
					<TotalCard label='Recoveries' value={pendingVoiceMemoRecoveries.length} styles={styles} />
				</View>

				{pendingVoiceMemos.length > 0 ? (
					<View style={styles.stack}>
						<SectionHeader title='Pending memos' count={pendingVoiceMemos.length} styles={styles} />
						{pendingVoiceMemos.map(memo => (
							<PendingVoiceMemoCard
								key={memo.id}
								memo={memo}
								availableTags={availableTags}
								tagDraft={tagDrafts[`voice-${memo.id}`] ?? ''}
								isAddingTags={addVoiceMemoTagsMutation.isPending}
								isProcessing={processVoiceMemoMutation.isPending}
								isDeleting={deleteVoiceMemoMutation.isPending}
								onSetTagDraft={value => setTagDraft(`voice-${memo.id}`, value)}
								onAddTags={() => addTagsToVoiceMemo(memo.id)}
								onReprocess={() => processVoiceMemoMutation.mutate({ voiceMemoId: memo.id })}
								onDelete={() => deleteVoiceMemo(memo)}
								styles={styles}
							/>
						))}
					</View>
				) : null}

				{pendingVoiceMemoRecoveries.length > 0 ? (
					<View style={styles.stack}>
						<SectionHeader
							title='Recovery memos'
							count={pendingVoiceMemoRecoveries.length}
							styles={styles}
						/>
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

				<View style={styles.stack}>
					<SectionHeader title='Diary' count={entries.length} styles={styles} />
					{entries.map(entry => (
						<EntryCard
							key={entry.id}
							entry={entry}
							onOpen={() => setSelectedEntry(entry)}
							styles={styles}
						/>
					))}
				</View>
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
					<TextInput
						value={tagText}
						editable={!isBusy}
						placeholder='Tags'
						placeholderTextColor={styles.placeholder.color}
						style={styles.input}
						autoCapitalize='none'
						onChangeText={setTagText}
					/>
					{availableTags.length > 0 ? (
						<ScrollView horizontal showsHorizontalScrollIndicator={false}>
							<View style={styles.tagRow}>
								{availableTags.map(tag => (
									<Pressable
										key={tag.name}
										onPress={() => setTagText(previous => appendTagText(previous, tag.name))}
									>
										<Tag small>{tag.name}</Tag>
									</Pressable>
								))}
							</View>
						</ScrollView>
					) : null}
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
				tagDraft={selectedEntry ? (tagDrafts[`entry-${selectedEntry.id}`] ?? '') : ''}
				isAddingTags={addEntryTagsMutation.isPending}
				onClose={() => setSelectedEntry(null)}
				onSetTagDraft={value => {
					if (!selectedEntry) return;
					setTagDraft(`entry-${selectedEntry.id}`, value);
				}}
				onAddTags={() => {
					if (!selectedEntry) return;
					addTagsToEntry(selectedEntry.id);
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
	styles,
}: {
	label: string;
	value: number;
	styles: ReturnType<typeof logStyles>;
}) {
	return (
		<View style={styles.totalCard}>
			<Text style={styles.totalValue}>{value}</Text>
			<Text style={styles.muted}>{label}</Text>
		</View>
	);
}

function SectionHeader({
	title,
	count,
	styles,
}: {
	title: string;
	count: number;
	styles: ReturnType<typeof logStyles>;
}) {
	return (
		<View style={styles.rowBetween}>
			<Text style={styles.sectionTitle}>{title}</Text>
			<Text style={styles.muted}>{count} rows</Text>
		</View>
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
	const preview = getEntryPreview(entry);
	return (
		<Card full>
			<Card.Body>
				<View style={styles.stack}>
					<Pressable onPress={onOpen} style={styles.stack}>
						<View style={styles.rowBetween}>
							<Text style={styles.cardTitle}>{formatDiaryTimestamp(entry.createdAt)}</Text>
							<Text style={styles.muted}>#{entry.id}</Text>
						</View>
						<Pressable
							onPress={() => {
								void Linking.openURL(mapsUrl(entry.location));
							}}
						>
							<Text style={styles.linkText}>{formatLocationLabel(entry.location)}</Text>
						</Pressable>
						<TagList tags={entry.tags} styles={styles} />
						<Text style={styles.body} numberOfLines={4}>
							{preview}
						</Text>
						{entry.summary ? (
							<Text style={styles.muted} numberOfLines={3}>
								{entry.summary}
							</Text>
						) : null}
						{entry.voiceMemos.length > 0 ? (
							<View style={styles.voiceMemoRow}>
								{entry.voiceMemos.map(memo => (
									<VoiceMemoButton key={memo.id} memo={memo} styles={styles} />
								))}
							</View>
						) : null}
					</Pressable>
				</View>
			</Card.Body>
		</Card>
	);
}

function PendingVoiceMemoCard({
	memo,
	availableTags,
	tagDraft,
	isAddingTags,
	isProcessing,
	isDeleting,
	onSetTagDraft,
	onAddTags,
	onReprocess,
	onDelete,
	styles,
}: {
	memo: DiaryPendingVoiceMemo;
	availableTags: TagRecord[];
	tagDraft: string;
	isAddingTags: boolean;
	isProcessing: boolean;
	isDeleting: boolean;
	onSetTagDraft: (value: string) => void;
	onAddTags: () => void;
	onReprocess: () => void;
	onDelete: () => void;
	styles: ReturnType<typeof logStyles>;
}) {
	return (
		<Card full>
			<Card.Body>
				<View style={styles.stack}>
					<View style={styles.rowBetween}>
						<Text style={styles.cardTitle}>{formatDiaryTimestamp(memo.createdAt)}</Text>
						<Tag small>{memo.transcriptionStatus}</Tag>
					</View>
					<Text style={styles.muted}>
						{memo.fileName} - {formatDuration(memo.durationSeconds)} -{' '}
						{formatBytes(memo.audioBytes)}
					</Text>
					<Pressable
						onPress={() => {
							void Linking.openURL(voiceMemoAudioUrl(memo.id));
						}}
					>
						<Text style={styles.linkText}>Open audio</Text>
					</Pressable>
					<Text style={styles.body} numberOfLines={4}>
						{memo.transcript?.trim() || memo.notes.trim() || 'No transcript yet'}
					</Text>
					{memo.transcriptionError ? (
						<Text selectable style={styles.errorText} numberOfLines={4}>
							{memo.transcriptionError}
						</Text>
					) : null}
					<TagList tags={memo.tags} styles={styles} />
					<TagAddControl
						value={tagDraft}
						availableTags={availableTags}
						loading={isAddingTags}
						onChange={onSetTagDraft}
						onAdd={onAddTags}
						styles={styles}
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
			</Card.Body>
		</Card>
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
		<Card full>
			<Card.Body>
				<View style={styles.stack}>
					<View style={styles.rowBetween}>
						<Text style={styles.cardTitle}>{formatDiaryTimestamp(recovery.createdAt)}</Text>
						<Tag small>{recovery.status}</Tag>
					</View>
					<Text style={styles.muted}>
						{recovery.fileName} - {formatDuration(recovery.durationSeconds)} -{' '}
						{formatBytes(recovery.audioBytes)}
					</Text>
					<Text style={styles.body} numberOfLines={4}>
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
			</Card.Body>
		</Card>
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

function TagList({
	tags,
	styles,
}: {
	tags: Array<{ id: number; name: string; color: string }>;
	styles: ReturnType<typeof logStyles>;
}) {
	if (tags.length === 0) {
		return <Text style={styles.muted}>No tags</Text>;
	}

	return (
		<View style={styles.tagRow}>
			{tags.map(tag => (
				<Tag key={tag.id} small>
					{tag.name}
				</Tag>
			))}
		</View>
	);
}

function TagAddControl({
	value,
	availableTags,
	loading,
	onChange,
	onAdd,
	styles,
}: {
	value: string;
	availableTags: TagRecord[];
	loading: boolean;
	onChange: (value: string) => void;
	onAdd: () => void;
	styles: ReturnType<typeof logStyles>;
}) {
	const tagCount = parseTagText(value).length;
	return (
		<View style={styles.stack}>
			<View style={styles.inline}>
				<TextInput
					value={value}
					placeholder='Add tags'
					placeholderTextColor={styles.placeholder.color}
					style={[styles.input, styles.tagInput]}
					autoCapitalize='none'
					onChangeText={onChange}
				/>
				<Button size='small' onPress={onAdd} disabled={tagCount === 0} loading={loading}>
					Add
				</Button>
			</View>
			{availableTags.length > 0 ? (
				<ScrollView horizontal showsHorizontalScrollIndicator={false}>
					<View style={styles.tagRow}>
						{availableTags.map(tag => (
							<Pressable key={tag.name} onPress={() => onChange(appendTagText(value, tag.name))}>
								<Tag small>{tag.name}</Tag>
							</Pressable>
						))}
					</View>
				</ScrollView>
			) : null}
		</View>
	);
}

function EntryDetailSheet({
	entry,
	visible,
	availableTags,
	tagDraft,
	isAddingTags,
	onClose,
	onSetTagDraft,
	onAddTags,
	onDelete,
	styles,
}: {
	entry: DiaryEntry | null;
	visible: boolean;
	availableTags: TagRecord[];
	tagDraft: string;
	isAddingTags: boolean;
	onClose: () => void;
	onSetTagDraft: (value: string) => void;
	onAddTags: () => void;
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
					<TagList tags={entry.tags} styles={styles} />
					<TagAddControl
						value={tagDraft}
						availableTags={availableTags}
						loading={isAddingTags}
						onChange={onSetTagDraft}
						onAdd={onAddTags}
						styles={styles}
					/>
					<View style={styles.stack}>
						<Text style={styles.sectionTitle}>Transcript</Text>
						<Text style={styles.body} numberOfLines={8}>
							{getEntryTranscriptText(entry) || entry.notes.trim() || 'No transcript'}
						</Text>
					</View>
					{entry.summary ? (
						<View style={styles.stack}>
							<Text style={styles.sectionTitle}>Summary</Text>
							<Text style={styles.body} numberOfLines={5}>
								{entry.summary}
							</Text>
						</View>
					) : null}
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
		headerRow: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 12,
			justifyContent: 'space-between' as const,
		},
		stack: {
			gap: 12,
		},
		rowBetween: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 10,
			justifyContent: 'space-between' as const,
		},
		inline: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 8,
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
		title: {
			color: text,
			fontSize: 24,
			fontWeight: '800' as const,
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
			borderRadius: 8,
			borderWidth: 1,
			flex: 1,
			padding: 10,
		},
		totalValue: {
			color: text,
			fontSize: 18,
			fontWeight: '800' as const,
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
		tagInput: {
			flex: 1,
		},
		placeholder: {
			color: muted,
		},
		tagRow: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			flexWrap: 'wrap' as const,
			gap: 4,
		},
		voiceMemoRow: {
			flexDirection: 'row' as const,
			flexWrap: 'wrap' as const,
			gap: 6,
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
