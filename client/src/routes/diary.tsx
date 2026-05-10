import {
	ArrowClockwise as ReloadOutlined,
	Microphone as AudioOutlined,
	Plus as PlusOutlined,
	Stop as StopOutlined,
	Trash as DeleteOutlined,
} from '@phosphor-icons/react';
import { toast } from '@tamagui/toast/v2';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import type { Key, ReactNode } from 'react';
import { useEffect, useMemo, useRef, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { Anchor, Button, Card, Paragraph, Text, XStack, YStack, useTheme } from 'tamagui';

import type {
	DiaryEntry,
	DiaryPendingVoiceMemo,
	DiaryPendingVoiceMemoRecovery,
	DiaryVoiceMemo,
} from '../utils/api';
import { AutoResizeTextArea } from '../components/AutoResizeTextArea';
import { DataTable, type DataColumn } from '../components/DataTable';
import { PageNav } from '../components/PageNav';
import { TagChip } from '../components/TagChip';
import { TagInput } from '../components/TagInput';
import { useTRPC } from '../utils/trpc';

export const Route = createFileRoute('/diary')({
	component: DiaryRouteComponent,
});

type LocationInput = {
	capturedAt: string;
	latitude: number;
	longitude: number;
	accuracy: number | null;
	altitude: number | null;
	altitudeAccuracy: number | null;
	heading: number | null;
	speed: number | null;
};

type StreamingTranscriptEvent =
	| { type: 'transcript.created' }
	| {
			type: 'transcript.partial';
			text?: string;
			is_final?: boolean;
			speech_final?: boolean;
	  }
	| {
			type: 'transcript.done';
			text?: string;
			duration?: number;
	  }
	| {
			type: 'error';
			message?: string;
	  };

type StreamingTranscriptDeferred = {
	promise: Promise<string>;
	resolve: (transcript: string) => void;
	reject: (error: Error) => void;
	isSettled: boolean;
};

type DiaryErrorDetails = {
	message: string;
	details: string;
};

function DiaryRouteComponent() {
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const [notes, setNotes] = useState('');
	const [tagNames, setTagNames] = useState<string[]>([]);
	const [currentLocation, setCurrentLocation] = useState<LocationInput | null>(null);
	const [locationError, setLocationError] = useState<string | null>(null);
	const [isRecording, setIsRecording] = useState(false);
	const [isStoppingRecording, setIsStoppingRecording] = useState(false);
	const [isParsingVoiceMemo, setIsParsingVoiceMemo] = useState(false);
	const [expandedRowKeys, setExpandedRowKeys] = useState<Key[]>([]);
	const [deletingEntryId, setDeletingEntryId] = useState<number | null>(null);
	const [reprocessingVoiceMemoId, setReprocessingVoiceMemoId] = useState<number | null>(null);
	const [reprocessingRecoveryId, setReprocessingRecoveryId] = useState<string | null>(null);
	const [deletingVoiceMemoId, setDeletingVoiceMemoId] = useState<number | null>(null);
	const [addingTagsEntryId, setAddingTagsEntryId] = useState<number | null>(null);
	const [addingTagsVoiceMemoId, setAddingTagsVoiceMemoId] = useState<number | null>(null);
	const [errorDetails, setErrorDetails] = useState<DiaryErrorDetails | null>(null);
	const mediaRecorderRef = useRef<MediaRecorder | null>(null);
	const mediaStreamRef = useRef<MediaStream | null>(null);
	const sttWebSocketRef = useRef<WebSocket | null>(null);
	const audioContextRef = useRef<AudioContext | null>(null);
	const audioSourceRef = useRef<MediaStreamAudioSourceNode | null>(null);
	const audioProcessorRef = useRef<ScriptProcessorNode | null>(null);
	const recordingRecoveryIdRef = useRef<string | null>(null);
	const recordingChunkUploadRef = useRef<Promise<void>>(Promise.resolve());
	const recordingStartedAtRef = useRef<number | null>(null);
	const finalizedTranscriptRef = useRef('');
	const interimTranscriptRef = useRef('');
	const transcriptDoneRef = useRef<StreamingTranscriptDeferred | null>(null);

	const entriesQuery = useQuery(trpc.diary.list.queryOptions());
	const pendingVoiceMemosQuery = useQuery(trpc.diary.listPendingVoiceMemos.queryOptions());
	const pendingVoiceMemoRecoveriesQuery = useQuery(
		trpc.diary.listPendingVoiceMemoRecoveries.queryOptions(),
	);
	const tagsQuery = useQuery(trpc.tags.list.queryOptions());

	const createEntryMutation = useMutation({
		...trpc.diary.createEntry.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			setNotes('');
			setTagNames([]);
			toast.success('Entry added.');
		},
		onError: error => {
			showError(error);
		},
		onSettled: () => {
			void invalidateDiary();
		},
	});

	const saveVoiceMemoMutation = useMutation({
		...trpc.diary.saveVoiceMemo.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
		},
		onSettled: () => {
			void invalidateDiary();
		},
	});

	const startVoiceMemoDraftMutation = useMutation({
		...trpc.diary.startVoiceMemoDraft.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
		},
		onSettled: () => {
			void invalidateDiary();
		},
	});

	const appendVoiceMemoDraftMutation = useMutation({
		...trpc.diary.appendVoiceMemoDraft.mutationOptions(),
	});

	const finishVoiceMemoDraftMutation = useMutation({
		...trpc.diary.finishVoiceMemoDraft.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
		},
		onSettled: () => {
			void invalidateDiary();
		},
	});

	const processVoiceMemoMutation = useMutation({
		...trpc.diary.processVoiceMemo.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			setNotes('');
			setTagNames([]);
			toast.success('Voice memo saved.');
		},
		onError: showError,
		onSettled: () => {
			setReprocessingVoiceMemoId(null);
			void invalidateDiary();
		},
	});

	const processVoiceMemoRecoveryMutation = useMutation({
		...trpc.diary.processVoiceMemoRecovery.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			toast.success('Voice memo saved.');
		},
		onError: showError,
		onSettled: () => {
			setReprocessingRecoveryId(null);
			void invalidateDiary();
		},
	});

	const failVoiceMemoMutation = useMutation({
		...trpc.diary.failVoiceMemo.mutationOptions(),
		onSettled: () => {
			void invalidateDiary();
		},
	});

	const deleteVoiceMemoMutation = useMutation({
		...trpc.diary.deleteVoiceMemo.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			toast.success('Voice memo deleted.');
		},
		onError: showError,
		onSettled: () => {
			setDeletingVoiceMemoId(null);
			void invalidateDiary();
		},
	});

	const addEntryTagsMutation = useMutation({
		...trpc.diary.addEntryTags.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
		},
		onError: showError,
		onSettled: () => {
			setAddingTagsEntryId(null);
			void invalidateDiary();
		},
	});

	const addVoiceMemoTagsMutation = useMutation({
		...trpc.diary.addVoiceMemoTags.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
		},
		onError: showError,
		onSettled: () => {
			setAddingTagsVoiceMemoId(null);
			void invalidateDiary();
		},
	});

	const deleteEntryMutation = useMutation({
		...trpc.table.diaryEntries.deleteMany.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			toast.success('Entry deleted.');
		},
		onError: error => {
			showError(error);
		},
		onSettled: () => {
			setDeletingEntryId(null);
		},
	});

	async function invalidateDiary() {
		await Promise.all([
			queryClient.invalidateQueries({ queryKey: [['diary']] }),
			queryClient.invalidateQueries({ queryKey: [['tags']] }),
		]);
	}

	function showError(error: unknown) {
		setErrorDetails(formatErrorDetails(error));
	}

	useEffect(() => {
		if (!navigator.geolocation) {
			setLocationError('Geolocation is not available in this browser.');
			return;
		}

		const watchId = navigator.geolocation.watchPosition(
			position => {
				setCurrentLocation(locationFromPosition(position));
				setLocationError(null);
			},
			error => {
				const messageText = formatGeolocationError(error);
				setLocationError(messageText);
				showError(error);
			},
			{
				enableHighAccuracy: true,
				maximumAge: 60_000,
				timeout: 15_000,
			},
		);

		return () => {
			navigator.geolocation.clearWatch(watchId);
		};
	}, []);

	useEffect(
		() => () => {
			const recorder = mediaRecorderRef.current;
			if (recorder && recorder.state !== 'inactive') {
				recorder.onstop = null;
				recorder.stop();
			}
			stopLiveTranscription();
			stopMediaStream();
		},
		[],
	);

	const tagOptions = useMemo(
		() =>
			(tagsQuery.data ?? []).map(tag => ({
				label: tag.name,
				value: tag.name,
				color: tag.color,
			})),
		[tagsQuery.data],
	);

	const isSaving =
		createEntryMutation.isPending ||
		saveVoiceMemoMutation.isPending ||
		startVoiceMemoDraftMutation.isPending ||
		finishVoiceMemoDraftMutation.isPending ||
		processVoiceMemoMutation.isPending;
	const canAddEntry = notes.trim().length > 0 && currentLocation !== null && !isSaving;
	const entries = entriesQuery.data ?? [];
	const pendingVoiceMemos = pendingVoiceMemosQuery.data ?? [];
	const pendingVoiceMemoRecoveries = pendingVoiceMemoRecoveriesQuery.data ?? [];
	const columns = getColumns();
	const pendingVoiceMemoColumns = getPendingVoiceMemoColumns();
	const pendingVoiceMemoRecoveryColumns = getPendingVoiceMemoRecoveryColumns();

	function getRequiredLocation() {
		if (!currentLocation) {
			throw new Error(locationError ?? 'Waiting for location permission.');
		}

		return currentLocation;
	}

	async function handleAddEntry() {
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

	async function handleRecordButton() {
		if (isRecording) {
			stopRecording();
			return;
		}

		await startRecording();
	}

	async function startRecording() {
		try {
			getRequiredLocation();

			if (!navigator.mediaDevices?.getUserMedia) {
				throw new Error('Microphone recording is not available in this browser.');
			}
			if (typeof MediaRecorder === 'undefined') {
				throw new Error('MediaRecorder is not available in this browser.');
			}

			setNotes('');
			finalizedTranscriptRef.current = '';
			interimTranscriptRef.current = '';
			const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
			mediaStreamRef.current = stream;
			await openLiveTranscription();
			await startPcmStreaming(stream);
			const mimeType = getPreferredRecordingMimeType();
			const recorder = new MediaRecorder(stream, mimeType ? { mimeType } : undefined);
			const resolvedMimeType = recorder.mimeType || mimeType || 'audio/webm';
			const extension = extensionFromMimeType(resolvedMimeType);
			const fileName = `diary-${new Date().toISOString().replace(/[:.]/g, '-')}.${extension}`;
			const draft = await startVoiceMemoDraftMutation.mutateAsync({
				notes: '',
				transcript: '',
				tagNames,
				location: getRequiredLocation(),
				fileName,
				mimeType: resolvedMimeType,
			});

			recordingRecoveryIdRef.current = draft.recoveryId;
			recordingChunkUploadRef.current = Promise.resolve();
			recordingStartedAtRef.current = Date.now();
			mediaRecorderRef.current = recorder;

			recorder.ondataavailable = event => {
				if (event.data.size > 0) {
					queueRecordingChunkUpload(event.data);
				}
			};
			recorder.onerror = event => {
				showError(event.error);
			};
			recorder.onstop = () => {
				void uploadStoppedRecording();
			};

			recorder.start(1_000);
			setIsRecording(true);
		} catch (error) {
			showError(error);
			stopLiveTranscription();
			stopMediaStream();
			setIsRecording(false);
		}
	}

	function stopRecording() {
		const recorder = mediaRecorderRef.current;
		if (!recorder || recorder.state === 'inactive') {
			return;
		}

		setIsStoppingRecording(true);
		stopPcmStreaming();
		sttWebSocketRef.current?.send(JSON.stringify({ type: 'audio.done' }));
		recorder.stop();
	}

	function queueRecordingChunkUpload(blob: Blob) {
		const recoveryId = recordingRecoveryIdRef.current;
		if (!recoveryId) {
			throw new Error('Recording recovery id is missing.');
		}

		recordingChunkUploadRef.current = recordingChunkUploadRef.current.then(async () => {
			await appendVoiceMemoDraftMutation.mutateAsync({
				recoveryId,
				dataBase64: await blobToBase64(blob),
			});
		});
		void recordingChunkUploadRef.current.catch(showError);
	}

	async function uploadStoppedRecording() {
		let savedVoiceMemoId: number | null = null;
		try {
			setIsParsingVoiceMemo(true);
			const recoveryId = recordingRecoveryIdRef.current;
			if (!recoveryId) {
				throw new Error('Recording recovery id is missing.');
			}

			await recordingChunkUploadRef.current;
			const startedAt = recordingStartedAtRef.current ?? Date.now();
			const durationSeconds = Math.max((Date.now() - startedAt) / 1000, 0.1);

			const savedVoiceMemo = await finishVoiceMemoDraftMutation.mutateAsync({
				recoveryId,
				transcript: getDisplayedStreamingTranscript(),
				durationSeconds,
			});
			savedVoiceMemoId = savedVoiceMemo.voiceMemoId;

			const transcript = await waitForStreamingTranscript();

			await processVoiceMemoMutation.mutateAsync({
				voiceMemoId: savedVoiceMemo.voiceMemoId,
				transcript,
			});
		} catch (error) {
			showError(error);
			if (savedVoiceMemoId) {
				try {
					await failVoiceMemoMutation.mutateAsync({
						voiceMemoId: savedVoiceMemoId,
						error: formatErrorDetails(error).details,
					});
				} catch (failError) {
					showError(failError);
				}
			}
		} finally {
			recordingRecoveryIdRef.current = null;
			recordingChunkUploadRef.current = Promise.resolve();
			recordingStartedAtRef.current = null;
			mediaRecorderRef.current = null;
			transcriptDoneRef.current = null;
			stopLiveTranscription();
			stopMediaStream();
			setIsRecording(false);
			setIsStoppingRecording(false);
			setIsParsingVoiceMemo(false);
		}
	}

	async function openLiveTranscription() {
		const socket = new WebSocket(getDiarySttWebSocketUrl());
		sttWebSocketRef.current = socket;
		transcriptDoneRef.current = createStreamingTranscriptDeferred();
		void transcriptDoneRef.current.promise.catch(() => undefined);

		socket.onmessage = event => {
			const data =
				typeof event.data === 'string'
					? event.data
					: new TextDecoder().decode(event.data as ArrayBuffer);
			handleStreamingTranscriptEvent(JSON.parse(data) as StreamingTranscriptEvent);
		};
		socket.onerror = () => {
			rejectStreamingTranscript(new Error('Live transcription connection failed.'));
		};
		socket.onclose = () => {
			const deferred = transcriptDoneRef.current;
			if (deferred && !deferred.isSettled) {
				rejectStreamingTranscript(new Error('Live transcription connection closed.'));
			}
		};

		await new Promise<void>((resolve, reject) => {
			socket.onopen = () => resolve();
			socket.onerror = () => {
				const error = new Error('Live transcription connection failed.');
				rejectStreamingTranscript(error);
				reject(error);
			};
		});
	}

	async function startPcmStreaming(stream: MediaStream) {
		const AudioContextConstructor = getAudioContextConstructor();
		const audioContext = new AudioContextConstructor();
		const source = audioContext.createMediaStreamSource(stream);
		const processor = audioContext.createScriptProcessor(4096, 1, 1);

		processor.onaudioprocess = event => {
			const socket = sttWebSocketRef.current;
			if (!socket || socket.readyState !== WebSocket.OPEN) {
				return;
			}

			const input = event.inputBuffer.getChannelData(0);
			const output = event.outputBuffer.getChannelData(0);
			output.fill(0);
			socket.send(float32ToPcm16(downsampleAudio(input, audioContext.sampleRate, 16_000)));
		};

		source.connect(processor);
		processor.connect(audioContext.destination);
		audioContextRef.current = audioContext;
		audioSourceRef.current = source;
		audioProcessorRef.current = processor;
	}

	function stopPcmStreaming() {
		const processor = audioProcessorRef.current;
		if (processor) {
			processor.onaudioprocess = null;
			processor.disconnect();
		}
		audioSourceRef.current?.disconnect();
		void audioContextRef.current?.close();
		audioProcessorRef.current = null;
		audioSourceRef.current = null;
		audioContextRef.current = null;
	}

	function stopLiveTranscription() {
		stopPcmStreaming();
		const socket = sttWebSocketRef.current;
		if (socket && socket.readyState !== WebSocket.CLOSED) {
			socket.close();
		}
		sttWebSocketRef.current = null;
	}

	async function waitForStreamingTranscript() {
		const transcript =
			normalizeTranscript(await (transcriptDoneRef.current?.promise ?? Promise.resolve(''))) ||
			getDisplayedStreamingTranscript();
		if (!transcript) {
			throw new Error('Live transcription did not return text.');
		}

		return transcript;
	}

	function handleStreamingTranscriptEvent(event: StreamingTranscriptEvent) {
		switch (event.type) {
			case 'transcript.created':
				return;
			case 'transcript.partial':
				updateLiveTranscript(event);
				return;
			case 'transcript.done': {
				const resolvedTranscript =
					normalizeTranscript(event.text ?? '') || getDisplayedStreamingTranscript();
				finalizedTranscriptRef.current = resolvedTranscript;
				interimTranscriptRef.current = '';
				setNotes(resolvedTranscript);
				resolveStreamingTranscript(resolvedTranscript);
				return;
			}
			case 'error':
				rejectStreamingTranscript(new Error(event.message ?? 'Live transcription failed.'));
				return;
		}
	}

	function updateLiveTranscript(
		event: Extract<StreamingTranscriptEvent, { type: 'transcript.partial' }>,
	) {
		const text = normalizeTranscript(event.text ?? '');
		if (!text) {
			return;
		}

		if (event.is_final || event.speech_final) {
			finalizedTranscriptRef.current = appendTranscript(finalizedTranscriptRef.current, text);
			interimTranscriptRef.current = '';
			setNotes(finalizedTranscriptRef.current);
			return;
		}

		interimTranscriptRef.current = text;
		setNotes(getDisplayedStreamingTranscript());
	}

	function getDisplayedStreamingTranscript() {
		return appendTranscript(finalizedTranscriptRef.current, interimTranscriptRef.current);
	}

	function resolveStreamingTranscript(transcript: string) {
		const deferred = transcriptDoneRef.current;
		if (!deferred || deferred.isSettled) {
			return;
		}

		deferred.isSettled = true;
		deferred.resolve(transcript);
	}

	function rejectStreamingTranscript(error: Error) {
		const deferred = transcriptDoneRef.current;
		if (!deferred || deferred.isSettled) {
			return;
		}

		deferred.isSettled = true;
		deferred.reject(error);
	}

	function handleDeleteEntry(entryId: number) {
		setDeletingEntryId(entryId);
		deleteEntryMutation.mutate({
			where: [{ column: 'id', operator: 'eq', value: entryId }],
		});
	}

	function handleReprocessVoiceMemo(voiceMemoId: number) {
		setReprocessingVoiceMemoId(voiceMemoId);
		processVoiceMemoMutation.mutate({ voiceMemoId });
	}

	function handleReprocessVoiceMemoRecovery(recoveryId: string) {
		setReprocessingRecoveryId(recoveryId);
		processVoiceMemoRecoveryMutation.mutate({ recoveryId });
	}

	function handleDeleteVoiceMemo(voiceMemoId: number) {
		setDeletingVoiceMemoId(voiceMemoId);
		deleteVoiceMemoMutation.mutate({ voiceMemoId });
	}

	function handleAddTagsToEntry(entryId: number, nextTagNames: string[]) {
		const names = nextTagNames.map(name => name.trim()).filter(Boolean);
		if (names.length === 0) {
			return;
		}

		setAddingTagsEntryId(entryId);
		addEntryTagsMutation.mutate({
			entryId,
			tagNames: names,
		});
	}

	function handleAddTagsToVoiceMemo(voiceMemoId: number, nextTagNames: string[]) {
		const names = nextTagNames.map(name => name.trim()).filter(Boolean);
		if (names.length === 0) {
			return;
		}

		setAddingTagsVoiceMemoId(voiceMemoId);
		addVoiceMemoTagsMutation.mutate({
			voiceMemoId,
			tagNames: names,
		});
	}

	function toggleExpandedRow(entryId: number) {
		const rowKey = String(entryId);
		setExpandedRowKeys(keys =>
			keys.includes(rowKey) ? keys.filter(key => key !== rowKey) : [...keys, rowKey],
		);
	}

	function stopMediaStream() {
		mediaStreamRef.current?.getTracks().forEach(track => track.stop());
		mediaStreamRef.current = null;
	}

	return (
		<main className='pills-page'>
			<PageNav title='Diary' />

			<div className='pills-page-inner diary-page-inner'>
				{errorDetails ? (
					<InlineAlert
						kind='error'
						title={errorDetails.message}
						onClose={() => setErrorDetails(null)}
					>
						{
							<details>
								<summary>Stack details</summary>
								<pre className='diary-error-details'>{errorDetails.details}</pre>
							</details>
						}
					</InlineAlert>
				) : null}

				<Card bg='$bgContainer' borderColor='$borderSubtle' borderWidth={1}>
					<YStack gap={12} padding={16} width='100%'>
						<div className='diary-composer-note'>
							<AutoResizeTextArea
								value={notes}
								minRows={1}
								maxRows={14}
								disabled={isSaving || isRecording}
								onChange={event => {
									setNotes((event.target as unknown as HTMLTextAreaElement).value);
								}}
							/>
							<Button
								icon={<PlusOutlined />}
								disabled={!canAddEntry}
								backgroundColor='$primary'
								style={{ color: 'white' }}
								className='diary-add-button'
								onPress={() => {
									void handleAddEntry();
								}}
							>
								{createEntryMutation.isPending ? 'Adding...' : 'Add'}
							</Button>
						</div>

						<TagInput
							value={tagNames}
							options={tagOptions}
							placeholder='Tags'
							disabled={isSaving || isRecording}
							onChange={setTagNames}
						/>

						<div className='diary-actions'>
							<Button
								icon={isRecording ? <StopOutlined /> : <AudioOutlined />}
								backgroundColor={isRecording ? '$error' : undefined}
								style={isRecording ? { color: 'white' } : undefined}
								disabled={!currentLocation || createEntryMutation.isPending}
								onPress={() => {
									void handleRecordButton();
								}}
							>
								{isStoppingRecording || isParsingVoiceMemo
									? 'Working...'
									: isRecording
										? 'Stop'
										: 'Record'}
							</Button>
							{isParsingVoiceMemo ? <Text color='$textMuted'>Parsing</Text> : null}
						</div>
					</YStack>
				</Card>

				{pendingVoiceMemos.length > 0 ? (
					<SectionCard
						title='Pending memos'
						extra={<Text color='$textMuted'>{pendingVoiceMemos.length} pending</Text>}
					>
						<DataTable
							getRowKey={row => row.id}
							loading={pendingVoiceMemosQuery.isLoading}
							columns={pendingVoiceMemoColumns}
							rows={pendingVoiceMemos}
							minWidth={1400}
						/>
					</SectionCard>
				) : null}

				{pendingVoiceMemoRecoveries.length > 0 ? (
					<SectionCard
						title='Pending recovery memos'
						extra={<Text color='$textMuted'>{pendingVoiceMemoRecoveries.length} pending</Text>}
					>
						<DataTable
							getRowKey={row => row.id}
							loading={pendingVoiceMemoRecoveriesQuery.isLoading}
							columns={pendingVoiceMemoRecoveryColumns}
							rows={pendingVoiceMemoRecoveries}
							minWidth={1200}
						/>
					</SectionCard>
				) : null}

				<SectionCard title='Diary' extra={<Text color='$textMuted'>{entries.length} entries</Text>}>
					<DataTable
						getRowKey={row => row.id}
						loading={entriesQuery.isLoading}
						columns={columns}
						rows={entries}
						minWidth={1280}
						expandedRowKeys={expandedRowKeys}
						onExpandedRowKeysChange={keys => setExpandedRowKeys(keys.map(String))}
						canExpandRow={row => getEntryTranscriptText(row).length > 0}
						renderExpandedRow={row => <TranscriptPanel entry={row} />}
					/>
				</SectionCard>
			</div>
		</main>
	);

	function getPendingVoiceMemoRecoveryColumns(): Array<DataColumn<DiaryPendingVoiceMemoRecovery>> {
		return [
			{
				key: 'createdAt',
				header: 'When',
				width: 180,
				cell: row => (
					<YStack gap={0}>
						<Text>{formatDiaryDate(row.createdAt)}</Text>
						<Text color='$textMuted'>{formatDiaryTime(row.createdAt)}</Text>
					</YStack>
				),
			},
			{
				key: 'status',
				header: 'Status',
				width: 140,
				cell: row => <TagChip>{row.status}</TagChip>,
			},
			{
				key: 'metadata',
				header: 'Metadata',
				width: 360,
				cell: row => (
					<YStack gap={0}>
						<Text>{row.fileName}</Text>
						<Text color='$textMuted'>{row.mimeType}</Text>
						<Text color='$textMuted'>
							{formatDuration(row.durationSeconds)} · {formatBytes(row.audioBytes)}
						</Text>
						<Text color='$textMuted'>{row.metadataPath}</Text>
						{row.audioPath ? <Text color='$textMuted'>{row.audioPath}</Text> : null}
					</YStack>
				),
			},
			{
				key: 'transcript',
				header: 'Transcript',
				width: 320,
				cell: row => <MarkdownBlock emptyText='No transcript'>{row.transcript}</MarkdownBlock>,
			},
			{
				key: 'error',
				header: 'Error',
				width: 360,
				cell: row =>
					row.error ? (
						<details>
							<summary>{row.error.split('\n')[0]}</summary>
							<pre className='diary-error-details'>{row.error}</pre>
						</details>
					) : (
						<Text color='$textMuted'>None</Text>
					),
			},
			{
				key: 'actions',
				header: '',
				width: 130,
				align: 'right',
				cell: row => (
					<Button
						size='$2'
						icon={<ReloadOutlined />}
						disabled={processVoiceMemoRecoveryMutation.isPending || isRecording}
						onPress={() => handleReprocessVoiceMemoRecovery(row.id)}
					>
						{processVoiceMemoRecoveryMutation.isPending && reprocessingRecoveryId === row.id
							? 'Working...'
							: 'Reprocess'}
					</Button>
				),
			},
		];
	}

	function getPendingVoiceMemoColumns(): Array<DataColumn<DiaryPendingVoiceMemo>> {
		return [
			{
				key: 'createdAt',
				header: 'When',
				width: 180,
				cell: row => (
					<YStack gap={0}>
						<Text>{formatDiaryDate(row.createdAt)}</Text>
						<Text color='$textMuted'>{formatDiaryTime(row.createdAt)}</Text>
					</YStack>
				),
			},
			{
				key: 'transcriptionStatus',
				header: 'Status',
				width: 120,
				cell: row => <TagChip>{row.transcriptionStatus}</TagChip>,
			},
			{
				key: 'location',
				header: 'Location',
				width: 180,
				cell: row => <PendingLocationCell location={row.location} />,
			},
			{
				key: 'tags',
				header: 'Tags',
				width: 220,
				cell: row => (
					<YStack gap={6} width='100%'>
						{row.tags.length > 0 ? (
							<XStack gap={4} flexWrap='wrap'>
								{row.tags.map(tag => (
									<TagChip key={tag.id} color={tag.color}>
										{tag.name}
									</TagChip>
								))}
							</XStack>
						) : (
							<Text color='$textMuted'>No tags</Text>
						)}
						<TagAddControl
							targetId={row.id}
							tagOptions={tagOptions}
							loading={addVoiceMemoTagsMutation.isPending && addingTagsVoiceMemoId === row.id}
							disabled={addVoiceMemoTagsMutation.isPending}
							onAddTags={handleAddTagsToVoiceMemo}
						/>
					</YStack>
				),
			},
			{
				key: 'audio',
				header: 'Audio',
				width: 300,
				cell: row => (
					<audio controls src={`/api/diary/voice-memos/${row.id}/audio`} className='diary-audio' />
				),
			},
			{
				key: 'metadata',
				header: 'Metadata',
				width: 300,
				cell: row => (
					<YStack gap={0}>
						<Text>{row.fileName}</Text>
						<Text color='$textMuted'>{row.mimeType}</Text>
						<Text color='$textMuted'>
							{formatDuration(row.durationSeconds)} · {formatBytes(row.audioBytes)}
						</Text>
						{row.processedAt ? (
							<Text color='$textMuted'>processed {formatDiaryTime(row.processedAt)}</Text>
						) : null}
					</YStack>
				),
			},
			{
				key: 'transcript',
				header: 'Transcript',
				width: 320,
				cell: row => <MarkdownBlock emptyText='No transcript'>{row.transcript}</MarkdownBlock>,
			},
			{
				key: 'error',
				header: 'Error',
				width: 360,
				cell: row =>
					row.transcriptionError ? (
						<details>
							<summary>{row.transcriptionError.split('\n')[0]}</summary>
							<pre className='diary-error-details'>{row.transcriptionError}</pre>
						</details>
					) : (
						<Text color='$textMuted'>None</Text>
					),
			},
			{
				key: 'actions',
				header: '',
				width: 160,
				align: 'right',
				cell: row => (
					<XStack gap={4}>
						<Button
							size='$2'
							icon={<ReloadOutlined />}
							disabled={processVoiceMemoMutation.isPending || deleteVoiceMemoMutation.isPending}
							onPress={() => handleReprocessVoiceMemo(row.id)}
						>
							{processVoiceMemoMutation.isPending && reprocessingVoiceMemoId === row.id
								? 'Working...'
								: 'Reprocess'}
						</Button>
						<Button
							size='$2'
							chromeless
							icon={<DeleteOutlined />}
							disabled={deleteVoiceMemoMutation.isPending}
							onPress={() => {
								if (
									window.confirm(
										'Delete pending memo?\n\nThis removes the pending audio memo and its diary entry if it is empty.',
									)
								) {
									handleDeleteVoiceMemo(row.id);
								}
							}}
						>
							{deleteVoiceMemoMutation.isPending && deletingVoiceMemoId === row.id ? '...' : null}
						</Button>
					</XStack>
				),
			},
		];
	}

	function getColumns(): Array<DataColumn<DiaryEntry>> {
		return [
			{
				key: 'createdAt',
				header: 'When',
				width: 140,
				cell: row => <Text>{formatDiaryDate(row.createdAt)}</Text>,
			},
			{
				key: 'location',
				header: 'Location',
				width: 180,
				cell: row => <LocationCell location={row.location} />,
			},
			{
				key: 'tags',
				header: 'Tags',
				width: 220,
				cell: row => (
					<YStack gap={6} width='100%'>
						{row.tags.length > 0 ? (
							<XStack gap={4} flexWrap='wrap'>
								{row.tags.map(tag => (
									<TagChip key={tag.id} color={tag.color}>
										{tag.name}
									</TagChip>
								))}
							</XStack>
						) : (
							<Text color='$textMuted'>No tags</Text>
						)}
						<TagAddControl
							targetId={row.id}
							tagOptions={tagOptions}
							loading={addEntryTagsMutation.isPending && addingTagsEntryId === row.id}
							disabled={addEntryTagsMutation.isPending}
							onAddTags={handleAddTagsToEntry}
						/>
					</YStack>
				),
			},
			{
				key: 'transcript',
				header: 'Transcript',
				width: 360,
				cell: row => (
					<TranscriptPreview
						entry={row}
						onClick={() => {
							toggleExpandedRow(row.id);
						}}
					/>
				),
			},
			{
				key: 'voiceMemos',
				header: 'Voice memos',
				width: 300,
				cell: row => <VoiceMemosCell memos={row.voiceMemos} />,
			},
			{
				key: 'summary',
				header: 'Summary',
				width: 360,
				cell: row => <MarkdownBlock emptyText='No summary yet'>{row.summary}</MarkdownBlock>,
			},
			{
				key: 'actions',
				header: '',
				width: 72,
				align: 'right',
				cell: row => (
					<Button
						size='$2'
						chromeless
						icon={<DeleteOutlined />}
						disabled={deleteEntryMutation.isPending}
						onPress={() => {
							if (
								window.confirm(
									'Delete diary entry?\n\nThis removes the entry, transcript, and attached voice memos.',
								)
							) {
								handleDeleteEntry(row.id);
							}
						}}
					>
						{deleteEntryMutation.isPending && deletingEntryId === row.id ? '...' : null}
					</Button>
				),
			},
		];
	}
}

function SectionCard(props: { title: string; extra?: ReactNode; children: ReactNode }) {
	return (
		<Card bg='$bgContainer' borderColor='$borderSubtle' borderWidth={1}>
			<XStack
				alignItems='center'
				justifyContent='space-between'
				gap={12}
				padding={12}
				borderBottomWidth={1}
				borderBottomColor='$borderSubtle'
			>
				<Text fontWeight='700'>{props.title}</Text>
				{props.extra}
			</XStack>
			<YStack padding={16}>{props.children}</YStack>
		</Card>
	);
}

function InlineAlert(props: {
	kind: 'error' | 'warning' | 'info' | 'success';
	title: ReactNode;
	children?: ReactNode;
	onClose?: () => void;
}) {
	const theme = useTheme();
	const palette = {
		error: [theme.errorBg?.get('web'), theme.errorBorder?.get('web'), theme.error?.get('web')],
		warning: [
			theme.warningBg?.get('web'),
			theme.warningBorder?.get('web'),
			theme.warning?.get('web'),
		],
		info: [theme.infoBg?.get('web'), theme.infoBorder?.get('web'), theme.infoText?.get('web')],
		success: [
			theme.successBg?.get('web'),
			theme.successBorder?.get('web'),
			theme.success?.get('web'),
		],
	}[props.kind];

	return (
		<XStack
			gap={10}
			padding={12}
			borderWidth={1}
			borderRadius={8}
			style={{ background: palette[0], borderColor: palette[1] }}
		>
			<Text fontWeight='700' style={{ color: palette[2] }}>
				●
			</Text>
			<YStack flex={1} minWidth={0}>
				<Text fontWeight='700'>{props.title}</Text>
				{props.children}
			</YStack>
			{props.onClose ? (
				<Button size='$2' chromeless onPress={props.onClose}>
					×
				</Button>
			) : null}
		</XStack>
	);
}

function LocationCell(props: { location: DiaryEntry['location'] }) {
	const { location } = props;

	return (
		<Anchor href={mapsUrl(location)} target='_blank' rel='noreferrer' color='$primary'>
			{location.name}
		</Anchor>
	);
}

function PendingLocationCell(props: { location: DiaryPendingVoiceMemo['location'] }) {
	const { location } = props;
	const name = location.name ?? `${location.latitude}, ${location.longitude}`;

	return (
		<Anchor href={mapsUrl(location)} target='_blank' rel='noreferrer' color='$primary'>
			{name}
		</Anchor>
	);
}

function TagAddControl(props: {
	targetId: number;
	tagOptions: Array<{ label: string; value: string }>;
	loading: boolean;
	disabled: boolean;
	onAddTags: (targetId: number, tagNames: string[]) => void;
}) {
	const [selectedTagNames, setSelectedTagNames] = useState<string[]>([]);

	function addTags(tagNames = selectedTagNames) {
		const names = tagNames.map(name => name.trim()).filter(Boolean);
		if (names.length === 0) {
			return;
		}

		props.onAddTags(props.targetId, names);
		setSelectedTagNames([]);
	}

	return (
		<XStack gap={6} className='diary-tag-add-control'>
			<TagInput
				value={selectedTagNames}
				options={props.tagOptions}
				placeholder='Tags'
				disabled={props.disabled || props.loading}
				onChange={setSelectedTagNames}
			/>
			<Button
				size='$2'
				icon={<PlusOutlined />}
				disabled={props.disabled || selectedTagNames.length === 0}
				onPress={() => addTags()}
			/>
		</XStack>
	);
}

function TranscriptPreview(props: { entry: DiaryEntry; onClick: () => void }) {
	const transcript = getEntryTranscriptText(props.entry);

	if (!transcript) {
		return <Text color='$textMuted'>No transcript</Text>;
	}

	return (
		<Paragraph className='diary-transcript-preview' numberOfLines={2} onPress={props.onClick}>
			{transcript}
		</Paragraph>
	);
}

function VoiceMemosCell(props: { memos: DiaryVoiceMemo[] }) {
	if (props.memos.length === 0) {
		return <Text color='$textMuted'>None</Text>;
	}

	return (
		<YStack gap={8} width='100%'>
			{props.memos.map(memo => (
				<div key={memo.id} className='diary-voice-memo'>
					<audio controls src={`/api/diary/voice-memos/${memo.id}/audio`} className='diary-audio' />
				</div>
			))}
		</YStack>
	);
}

function TranscriptPanel(props: { entry: DiaryEntry }) {
	const transcript = getEntryTranscriptText(props.entry);

	return (
		<YStack gap={8} width='100%'>
			<Text color='$textMuted'>{formatDiaryTime(props.entry.createdAt)}</Text>
			<MarkdownBlock>{transcript}</MarkdownBlock>
		</YStack>
	);
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

function MarkdownBlock(props: { children?: string | null; emptyText?: string }) {
	const value = props.children?.trim();

	if (!value) {
		return <Text color='$textMuted'>{props.emptyText ?? 'Empty'}</Text>;
	}

	return (
		<div className='diary-markdown'>
			<ReactMarkdown remarkPlugins={[remarkGfm]}>{value}</ReactMarkdown>
		</div>
	);
}

function createStreamingTranscriptDeferred(): StreamingTranscriptDeferred {
	let resolveDeferred: StreamingTranscriptDeferred['resolve'] | null = null;
	let rejectDeferred: StreamingTranscriptDeferred['reject'] | null = null;
	const promise = new Promise<string>((resolve, reject) => {
		resolveDeferred = resolve;
		rejectDeferred = reject;
	});

	if (!resolveDeferred || !rejectDeferred) {
		throw new Error('Failed to create streaming transcript promise.');
	}

	return {
		promise,
		resolve: resolveDeferred,
		reject: rejectDeferred,
		isSettled: false,
	};
}

function getDiarySttWebSocketUrl() {
	const url = new URL('/diary/stt/live', import.meta.env.VITE_API_URL.trim());
	url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
	return url.toString();
}

function getAudioContextConstructor() {
	const audioContextConstructor =
		window.AudioContext ??
		(window as typeof window & { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;

	if (!audioContextConstructor) {
		throw new Error('AudioContext is not available in this browser.');
	}

	return audioContextConstructor;
}

function downsampleAudio(input: Float32Array, inputSampleRate: number, outputSampleRate: number) {
	if (inputSampleRate === outputSampleRate) {
		return input;
	}
	if (inputSampleRate < outputSampleRate) {
		throw new Error(`Cannot downsample ${inputSampleRate}Hz audio to ${outputSampleRate}Hz.`);
	}

	const sampleRateRatio = inputSampleRate / outputSampleRate;
	const outputLength = Math.round(input.length / sampleRateRatio);
	const output = new Float32Array(outputLength);

	for (let outputIndex = 0; outputIndex < outputLength; outputIndex += 1) {
		const start = Math.floor(outputIndex * sampleRateRatio);
		const end = Math.min(Math.floor((outputIndex + 1) * sampleRateRatio), input.length);
		let sum = 0;
		for (let inputIndex = start; inputIndex < end; inputIndex += 1) {
			sum += input[inputIndex] ?? 0;
		}
		output[outputIndex] = sum / Math.max(end - start, 1);
	}

	return output;
}

function float32ToPcm16(input: Float32Array) {
	const output = new ArrayBuffer(input.length * 2);
	const view = new DataView(output);

	for (let index = 0; index < input.length; index += 1) {
		const sample = Math.max(-1, Math.min(1, input[index] ?? 0));
		view.setInt16(index * 2, sample < 0 ? sample * 0x8000 : sample * 0x7fff, true);
	}

	return output;
}

function normalizeTranscript(value: string) {
	return value.replace(/\s+/g, ' ').trim();
}

function appendTranscript(existingText: string, nextText: string) {
	const existing = normalizeTranscript(existingText);
	const next = normalizeTranscript(nextText);

	if (!existing) {
		return next;
	}
	if (!next || existing.endsWith(next)) {
		return existing;
	}
	if (next.startsWith(existing)) {
		return next;
	}

	return `${existing} ${next}`;
}

function formatErrorDetails(error: unknown): DiaryErrorDetails {
	const message =
		error instanceof Error ? `${error.name}: ${error.message}` : `Error: ${String(error)}`;
	const details = [
		error instanceof Error ? error.stack : null,
		safeJsonStringify(serializeError(error)),
		typeof error === 'object' && error && 'cause' in error
			? `cause:\n${safeJsonStringify(serializeError((error as { cause: unknown }).cause))}`
			: null,
	]
		.filter(Boolean)
		.join('\n\n');

	return {
		message,
		details: details || message,
	};
}

function serializeError(error: unknown): unknown {
	if (typeof error !== 'object' || error === null) {
		return error;
	}

	const output: Record<string, unknown> = {};
	for (const key of Reflect.ownKeys(error)) {
		output[String(key)] = (error as Record<PropertyKey, unknown>)[key];
	}
	if (error instanceof Error) {
		output.name = error.name;
		output.message = error.message;
		output.stack = error.stack;
	}

	return output;
}

function safeJsonStringify(value: unknown) {
	try {
		return JSON.stringify(value, getCircularJsonReplacer(), 2);
	} catch {
		return String(value);
	}
}

function getCircularJsonReplacer() {
	const seen = new WeakSet<object>();
	return (_key: string, value: unknown) => {
		if (typeof value !== 'object' || value === null) {
			return value;
		}
		if (seen.has(value)) {
			return '[Circular]';
		}
		seen.add(value);
		return value;
	};
}

function locationFromPosition(position: GeolocationPosition): LocationInput {
	const { coords } = position;

	return {
		capturedAt: new Date(position.timestamp).toISOString(),
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
	return Number.isFinite(value) ? value : null;
}

function formatGeolocationError(error: GeolocationPositionError) {
	switch (error.code) {
		case error.PERMISSION_DENIED:
			return 'Location permission was denied. Diary entries require a location.';
		case error.POSITION_UNAVAILABLE:
			return 'Current location is unavailable.';
		case error.TIMEOUT:
			return 'Timed out while requesting current location.';
		default:
			return error.message || 'Failed to read current location.';
	}
}

function getPreferredRecordingMimeType() {
	const supportedTypes = [
		'audio/webm;codecs=opus',
		'audio/webm',
		'audio/mp4',
		'audio/ogg;codecs=opus',
	];

	return supportedTypes.find(type => MediaRecorder.isTypeSupported(type)) ?? '';
}

async function blobToBase64(blob: Blob) {
	const dataUrl = await new Promise<string>((resolve, reject) => {
		const reader = new FileReader();
		reader.onload = () => resolve(String(reader.result));
		reader.onerror = () => reject(reader.error ?? new Error('Failed to read recording.'));
		reader.readAsDataURL(blob);
	});
	const base64 = dataUrl.split(',')[1];
	if (!base64) {
		throw new Error('Recording data URL did not contain base64 audio.');
	}

	return base64;
}

function extensionFromMimeType(mimeType: string) {
	const normalized = mimeType.toLowerCase();
	if (normalized.includes('webm')) return 'webm';
	if (normalized.includes('ogg')) return 'ogg';
	if (normalized.includes('mp4')) return 'm4a';
	if (normalized.includes('wav')) return 'wav';

	const subtype = normalized
		.split('/')[1]
		?.split(';')[0]
		?.replace(/[^a-z0-9]/g, '');
	if (!subtype) {
		throw new Error(`Unsupported recording mime type: ${mimeType}`);
	}

	return subtype;
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

function mapsUrl(location: { latitude: number; longitude: number }) {
	return `https://www.google.com/maps?q=${location.latitude},${location.longitude}`;
}

function formatDiaryDate(value: string) {
	return new Date(value).toLocaleDateString('en-US', {
		month: 'long',
		day: 'numeric',
		year: '2-digit',
	});
}

function formatDiaryTime(value: string) {
	return new Date(value).toLocaleTimeString('en-US', {
		hour: 'numeric',
		minute: '2-digit',
	});
}
