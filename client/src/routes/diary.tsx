import {
	AudioOutlined,
	DeleteOutlined,
	PlusOutlined,
	ReloadOutlined,
	StopOutlined,
} from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import {
	Button,
	Card,
	Input,
	Alert,
	Modal,
	Popconfirm,
	Select,
	Space,
	Table,
	Tag,
	Typography,
	message,
} from 'antd';
import type { TableColumnsType } from 'antd';
import type { Key } from 'react';
import { useEffect, useMemo, useRef, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import type {
	DiaryEntry,
	DiaryPendingVoiceMemo,
	DiaryPendingVoiceMemoRecovery,
	DiaryVoiceMemo,
} from '../utils/api';
import { withAuthToken } from '../utils/auth';
import { PageNav } from '../components/PageNav';
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

type PlayableDiaryVideoMemo = {
	id: number;
	fileName: string;
};

function DiaryRouteComponent() {
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const [notes, setNotes] = useState('');
	const [tagNames, setTagNames] = useState<string[]>([]);
	const [currentLocation, setCurrentLocation] = useState<LocationInput | null>(null);
	const [locationError, setLocationError] = useState<string | null>(null);
	const [isRequestingLocation, setIsRequestingLocation] = useState(false);
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
	const [selectedVideoMemo, setSelectedVideoMemo] = useState<PlayableDiaryVideoMemo | null>(null);
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
			message.success('Entry added.');
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
			message.success('Voice memo saved.');
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
			message.success('Voice memo saved.');
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
			message.success('Voice memo deleted.');
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
			message.success('Entry deleted.');
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
				setErrorDetails({
					message: messageText,
					details: messageText,
				});
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

	function requestCurrentLocation() {
		if (!navigator.geolocation) {
			setLocationError('Geolocation is not available in this browser.');
			return;
		}

		setIsRequestingLocation(true);
		navigator.geolocation.getCurrentPosition(
			position => {
				setCurrentLocation(locationFromPosition(position));
				setLocationError(null);
				setIsRequestingLocation(false);
			},
			error => {
				const messageText = formatGeolocationError(error);
				setLocationError(messageText);
				setErrorDetails({
					message: messageText,
					details: messageText,
				});
				setIsRequestingLocation(false);
			},
			{
				enableHighAccuracy: true,
				maximumAge: 60_000,
				timeout: 15_000,
			},
		);
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
			await startLiveTranscriptionIfAvailable(stream);
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
		if (sttWebSocketRef.current?.readyState === WebSocket.OPEN) {
			sttWebSocketRef.current.send(JSON.stringify({ type: 'audio.done' }));
		}
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

			const transcript = sttWebSocketRef.current
				? await waitForStreamingTranscript().catch(() => undefined)
				: undefined;

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

	async function startLiveTranscriptionIfAvailable(stream: MediaStream) {
		try {
			await openLiveTranscription();
			await startPcmStreaming(stream);
		} catch (error) {
			console.warn(error);
			stopLiveTranscription();
			transcriptDoneRef.current = null;
		}
	}

	async function openLiveTranscription() {
		const socketUrl = getDiarySttWebSocketUrl();
		const socket = new WebSocket(socketUrl);
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
		socket.onerror = event => {
			rejectStreamingTranscript(createLiveTranscriptionConnectionError(socket, socketUrl, event));
		};
		socket.onclose = event => {
			const deferred = transcriptDoneRef.current;
			if (deferred && !deferred.isSettled) {
				rejectStreamingTranscript(createLiveTranscriptionConnectionError(socket, socketUrl, event));
			}
		};

		await new Promise<void>((resolve, reject) => {
			socket.onopen = () => resolve();
			socket.onerror = event => {
				const error = createLiveTranscriptionConnectionError(socket, socketUrl, event);
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

	function openVideoMemo(memo: DiaryVoiceMemo | DiaryPendingVoiceMemo) {
		setSelectedVideoMemo({
			id: memo.id,
			fileName: getMemoDisplayFileName(memo),
		});
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
					<Alert
						type='error'
						showIcon
						closable
						message={errorDetails.message}
						description={
							<details>
								<summary>Stack details</summary>
								<pre className='diary-error-details'>{errorDetails.details}</pre>
							</details>
						}
						onClose={() => setErrorDetails(null)}
					/>
				) : null}

				{locationError && !currentLocation ? (
					<Alert
						type='warning'
						showIcon
						message={locationError}
						action={
							<Button size='small' loading={isRequestingLocation} onClick={requestCurrentLocation}>
								Retry location
							</Button>
						}
					/>
				) : null}

				<Card>
					<Space direction='vertical' size={12} style={{ width: '100%' }}>
						<div className='diary-composer-note'>
							<Input.TextArea
								value={notes}
								autoSize={{ minRows: 1, maxRows: 14 }}
								disabled={isSaving || isRecording}
								onChange={event => {
									setNotes(event.target.value);
								}}
							/>
							<Button
								type='primary'
								icon={<PlusOutlined />}
								disabled={!canAddEntry}
								loading={createEntryMutation.isPending}
								className='diary-add-button'
								onClick={() => {
									void handleAddEntry();
								}}
							>
								Add
							</Button>
						</div>

						<Select
							mode='tags'
							value={tagNames}
							options={tagOptions}
							placeholder='Tags'
							disabled={isSaving || isRecording}
							style={{ width: '100%' }}
							tokenSeparators={[',']}
							onChange={setTagNames}
						/>

						<div className='diary-actions'>
							<Button
								icon={isRecording ? <StopOutlined /> : <AudioOutlined />}
								danger={isRecording}
								loading={isStoppingRecording || isParsingVoiceMemo}
								disabled={!currentLocation || createEntryMutation.isPending || isRequestingLocation}
								onClick={() => {
									void handleRecordButton();
								}}
							>
								{isRecording ? 'Stop' : 'Record'}
							</Button>
							{isParsingVoiceMemo ? (
								<Typography.Text type='secondary'>Parsing</Typography.Text>
							) : null}
						</div>
					</Space>
				</Card>

				{pendingVoiceMemos.length > 0 ? (
					<Card
						title='Pending memos'
						extra={
							<Typography.Text type='secondary'>{pendingVoiceMemos.length} pending</Typography.Text>
						}
					>
						<Table<DiaryPendingVoiceMemo>
							rowKey={row => String(row.id)}
							size='small'
							loading={pendingVoiceMemosQuery.isLoading}
							columns={pendingVoiceMemoColumns}
							dataSource={pendingVoiceMemos}
							pagination={false}
							scroll={{ x: 1200 }}
						/>
					</Card>
				) : null}

				{pendingVoiceMemoRecoveries.length > 0 ? (
					<Card
						title='Pending recovery memos'
						extra={
							<Typography.Text type='secondary'>
								{pendingVoiceMemoRecoveries.length} pending
							</Typography.Text>
						}
					>
						<Table<DiaryPendingVoiceMemoRecovery>
							rowKey={row => row.id}
							size='small'
							loading={pendingVoiceMemoRecoveriesQuery.isLoading}
							columns={pendingVoiceMemoRecoveryColumns}
							dataSource={pendingVoiceMemoRecoveries}
							pagination={false}
							scroll={{ x: 1200 }}
						/>
					</Card>
				) : null}

				<Card
					title='Diary'
					extra={<Typography.Text type='secondary'>{entries.length} entries</Typography.Text>}
				>
					<Table<DiaryEntry>
						rowKey={row => String(row.id)}
						size='small'
						loading={entriesQuery.isLoading}
						columns={columns}
						dataSource={entries}
						pagination={false}
						scroll={{ x: 1100 }}
						expandable={{
							expandedRowKeys,
							onExpandedRowsChange: keys => setExpandedRowKeys([...keys]),
							rowExpandable: row => getEntryTranscriptText(row).length > 0,
							expandedRowRender: row => <TranscriptPanel entry={row} />,
						}}
					/>
				</Card>

				<Modal
					open={selectedVideoMemo !== null}
					footer={null}
					width='100vw'
					title={selectedVideoMemo?.fileName ?? 'Video memo'}
					className='diary-video-modal'
					destroyOnHidden
					style={{ top: 0, paddingBottom: 0 }}
					onCancel={() => setSelectedVideoMemo(null)}
				>
					{selectedVideoMemo ? (
						<video
							controls
							autoPlay
							playsInline
							src={voiceMemoVideoUrl(selectedVideoMemo.id)}
							className='diary-video-player'
						/>
					) : null}
				</Modal>
			</div>
		</main>
	);

	function getPendingVoiceMemoRecoveryColumns(): TableColumnsType<DiaryPendingVoiceMemoRecovery> {
		return [
			{
				title: 'When',
				dataIndex: 'createdAt',
				key: 'createdAt',
				width: 180,
				render: (createdAt: string) => (
					<Space direction='vertical' size={0}>
						<Typography.Text>{formatDiaryDate(createdAt)}</Typography.Text>
						<Typography.Text type='secondary'>{formatDiaryTime(createdAt)}</Typography.Text>
					</Space>
				),
			},
			{
				title: 'Status',
				dataIndex: 'status',
				key: 'status',
				width: 140,
				render: (status: string) => <Tag>{status}</Tag>,
			},
			{
				title: 'Metadata',
				key: 'metadata',
				width: 360,
				render: (_: unknown, row: DiaryPendingVoiceMemoRecovery) => (
					<Space direction='vertical' size={0}>
						<Typography.Text>{row.fileName}</Typography.Text>
						<Typography.Text type='secondary'>{row.mimeType}</Typography.Text>
						<Typography.Text type='secondary'>
							{formatDuration(row.durationSeconds)} · {formatBytes(row.audioBytes)}
						</Typography.Text>
						<Typography.Text type='secondary'>{row.metadataPath}</Typography.Text>
						{row.audioPath ? (
							<Typography.Text type='secondary'>{row.audioPath}</Typography.Text>
						) : null}
					</Space>
				),
			},
			{
				title: 'Transcript',
				key: 'transcript',
				width: 320,
				render: (_: unknown, row: DiaryPendingVoiceMemoRecovery) => (
					<MarkdownBlock emptyText='No transcript'>{row.transcript}</MarkdownBlock>
				),
			},
			{
				title: 'Error',
				key: 'error',
				width: 360,
				render: (_: unknown, row: DiaryPendingVoiceMemoRecovery) =>
					row.error ? (
						<details>
							<summary>{row.error.split('\n')[0]}</summary>
							<pre className='diary-error-details'>{row.error}</pre>
						</details>
					) : (
						<Typography.Text type='secondary'>None</Typography.Text>
					),
			},
			{
				title: '',
				key: 'actions',
				width: 130,
				align: 'right',
				render: (_: unknown, row: DiaryPendingVoiceMemoRecovery) => (
					<Button
						size='small'
						icon={<ReloadOutlined />}
						loading={
							processVoiceMemoRecoveryMutation.isPending && reprocessingRecoveryId === row.id
						}
						disabled={processVoiceMemoRecoveryMutation.isPending || isRecording}
						onClick={() => handleReprocessVoiceMemoRecovery(row.id)}
					>
						Reprocess
					</Button>
				),
			},
		];
	}

	function getPendingVoiceMemoColumns(): TableColumnsType<DiaryPendingVoiceMemo> {
		return [
			{
				title: 'When',
				key: 'when',
				width: 280,
				render: (_: unknown, row: DiaryPendingVoiceMemo) => (
					<PendingVoiceMemoMetaCell
						memo={row}
						tagOptions={tagOptions}
						loading={addVoiceMemoTagsMutation.isPending && addingTagsVoiceMemoId === row.id}
						disabled={addVoiceMemoTagsMutation.isPending}
						onAddTags={handleAddTagsToVoiceMemo}
					/>
				),
			},
			{
				title: 'Status',
				dataIndex: 'transcriptionStatus',
				key: 'transcriptionStatus',
				width: 120,
				render: (status: string) => <Tag>{status}</Tag>,
			},
			{
				title: 'Media',
				key: 'media',
				width: 280,
				render: (_: unknown, row: DiaryPendingVoiceMemo) => (
					<PendingVoiceMemoMedia memo={row} onPlayVideo={() => openVideoMemo(row)} />
				),
			},
			{
				title: 'Metadata',
				key: 'metadata',
				width: 300,
				render: (_: unknown, row: DiaryPendingVoiceMemo) => (
					<Space direction='vertical' size={0}>
						<Typography.Text>{getMemoDisplayFileName(row)}</Typography.Text>
						<Typography.Text type='secondary'>{getMemoDisplayMimeType(row)}</Typography.Text>
						<Typography.Text type='secondary'>
							{formatDuration(row.durationSeconds)} · {formatBytes(getMemoDisplayBytes(row))}
						</Typography.Text>
						{row.processedAt ? (
							<Typography.Text type='secondary'>
								processed {formatDiaryTime(row.processedAt)}
							</Typography.Text>
						) : null}
					</Space>
				),
			},
			{
				title: 'Transcript',
				key: 'transcript',
				width: 320,
				render: (_: unknown, row: DiaryPendingVoiceMemo) => (
					<MarkdownBlock emptyText='No transcript'>{row.transcript}</MarkdownBlock>
				),
			},
			{
				title: 'Error',
				key: 'error',
				width: 360,
				render: (_: unknown, row: DiaryPendingVoiceMemo) =>
					row.transcriptionError ? (
						<details>
							<summary>{row.transcriptionError.split('\n')[0]}</summary>
							<pre className='diary-error-details'>{row.transcriptionError}</pre>
						</details>
					) : (
						<Typography.Text type='secondary'>None</Typography.Text>
					),
			},
			{
				title: '',
				key: 'actions',
				width: 160,
				align: 'right',
				render: (_: unknown, row: DiaryPendingVoiceMemo) => (
					<Space size={4}>
						<Button
							size='small'
							icon={<ReloadOutlined />}
							loading={processVoiceMemoMutation.isPending && reprocessingVoiceMemoId === row.id}
							disabled={processVoiceMemoMutation.isPending || deleteVoiceMemoMutation.isPending}
							onClick={() => handleReprocessVoiceMemo(row.id)}
						>
							Reprocess
						</Button>
						<Popconfirm
							title='Delete pending memo?'
							description='This removes the pending memo and its diary entry if it is empty.'
							okText='Delete'
							okButtonProps={{ danger: true }}
							onConfirm={() => handleDeleteVoiceMemo(row.id)}
							disabled={deleteVoiceMemoMutation.isPending}
						>
							<Button
								size='small'
								type='text'
								danger
								icon={<DeleteOutlined />}
								loading={deleteVoiceMemoMutation.isPending && deletingVoiceMemoId === row.id}
								disabled={deleteVoiceMemoMutation.isPending}
							/>
						</Popconfirm>
					</Space>
				),
			},
		];
	}

	function getColumns(): TableColumnsType<DiaryEntry> {
		return [
			{
				title: 'When',
				key: 'when',
				width: 280,
				render: (_: unknown, row: DiaryEntry) => (
					<DiaryEntryMetaCell
						entry={row}
						tagOptions={tagOptions}
						loading={addEntryTagsMutation.isPending && addingTagsEntryId === row.id}
						disabled={addEntryTagsMutation.isPending}
						onAddTags={handleAddTagsToEntry}
					/>
				),
			},
			{
				title: 'Transcript',
				key: 'transcript',
				width: 360,
				render: (_: unknown, row: DiaryEntry) => (
					<TranscriptPreview
						entry={row}
						onClick={() => {
							toggleExpandedRow(row.id);
						}}
					/>
				),
			},
			{
				title: 'Media',
				key: 'voiceMemos',
				width: 280,
				render: (_: unknown, row: DiaryEntry) => (
					<VoiceMemosCell memos={row.voiceMemos} onPlayVideo={openVideoMemo} />
				),
			},
			{
				title: 'Summary',
				key: 'summary',
				width: 360,
				render: (_: unknown, row: DiaryEntry) => (
					<MarkdownBlock emptyText='No summary yet'>{row.summary}</MarkdownBlock>
				),
			},
			{
				title: '',
				key: 'actions',
				width: 72,
				align: 'right',
				render: (_: unknown, row: DiaryEntry) => (
					<Popconfirm
						title='Delete diary entry?'
						description='This removes the entry, transcript, and attached voice memos.'
						okText='Delete'
						okButtonProps={{ danger: true }}
						onConfirm={() => handleDeleteEntry(row.id)}
						disabled={deleteEntryMutation.isPending}
					>
						<Button
							size='small'
							type='text'
							danger
							icon={<DeleteOutlined />}
							loading={deleteEntryMutation.isPending && deletingEntryId === row.id}
							disabled={deleteEntryMutation.isPending}
						/>
					</Popconfirm>
				),
			},
		];
	}
}

function LocationCell(props: { location: DiaryEntry['location'] }) {
	const { location } = props;

	return (
		<Typography.Link href={mapsUrl(location)} target='_blank' rel='noreferrer'>
			{location.name}
		</Typography.Link>
	);
}

function PendingLocationCell(props: { location: DiaryPendingVoiceMemo['location'] }) {
	const { location } = props;
	const name = location.name ?? `${location.latitude}, ${location.longitude}`;

	return (
		<Typography.Link href={mapsUrl(location)} target='_blank' rel='noreferrer'>
			{name}
		</Typography.Link>
	);
}

function DiaryEntryMetaCell(props: {
	entry: DiaryEntry;
	tagOptions: Array<{ label: string; value: string }>;
	loading: boolean;
	disabled: boolean;
	onAddTags: (entryId: number, tagNames: string[]) => void;
}) {
	return (
		<Space direction='vertical' size={6} className='diary-meta-cell'>
			<DiaryTimeCell createdAt={props.entry.createdAt} />
			<LocationCell location={props.entry.location} />
			<DiaryTagList tags={props.entry.tags} />
			<TagAddControl
				targetId={props.entry.id}
				tagOptions={props.tagOptions}
				loading={props.loading}
				disabled={props.disabled}
				onAddTags={props.onAddTags}
			/>
		</Space>
	);
}

function PendingVoiceMemoMetaCell(props: {
	memo: DiaryPendingVoiceMemo;
	tagOptions: Array<{ label: string; value: string }>;
	loading: boolean;
	disabled: boolean;
	onAddTags: (voiceMemoId: number, tagNames: string[]) => void;
}) {
	return (
		<Space direction='vertical' size={6} className='diary-meta-cell'>
			<DiaryTimeCell createdAt={props.memo.createdAt} />
			<PendingLocationCell location={props.memo.location} />
			<DiaryTagList tags={props.memo.tags} />
			<TagAddControl
				targetId={props.memo.id}
				tagOptions={props.tagOptions}
				loading={props.loading}
				disabled={props.disabled}
				onAddTags={props.onAddTags}
			/>
		</Space>
	);
}

function DiaryTimeCell(props: { createdAt: string }) {
	return (
		<Space direction='vertical' size={0}>
			<Typography.Text>{formatDiaryDate(props.createdAt)}</Typography.Text>
			<Typography.Text type='secondary'>{formatDiaryTime(props.createdAt)}</Typography.Text>
		</Space>
	);
}

function DiaryTagList(props: { tags: DiaryEntry['tags'] | DiaryPendingVoiceMemo['tags'] }) {
	if (props.tags.length === 0) {
		return null;
	}

	return (
		<Space size={[4, 4]} wrap>
			{props.tags.map(tag => (
				<Tag key={tag.id} color={tag.color}>
					{tag.name}
				</Tag>
			))}
		</Space>
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
		<Space.Compact>
			<Select
				size='small'
				mode='tags'
				value={selectedTagNames}
				options={props.tagOptions}
				placeholder='Tags'
				loading={props.loading}
				disabled={props.disabled}
				tokenSeparators={[',']}
				maxTagCount={0}
				maxTagPlaceholder={`${selectedTagNames.length} tags`}
				style={{ width: 112 }}
				onChange={setSelectedTagNames}
			/>
			<Button
				size='small'
				icon={<PlusOutlined />}
				loading={props.loading}
				disabled={props.disabled || selectedTagNames.length === 0}
				onClick={() => addTags()}
			/>
		</Space.Compact>
	);
}

function TranscriptPreview(props: { entry: DiaryEntry; onClick: () => void }) {
	const transcript = getEntryTranscriptText(props.entry);

	if (!transcript) {
		return <Typography.Text type='secondary'>No transcript</Typography.Text>;
	}

	return (
		<Typography.Paragraph
			className='diary-transcript-preview'
			ellipsis={{ rows: 12 }}
			onClick={props.onClick}
		>
			{transcript}
		</Typography.Paragraph>
	);
}

function VoiceMemosCell(props: {
	memos: DiaryVoiceMemo[];
	onPlayVideo: (memo: DiaryVoiceMemo) => void;
}) {
	if (props.memos.length === 0) {
		return <Typography.Text type='secondary'>None</Typography.Text>;
	}

	return (
		<Space direction='vertical' size={8} style={{ width: '100%' }}>
			{props.memos.map(memo =>
				memo.mediaKind === 'video' ? (
					<DiaryVideoMemoItem
						key={memo.id}
						memo={memo}
						onPlayVideo={() => props.onPlayVideo(memo)}
					/>
				) : (
					<div key={memo.id} className='diary-voice-memo'>
						<audio controls src={voiceMemoAudioUrl(memo.id)} className='diary-audio' />
					</div>
				),
			)}
		</Space>
	);
}

function PendingVoiceMemoMedia(props: { memo: DiaryPendingVoiceMemo; onPlayVideo: () => void }) {
	if (props.memo.mediaKind === 'video') {
		return <DiaryVideoMemoItem memo={props.memo} onPlayVideo={props.onPlayVideo} />;
	}

	return (
		<div className='diary-voice-memo'>
			<audio controls src={voiceMemoAudioUrl(props.memo.id)} className='diary-audio' />
		</div>
	);
}

function DiaryVideoMemoItem(props: {
	memo: DiaryVoiceMemo | DiaryPendingVoiceMemo;
	onPlayVideo: () => void;
}) {
	const fileName = getMemoDisplayFileName(props.memo);
	const duration = getDurationBadge(props.memo.durationSeconds);

	return (
		<button
			type='button'
			className='diary-video-memo'
			aria-label={`Play ${fileName}`}
			onClick={props.onPlayVideo}
		>
			<span className='diary-video-thumb-wrap'>
				<video
					muted
					playsInline
					preload='metadata'
					src={`${voiceMemoVideoUrl(props.memo.id)}#t=0.001`}
					className='diary-video-thumb'
				/>
				{duration ? <span className='diary-video-duration'>{duration}</span> : null}
			</span>
		</button>
	);
}

function TranscriptPanel(props: { entry: DiaryEntry }) {
	const transcript = getEntryTranscriptText(props.entry);

	return (
		<Space direction='vertical' size={8} style={{ width: '100%' }}>
			<Typography.Text type='secondary'>{formatDiaryTime(props.entry.createdAt)}</Typography.Text>
			<MarkdownBlock>{transcript}</MarkdownBlock>
		</Space>
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
		return <Typography.Text type='secondary'>{props.emptyText ?? 'Empty'}</Typography.Text>;
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
	const apiUrl = new URL(import.meta.env.VITE_API_URL.trim());
	const host = apiUrl.hostname.endsWith('.localhost') ? 'localhost' : apiUrl.hostname;
	const port = apiUrl.port || (host === 'localhost' ? '6001' : '');
	const url = new URL('/diary/stt/live', `${apiUrl.protocol}//${host}${port ? `:${port}` : ''}`);
	url.protocol = 'ws:';
	return withAuthToken(url.toString());
}

function createLiveTranscriptionConnectionError(
	socket: WebSocket,
	socketUrl: string,
	event: Event | CloseEvent,
) {
	const details = [
		`url=${socketUrl}`,
		`readyState=${formatWebSocketReadyState(socket.readyState)}`,
		`page=${window.location.href}`,
		`secureContext=${String(window.isSecureContext)}`,
		`online=${String(navigator.onLine)}`,
		`eventType=${event.type}`,
		event instanceof CloseEvent ? `closeCode=${event.code}` : null,
		event instanceof CloseEvent && event.reason ? `closeReason=${event.reason}` : null,
	]
		.filter(Boolean)
		.join('\n');

	return new Error(`Live transcription connection failed.\n${details}`);
}

function formatWebSocketReadyState(readyState: number) {
	switch (readyState) {
		case WebSocket.CONNECTING:
			return 'CONNECTING';
		case WebSocket.OPEN:
			return 'OPEN';
		case WebSocket.CLOSING:
			return 'CLOSING';
		case WebSocket.CLOSED:
			return 'CLOSED';
		default:
			return String(readyState);
	}
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

function voiceMemoAudioUrl(voiceMemoId: number) {
	return withAuthToken(`/api/asset/diary_voice_memos/${voiceMemoId}/audio`);
}

function voiceMemoVideoUrl(voiceMemoId: number) {
	return withAuthToken(`/api/asset/diary_voice_memos/${voiceMemoId}/video`);
}

function getMemoDisplayFileName(
	memo: Pick<DiaryVoiceMemo | DiaryPendingVoiceMemo, 'mediaKind' | 'fileName' | 'videoFileName'>,
) {
	return memo.mediaKind === 'video' ? (memo.videoFileName ?? memo.fileName) : memo.fileName;
}

function getMemoDisplayMimeType(
	memo: Pick<DiaryPendingVoiceMemo, 'mediaKind' | 'mimeType' | 'videoMimeType'>,
) {
	return memo.mediaKind === 'video' ? (memo.videoMimeType ?? memo.mimeType) : memo.mimeType;
}

function getMemoDisplayBytes(
	memo: Pick<DiaryPendingVoiceMemo, 'mediaKind' | 'audioBytes' | 'videoBytes'>,
) {
	return memo.mediaKind === 'video' ? memo.videoBytes : memo.audioBytes;
}

function getDurationBadge(value: number | null) {
	if (!Number.isFinite(value)) {
		return null;
	}

	return formatDuration(value);
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
