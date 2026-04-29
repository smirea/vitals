import { AudioOutlined, DeleteOutlined, PlusOutlined, StopOutlined } from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import {
	Button,
	Card,
	Input,
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

import type { DiaryEntry, DiaryVoiceMemo } from '../utils/api';
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
	const mediaRecorderRef = useRef<MediaRecorder | null>(null);
	const mediaStreamRef = useRef<MediaStream | null>(null);
	const sttWebSocketRef = useRef<WebSocket | null>(null);
	const audioContextRef = useRef<AudioContext | null>(null);
	const audioSourceRef = useRef<MediaStreamAudioSourceNode | null>(null);
	const audioProcessorRef = useRef<ScriptProcessorNode | null>(null);
	const recordingChunksRef = useRef<Blob[]>([]);
	const recordingStartedAtRef = useRef<number | null>(null);
	const finalizedTranscriptRef = useRef('');
	const interimTranscriptRef = useRef('');
	const transcriptDoneRef = useRef<StreamingTranscriptDeferred | null>(null);

	const entriesQuery = useQuery(trpc.diary.list.queryOptions());
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
			message.error(error.message);
		},
		onSettled: () => {
			void invalidateDiary();
		},
	});

	const uploadVoiceMemoMutation = useMutation({
		...trpc.diary.uploadVoiceMemo.mutationOptions(),
		onSuccess: async () => {
			await invalidateDiary();
			setNotes('');
			setTagNames([]);
			message.success('Voice memo saved.');
		},
		onSettled: () => {
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
			message.error(error.message);
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
				message.error(messageText);
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

	const isSaving = createEntryMutation.isPending || uploadVoiceMemoMutation.isPending;
	const canAddEntry = notes.trim().length > 0 && currentLocation !== null && !isSaving;
	const entries = entriesQuery.data ?? [];
	const columns = getColumns();

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

			recordingChunksRef.current = [];
			recordingStartedAtRef.current = Date.now();
			mediaRecorderRef.current = recorder;

			recorder.ondataavailable = event => {
				if (event.data.size > 0) {
					recordingChunksRef.current.push(event.data);
				}
			};
			recorder.onerror = event => {
				message.error(event.error.message);
			};
			recorder.onstop = () => {
				void uploadStoppedRecording(recorder.mimeType || mimeType || 'audio/webm');
			};

			recorder.start();
			setIsRecording(true);
		} catch (error) {
			message.error(error instanceof Error ? error.message : String(error));
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

	async function uploadStoppedRecording(mimeType: string) {
		try {
			setIsParsingVoiceMemo(true);
			const chunks = recordingChunksRef.current;
			if (chunks.length === 0) {
				throw new Error('Recording did not produce any audio data.');
			}

			const blob = new Blob(chunks, { type: mimeType });
			const startedAt = recordingStartedAtRef.current ?? Date.now();
			const durationSeconds = Math.max((Date.now() - startedAt) / 1000, 0.1);
			const dataBase64 = await blobToBase64(blob);
			const extension = extensionFromMimeType(mimeType);
			const fileName = `diary-${new Date().toISOString().replace(/[:.]/g, '-')}.${extension}`;
			const transcript = await waitForStreamingTranscript();

			await uploadVoiceMemoMutation.mutateAsync({
				notes: '',
				transcript,
				tagNames,
				location: getRequiredLocation(),
				fileName,
				mimeType,
				dataBase64,
				durationSeconds,
			});
		} catch (error) {
			message.error(error instanceof Error ? error.message : String(error));
		} finally {
			recordingChunksRef.current = [];
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
								disabled={!currentLocation || createEntryMutation.isPending}
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
						scroll={{ x: 1280 }}
						expandable={{
							expandedRowKeys,
							onExpandedRowsChange: keys => setExpandedRowKeys([...keys]),
							rowExpandable: row => getEntryTranscriptText(row).length > 0,
							expandedRowRender: row => <TranscriptPanel entry={row} />,
						}}
					/>
				</Card>
			</div>
		</main>
	);

	function getColumns(): TableColumnsType<DiaryEntry> {
		return [
			{
				title: 'When',
				dataIndex: 'createdAt',
				key: 'createdAt',
				width: 140,
				render: (createdAt: string) => (
					<Typography.Text>{formatDiaryDate(createdAt)}</Typography.Text>
				),
			},
			{
				title: 'Location',
				key: 'location',
				width: 180,
				render: (_: unknown, row: DiaryEntry) => <LocationCell location={row.location} />,
			},
			{
				title: 'Tags',
				key: 'tags',
				width: 160,
				render: (_: unknown, row: DiaryEntry) =>
					row.tags.length > 0 ? (
						<Space size={[4, 4]} wrap>
							{row.tags.map(tag => (
								<Tag key={tag.id} color={tag.color}>
									{tag.name}
								</Tag>
							))}
						</Space>
					) : (
						<Typography.Text type='secondary'>No tags</Typography.Text>
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
				title: 'Voice memos',
				key: 'voiceMemos',
				width: 300,
				render: (_: unknown, row: DiaryEntry) => <VoiceMemosCell memos={row.voiceMemos} />,
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

function TranscriptPreview(props: { entry: DiaryEntry; onClick: () => void }) {
	const transcript = getEntryTranscriptText(props.entry);

	if (!transcript) {
		return <Typography.Text type='secondary'>No transcript</Typography.Text>;
	}

	return (
		<Typography.Paragraph
			className='diary-transcript-preview'
			ellipsis={{ rows: 2 }}
			onClick={props.onClick}
		>
			{transcript}
		</Typography.Paragraph>
	);
}

function VoiceMemosCell(props: { memos: DiaryVoiceMemo[] }) {
	if (props.memos.length === 0) {
		return <Typography.Text type='secondary'>None</Typography.Text>;
	}

	return (
		<Space direction='vertical' size={8} style={{ width: '100%' }}>
			{props.memos.map(memo => (
				<div key={memo.id} className='diary-voice-memo'>
					<audio controls src={`/api/diary/voice-memos/${memo.id}/audio`} className='diary-audio' />
				</div>
			))}
		</Space>
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

function mapsUrl(location: DiaryEntry['location']) {
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
