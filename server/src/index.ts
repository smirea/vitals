import { fetchRequestHandler } from '@trpc/server/adapters/fetch';

import { resolveAssetByRecord, type ResolvedAsset } from 'server/db/assets.ts';
import { getDatabase } from 'server/db/client.ts';
import env from 'server/env.ts';
import { appRouter, createTrpcContext } from 'server/trpc/index.ts';
import { startLabProcessor } from 'server/trpc/routers/labs.ts';
import { type AssetKind, type AssetTable } from 'server/utils/assetUrls.ts';
import { getS3Asset, getSignedS3AssetUrl, s3BodyToReadableStream } from 'server/utils/s3Assets.ts';
import { s3PathUtil } from 'shared/s3PathUtil.ts';

const port = env.API_PORT;
const elevenLabsRealtimeSpeechToTextUrl = new URL(
	'wss://api.elevenlabs.io/v1/speech-to-text/realtime',
);
elevenLabsRealtimeSpeechToTextUrl.searchParams.set('model_id', 'scribe_v2_realtime');
elevenLabsRealtimeSpeechToTextUrl.searchParams.set('audio_format', 'pcm_16000');
elevenLabsRealtimeSpeechToTextUrl.searchParams.set('language_code', 'en');
elevenLabsRealtimeSpeechToTextUrl.searchParams.set('commit_strategy', 'manual');
const ELEVENLABS_AUDIO_CHUNK_BYTES = 3_200;
const elevenLabsErrorMessageTypes = new Set([
	'auth_error',
	'quota_exceeded',
	'transcriber_error',
	'input_error',
	'error',
	'commit_throttled',
	'unaccepted_terms',
	'rate_limited',
	'queue_overflow',
	'resource_exhausted',
	'session_time_limit_exceeded',
	'chunk_size_exceeded',
	'insufficient_audio_activity',
]);

type DiarySttSocketData = {
	kind: 'diary-stt';
	elevenLabsSocket: WebSocket | null;
	isElevenLabsReady: boolean;
	isCommitRequested: boolean;
	isClosed: boolean;
	pendingAudioChunks: Uint8Array[];
	audioBufferChunks: Uint8Array[];
	audioBufferByteLength: number;
	committedTranscript: string;
};
type BunWebSocketConstructor = new (url: string | URL, options?: Bun.WebSocketOptions) => WebSocket;
type ElevenLabsSttEvent = {
	message_type?: string;
	text?: string;
	message?: string;
	error?: string;
	detail?: string;
};

function getCorsHeaders(req: Request) {
	const origin = req.headers.get('origin');
	const allowedOrigin = origin?.startsWith('http://localhost:') ? origin : '*';

	return {
		'Access-Control-Allow-Origin': allowedOrigin,
		'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
		'Access-Control-Allow-Headers':
			req.headers.get('access-control-request-headers') ?? 'content-type',
	};
}

const assetTables = new Set<AssetTable>(['lab_documents', 'pill_images', 'diary_voice_memos']);
const assetKinds = new Set<AssetKind>(['pdf', 'image', 'audio', 'video']);

function parsePositiveInteger(value: string | undefined, label: string) {
	const id = Number.parseInt(value ?? '', 10);
	if (!Number.isFinite(id) || id <= 0) {
		throw new Error(`Invalid ${label}`);
	}
	return id;
}

async function getAssetDeliveryResponse(req: Request, asset: ResolvedAsset) {
	const url = new URL(req.url);
	if (url.searchParams.get('proxy') !== 'true') {
		const signedUrl = await getSignedS3AssetUrl(asset.s3Path);
		const headers = new Headers(getCorsHeaders(req));
		headers.set('Cache-Control', 'no-store');
		headers.set('Location', signedUrl);
		return new Response(null, {
			status: 302,
			headers,
		});
	}

	const object = await getS3Asset({
		s3Path: asset.s3Path,
		range: req.headers.get('range'),
	});
	const headers = new Headers(getCorsHeaders(req));
	headers.set('Content-Type', object.ContentType ?? asset.mimeType);
	headers.set('Content-Disposition', getInlineContentDisposition(asset.fileName));
	headers.set('Cache-Control', 'no-store');
	if (object.ContentLength !== undefined) {
		headers.set('Content-Length', String(object.ContentLength));
	}
	if (object.ContentRange) {
		headers.set('Content-Range', object.ContentRange);
	}
	if (object.ETag) {
		headers.set('ETag', object.ETag);
	}
	if (object.LastModified) {
		headers.set('Last-Modified', object.LastModified.toUTCString());
	}
	headers.set('Accept-Ranges', object.AcceptRanges ?? 'bytes');

	return new Response(s3BodyToReadableStream(object.Body), {
		status: object.ContentRange ? 206 : 200,
		headers,
	});
}

function getInlineContentDisposition(fileName: string) {
	const fallbackFileName = fileName.replace(/[^\x20-\x7E]/g, '_').replace(/"/g, '');
	return `inline; filename="${fallbackFileName}"; filename*=UTF-8''${encodeURIComponent(fileName)}`;
}

async function getAssetResponse(req: Request) {
	const pathname = new URL(req.url).pathname;
	const s3Match = pathname.match(/^\/asset\/s3\/([^/]+)\/(.+)$/);
	if (s3Match) {
		const bucket = decodeURIComponent(s3Match[1] ?? '');
		const key = decodeURIComponent(s3Match[2] ?? '');
		const s3Path = s3PathUtil.create(bucket, key);
		const fileName = key.split('/').at(-1) ?? 'asset';
		return getAssetDeliveryResponse(req, {
			s3Path,
			fileName,
			mimeType: 'application/octet-stream',
			sizeBytes: 0,
		});
	}

	const match = pathname.match(/^\/asset\/([^/]+)\/(\d+)(?:\/([^/]+))?$/);
	if (!match) {
		return null;
	}

	const table = match[1] as AssetTable | undefined;
	const id = parsePositiveInteger(match[2], 'asset id');
	const kind = match[3] as AssetKind | undefined;
	if (!table || !assetTables.has(table)) {
		return Response.json({ ok: false, error: 'Invalid asset table' }, { status: 400 });
	}
	if (kind && !assetKinds.has(kind)) {
		return Response.json({ ok: false, error: 'Invalid asset kind' }, { status: 400 });
	}

	const asset = resolveAssetByRecord(getDatabase(), table, id, kind);
	if (!asset) {
		return Response.json({ ok: false, error: 'Asset not found' }, { status: 404 });
	}

	return getAssetDeliveryResponse(req, asset);
}

async function getLegacyLabDocumentPdfResponse(req: Request) {
	const match = new URL(req.url).pathname.match(/^\/labs\/documents\/(\d+)\/pdf$/);
	if (!match) return null;

	const documentId = parsePositiveInteger(match[1], 'document id');
	const asset = resolveAssetByRecord(getDatabase(), 'lab_documents', documentId, 'pdf');
	if (!asset) return Response.json({ ok: false, error: 'Document not found' }, { status: 404 });
	return getAssetDeliveryResponse(req, asset);
}

async function getLegacyStaticImageResponse(req: Request) {
	const match = new URL(req.url).pathname.match(/^\/db-image\/pill_images\/(\d+)$/);
	if (!match) return null;

	const id = parsePositiveInteger(match[1], 'image id');
	const asset = resolveAssetByRecord(getDatabase(), 'pill_images', id, 'image');
	if (!asset) return Response.json({ ok: false, error: 'Image not found' }, { status: 404 });
	return getAssetDeliveryResponse(req, asset);
}

async function getLegacyDiaryVoiceMemoResponse(req: Request) {
	const match = new URL(req.url).pathname.match(/^\/diary\/voice-memos\/(\d+)\/(audio|video)$/);
	if (!match) return null;

	const voiceMemoId = parsePositiveInteger(match[1], 'voice memo id');
	const kind = match[2] as 'audio' | 'video';
	const asset = resolveAssetByRecord(getDatabase(), 'diary_voice_memos', voiceMemoId, kind);
	if (!asset) {
		return Response.json({ ok: false, error: 'Voice memo asset not found' }, { status: 404 });
	}
	return getAssetDeliveryResponse(req, asset);
}

function createDiarySttSocketData(): DiarySttSocketData {
	return {
		kind: 'diary-stt',
		elevenLabsSocket: null,
		isElevenLabsReady: false,
		isCommitRequested: false,
		isClosed: false,
		pendingAudioChunks: [],
		audioBufferChunks: [],
		audioBufferByteLength: 0,
		committedTranscript: '',
	};
}

function openDiarySttProxy(ws: Bun.ServerWebSocket<DiarySttSocketData>) {
	const BunWebSocket = WebSocket as unknown as BunWebSocketConstructor;
	const elevenLabsSocket = new BunWebSocket(elevenLabsRealtimeSpeechToTextUrl, {
		headers: {
			'xi-api-key': env.ELEVENLABS_API_KEY,
		},
	});
	ws.data.elevenLabsSocket = elevenLabsSocket;

	elevenLabsSocket.addEventListener('message', event => {
		const text =
			typeof event.data === 'string'
				? event.data
				: new TextDecoder().decode(event.data as ArrayBuffer | Uint8Array);
		handleElevenLabsSttEvent(ws, JSON.parse(text) as ElevenLabsSttEvent);
	});

	elevenLabsSocket.addEventListener('error', () => {
		closeDiarySttWithError(ws, 'ElevenLabs streaming transcription failed.');
	});

	elevenLabsSocket.addEventListener('close', () => {
		if (!ws.data.isClosed) {
			ws.data.isClosed = true;
			ws.close();
		}
	});
}

function handleDiarySttClientMessage(
	ws: Bun.ServerWebSocket<DiarySttSocketData>,
	message: string | Buffer,
) {
	if (typeof message === 'string') {
		const payload = JSON.parse(message) as { type?: string; dataBase64?: string };
		if (payload.type === 'audio.done') {
			requestDiarySttCommit(ws);
			return;
		}
		if (payload.type === 'audio.chunk' && payload.dataBase64) {
			queueDiarySttAudio(ws, Buffer.from(payload.dataBase64, 'base64'));
			return;
		}

		closeDiarySttWithError(ws, `Unsupported diary STT message: ${payload.type ?? 'unknown'}`);
		return;
	}

	queueDiarySttAudio(ws, new Uint8Array(message));
}

function handleElevenLabsSttEvent(
	ws: Bun.ServerWebSocket<DiarySttSocketData>,
	payload: ElevenLabsSttEvent,
) {
	switch (payload.message_type) {
		case 'session_started':
			ws.data.isElevenLabsReady = true;
			ws.send(JSON.stringify({ type: 'transcript.created' }));
			flushDiarySttPendingAudio(ws);
			if (ws.data.isCommitRequested) {
				commitDiarySttAudio(ws);
			}
			return;
		case 'partial_transcript':
			ws.send(JSON.stringify({ type: 'transcript.partial', text: payload.text ?? '' }));
			return;
		case 'committed_transcript':
		case 'committed_transcript_with_timestamps':
			ws.data.committedTranscript = appendTranscript(
				ws.data.committedTranscript,
				payload.text ?? '',
			);
			ws.send(
				JSON.stringify({
					type: 'transcript.partial',
					text: payload.text ?? '',
					is_final: true,
				}),
			);
			if (ws.data.isCommitRequested) {
				closeDiarySttWithTranscript(ws);
			}
			return;
		default:
			if (payload.message_type && elevenLabsErrorMessageTypes.has(payload.message_type)) {
				closeDiarySttWithError(ws, getElevenLabsErrorMessage(payload));
				return;
			}
	}
}

function queueDiarySttAudio(ws: Bun.ServerWebSocket<DiarySttSocketData>, chunk: Uint8Array) {
	const elevenLabsSocket = ws.data.elevenLabsSocket;
	if (
		!elevenLabsSocket ||
		!ws.data.isElevenLabsReady ||
		elevenLabsSocket.readyState !== WebSocket.OPEN
	) {
		ws.data.pendingAudioChunks.push(chunk);
		return;
	}

	bufferDiarySttAudio(ws, chunk);
}

function flushDiarySttPendingAudio(ws: Bun.ServerWebSocket<DiarySttSocketData>) {
	for (const chunk of ws.data.pendingAudioChunks) {
		bufferDiarySttAudio(ws, chunk);
	}
	ws.data.pendingAudioChunks = [];
}

function bufferDiarySttAudio(ws: Bun.ServerWebSocket<DiarySttSocketData>, chunk: Uint8Array) {
	ws.data.audioBufferChunks.push(chunk);
	ws.data.audioBufferByteLength += chunk.byteLength;

	if (ws.data.audioBufferByteLength >= ELEVENLABS_AUDIO_CHUNK_BYTES) {
		flushDiarySttAudio(ws, false);
	}
}

function requestDiarySttCommit(ws: Bun.ServerWebSocket<DiarySttSocketData>) {
	ws.data.isCommitRequested = true;
	if (!ws.data.isElevenLabsReady) {
		return;
	}

	flushDiarySttPendingAudio(ws);
	commitDiarySttAudio(ws);
}

function commitDiarySttAudio(ws: Bun.ServerWebSocket<DiarySttSocketData>) {
	flushDiarySttAudio(ws, true);
}

function flushDiarySttAudio(ws: Bun.ServerWebSocket<DiarySttSocketData>, commit: boolean) {
	const elevenLabsSocket = ws.data.elevenLabsSocket;
	if (!elevenLabsSocket || elevenLabsSocket.readyState !== WebSocket.OPEN) {
		return;
	}

	const audioData = concatUint8Arrays(ws.data.audioBufferChunks, ws.data.audioBufferByteLength);
	ws.data.audioBufferChunks = [];
	ws.data.audioBufferByteLength = 0;
	elevenLabsSocket.send(
		JSON.stringify({
			message_type: 'input_audio_chunk',
			audio_base_64: Buffer.from(audioData).toString('base64'),
			sample_rate: 16_000,
			...(commit ? { commit: true } : {}),
		}),
	);
}

function closeDiarySttWithTranscript(ws: Bun.ServerWebSocket<DiarySttSocketData>) {
	if (ws.data.isClosed) {
		return;
	}

	ws.data.isClosed = true;
	ws.send(JSON.stringify({ type: 'transcript.done', text: ws.data.committedTranscript }));
	ws.data.elevenLabsSocket?.close();
	ws.close();
}

function closeDiarySttWithError(ws: Bun.ServerWebSocket<DiarySttSocketData>, message: string) {
	if (ws.data.isClosed) {
		return;
	}

	ws.data.isClosed = true;
	ws.send(JSON.stringify({ type: 'error', message }));
	ws.data.elevenLabsSocket?.close();
	ws.close();
}

function getElevenLabsErrorMessage(payload: ElevenLabsSttEvent) {
	const message = payload.message ?? payload.error ?? payload.detail;
	return message
		? `ElevenLabs transcription failed: ${message}`
		: `ElevenLabs transcription failed: ${payload.message_type ?? 'unknown error'}`;
}

function concatUint8Arrays(chunks: Uint8Array[], byteLength: number) {
	const output = new Uint8Array(byteLength);
	let offset = 0;
	for (const chunk of chunks) {
		output.set(chunk, offset);
		offset += chunk.byteLength;
	}

	return output;
}

function appendTranscript(existingText: string, nextText: string) {
	const existing = existingText.replace(/\s+/g, ' ').trim();
	const next = nextText.replace(/\s+/g, ' ').trim();

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

startLabProcessor();

const server = Bun.serve<DiarySttSocketData>({
	development: true,
	port,
	idleTimeout: 255,
	routes: {
		'/status': Response.json({ ok: true }),
		'/diary/stt/live': (req: Request, server: Bun.Server<DiarySttSocketData>) => {
			if (req.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
				return Response.json({ ok: false, error: 'Expected WebSocket upgrade' }, { status: 400 });
			}

			const upgraded = server.upgrade(req, {
				data: createDiarySttSocketData(),
			});

			if (!upgraded) {
				return Response.json({ ok: false, error: 'WebSocket upgrade failed' }, { status: 400 });
			}
		},
		'/trpc/*': async (req: Request) => {
			if (req.method === 'OPTIONS') {
				return new Response(null, {
					status: 204,
					headers: getCorsHeaders(req),
				});
			}

			const response = await fetchRequestHandler({
				endpoint: '/trpc',
				req,
				router: appRouter,
				createContext: () => createTrpcContext(),
			});

			const nextHeaders = new Headers(response.headers);
			for (const [key, value] of Object.entries(getCorsHeaders(req))) {
				nextHeaders.set(key, value);
			}

			return new Response(response.body, {
				status: response.status,
				statusText: response.statusText,
				headers: nextHeaders,
			});
		},
		'/asset/*': async (req: Request) =>
			(await getAssetResponse(req)) ??
			Response.json({ ok: false, error: 'Not found' }, { status: 404 }),
		'/labs/documents/*': async (req: Request) =>
			(await getLegacyLabDocumentPdfResponse(req)) ??
			Response.json({ ok: false, error: 'Not found' }, { status: 404 }),
		'/db-image/*': async (req: Request) =>
			(await getLegacyStaticImageResponse(req)) ??
			Response.json({ ok: false, error: 'Not found' }, { status: 404 }),
		'/diary/voice-memos/*': async (req: Request) =>
			(await getLegacyDiaryVoiceMemoResponse(req)) ??
			Response.json({ ok: false, error: 'Not found' }, { status: 404 }),
		'/*': Response.json({ ok: false, error: 'Not found' }, { status: 404 }),
	},
	websocket: {
		data: createDiarySttSocketData(),
		open: openDiarySttProxy,
		message: handleDiarySttClientMessage,
		close: ws => {
			ws.data.isClosed = true;
			ws.data.elevenLabsSocket?.close();
		},
	},
});

console.log('Server running at:', server.url);
