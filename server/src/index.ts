import path from 'path';

import { fetchRequestHandler } from '@trpc/server/adapters/fetch';

import { getBloodworkDocumentPdf, startBloodworkProcessor } from 'server/db/bloodwork.ts';
import { getDatabase } from 'server/db/client.ts';
import env from 'server/env.ts';
import { appRouter, createTrpcContext } from 'server/trpc/index.ts';

const port = env.API_PORT;
const projectRoot = path.resolve(import.meta.dir, '..', '..');

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

function getBloodworkDocumentPdfResponse(req: Request) {
	const match = new URL(req.url).pathname.match(/^\/bloodwork\/documents\/(\d+)\/pdf$/);
	if (!match) {
		return null;
	}

	const documentId = Number.parseInt(match[1] ?? '', 10);
	if (!Number.isFinite(documentId) || documentId <= 0) {
		return Response.json({ ok: false, error: 'Invalid document id' }, { status: 400 });
	}

	const document = getBloodworkDocumentPdf(getDatabase(), documentId);
	if (!document) {
		return Response.json({ ok: false, error: 'Document not found' }, { status: 404 });
	}

	const headers = new Headers(getCorsHeaders(req));
	headers.set('Content-Type', document.mimeType || 'application/pdf');
	headers.set('Content-Disposition', `inline; filename="${document.fileName.replace(/"/g, '')}"`);

	return new Response(new Uint8Array(document.pdfData), {
		status: 200,
		headers,
	});
}

await Bun.$`bunx drizzle-kit push --config drizzle.config.ts --force`.cwd(projectRoot);
startBloodworkProcessor();

const server = Bun.serve({
	development: true,
	port,
	routes: {
		'/status': Response.json({ ok: true }),
		'/trpc/*': async req => {
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
		'/bloodwork/documents/*': req =>
			getBloodworkDocumentPdfResponse(req) ??
			Response.json({ ok: false, error: 'Not found' }, { status: 404 }),
		'/*': Response.json({ ok: false, error: 'Not found' }, { status: 404 }),
	},
});

console.log('Server running at:', server.url);
