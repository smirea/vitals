import { fetchRequestHandler } from '@trpc/server/adapters/fetch';

import { PROJECT_ROOT } from 'scripts/project-paths.ts';
import { appRouter, createTrpcContext } from 'server/trpc/index.ts';

const port = Number(process.env.API_PORT);
if (!Number.isFinite(port)) {
	throw new Error('process.env.API_PORT must be a number');
}

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

await Bun.$`bunx drizzle-kit push --config drizzle.config.ts --force`.cwd(PROJECT_ROOT);

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
		'/*': Response.json({ ok: false, error: 'Not found' }, { status: 404 }),
	},
});

console.log('Server running at:', server.url);
