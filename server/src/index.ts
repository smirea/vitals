import { fetchRequestHandler } from '@trpc/server/adapters/fetch';

import { pushDatabaseSchema } from 'server/db/push.ts';
import { appRouter, createTrpcContext } from 'server/trpc/index.ts';

const port = Number(process.env.API_PORT);
if (!Number.isFinite(port)) {
    throw new Error('process.env.API_PORT must be a number');
}

await pushDatabaseSchema();

const server = Bun.serve({
    development: true,
    port,
    routes: {
        '/status': Response.json({ ok: true }),
        '/trpc/*': req => fetchRequestHandler({
            endpoint: '/trpc',
            req,
            router: appRouter,
            createContext: () => createTrpcContext(),
        }),
        '/*': Response.json({ ok: false, error: 'Not found' }, { status: 404 }),
    },
});

console.log('Server running at:', server.url);
