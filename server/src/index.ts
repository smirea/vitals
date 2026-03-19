import { fetchRequestHandler } from '@trpc/server/adapters/fetch';

import { PROJECT_ROOT } from 'scripts/project-paths.ts';
import { appRouter, createTrpcContext } from 'server/trpc/index.ts';

const port = Number(process.env.API_PORT);
if (!Number.isFinite(port)) {
    throw new Error('process.env.API_PORT must be a number');
}

await Bun.$`bunx drizzle-kit push --config drizzle.config.ts --force`.cwd(PROJECT_ROOT);

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
