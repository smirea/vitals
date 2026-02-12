if (!process.env.API_PORT) throw new Error('process.env.API_PORT is not set');

const server = Bun.serve({
    development: true,
    port: process.env.API_PORT,
    routes: {
        '/status': Response.json({ ok: true }),
        '/*':  Response.json({ ok: false, error: 'Not found' }, { status: 404 }),
    },
});

console.log('Server running at:', server.url);
