import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { tanstackRouter } from '@tanstack/router-plugin/vite';
import tsconfigPaths from 'vite-tsconfig-paths';
import tailwindcss from '@tailwindcss/vite';

const clientPort = Number(process.env.CLIENT_PORT);
if (!Number.isFinite(clientPort)) {
    throw new Error('process.env.CLIENT_PORT must be a number');
}

export default defineConfig({
    server: {
        port: clientPort,
        strictPort: true,
        proxy: {
            '/api': {
                target: process.env.VITE_API_URL,
                changeOrigin: true,
                rewrite: (path: string) => path.replace(/^\/api/, ''),
            },
        },
    },
    plugins: [
        tsconfigPaths(),
        tanstackRouter({
            target: 'react',
            autoCodeSplitting: true,
        }) as any,
        react(),
        tailwindcss() as any,
    ],
});
