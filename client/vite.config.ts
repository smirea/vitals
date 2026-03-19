import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { tanstackRouter } from '@tanstack/router-plugin/vite';
import tsconfigPaths from 'vite-tsconfig-paths';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig(({ command }) => {
    const clientPort = Number(process.env.CLIENT_PORT);

    if (command === 'serve' && !Number.isFinite(clientPort)) {
        throw new Error('process.env.CLIENT_PORT must be a number');
    }

    return {
        server: command === 'serve' ? {
            port: clientPort,
            strictPort: true,
            proxy: {
                '/api': {
                    target: process.env.VITE_API_URL,
                    changeOrigin: true,
                    rewrite: (path: string) => path.replace(/^\/api/, ''),
                },
            },
        } : undefined,
        plugins: [
            tsconfigPaths(),
            tanstackRouter({
                target: 'react',
                autoCodeSplitting: true,
            }) as any,
            react(),
            tailwindcss() as any,
        ],
    };
});
