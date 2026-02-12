import { defineConfig } from 'vite';
// import MillionLint from "@million/lint";
import react from '@vitejs/plugin-react';
import { tanstackRouter } from '@tanstack/router-plugin/vite';
import tsconfigPaths from 'vite-tsconfig-paths';
import tailwindcss from '@tailwindcss/vite';

const clientPort = Number(process.env.CLIENT_PORT);
if (!Number.isFinite(clientPort)) {
    throw new Error('process.env.CLIENT_PORT must be a number');
}

// https://vite.dev/config/
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
        react({
            jsxImportSource: '@emotion/react',
            babel: {
                plugins: [
                    // ['babel-plugin-react-compiler', {}],
                    [
                        '@emotion/babel-plugin',
                        {
                            sourceMap: true,
                            autoLabel: 'always',
                            labelFormat: '[dirname]_[filename]_[local]',
                        },
                    ],
                ],
            },
        }),
        tailwindcss() as any,
    ],
});
