import path from 'path';

import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import { tanstackRouter } from '@tanstack/router-plugin/vite';
import tsconfigPaths from 'vite-tsconfig-paths';
import { z } from 'zod';

const devEnvSchema = z.object({
	API_PORT: z.coerce.number().int().min(1).max(65535),
	CLIENT_PORT: z.coerce.number().int().min(1).max(65535),
	VITE_HOST: z.string(),
});

const buildEnvSchema = z.object({
	VITE_API_URL: z.url(),
});

export default defineConfig(async ({ command, mode }) => {
	const projectRoot = path.resolve(import.meta.dirname, '..');

	Object.assign(process.env, loadEnv(mode, projectRoot, ''));
	const plugins = [
		tsconfigPaths(),
		tanstackRouter({
			target: 'react',
			autoCodeSplitting: true,
			routeFileIgnorePattern: '(^|/)_[^_].+',
		}) as any,
		react(),
	];

	if (command !== 'serve') {
		buildEnvSchema.parse(process.env);
		return { plugins };
	}

	const devEnv = devEnvSchema.parse(process.env);

	return {
		server: {
			port: devEnv.CLIENT_PORT,
			strictPort: true,
			allowedHosts: [devEnv.VITE_HOST],
			proxy: {
				'/api': {
					target: `http://127.0.0.1:${devEnv.API_PORT}`,
					changeOrigin: true,
					secure: false,
					ws: true,
					rewrite: (path: string) => path.replace(/^\/api/, ''),
				},
			},
		},
		plugins,
	};
});
