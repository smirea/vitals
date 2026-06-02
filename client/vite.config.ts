import path from 'path';

import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import { tanstackRouter } from '@tanstack/router-plugin/vite';
import tsconfigPaths from 'vite-tsconfig-paths';
import { z } from 'zod';

const clientEnvSchema = z.object({
	CLIENT_PORT: z.coerce.number().int().min(1).max(65535),
	VITE_HOST: z.string(),
	VITE_API_URL: z.url(),
});

export default defineConfig(async ({ command, mode }) => {
	const projectRoot = path.resolve(import.meta.dirname, '..');

	Object.assign(process.env, loadEnv(mode, projectRoot, ''));
	const env = clientEnvSchema.parse(process.env);

	const clientPort = env.CLIENT_PORT;

	return {
		server:
			command === 'serve'
				? {
						port: clientPort,
						strictPort: true,
						allowedHosts: [env.VITE_HOST],
						proxy: {
							'/api': {
								target: env.VITE_API_URL,
								changeOrigin: true,
								secure: false,
								ws: true,
								rewrite: (path: string) => path.replace(/^\/api/, ''),
							},
						},
					}
				: undefined,
		plugins: [
			tsconfigPaths(),
			tanstackRouter({
				target: 'react',
				autoCodeSplitting: true,
				routeFileIgnorePattern: '(^|/)_[^_].+',
			}) as any,
			react(),
		],
	};
});
