import path from 'path';

import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import { tanstackRouter } from '@tanstack/router-plugin/vite';
import tsconfigPaths from 'vite-tsconfig-paths';

export default defineConfig(async ({ command, mode }) => {
	const projectRoot = path.resolve(import.meta.dirname, '..');

	Object.assign(process.env, loadEnv(mode, projectRoot, ''));
	const { default: env } = await import('../server/src/env.ts');

	const clientPort = env.CLIENT_PORT;

	return {
		server:
			command === 'serve'
				? {
						port: clientPort,
						strictPort: true,
						proxy: {
							'/api': {
								target: env.VITE_API_URL,
								changeOrigin: true,
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
