import { initTRPC } from '@trpc/server';

import { getDatabase } from 'server/db/client.ts';

export function createTrpcContext() {
	return {
		db: getDatabase(),
	};
}

type TrpcContext = ReturnType<typeof createTrpcContext>;

const trpc = initTRPC.context<TrpcContext>().create();

export const createRouter = trpc.router;
export const publicProcedure = trpc.procedure;
