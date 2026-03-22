type PromiseParallelOptions = {
	concurrency?: number;
	retries?: number;
};

export async function promiseParallel<TItem, TResult>(
	items: TItem[],
	worker: (item: TItem, index: number) => Promise<TResult>,
	options: PromiseParallelOptions = {},
) {
	const concurrency = Math.max(1, options.concurrency ?? 5);
	const retries = Math.max(0, options.retries ?? 1);
	const results = Array.from({ length: items.length }) as TResult[];
	let nextIndex = 0;

	async function runWorker() {
		for (;;) {
			const currentIndex = nextIndex;
			nextIndex += 1;

			if (currentIndex >= items.length) {
				return;
			}

			let attempt = 0;
			for (;;) {
				try {
					results[currentIndex] = await worker(items[currentIndex]!, currentIndex);
					break;
				} catch (error) {
					attempt += 1;
					if (attempt > retries) {
						throw error;
					}
				}
			}
		}
	}

	await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => runWorker()));

	return results;
}
