import { useEffect, useState } from 'react';

export default function createLocalStorage<Shape extends Record<string, any>>(
	options: LocalStorageOptions<Shape>,
) {
	const LS = new LocalStorage<Shape>(options);

	const useLocalStorage = <K extends keyof Shape>(key: K, defaultValue?: Shape[K]) => {
		const [value, setValue] = useState<Shape[K]>((LS.get(key) as any) ?? defaultValue);

		useEffect(() => LS.set({ [key]: value } as any), [value, key]);

		useEffect(() => {
			return LS.onChange(diff => {
				if (key in diff) setValue(diff[key] as any);
			});
		}, [key]);

		return [value, setValue] as const;
	};

	return { useLocalStorage, LS };
}

type AnyObject = Record<string, any>;

export type LocalStorageOptions<T extends AnyObject> = {
	namespace: string;
	getDefaults?: () => Partial<T>;
	store?: AnyObject;
};

/**
 * LocalStorage interface that has 2 main features:
 * - it automatically stores JSON and returns parsed objects
 * - it is namespaced by default, with each key stored individually with the namespace prefix
 */
export class LocalStorage<T extends AnyObject = AnyObject> {
	options: Required<LocalStorageOptions<T>>;
	public readonly prefix: string;
	private onChangeEvents = new Set<(...args: any[]) => any>();
	private eventsEnabled = false;

	constructor({ namespace, getDefaults, store }: LocalStorageOptions<T>) {
		this.options = {
			namespace,
			getDefaults: getDefaults || (() => ({})),
			store: store || globalThis.localStorage,
		};
		this.prefix = this.options.namespace + ':';
		this.set({
			...this.options.getDefaults(),
			...this.getAll(),
		});
		this.eventsEnabled = true;
	}

	private getKeyName(key: keyof T) {
		return this.prefix + String(key);
	}

	/**
	 * Includes all the keys prefixed from storage and default keys to account for schema changes
	 */
	private getAllKeys(): (keyof T)[] {
		return Object.keys(this.options.store)
			.filter(k => k.startsWith(this.prefix))
			.map(k => k.slice(this.prefix.length) as keyof T);
	}

	private setKey(key: string, value: any) {
		this.options.store[this.getKeyName(key)] = JSON.stringify(value);
	}

	private deleteKey(key: keyof T) {
		delete this.options.store[this.getKeyName(key)];
	}

	/**
	 * Returns all keys stored in the store
	 */
	getAll({ defaults = true } = {}): T {
		const result: any = defaults ? this.options.getDefaults() : {};
		this.getAllKeys().forEach(key => (result[key] = this.get(key)));
		return result as any;
	}

	/**
	 * Returns the value of a single key or a default if not found
	 */
	get<K extends keyof T, D extends T[K]>(
		key: K,
		defaultValue?: D,
	): D extends undefined
		? ReturnType<this['options']['getDefaults']>[K] extends undefined
			? T[K] | undefined
			: T[K]
		: T[K] {
		try {
			const value = this.options.store[this.getKeyName(key)];
			if (value !== undefined) return JSON.parse(value);
		} catch {
			delete this.options.store[this.getKeyName(key)];
		}

		return defaultValue !== undefined ? defaultValue : (this.options.getDefaults()[key] as any);
	}

	/**
	 * Save any number of keys in the store
	 */
	set(diff: Partial<T>) {
		for (const [key, value] of Object.entries(diff)) {
			if (value !== undefined) {
				this.setKey(key as string, value);
			} else {
				this.deleteKey(key as keyof T);
			}
		}

		this.triggerChange(diff);
	}

	delete(key: keyof T) {
		delete this.options.store[this.getKeyName(key)];
		this.triggerChange({ [key]: undefined } as any);
	}

	/**
	 * Deletes all the data in the store and restores defaults
	 */
	reset() {
		this.getAllKeys().forEach(key => this.deleteKey(key));
		this.set(this.options.getDefaults());
	}

	/**
	 * Listen to storage changes
	 *
	 * Receives the diff of changed values AFTER the change
	 */
	onChange(callback: (diff: Partial<T>) => void) {
		this.onChangeEvents.add(callback);

		return () => void this.onChangeEvents.delete(callback);
	}

	private triggerChange(diff: Partial<T>) {
		if (!this.eventsEnabled) return;
		for (const cb of this.onChangeEvents) {
			cb(diff);
		}
	}
}
