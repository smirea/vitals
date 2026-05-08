import env from 'server/env.ts';

export const staticImageTables = ['pill_images'] as const;
export type StaticImageTable = (typeof staticImageTables)[number];

export function isStaticImageTable(value: string): value is StaticImageTable {
	return (staticImageTables as readonly string[]).includes(value);
}

export function getStaticImageUrl(table: StaticImageTable, id: number) {
	if (!Number.isInteger(id) || id <= 0) {
		throw new Error(`Invalid static image id: ${id}`);
	}

	return new URL(`/api/db-image/${table}/${id}`, `https://${env.VITE_HOST}`).toString();
}
