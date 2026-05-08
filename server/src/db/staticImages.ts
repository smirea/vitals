import { eq } from 'drizzle-orm';

import type { VitalsDatabase } from 'server/db/client.ts';
import { pillImages } from 'server/db/schema.ts';
import type { StaticImageTable } from 'server/utils/getStaticImageUrl.ts';

export type StaticImageRecord = {
	fileName: string;
	mimeType: string;
	data: Uint8Array;
};

function parseDataUrl(dataUrl: string) {
	const match = dataUrl.match(/^data:([^;]+);base64,(.+)$/);
	if (!match) {
		throw new Error('Image payload must be a base64 data URL.');
	}

	return {
		mimeType: match[1],
		data: Buffer.from(match[2], 'base64'),
	};
}

export function getStaticImage(
	db: VitalsDatabase,
	table: StaticImageTable,
	id: number,
): StaticImageRecord | null {
	switch (table) {
		case 'pill_images': {
			const row = db
				.select({
					fileName: pillImages.fileName,
					dataUrl: pillImages.dataUrl,
				})
				.from(pillImages)
				.where(eq(pillImages.id, id))
				.get();

			if (!row) {
				return null;
			}

			const parsedImage = parseDataUrl(row.dataUrl);
			return {
				fileName: row.fileName,
				mimeType: parsedImage.mimeType,
				data: parsedImage.data,
			};
		}
	}
}
