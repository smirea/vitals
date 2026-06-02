import env from 'server/env.ts';
import { s3PathUtil } from 'shared/s3PathUtil.ts';

export type AssetTable = 'lab_documents' | 'pill_images' | 'diary_voice_memos';
export type DiaryVoiceMemoAssetKind = 'audio' | 'video';
export type AssetKind = DiaryVoiceMemoAssetKind | 'pdf' | 'image';

export function getAssetUrlForS3Path(s3Path: string) {
	const { Bucket, Key } = s3PathUtil.parse(s3Path);
	return getClientUrl(`/api/asset/s3/${Bucket}/${Key}`);
}

export function getAssetUrlForRecord(table: AssetTable, id: number, kind?: AssetKind) {
	if (!Number.isInteger(id) || id <= 0) {
		throw new Error(`Invalid asset id: ${id}`);
	}

	const suffix = kind ? `/${kind}` : '';
	return getClientUrl(`/api/asset/${table}/${id}${suffix}`);
}

function getClientUrl(path: string) {
	return new URL(path, `https://${env.VITE_HOST}`).toString();
}
