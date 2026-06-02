export const s3PathUtil = {
	parse: function s3PathParse(s3Path: string): { Bucket: string; Key: string } {
		const [Bucket, ...keyParts] = s3PathUtil.sanitizeBucket(s3Path).split('/');
		if (!Bucket || !keyParts.length) throw new Error(`Invalid s3Path "${s3Path}"`);
		const Key = keyParts.join('/');
		return { Bucket, Key };
	},
	create: function s3PathCreate(bucket: string, key: string): `s3://${string}/${string}` {
		bucket = s3PathUtil.sanitizeBucket(bucket);
		key = s3PathUtil.sanitizeKey(key);
		if (!bucket) throw new Error('Missing bucket');
		if (!key) throw new Error('Missing key');
		return `s3://${bucket}/${key}`;
	},
	join: function s3PathJoin(bucket: string, ...parts: string[]): `s3://${string}/${string}` {
		return s3PathUtil.create(bucket, parts.map(s3PathUtil.sanitizeKey).filter(Boolean).join('/'));
	},
	stripPrefix: (s3Path: string) => '/' + s3Path.replace(/^s3:\/+/, '').replace(/^\/+/, ''),
	sanitizeKey: (key: string) => key.replace(/^\/+/, '').replace(/\/+$/, ''),
	sanitizeBucket: (bucket: string) => bucket.replace(/^s3:\/\//, '').replace(/^\/+/, ''),
};
