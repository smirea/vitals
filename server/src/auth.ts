import env from 'server/env.ts';

const jwtAlgorithm = { name: 'HMAC', hash: 'SHA-256' };
const jwtHeader = { alg: 'HS256', typ: 'JWT' };
const jwtSubject = 'vitals-server';

type JwtPayload = {
	sub: typeof jwtSubject;
	iat: number;
	exp: number;
};

function base64UrlEncode(value: string | Uint8Array) {
	return Buffer.from(value).toString('base64url');
}

function base64UrlDecodeJson<T>(value: string) {
	return JSON.parse(Buffer.from(value, 'base64url').toString('utf8')) as T;
}

async function getJwtKey(usages: KeyUsage[]) {
	return crypto.subtle.importKey(
		'raw',
		new TextEncoder().encode(env.SERVER_SECRET),
		jwtAlgorithm,
		false,
		usages,
	);
}

function getExpiresAt(now = new Date()) {
	const expiresAt = new Date(now);
	expiresAt.setMonth(expiresAt.getMonth() + 6);
	return Math.floor(expiresAt.getTime() / 1000);
}

export async function createServerJwt() {
	const now = Math.floor(Date.now() / 1000);
	const payload: JwtPayload = {
		sub: jwtSubject,
		iat: now,
		exp: getExpiresAt(),
	};
	const encodedHeader = base64UrlEncode(JSON.stringify(jwtHeader));
	const encodedPayload = base64UrlEncode(JSON.stringify(payload));
	const signingInput = `${encodedHeader}.${encodedPayload}`;
	const signature = new Uint8Array(
		await crypto.subtle.sign(
			jwtAlgorithm,
			await getJwtKey(['sign']),
			new TextEncoder().encode(signingInput),
		),
	);

	return {
		token: `${signingInput}.${base64UrlEncode(signature)}`,
		expiresAt: new Date(payload.exp * 1000).toISOString(),
	};
}

export async function verifyServerJwt(token: string) {
	try {
		const [encodedHeader, encodedPayload, encodedSignature, extraPart] = token.split('.');
		if (!encodedHeader || !encodedPayload || !encodedSignature || extraPart !== undefined) {
			return false;
		}

		const header = base64UrlDecodeJson<{ alg?: string; typ?: string }>(encodedHeader);
		if (header.alg !== jwtHeader.alg || header.typ !== jwtHeader.typ) {
			return false;
		}

		const signingInput = `${encodedHeader}.${encodedPayload}`;
		const verified = await crypto.subtle.verify(
			jwtAlgorithm,
			await getJwtKey(['verify']),
			Buffer.from(encodedSignature, 'base64url'),
			new TextEncoder().encode(signingInput),
		);
		if (!verified) {
			return false;
		}

		const payload = base64UrlDecodeJson<Partial<JwtPayload>>(encodedPayload);
		return (
			payload.sub === jwtSubject &&
			typeof payload.exp === 'number' &&
			payload.exp > Math.floor(Date.now() / 1000)
		);
	} catch {
		return false;
	}
}
