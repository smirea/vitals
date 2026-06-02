import * as FileSystem from 'expo-file-system/legacy';

import { API_BASE_URL } from '@/src/api/base-url';

const AUTH_TOKEN_FILE_NAME = 'vitals-server-token.txt';

type AuthTokenResponse = {
	ok?: boolean;
	token?: unknown;
	expiresAt?: unknown;
	error?: unknown;
};

let authToken: string | null = null;
const authInvalidListeners = new Set<() => void>();

function getAuthTokenPath() {
	if (!FileSystem.documentDirectory) {
		throw new Error('Document directory is unavailable.');
	}

	return `${FileSystem.documentDirectory}${AUTH_TOKEN_FILE_NAME}`;
}

function notifyAuthInvalid() {
	for (const listener of authInvalidListeners) {
		listener();
	}
}

export function getNativeAuthToken() {
	return authToken;
}

export function getNativeAuthHeaders(): Record<string, string> {
	return authToken ? { Authorization: `Bearer ${authToken}` } : {};
}

export function withNativeAuthToken(input: string) {
	if (!authToken) {
		return input;
	}

	const url = new URL(input);
	url.searchParams.set('token', authToken);
	return url.toString();
}

export async function loadNativeAuthToken() {
	const tokenPath = getAuthTokenPath();
	const info = await FileSystem.getInfoAsync(tokenPath);
	if (!info.exists) {
		authToken = null;
		return null;
	}

	const token = (await FileSystem.readAsStringAsync(tokenPath)).trim();
	authToken = token ? token : null;
	return authToken;
}

export async function storeNativeAuthToken(token: string) {
	authToken = token;
	await FileSystem.writeAsStringAsync(getAuthTokenPath(), token, {
		encoding: FileSystem.EncodingType.UTF8,
	});
}

export async function clearNativeAuthToken() {
	authToken = null;
	await FileSystem.deleteAsync(getAuthTokenPath(), { idempotent: true });
}

export async function requestNativeAuthToken(password: string) {
	const response = await fetch(`${API_BASE_URL}/auth/token`, {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
		},
		body: JSON.stringify({ password }),
	});
	const payload = (await response.json()) as AuthTokenResponse;

	if (!response.ok || payload.ok !== true || typeof payload.token !== 'string') {
		throw new Error(typeof payload.error === 'string' ? payload.error : 'Authentication failed');
	}

	await storeNativeAuthToken(payload.token);
	return {
		token: payload.token,
		expiresAt: typeof payload.expiresAt === 'string' ? payload.expiresAt : null,
	};
}

export async function verifyNativeAuthToken() {
	if (!authToken) {
		return false;
	}

	const response = await fetch(`${API_BASE_URL}/status`, {
		headers: getNativeAuthHeaders(),
	});
	if (response.status === 401) {
		await clearNativeAuthToken();
		return false;
	}
	if (!response.ok) {
		throw new Error(`Server status failed with ${response.status}`);
	}

	const payload = (await response.json()) as { ok?: boolean };
	if (payload.ok !== true) {
		throw new Error('Server status returned an invalid payload');
	}

	return true;
}

export function subscribeNativeAuthInvalid(listener: () => void) {
	authInvalidListeners.add(listener);
	return () => {
		authInvalidListeners.delete(listener);
	};
}

export async function nativeAuthFetch(input: URL | RequestInfo, init?: RequestInit) {
	const response = await fetch(input, init);
	if (response.status === 401) {
		await clearNativeAuthToken();
		notifyAuthInvalid();
	}

	return response;
}
