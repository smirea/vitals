const AUTH_TOKEN_STORAGE_KEY = 'vitals.server-token';
const AUTH_INVALID_EVENT = 'vitals-auth-invalid';

type AuthTokenResponse = {
	ok?: boolean;
	token?: unknown;
	expiresAt?: unknown;
	error?: unknown;
};

export function getStoredAuthToken() {
	const token = window.localStorage.getItem(AUTH_TOKEN_STORAGE_KEY)?.trim();
	return token ? token : null;
}

export function setStoredAuthToken(token: string) {
	window.localStorage.setItem(AUTH_TOKEN_STORAGE_KEY, token);
}

export function clearStoredAuthToken() {
	window.localStorage.removeItem(AUTH_TOKEN_STORAGE_KEY);
}

export function getAuthHeaders(): Record<string, string> {
	const token = getStoredAuthToken();
	return token ? { Authorization: `Bearer ${token}` } : {};
}

export function withAuthToken(input: string) {
	const token = getStoredAuthToken();
	if (!token) {
		return input;
	}

	const url = new URL(input, window.location.origin);
	url.searchParams.set('token', token);

	if (/^(https?|wss?):\/\//.test(input)) {
		return url.toString();
	}

	return `${url.pathname}${url.search}${url.hash}`;
}

export async function requestServerToken(password: string) {
	const response = await fetch('/api/auth/token', {
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

	setStoredAuthToken(payload.token);
	return {
		token: payload.token,
		expiresAt: typeof payload.expiresAt === 'string' ? payload.expiresAt : null,
	};
}

export async function verifyStoredServerToken() {
	const token = getStoredAuthToken();
	if (!token) {
		return false;
	}

	const response = await fetch('/api/status', {
		headers: getAuthHeaders(),
	});
	if (response.status === 401) {
		clearStoredAuthToken();
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

export function subscribeToAuthInvalid(listener: () => void) {
	window.addEventListener(AUTH_INVALID_EVENT, listener);
	return () => window.removeEventListener(AUTH_INVALID_EVENT, listener);
}

export async function authFetch(input: RequestInfo | URL, init?: RequestInit) {
	const response = await fetch(input, init);
	if (response.status === 401) {
		clearStoredAuthToken();
		window.dispatchEvent(new Event(AUTH_INVALID_EVENT));
	}

	return response;
}
