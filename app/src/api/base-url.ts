import { Platform } from 'react-native';

const API_PORT = '6001';

function normalizeApiBaseUrl(value: string) {
	const url = new URL(value);
	url.pathname = url.pathname.replace(/\/+$/, '');
	url.search = '';
	url.hash = '';
	return url.toString().replace(/\/$/, '');
}

function getApiBaseUrl() {
	if (Platform.OS === 'web') {
		const hostname = typeof window === 'undefined' ? '127.0.0.1' : window.location.hostname;
		return `http://${hostname}:${API_PORT}`;
	}

	const apiUrl = process.env.EXPO_PUBLIC_API_URL?.trim();
	if (!apiUrl) {
		throw new Error('EXPO_PUBLIC_API_URL is required for the native Vitals app.');
	}

	return normalizeApiBaseUrl(apiUrl);
}

export const API_BASE_URL = getApiBaseUrl();
