import Constants from 'expo-constants';
import { Platform } from 'react-native';

const API_PORT = '6001';

function getApiBaseUrl() {
	if (Platform.OS === 'web') {
		const hostname = typeof window === 'undefined' ? '127.0.0.1' : window.location.hostname;
		return `http://${hostname}:${API_PORT}`;
	}

	const hostUri = Constants.expoConfig?.hostUri;
	if (!hostUri) {
		throw new Error(
			'Expo hostUri is missing; start Expo on LAN so Vitals can find the laptop API.',
		);
	}

	const expoUrl = new URL(`http://${hostUri}`);
	if (expoUrl.hostname.endsWith('.exp.direct')) {
		throw new Error(
			'Expo tunnel host cannot expose the local Vitals API; use LAN mode or set up an API tunnel.',
		);
	}

	expoUrl.protocol = 'http:';
	expoUrl.port = API_PORT;
	expoUrl.pathname = '';
	expoUrl.search = '';
	expoUrl.hash = '';

	return expoUrl.origin;
}

export const API_BASE_URL = getApiBaseUrl();
