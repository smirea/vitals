import React, { useContext, useEffect, useState } from 'react';

type AppTheme = 'dark' | 'light';

interface AppContextT {
	theme: AppTheme;
	setTheme: React.Dispatch<React.SetStateAction<AppTheme>>;
}

const APP_THEME_STORAGE_KEY = 'vitals.app-theme';

const AppContext = React.createContext<AppContextT>(null as any);

function getInitialTheme(): AppTheme {
	if (typeof window === 'undefined') {
		return 'light';
	}

	const storedTheme = window.localStorage.getItem(APP_THEME_STORAGE_KEY);

	return storedTheme === 'dark' ? 'dark' : 'light';
}

export default function useAppContext() {
	return useContext(AppContext);
}

export const AppContextProvider = ({ children }: { children: React.ReactNode }) => {
	const [theme, setTheme] = useState<AppTheme>(getInitialTheme);

	useEffect(() => {
		window.localStorage.setItem(APP_THEME_STORAGE_KEY, theme);

		document.body.classList.remove('app-theme-dark', 'app-theme-light');
		document.body.classList.add(theme === 'dark' ? 'app-theme-dark' : 'app-theme-light');
	}, [theme]);

	return <AppContext.Provider value={{ theme, setTheme }}>{children}</AppContext.Provider>;
};
