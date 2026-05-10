import { Outlet, createRootRoute } from '@tanstack/react-router';
import { useTheme } from 'tamagui';

import { AppNavigation } from '../components/AppNavigation';

export const Route = createRootRoute({
	component: function RootComponent() {
		const theme = useTheme();

		return (
			<div className='app-shell' style={{ background: theme.bgLayout?.get('web') }}>
				<aside
					className='app-shell-sider'
					style={{ width: 240, borderRight: `1px solid ${theme.borderSubtle?.get('web')}` }}
				>
					<AppNavigation />
				</aside>

				<div className='app-main-layout' style={{ background: theme.bgLayout?.get('web') }}>
					<main className='app-main-content'>
						<Outlet />
					</main>
				</div>
			</div>
		);
	},
});
