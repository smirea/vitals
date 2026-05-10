import { Outlet, createRootRoute } from '@tanstack/react-router';

import { AppNavigation } from '../components/AppNavigation';
import { theme as uiTheme } from '../components/ui';

export const Route = createRootRoute({
	component: function RootComponent() {
		const { token } = uiTheme.useToken();

		return (
			<div className='app-shell' style={{ background: token.colorBgLayout }}>
				<aside
					className='app-shell-sider'
					style={{ width: 240, borderRight: `1px solid ${token.colorBorderSecondary}` }}
				>
					<AppNavigation />
				</aside>

				<div className='app-main-layout' style={{ background: token.colorBgLayout }}>
					<main className='app-main-content'>
						<Outlet />
					</main>
				</div>
			</div>
		);
	},
});
