import { Database, House, Moon, NotePencil, Pill, Sun, Tag, TestTube } from '@phosphor-icons/react';
import { Link, useRouterState } from '@tanstack/react-router';

import useAppContext from '../hooks/useAppContext';
import { Switch, Typography, theme as uiTheme } from './ui';

const navigationItems = [
	{
		key: '/',
		to: '/',
		label: 'Home',
		icon: <House size={18} />,
	},
	{
		key: '/labs',
		to: '/labs',
		label: 'Labs',
		icon: <Pill size={18} />,
	},
	{
		key: '/pills',
		to: '/pills',
		label: 'Pills',
		icon: <TestTube size={18} />,
	},
	{
		key: '/diary',
		to: '/diary',
		label: "Captain's Log",
		icon: <NotePencil size={18} />,
	},
	{
		key: '/sensors',
		to: '/sensors',
		label: 'Sensors',
		icon: <Database size={18} />,
	},
	{
		key: '/tags',
		to: '/tags',
		label: 'Tags',
		icon: <Tag size={18} />,
	},
] as const;

export function AppNavigation() {
	const { theme, setTheme } = useAppContext();
	const { token } = uiTheme.useToken();
	const pathname = useRouterState({
		select: state => state.location.pathname,
	});

	const selectedKey =
		navigationItems.find(item =>
			item.to === '/' ? pathname === item.to : pathname.startsWith(item.to),
		)?.key ?? '/';

	return (
		<div className='app-nav' style={{ background: token.colorBgContainer }}>
			<div
				className='app-nav-header'
				style={{ borderBottom: `1px solid ${token.colorBorderSecondary}` }}
			>
				<Typography.Title level={4} className='app-nav-title'>
					Vitals
				</Typography.Title>
				<Typography.Text type='secondary'>Pages</Typography.Text>
			</div>

			<nav className='app-nav-menu' style={{ background: token.colorBgContainer }}>
				{navigationItems.map(item => (
					<Link
						key={item.key}
						to={item.to}
						className={`app-nav-link ${selectedKey === item.key ? 'app-nav-link-active' : ''}`}
						activeProps={{ className: 'app-nav-link app-nav-link-active' }}
						inactiveProps={{ className: 'app-nav-link' }}
					>
						<span className='app-nav-link-icon'>{item.icon}</span>
						<span>{item.label}</span>
					</Link>
				))}
			</nav>

			<div
				className='app-nav-footer'
				style={{
					borderTop: `1px solid ${token.colorBorderSecondary}`,
					background: token.colorBgContainer,
				}}
			>
				<div className='app-nav-footer-copy'>
					<Typography.Text strong>Dark mode</Typography.Text>
				</div>

				<Switch
					checked={theme === 'dark'}
					onChange={checked => setTheme(checked ? 'dark' : 'light')}
					checkedChildren={<Moon size={12} />}
					unCheckedChildren={<Sun size={12} />}
					aria-label='Toggle dark mode'
				/>
			</div>
		</div>
	);
}
