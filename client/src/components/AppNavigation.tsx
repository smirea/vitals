import { Database, House, Moon, NotePencil, Pill, Sun, Tag, TestTube } from '@phosphor-icons/react';
import { Link, useRouterState } from '@tanstack/react-router';
import { H4, Switch, Text, useTheme } from 'tamagui';

import useAppContext from '../hooks/useAppContext';

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
		icon: <TestTube size={18} />,
	},
	{
		key: '/pills',
		to: '/pills',
		label: 'Pills',
		icon: <Pill size={18} />,
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
	const colors = useTheme();
	const pathname = useRouterState({
		select: state => state.location.pathname,
	});

	const selectedKey =
		navigationItems.find(item =>
			item.to === '/' ? pathname === item.to : pathname.startsWith(item.to),
		)?.key ?? '/';

	return (
		<div className='app-nav' style={{ background: colors.bgContainer?.get('web') }}>
			<div
				className='app-nav-header'
				style={{ borderBottom: `1px solid ${colors.borderSubtle?.get('web')}` }}
			>
				<H4 className='app-nav-title' m={0}>
					Vitals
				</H4>
				<Text color='$textMuted' fontSize={12}>
					Dashboard
				</Text>
			</div>

			<nav className='app-nav-menu' style={{ background: colors.bgContainer?.get('web') }}>
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
					borderTop: `1px solid ${colors.borderSubtle?.get('web')}`,
					background: colors.bgContainer?.get('web'),
				}}
			>
				<div className='app-nav-footer-copy'>
					<Text fontWeight='700'>Dark mode</Text>
				</div>

				<Switch
					size='$2'
					checked={theme === 'dark'}
					onCheckedChange={checked => setTheme(checked ? 'dark' : 'light')}
					aria-label='Toggle dark mode'
				>
					<Switch.Thumb>
						<span className='app-nav-switch-icon'>
							{theme === 'dark' ? <Moon size={10} /> : <Sun size={10} />}
						</span>
					</Switch.Thumb>
				</Switch>
			</div>
		</div>
	);
}
