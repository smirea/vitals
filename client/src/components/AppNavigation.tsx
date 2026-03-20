import {
	BulbOutlined,
	ExperimentOutlined,
	HomeOutlined,
	MedicineBoxOutlined,
	MoonOutlined,
	TagsOutlined,
} from '@ant-design/icons';
import { Link, useRouterState } from '@tanstack/react-router';
import { Menu, Switch, Typography, theme as antdTheme } from 'antd';

import useAppContext from '../hooks/useAppContext';

const navigationItems = [
	{
		key: '/',
		to: '/',
		label: 'Home',
		icon: <HomeOutlined />,
	},
	{
		key: '/bloodwork',
		to: '/bloodwork',
		label: 'Bloodwork',
		icon: <MedicineBoxOutlined />,
	},
	{
		key: '/pills',
		to: '/pills',
		label: 'Pills',
		icon: <ExperimentOutlined />,
	},
	{
		key: '/tags',
		to: '/tags',
		label: 'Tags',
		icon: <TagsOutlined />,
	},
] as const;

export function AppNavigation() {
	const { theme, setTheme } = useAppContext();
	const { token } = antdTheme.useToken();
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

			<Menu
				mode='inline'
				theme={theme}
				selectedKeys={[selectedKey]}
				className='app-nav-menu'
				style={{ background: token.colorBgContainer }}
				items={navigationItems.map(item => ({
					key: item.key,
					icon: item.icon,
					label: (
						<Link
							to={item.to}
							className='app-nav-link'
							activeProps={{ className: 'app-nav-link' }}
							inactiveProps={{ className: 'app-nav-link' }}
						>
							{item.label}
						</Link>
					),
				}))}
			/>

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
					checkedChildren={<MoonOutlined />}
					unCheckedChildren={<BulbOutlined />}
					aria-label='Toggle dark mode'
				/>
			</div>
		</div>
	);
}
