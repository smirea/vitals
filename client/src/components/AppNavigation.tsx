import {
    BulbOutlined,
    ExperimentOutlined,
    HomeOutlined,
    MedicineBoxOutlined,
    MoonOutlined,
} from '@ant-design/icons'
import { Link, useRouterState } from '@tanstack/react-router'
import { Menu, Switch, Typography, theme as antdTheme } from 'antd'

import useAppContext from '../hooks/useAppContext'

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
] as const

export function AppNavigation() {
    const { theme, setTheme } = useAppContext()
    const { token } = antdTheme.useToken()
    const pathname = useRouterState({
        select: state => state.location.pathname,
    })

    const selectedKey = navigationItems.find(item => (
        item.to === '/' ? pathname === item.to : pathname.startsWith(item.to)
    ))?.key ?? '/'

    return (
        <div className='relative flex h-full flex-col' style={{ background: token.colorBgContainer }}>
            <div
                className='px-5 py-4'
                style={{ borderBottom: `1px solid ${token.colorBorderSecondary}` }}
            >
                <Typography.Title level={4} className='!mb-1'>
                    Vitals
                </Typography.Title>
                <Typography.Text type='secondary'>
                    Pages
                </Typography.Text>
            </div>

            <Menu
                mode='inline'
                theme={theme}
                selectedKeys={[selectedKey]}
                className='flex-1 border-e-0 pt-3 pb-24'
                style={{ background: token.colorBgContainer }}
                items={navigationItems.map(item => ({
                    key: item.key,
                    icon: item.icon,
                    label: (
                        <Link
                            to={item.to}
                            className='block w-full text-inherit no-underline'
                            activeProps={{ className: 'text-inherit no-underline' }}
                            inactiveProps={{ className: 'text-inherit no-underline' }}
                        >
                            {item.label}
                        </Link>
                    ),
                }))}
            />

            <div
                className='absolute bottom-0 left-0 right-0 flex items-center justify-between gap-3 px-5 py-4'
                style={{
                    borderTop: `1px solid ${token.colorBorderSecondary}`,
                    background: token.colorBgContainer,
                }}
            >
                <div className='min-w-0'>
                    <Typography.Text strong>
                        Dark mode
                    </Typography.Text>
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
    )
}
