import { ExperimentOutlined, HomeOutlined, MedicineBoxOutlined } from '@ant-design/icons'
import { Link, useRouterState } from '@tanstack/react-router'
import { Menu, Typography } from 'antd'

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
    const pathname = useRouterState({
        select: state => state.location.pathname,
    })

    const selectedKey = navigationItems.find(item => (
        item.to === '/' ? pathname === item.to : pathname.startsWith(item.to)
    ))?.key ?? '/'

    return (
        <div className='flex h-full flex-col bg-white'>
            <div className='border-b border-slate-200 px-5 py-4'>
                <Typography.Title level={4} className='!mb-1'>
                    Vitals
                </Typography.Title>
                <Typography.Text type='secondary'>
                    Pages
                </Typography.Text>
            </div>

            <Menu
                mode='inline'
                selectedKeys={[selectedKey]}
                className='flex-1 border-e-0 pt-3'
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
        </div>
    )
}
