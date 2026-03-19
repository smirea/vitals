import { Layout } from 'antd'
import { Outlet, createRootRoute } from '@tanstack/react-router'

import { AppNavigation } from '../components/AppNavigation'

export const Route = createRootRoute({
    component: function RootComponent() {
        return (
            <Layout className='h-screen bg-slate-100'>
                <Layout.Sider
                    width={240}
                    theme='light'
                    className='border-r border-slate-200'
                >
                    <AppNavigation />
                </Layout.Sider>

                <Layout className='min-w-0 bg-slate-100'>
                    <Layout.Content className='min-h-0 min-w-0'>
                        <Outlet />
                    </Layout.Content>
                </Layout>
            </Layout>
        )
    },
})
