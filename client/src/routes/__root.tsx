import { Layout, theme as antdTheme } from 'antd'
import { Outlet, createRootRoute } from '@tanstack/react-router'

import { AppNavigation } from '../components/AppNavigation'
import useAppContext from '../hooks/useAppContext'

export const Route = createRootRoute({
    component: function RootComponent() {
        const { theme } = useAppContext()
        const { token } = antdTheme.useToken()

        return (
            <Layout className='h-screen' style={{ background: token.colorBgLayout }}>
                <Layout.Sider
                    width={240}
                    theme={theme}
                    style={{ borderRight: `1px solid ${token.colorBorderSecondary}` }}
                >
                    <AppNavigation />
                </Layout.Sider>

                <Layout className='min-w-0' style={{ background: token.colorBgLayout }}>
                    <Layout.Content className='min-h-0 min-w-0'>
                        <Outlet />
                    </Layout.Content>
                </Layout>
            </Layout>
        )
    },
})
