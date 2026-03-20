import { Layout, theme as antdTheme } from 'antd'
import { Outlet, createRootRoute } from '@tanstack/react-router'

import { AppNavigation } from '../components/AppNavigation'
import useAppContext from '../hooks/useAppContext'

export const Route = createRootRoute({
    component: function RootComponent() {
        const { theme } = useAppContext()
        const { token } = antdTheme.useToken()

        return (
            <Layout className='app-shell' style={{ background: token.colorBgLayout }}>
                <Layout.Sider
                    width={240}
                    theme={theme}
                    style={{ borderRight: `1px solid ${token.colorBorderSecondary}` }}
                >
                    <AppNavigation />
                </Layout.Sider>

                <Layout className='app-main-layout' style={{ background: token.colorBgLayout }}>
                    <Layout.Content className='app-main-content'>
                        <Outlet />
                    </Layout.Content>
                </Layout>
            </Layout>
        )
    },
})
