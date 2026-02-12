import { Outlet, createRootRoute } from '@tanstack/react-router';
import { Layout } from 'antd';
import styled from '@emotion/styled';

export const Route = createRootRoute({
    component: function RootComponent() {
        return (
            <>
                <Content>
                    <Outlet />
                </Content>
            </>
        );
    },
});

const Content = styled(Layout.Content)`
    display: flex;
    align-items: stretch;
    justify-content: stretch;
    min-height: 100vh;
`;
