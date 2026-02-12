import styled from '@emotion/styled';
import { RocketLaunch } from '@phosphor-icons/react';
import { Card, Space, Typography } from 'antd';

const { Paragraph, Text } = Typography;

export default function App() {
    return (
        <Page>
            <Card
                title={
                    <Space align='center' size={8}>
                        <RocketLaunch size={20} weight='duotone' />
                        <Text strong>Example Page</Text>
                    </Space>
                }
            >
                <Space direction='vertical' size={8}>
                    <Paragraph>
                        <Text code>@phosphor-icons/react</Text> is installed. Replace <Text code>RocketLaunch</Text> with
                        any icon from the Phosphor Icons catalog.
                    </Paragraph>
                    <div className='rounded-md bg-slate-50 p-3 text-sm text-slate-700 ring-1 ring-slate-200'>
                        Tailwind CSS utilities are enabled (this box is styled by Tailwind classes).
                    </div>
                    <Space align='center' size={8}>
                        <RocketLaunch size={24} weight='fill' />
                        <Text>Phosphor icon rendering works in this template.</Text>
                    </Space>
                </Space>
            </Card>
        </Page>
    );
}

const Page = styled.main`
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 100vh;
    padding: 24px;
    background: #f5f5f5;
`;
