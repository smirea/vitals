import { HomeOutlined } from '@ant-design/icons'
import { createFileRoute } from '@tanstack/react-router'
import { Card, Typography, theme as antdTheme } from 'antd'

export const Route = createFileRoute('/')({
    component: HomePage,
})

function HomePage() {
    const { token } = antdTheme.useToken()

    return (
        <main
            className='flex h-full min-h-full items-center justify-center p-8'
            style={{ background: token.colorBgLayout }}
        >
            <Card className='w-full max-w-2xl'>
                <div className='flex items-start gap-4'>
                    <div
                        className='rounded-xl p-3'
                        style={{
                            background: token.colorFillTertiary,
                            color: token.colorTextSecondary,
                        }}
                    >
                        <HomeOutlined className='text-2xl' />
                    </div>

                    <div className='space-y-2'>
                        <Typography.Title level={2} className='!mb-0'>
                            Home
                        </Typography.Title>
                        <Typography.Paragraph
                            className='!mb-0 text-base'
                            style={{ color: token.colorTextSecondary }}
                        >
                            This is a stub page for the routed app shell. Bloodwork now lives under the dedicated
                            `/bloodwork` page.
                        </Typography.Paragraph>
                    </div>
                </div>
            </Card>
        </main>
    )
}
