import { HomeOutlined } from '@ant-design/icons'
import { createFileRoute } from '@tanstack/react-router'
import { Card, Typography } from 'antd'

export const Route = createFileRoute('/')({
    component: HomePage,
})

function HomePage() {
    return (
        <main className='flex h-full min-h-full items-center justify-center bg-slate-100 p-8'>
            <Card className='w-full max-w-2xl shadow-sm'>
                <div className='flex items-start gap-4'>
                    <div className='rounded-xl bg-slate-100 p-3 text-slate-700'>
                        <HomeOutlined className='text-2xl' />
                    </div>

                    <div className='space-y-2'>
                        <Typography.Title level={2} className='!mb-0'>
                            Home
                        </Typography.Title>
                        <Typography.Paragraph className='!mb-0 text-base text-slate-600'>
                            This is a stub page for the routed app shell. Bloodwork now lives under the dedicated
                            `/bloodwork` page.
                        </Typography.Paragraph>
                    </div>
                </div>
            </Card>
        </main>
    )
}
