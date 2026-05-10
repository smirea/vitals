import { House } from '@phosphor-icons/react';
import { createFileRoute } from '@tanstack/react-router';

import { Card, Typography, theme as uiTheme } from '../components/ui';

export const Route = createFileRoute('/')({
	component: HomePage,
});

function HomePage() {
	const { token } = uiTheme.useToken();

	return (
		<main className='home-page' style={{ background: token.colorBgLayout }}>
			<Card className='home-card'>
				<div className='home-card-content'>
					<div
						className='home-icon-shell'
						style={{
							background: token.colorFillTertiary,
							color: token.colorTextSecondary,
						}}
					>
						<House className='home-icon' />
					</div>

					<div className='home-copy'>
						<Typography.Title level={2} className='home-copy-title'>
							Home
						</Typography.Title>
						<Typography.Paragraph
							className='home-copy-body'
							style={{ color: token.colorTextSecondary, fontSize: 16 }}
						>
							This is a stub page for the routed app shell. Labs now lives under the dedicated
							`/labs` page.
						</Typography.Paragraph>
					</div>
				</div>
			</Card>
		</main>
	);
}
