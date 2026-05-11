import { House } from '@phosphor-icons/react';
import { createFileRoute } from '@tanstack/react-router';
import { Card, H2, Paragraph, useTheme } from 'tamagui';

export const Route = createFileRoute('/')({
	component: HomePage,
});

function HomePage() {
	const theme = useTheme();

	return (
		<main className='home-page' style={{ background: theme.bgLayout?.get('web') }}>
			<Card
				className='home-card'
				bg='$bgContainer'
				borderColor='$borderSubtle'
				borderWidth={1}
				borderRadius={5}
			>
				<div className='home-card-content'>
					<div
						className='home-icon-shell'
						style={{
							background: theme.fillSoft?.get('web'),
							color: theme.textMuted?.get('web'),
						}}
					>
						<House className='home-icon' />
					</div>

					<div className='home-copy'>
						<H2 className='home-copy-title' m={0}>
							Home
						</H2>
						<Paragraph className='home-copy-body' color='$textMuted' fontSize={16}>
							This is a stub page for the routed app shell. Labs now lives under the dedicated
							`/labs` page.
						</Paragraph>
					</div>
				</div>
			</Card>
		</main>
	);
}
