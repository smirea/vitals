import type { CSSProperties, ReactNode } from 'react';

import { Typography, theme as uiTheme } from './ui';

type PageNavProps = {
	title: ReactNode;
	actions?: ReactNode;
	className?: string;
	style?: CSSProperties;
};

export function PageNav({ title, actions, className, style }: PageNavProps) {
	const { token } = uiTheme.useToken();

	return (
		<div
			className={['page-nav', className].filter(Boolean).join(' ')}
			style={{
				background: token.colorBgContainer,
				borderBottom: `1px solid ${token.colorBorderSecondary}`,
				...style,
			}}
		>
			<div className='page-nav-inner'>
				<div className='page-nav-title-shell'>
					{typeof title === 'string' ? (
						<Typography.Text strong className='page-nav-title'>
							{title}
						</Typography.Text>
					) : (
						title
					)}
				</div>

				{actions ? <div className='page-nav-actions'>{actions}</div> : null}
			</div>
		</div>
	);
}
