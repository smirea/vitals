import type { CSSProperties, ReactNode } from 'react';
import { Text, useTheme } from 'tamagui';

type PageNavProps = {
	title: ReactNode;
	actions?: ReactNode;
	className?: string;
	style?: CSSProperties;
};

export function PageNav({ title, actions, className, style }: PageNavProps) {
	const theme = useTheme();

	return (
		<div
			className={['page-nav', className].filter(Boolean).join(' ')}
			style={{
				background: theme.bgContainer?.get('web'),
				borderBottom: `1px solid ${theme.borderSubtle?.get('web')}`,
				...style,
			}}
		>
			<div className='page-nav-inner'>
				<div className='page-nav-title-shell'>
					{typeof title === 'string' ? (
						<Text fontWeight='700' className='page-nav-title'>
							{title}
						</Text>
					) : (
						title
					)}
				</div>

				{actions ? <div className='page-nav-actions'>{actions}</div> : null}
			</div>
		</div>
	);
}
