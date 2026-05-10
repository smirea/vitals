import type { CSSProperties, HTMLAttributes, Key, ReactNode } from 'react';
import React from 'react';
import { Button, Spinner, Text, useTheme } from 'tamagui';

export type DataColumn<T> = {
	key: Key;
	header?: ReactNode;
	width?: number | string;
	align?: CSSProperties['textAlign'];
	cell?: (row: T, index: number) => ReactNode;
	getCellProps?: (row: T, index: number) => HTMLAttributes<HTMLTableCellElement>;
};

type DataTableProps<T> = {
	rows: T[];
	columns: Array<DataColumn<T>>;
	getRowKey: (row: T, index: number) => Key;
	loading?: boolean;
	minWidth?: number | string;
	className?: string;
	getRowProps?: (row: T, index: number) => HTMLAttributes<HTMLTableRowElement>;
	expandedRowKeys?: readonly Key[];
	onExpandedRowKeysChange?: (keys: Key[]) => void;
	renderExpandedRow?: (row: T) => ReactNode;
	canExpandRow?: (row: T) => boolean;
	showExpandColumn?: boolean;
};

export function DataTable<T>({
	rows,
	columns,
	getRowKey,
	loading,
	minWidth,
	className,
	getRowProps,
	expandedRowKeys = [],
	onExpandedRowKeysChange,
	renderExpandedRow,
	canExpandRow,
	showExpandColumn,
}: DataTableProps<T>) {
	const theme = useTheme();
	const expandedKeySet = new Set(expandedRowKeys.map(String));
	const hasExpandableRows = Boolean(renderExpandedRow);
	const shouldShowExpandColumn = showExpandColumn ?? hasExpandableRows;

	function toggleExpandedRow(key: Key) {
		const keyText = String(key);
		const nextKeys = expandedKeySet.has(keyText)
			? expandedRowKeys.filter(item => String(item) !== keyText)
			: [...expandedRowKeys, key];
		onExpandedRowKeysChange?.(nextKeys);
	}

	return (
		<div className='data-table-shell' style={{ overflowX: minWidth ? 'auto' : undefined }}>
			<table
				className={['data-table', className].filter(Boolean).join(' ')}
				style={{
					minWidth,
					borderColor: theme.borderSubtle?.get('web'),
				}}
			>
				<thead>
					<tr>
						{shouldShowExpandColumn ? <th style={{ width: 34 }} /> : null}
						{columns.map(column => (
							<th
								key={String(column.key)}
								style={{
									width: column.width,
									textAlign: column.align,
									color: theme.textMuted?.get('web'),
									background: theme.fill?.get('web'),
									borderColor: theme.borderSubtle?.get('web'),
								}}
							>
								{column.header}
							</th>
						))}
					</tr>
				</thead>
				<tbody>
					{loading ? (
						<tr>
							<td colSpan={columns.length + (shouldShowExpandColumn ? 1 : 0)}>
								<div className='data-table-empty'>
									<Spinner />
								</div>
							</td>
						</tr>
					) : rows.length === 0 ? (
						<tr>
							<td colSpan={columns.length + (shouldShowExpandColumn ? 1 : 0)}>
								<div className='data-table-empty'>
									<Text color='$textMuted'>No rows</Text>
								</div>
							</td>
						</tr>
					) : (
						rows.map((row, rowIndex) => {
							const key = getRowKey(row, rowIndex);
							const keyText = String(key);
							const rowProps = getRowProps?.(row, rowIndex) ?? {};
							const canExpand = canExpandRow?.(row) ?? hasExpandableRows;
							const isExpanded = expandedKeySet.has(keyText);

							return (
								<React.Fragment key={keyText}>
									<tr {...rowProps}>
										{shouldShowExpandColumn ? (
											<td style={{ width: 34, borderColor: theme.borderSubtle?.get('web') }}>
												{canExpand ? (
													<Button size='$2' chromeless onPress={() => toggleExpandedRow(key)}>
														{isExpanded ? '-' : '+'}
													</Button>
												) : null}
											</td>
										) : null}
										{columns.map(column => {
											const cellProps = column.getCellProps?.(row, rowIndex) ?? {};
											return (
												<td
													key={String(column.key)}
													{...cellProps}
													style={{
														textAlign: column.align,
														borderColor: theme.borderSubtle?.get('web'),
														...cellProps.style,
													}}
												>
													{column.cell?.(row, rowIndex)}
												</td>
											);
										})}
									</tr>
									{isExpanded && renderExpandedRow ? (
										<tr>
											<td
												colSpan={columns.length + (shouldShowExpandColumn ? 1 : 0)}
												style={{ borderColor: theme.borderSubtle?.get('web') }}
											>
												{renderExpandedRow(row)}
											</td>
										</tr>
									) : null}
								</React.Fragment>
							);
						})
					)}
				</tbody>
			</table>
		</div>
	);
}
