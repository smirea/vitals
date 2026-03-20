import styled from '@emotion/styled';
import {
	CheckCircle,
	DownloadSimple,
	Drop,
	Flag,
	Star,
	WarningCircle,
} from '@phosphor-icons/react';
import { Empty, Slider, theme as antdTheme } from 'antd';
import {
	CartesianGrid,
	Legend,
	Line,
	LineChart,
	ReferenceLine,
	ResponsiveContainer,
	Tooltip as RechartsTooltip,
	XAxis,
	YAxis,
} from 'recharts';
import {
	memo,
	useCallback,
	useEffect,
	useLayoutEffect,
	useMemo,
	useRef,
	type CSSProperties,
	type ChangeEvent,
} from 'react';

import type {
	CategoryOverviewItem,
	CategorySelectionState,
	ChartSeriesModel,
	MeaningfulChangeDirection,
	MeaningfulChangeItem,
	MeasurementCell,
	MeasurementOverviewTally,
	SourceColumn,
	TrendChartDatum,
	VitalsCategoryRow,
	VitalsDisplayRow,
	VitalsRowModel,
} from './_bloodwork';
import {
	formatNormalizedYAxisTick,
	formatPrettyDate,
	MEASUREMENT_COLUMN_WIDTH,
	OVERVIEW_COLUMN_WIDTH,
	SELECTION_COLUMN_WIDTH,
	SOURCE_COLUMN_WIDTH,
} from './_bloodwork';

type ThemeVarsStyle = CSSProperties & Record<string, string>;

const VitalsScope = styled.div`
	.vitals-category-overview {
		--vitals-overview-section-min-width: 16px;
		--vitals-overview-pill-min-width: 10px;
		border-bottom: 1px solid var(--vitals-border);
		padding: 12px;
		background: var(--vitals-bg-container);
	}

	.vitals-category-overview-grid {
		display: grid;
		gap: 10px;
		grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
		max-width: calc((320px * 4) + (10px * 3));
	}

	.vitals-category-overview-item {
		border: 1px solid var(--vitals-border-secondary);
		border-radius: 6px;
		background: var(--vitals-header-bg);
		padding: 9px 10px;
	}

	.vitals-category-overview-row {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
	}

	.vitals-category-overview-row h3 {
		margin: 0;
		font-size: 13px;
		line-height: 1.2;
		color: var(--vitals-text);
	}

	.vitals-category-overview-row span {
		font-size: 13px;
		font-weight: 600;
		color: var(--vitals-text-secondary);
	}

	.vitals-category-overview-pills {
		display: flex;
		flex-wrap: nowrap;
		gap: 7px 12px;
		min-width: 0;
		width: 100%;
		margin-top: 8px;
	}

	.vitals-category-overview-pill-group {
		display: flex;
		align-items: center;
		gap: 4px;
		min-width: 0;
		flex: 0 1 auto;
	}

	.vitals-category-overview-pill {
		height: 13px;
		border-radius: 999px;
		flex: 1 1 auto;
		min-width: var(--vitals-overview-pill-min-width);
	}

	.vitals-category-overview-pill-in-range {
		background: var(--vitals-success);
	}

	.vitals-category-overview-pill-out-of-range {
		background: var(--vitals-error);
	}

	.vitals-category-overview-pill-unclassified {
		background: var(--vitals-text-tertiary);
	}

	.vitals-category-overview-pill-count {
		font-size: 13px;
		font-weight: 500;
		color: var(--vitals-text-secondary);
		line-height: 1;
		white-space: nowrap;
		flex: 0 0 auto;
	}

	.vitals-meaningful-changes {
		border-bottom: 1px solid var(--vitals-border);
		padding: 8px;
		background: var(--vitals-bg-container);
	}

	.vitals-meaningful-changes-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 10px;
		margin-bottom: 6px;
	}

	.vitals-meaningful-changes-header h2 {
		margin: 0;
		font-size: 15px;
		line-height: 1.2;
		color: var(--vitals-text);
	}

	.vitals-meaningful-changes-header p {
		margin: 0;
		font-size: 11px;
		color: var(--vitals-text-secondary);
		white-space: nowrap;
	}

	.vitals-meaningful-changes-list {
		display: grid;
		gap: 6px;
		grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
		max-height: 276px;
		overflow-y: auto;
		padding-right: 2px;
	}

	.vitals-meaningful-change-item {
		border: 1px solid var(--vitals-border-secondary);
		border-radius: 6px;
		background: var(--vitals-header-bg);
		padding: 7px 8px;
		display: flex;
		flex-direction: column;
		gap: 5px;
	}

	.vitals-meaningful-change-top {
		display: flex;
		align-items: flex-start;
		justify-content: space-between;
		gap: 8px;
	}

	.vitals-meaningful-change-titles {
		min-width: 0;
	}

	.vitals-meaningful-change-titles h3 {
		margin: 0;
		font-size: 13px;
		line-height: 1.2;
		color: var(--vitals-text);
	}

	.vitals-meaningful-change-titles span {
		display: inline-block;
		margin-top: 1px;
		font-size: 11px;
		color: var(--vitals-text-tertiary);
	}

	.vitals-meaningful-change-direction {
		display: inline-flex;
		align-items: center;
		border-radius: 999px;
		border: 1px solid transparent;
		padding: 1px 7px;
		font-size: 10px;
		line-height: 1.2;
		font-weight: 600;
		flex-shrink: 0;
	}

	.vitals-meaningful-change-direction-improved {
		background: var(--vitals-success-bg);
		border-color: var(--vitals-success-border);
		color: var(--vitals-success);
	}

	.vitals-meaningful-change-direction-worsened {
		background: var(--vitals-error-bg);
		border-color: var(--vitals-error-border);
		color: var(--vitals-error);
	}

	.vitals-meaningful-change-direction-changed {
		background: var(--vitals-bg-subtle);
		border-color: var(--vitals-border);
		color: var(--vitals-text-secondary);
	}

	.vitals-meaningful-change-comparison {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr);
		gap: 8px;
	}

	.vitals-meaningful-change-value {
		display: flex;
		min-width: 0;
		align-items: baseline;
		gap: 4px;
	}

	.vitals-meaningful-change-value strong {
		min-width: 0;
		font-size: 13px;
		line-height: 1.2;
		color: var(--vitals-text);
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.vitals-meaningful-change-value small {
		min-width: 0;
		font-size: 10px;
		color: var(--vitals-text-tertiary);
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.vitals-meaningful-change-arrow {
		align-self: baseline;
		color: var(--vitals-text-secondary);
		font-size: 12px;
		line-height: 1;
	}

	.vitals-meaningful-change-meta {
		display: flex;
		flex-wrap: wrap;
		gap: 4px;
	}

	.vitals-meaningful-change-reason,
	.vitals-meaningful-change-delta {
		display: inline-flex;
		align-items: center;
		border-radius: 999px;
		border: 1px solid var(--vitals-border);
		background: var(--vitals-bg-container);
		padding: 1px 7px;
		font-size: 10px;
		line-height: 1.2;
		color: var(--vitals-text-secondary);
	}

	.vitals-controls {
		display: grid;
		grid-template-columns: minmax(240px, 1fr) minmax(360px, 1.45fr) auto auto;
		gap: 10px 16px;
		border-bottom: 1px solid var(--vitals-border);
		padding: 10px 16px 14px;
		background: var(--vitals-bg-container);
		align-items: center;
	}

	.vitals-controls-search {
		display: flex;
		min-width: 0;
		align-items: center;
		gap: 8px;
		border: 1px solid var(--vitals-border);
		border-radius: 6px;
		background: var(--vitals-bg-container);
		padding: 0 10px;
	}

	.vitals-controls-search input {
		width: 100%;
		min-width: 0;
		border: 0;
		outline: 0;
		background: transparent;
		color: var(--vitals-text);
		font: inherit;
		padding: 8px 0;
	}

	.vitals-controls-toggle {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		color: var(--vitals-text);
		white-space: nowrap;
	}

	.vitals-controls-toggle input {
		accent-color: var(--vitals-primary);
	}

	.vitals-controls-button {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		border: 1px solid var(--vitals-border);
		border-radius: 6px;
		background: var(--vitals-bg-container);
		color: var(--vitals-text);
		font: inherit;
		padding: 6px 10px;
	}

	.vitals-controls-button:disabled {
		opacity: 0.55;
	}

	.vitals-controls-range {
		display: flex;
		flex-direction: column;
		gap: 6px;
		min-width: 0;
		align-self: stretch;
	}

	.vitals-controls-range-labels {
		display: flex;
		justify-content: space-between;
		gap: 12px;
		font-size: 12px;
		color: var(--vitals-text-secondary);
	}

	.trend-chart-table-shell {
		overflow: auto;
		border: 1px solid var(--vitals-border);
		border-radius: 8px;
		background: var(--vitals-bg-container);
	}

	.trend-chart-table {
		width: 100%;
		border-collapse: collapse;
		table-layout: fixed;
	}

	.trend-chart-table-head,
	.trend-chart-table-cell {
		border: 1px solid var(--vitals-border-secondary);
		padding: 6px 8px;
		text-align: left;
		font-size: 12px;
		vertical-align: top;
	}

	.vitals-table-shell {
		flex: 1;
		display: flex;
		flex-direction: column;
		min-height: 0;
		overflow: auto;
		background: var(--vitals-bg-container);
		scrollbar-gutter: stable;
	}

	.vitals-table {
		min-width: 100%;
		min-height: 100%;
		width: max-content;
		border-collapse: collapse;
		table-layout: fixed;
	}

	.vitals-head {
		position: sticky;
		top: 0;
		z-index: 5;
		min-width: var(--source-col-width);
		width: var(--source-col-width);
		border: 1px solid var(--vitals-border);
		background: var(--vitals-bg-subtle);
		padding: 8px;
		text-align: left;
		font-size: 14px;
		font-weight: 500;
		color: var(--vitals-text);
	}

	.vitals-source-head {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 4px;
	}

	.vitals-source-filter-toggle {
		display: inline-flex;
		align-items: center;
		gap: 4px;
		border: 1px solid var(--vitals-border);
		border-radius: 999px;
		padding: 1px 7px;
		font-size: 11px;
		line-height: 1.3;
		color: var(--vitals-error);
		background: var(--vitals-error-bg);
	}

	.vitals-source-filter-toggle:hover {
		border-color: var(--vitals-error-border);
		background: var(--vitals-header-bg-active);
	}

	.vitals-source-filter-toggle-active {
		border-color: var(--vitals-error);
		color: var(--vitals-white);
		background: var(--vitals-error);
	}

	.vitals-cell {
		min-width: var(--source-col-width);
		width: var(--source-col-width);
		border: 1px solid var(--vitals-border-secondary);
		background: var(--vitals-row-bg, var(--vitals-bg-container));
		padding: 7px 8px;
		vertical-align: top;
		font-size: 14px;
	}

	.vitals-row-selected > .vitals-cell {
		--vitals-row-bg: var(--vitals-bg-subtle);
		background: var(--vitals-bg-subtle);
	}

	.vitals-col-select {
		position: sticky;
		left: 0;
		min-width: var(--selection-col-width);
		width: var(--selection-col-width);
		max-width: var(--selection-col-width);
		z-index: 6;
		text-align: center;
	}

	.vitals-col-measurement {
		position: sticky;
		left: var(--selection-col-width);
		min-width: var(--measurement-col-width);
		width: var(--measurement-col-width);
		max-width: var(--measurement-col-width);
		z-index: 5;
	}

	.vitals-col-overview {
		position: sticky;
		left: calc(var(--selection-col-width) + var(--measurement-col-width));
		min-width: var(--overview-col-width);
		width: var(--overview-col-width);
		max-width: var(--overview-col-width);
		z-index: 4;
	}

	.vitals-cell.vitals-col-select,
	.vitals-cell.vitals-col-measurement,
	.vitals-cell.vitals-col-overview {
		background: var(--vitals-row-bg, var(--vitals-bg-container));
	}

	.vitals-head.vitals-col-select,
	.vitals-head.vitals-col-measurement,
	.vitals-head.vitals-col-overview {
		z-index: 8;
		background: var(--vitals-bg-subtle);
	}

	.vitals-head.vitals-source-filter-active {
		background: var(--vitals-error-bg-hover);
	}

	.vitals-cell.vitals-source-filter-active {
		background: var(--vitals-error-bg);
	}

	.vitals-category-row > .vitals-cell {
		--vitals-row-bg: var(--vitals-primary-bg);
		border-color: var(--vitals-border);
		background: var(--vitals-primary-bg);
	}

	.vitals-category-row:hover > .vitals-cell {
		--vitals-row-bg: var(--vitals-primary-bg-hover);
		background: var(--vitals-primary-bg-hover);
	}

	.vitals-category-row > .vitals-cell.vitals-source-filter-active {
		background: var(--vitals-error-bg);
	}

	.vitals-category-row:hover > .vitals-cell.vitals-source-filter-active {
		background: var(--vitals-error-bg-hover);
	}

	.vitals-cell-value {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.vitals-range-track {
		position: relative;
		height: 10px;
		border: 1px solid var(--vitals-border);
		background: var(--vitals-bg-muted);
	}

	.vitals-range-band {
		position: absolute;
		top: 1px;
		bottom: 1px;
		background: var(--vitals-primary-border);
	}

	.vitals-range-marker {
		position: absolute;
		top: -3px;
		bottom: -3px;
		width: 2px;
		transform: translateX(-50%);
		background: var(--vitals-text-secondary);
	}

	.vitals-value-marker {
		position: absolute;
		top: 50%;
		width: 8px;
		height: 8px;
		border: 1px solid var(--vitals-bg-container);
		background: var(--vitals-error);
		transform: translate(-50%, -50%);
	}

	.vitals-range-caption {
		font-size: 10px;
		line-height: 1.2;
		color: var(--vitals-text-secondary);
	}

	.vitals-flag {
		display: inline-flex;
		width: fit-content;
		border-radius: 2px;
		border: 1px solid;
		padding: 2px 6px;
		font-size: 10px;
		font-weight: 500;
		letter-spacing: 0.02em;
		text-transform: uppercase;
	}

	.vitals-flag-danger {
		border-color: var(--vitals-error-border);
		background: var(--vitals-error-bg);
		color: var(--vitals-error);
	}

	.vitals-flag-warning {
		border-color: var(--vitals-warning-border);
		background: var(--vitals-warning-bg);
		color: var(--vitals-warning);
	}

	@media (max-width: 899px) {
		.vitals-category-overview-grid {
			grid-template-columns: 1fr;
			max-width: none;
		}

		.vitals-category-overview-pills {
			flex-wrap: wrap;
		}

		.vitals-meaningful-changes-header {
			flex-direction: column;
			align-items: flex-start;
			gap: 2px;
		}

		.vitals-meaningful-changes-header p {
			white-space: normal;
		}

		.vitals-meaningful-changes-list {
			grid-template-columns: 1fr;
			max-height: none;
		}

		.vitals-meaningful-change-comparison {
			gap: 6px;
		}

		.vitals-meaningful-change-value {
			flex-direction: column;
			align-items: flex-start;
			gap: 1px;
		}

		.vitals-controls {
			grid-template-columns: 1fr;
		}

		.vitals-controls-range {
			grid-column: auto;
		}
	}
`;

type CategoriesOverviewProps = {
	items: CategoryOverviewItem[];
};

type MeaningfulChangesProps = {
	items: MeaningfulChangeItem[];
};

type TrendChartProps = {
	series: ChartSeriesModel[];
	orderedSources: SourceColumn[];
	isMobile: boolean;
};

type VitalsControlsProps = {
	measurementFilter: string;
	onMeasurementFilterChange: (event: ChangeEvent<HTMLInputElement>) => void;
	availableDates: string[];
	dateRangeValue: [number, number];
	onDateRangeSliderChange: (nextRange: [number, number]) => void;
	groupByCategory: boolean;
	onGroupByCategoryChange: (checked: boolean) => void;
	onDownloadCsv: () => void;
	isDownloadCsvDisabled: boolean;
};

type VitalsTableProps = {
	rows: VitalsDisplayRow[];
	tableSources: SourceColumn[];
	outOfRangeSourceFilterIdSet: Set<string>;
	outOfRangeMeasurementCountBySourceId: Map<string, number>;
	selectedRowKeySet: Set<string>;
	categorySelectionByName: Map<string, CategorySelectionState>;
	starredMeasurementSet: Set<string>;
	measurementOverviewByKey: Map<string, MeasurementOverviewTally>;
	measurementRangesTooltipByKey: Map<string, string>;
	tableScrollX: number;
	tableScrollY: number;
	onToggleRow: (key: string, checked: boolean) => void;
	onToggleAllRows: (checked: boolean) => void;
	onToggleCategory: (category: string, checked: boolean) => void;
	onToggleStar: (measurementKey: string) => void;
	onToggleOutOfRangeSourceFilter: (sourceId: string) => void;
};

type SelectionCheckboxProps = {
	checked: boolean;
	indeterminate?: boolean;
	disabled?: boolean;
	onChange: (checked: boolean) => void;
	ariaLabel?: string;
};

function getThemeVars(token: ReturnType<typeof antdTheme.useToken>['token']): ThemeVarsStyle {
	return {
		['--vitals-bg-layout' as string]: token.colorBgLayout,
		['--vitals-bg-container' as string]: token.colorBgContainer,
		['--vitals-bg-subtle' as string]: token.colorFillAlter,
		['--vitals-header-bg' as string]: token.colorBgContainer,
		['--vitals-header-bg-active' as string]: token.colorErrorBg,
		['--vitals-bg-muted' as string]: token.colorFillSecondary,
		['--vitals-bg-hover' as string]: token.colorFillTertiary,
		['--vitals-bg-row-alt' as string]: token.colorFillQuaternary,
		['--vitals-border' as string]: token.colorBorder,
		['--vitals-border-secondary' as string]: token.colorBorderSecondary,
		['--vitals-text' as string]: token.colorText,
		['--vitals-text-secondary' as string]: token.colorTextSecondary,
		['--vitals-text-tertiary' as string]: token.colorTextTertiary,
		['--vitals-primary' as string]: token.colorPrimary,
		['--vitals-primary-bg' as string]: token.colorPrimaryBg,
		['--vitals-primary-bg-hover' as string]: token.colorPrimaryBgHover,
		['--vitals-primary-border' as string]: token.colorPrimaryBorder,
		['--vitals-success' as string]: token.colorSuccess,
		['--vitals-success-bg' as string]: token.colorSuccessBg,
		['--vitals-success-border' as string]: token.colorSuccessBorder,
		['--vitals-error' as string]: token.colorError,
		['--vitals-error-bg' as string]: token.colorErrorBg,
		['--vitals-error-bg-hover' as string]: token.colorErrorBgHover,
		['--vitals-error-border' as string]: token.colorErrorBorder,
		['--vitals-warning' as string]: token.colorWarning,
		['--vitals-warning-bg' as string]: token.colorWarningBg,
		['--vitals-warning-border' as string]: token.colorWarningBorder,
		['--vitals-white' as string]: token.colorWhite,
	};
}

function ScopedVitals(props: { children: React.ReactNode }) {
	const { token } = antdTheme.useToken();

	return <VitalsScope style={getThemeVars(token)}>{props.children}</VitalsScope>;
}

function toSharePercent(value: number, maxTotal: number): number {
	if (value <= 0 || maxTotal <= 0) {
		return 0;
	}

	return (value / maxTotal) * 100;
}

function formatDelta(value: number | null): string | null {
	if (value === null || !Number.isFinite(value)) {
		return null;
	}
	if (value >= 1000) {
		return `${Math.round(value)}%`;
	}
	if (value >= 100) {
		return `${value.toFixed(0)}%`;
	}
	return `${value.toFixed(1)}%`;
}

function getDirectionLabel(direction: MeaningfulChangeDirection) {
	if (direction === 'improved') return 'Improved';
	if (direction === 'worsened') return 'Worsened';
	return 'Changed';
}

export function CategoriesOverview({ items }: CategoriesOverviewProps) {
	if (items.length === 0) {
		return null;
	}

	const maxTotal = Math.max(...items.map(item => item.total), 1);

	return (
		<ScopedVitals>
			<section className='vitals-category-overview'>
				<div className='vitals-category-overview-grid'>
					{items.map(item => {
						const statuses = [
							{
								key: 'in-range',
								count: item.inRange,
								className: 'vitals-category-overview-pill-in-range',
								sharePercent: toSharePercent(item.inRange, maxTotal),
							},
							{
								key: 'out-of-range',
								count: item.outOfRange,
								className: 'vitals-category-overview-pill-out-of-range',
								sharePercent: toSharePercent(item.outOfRange, maxTotal),
							},
							{
								key: 'unclassified',
								count: item.unclassified,
								className: 'vitals-category-overview-pill-unclassified',
								sharePercent: toSharePercent(item.unclassified, maxTotal),
							},
						].filter(status => status.count > 0);

						return (
							<article key={item.category} className='vitals-category-overview-item'>
								<div className='vitals-category-overview-row'>
									<h3>{item.category}</h3>
									<span>{item.total}</span>
								</div>

								<div className='vitals-category-overview-pills' role='presentation'>
									{statuses.map(status => (
										<span
											key={`${item.category}-${status.key}`}
											className='vitals-category-overview-pill-group'
											style={{
												width: `calc(var(--vitals-overview-section-min-width) + ${status.sharePercent}%)`,
											}}
										>
											<span className={`vitals-category-overview-pill ${status.className}`} />
											<span className='vitals-category-overview-pill-count'>{status.count}</span>
										</span>
									))}
								</div>
							</article>
						);
					})}
				</div>
			</section>
		</ScopedVitals>
	);
}

export function MeaningfulChanges({ items }: MeaningfulChangesProps) {
	if (items.length === 0) {
		return null;
	}

	const improvedCount = items.filter(item => item.direction === 'improved').length;
	const worsenedCount = items.filter(item => item.direction === 'worsened').length;

	return (
		<ScopedVitals>
			<section className='vitals-meaningful-changes'>
				<div className='vitals-meaningful-changes-header'>
					<h2>Last 6 months changes</h2>
					<p>
						{improvedCount} improved · {worsenedCount} worsened · {items.length} meaningful
					</p>
				</div>

				<div className='vitals-meaningful-changes-list'>
					{items.map(item => {
						const relativeDelta = formatDelta(item.relativeDeltaPercent);
						const rangeDelta = formatDelta(item.normalizedRangeDeltaPercent);

						return (
							<article key={item.key} className='vitals-meaningful-change-item'>
								<div className='vitals-meaningful-change-top'>
									<div className='vitals-meaningful-change-titles'>
										<h3>{item.measurement}</h3>
										<span>{item.category}</span>
									</div>
									<span
										className={`vitals-meaningful-change-direction vitals-meaningful-change-direction-${item.direction}`}
									>
										{getDirectionLabel(item.direction)}
									</span>
								</div>

								<div className='vitals-meaningful-change-comparison'>
									<span className='vitals-meaningful-change-value'>
										<strong>{item.previous.display}</strong>
										<small>{item.previous.prettyDate}</small>
									</span>
									<span className='vitals-meaningful-change-arrow' aria-hidden>
										→
									</span>
									<span className='vitals-meaningful-change-value'>
										<strong>{item.latest.display}</strong>
										<small>{item.latest.prettyDate}</small>
									</span>
								</div>

								<div className='vitals-meaningful-change-meta'>
									{item.reasons.map(reason => (
										<span key={`${item.key}-${reason}`} className='vitals-meaningful-change-reason'>
											{reason}
										</span>
									))}
									{relativeDelta ? (
										<span className='vitals-meaningful-change-delta'>Value Δ {relativeDelta}</span>
									) : null}
									{rangeDelta ? (
										<span className='vitals-meaningful-change-delta'>Range drift {rangeDelta}</span>
									) : null}
								</div>
							</article>
						);
					})}
				</div>
			</section>
		</ScopedVitals>
	);
}

export const TrendChart = memo(function TrendChart({
	series,
	orderedSources,
	isMobile,
}: TrendChartProps) {
	const { token } = antdTheme.useToken();

	const visibleSeries = useMemo(
		() =>
			series.filter(item =>
				orderedSources.some(source => {
					const cell = item.valuesBySourceIndex[source.index];
					if (!cell) return false;
					return cell.display !== '—' && cell.display !== '--' && cell.display.trim() !== '';
				}),
			),
		[orderedSources, series],
	);

	const tableSources = useMemo(
		() =>
			orderedSources.filter(source =>
				visibleSeries.some(item => {
					const cell = item.valuesBySourceIndex[source.index];
					if (!cell) return false;
					return cell.display !== '—' && cell.display !== '--' && cell.display.trim() !== '';
				}),
			),
		[orderedSources, visibleSeries],
	);

	const chartData = useMemo<TrendChartDatum[]>(
		() =>
			orderedSources.map(source => {
				const datum: TrendChartDatum = {
					sourceId: source.id,
					prettyDate: source.prettyDate,
				};

				for (const item of visibleSeries) {
					datum[item.chartKey] = item.normalizedValuesBySourceIndex[source.index] ?? null;
					datum[`${item.chartKey}__out`] = item.outOfRangeBySourceIndex[source.index] ?? false;
				}

				return datum;
			}),
		[orderedSources, visibleSeries],
	);

	const normalizedValues = useMemo(
		() =>
			visibleSeries
				.flatMap(item =>
					orderedSources.map(source => item.normalizedValuesBySourceIndex[source.index] ?? null),
				)
				.filter((value): value is number => value !== null && Number.isFinite(value)),
		[orderedSources, visibleSeries],
	);

	const hasNumericData = normalizedValues.length > 0;

	const yDomain = useMemo<[number, number]>(() => {
		if (!hasNumericData) {
			return [-0.2, 1.2];
		}

		const minValue = Math.min(0, ...normalizedValues);
		const maxValue = Math.max(1, ...normalizedValues);
		const spread = maxValue - minValue || 1;
		const padding = Math.max(0.08 * spread, 0.12);

		return [minValue - padding, maxValue + padding];
	}, [hasNumericData, normalizedValues]);

	const sourceById = useMemo(
		() => new Map(orderedSources.map(source => [source.id, source])),
		[orderedSources],
	);

	return (
		<ScopedVitals>
			<div
				style={{ display: 'flex', width: '100%', flexDirection: 'column', gap: 12, padding: 12 }}
			>
				<div
					style={{ width: '100%', height: isMobile ? 260 : 380, minHeight: isMobile ? 220 : 320 }}
				>
					{hasNumericData ? (
						<ResponsiveContainer width='100%' height='100%'>
							<LineChart
								data={chartData}
								margin={
									isMobile
										? { top: 12, right: 8, left: 0, bottom: 8 }
										: { top: 18, right: 20, left: 12, bottom: 10 }
								}
							>
								<CartesianGrid strokeDasharray='3 3' stroke='rgba(15, 23, 42, 0.16)' />
								<XAxis
									dataKey='sourceId'
									tickFormatter={sourceId =>
										sourceById.get(String(sourceId))?.prettyDate ?? String(sourceId)
									}
									tick={{ fontSize: 11, fill: token.colorTextSecondary }}
									minTickGap={isMobile ? 40 : 22}
									interval='preserveStartEnd'
								/>
								<YAxis
									domain={yDomain}
									tickFormatter={formatNormalizedYAxisTick}
									tick={{ fontSize: 11, fill: token.colorTextSecondary }}
									width={isMobile ? 44 : 56}
								/>
								<ReferenceLine
									y={0}
									stroke={token.colorTextTertiary}
									strokeDasharray='4 4'
									label={{
										value: 'Low',
										position: 'insideLeft',
										fill: token.colorTextTertiary,
										fontSize: 11,
									}}
								/>
								<ReferenceLine
									y={1}
									stroke={token.colorTextTertiary}
									strokeDasharray='4 4'
									label={{
										value: 'High',
										position: 'insideLeft',
										fill: token.colorTextTertiary,
										fontSize: 11,
									}}
								/>
								<RechartsTooltip
									content={({ active, label }) => {
										if (!active || typeof label !== 'string') {
											return null;
										}

										const source = sourceById.get(label);
										if (!source) {
											return null;
										}

										return (
											<div
												style={{
													minWidth: 260,
													maxWidth: 440,
													padding: '8px 12px',
													borderRadius: 8,
													borderStyle: 'solid',
													borderWidth: 1,
													borderColor: token.colorBorder,
													background: token.colorBgContainer,
													boxShadow: token.boxShadowSecondary,
												}}
											>
												<div
													style={{
														marginBottom: 8,
														color: token.colorText,
														fontSize: 12,
														fontWeight: 600,
													}}
												>
													{source.prettyDate}
												</div>
												{visibleSeries.map(item => {
													const cell = item.valuesBySourceIndex[source.index];
													const displayLabel = item.unitLabel
														? `${item.label} (${item.unitLabel})`
														: item.label;
													const displayValue = cell?.display ?? '--';
													const rangeLabel = cell?.rangeCaption?.trim();
													const valueWithRange = rangeLabel
														? `${displayValue} (${rangeLabel})`
														: displayValue;

													return (
														<div
															key={`${label}-${item.id}`}
															style={{
																display: 'flex',
																alignItems: 'center',
																gap: 8,
																padding: '2px 0',
															}}
														>
															<span
																style={{
																	width: 10,
																	height: 10,
																	borderRadius: '999px',
																	background: item.color,
																}}
															/>
															<span
																style={{
																	minWidth: 0,
																	flex: 1,
																	color: token.colorTextSecondary,
																	fontSize: 12,
																}}
															>
																{displayLabel}
															</span>
															<span
																style={{
																	marginLeft: 'auto',
																	color: token.colorText,
																	fontSize: 12,
																	fontWeight: 600,
																}}
															>
																{valueWithRange}
															</span>
														</div>
													);
												})}
											</div>
										);
									}}
								/>
								<Legend
									verticalAlign='bottom'
									align='left'
									iconSize={8}
									formatter={value => (
										<span
											style={{ color: token.colorTextSecondary, fontSize: 11, lineHeight: 1.2 }}
										>
											{value}
										</span>
									)}
									wrapperStyle={{
										paddingTop: 8,
										maxHeight: isMobile ? 68 : undefined,
										overflowY: isMobile ? 'auto' : undefined,
									}}
								/>
								{visibleSeries.map(item => (
									<Line
										key={item.id}
										type='linear'
										dataKey={item.chartKey}
										name={item.unitLabel ? `${item.label} (${item.unitLabel})` : item.label}
										stroke={item.color}
										strokeWidth={2.2}
										connectNulls
										isAnimationActive={false}
										dot={props => {
											const { cx, cy, payload } = props as {
												cx?: number;
												cy?: number;
												payload?: TrendChartDatum;
											};
											if (!Number.isFinite(cx) || !Number.isFinite(cy)) {
												return null;
											}
											const isOutOfRange = Boolean(payload?.[`${item.chartKey}__out`]);
											return (
												<circle
													cx={cx}
													cy={cy}
													r={4}
													fill={isOutOfRange ? token.colorError : item.color}
													stroke={token.colorBgContainer}
													strokeWidth={1.6}
												/>
											);
										}}
										activeDot={props => {
											const { cx, cy, payload } = props as {
												cx?: number;
												cy?: number;
												payload?: TrendChartDatum;
											};
											if (!Number.isFinite(cx) || !Number.isFinite(cy)) {
												return null;
											}
											const isOutOfRange = Boolean(payload?.[`${item.chartKey}__out`]);
											return (
												<circle
													cx={cx}
													cy={cy}
													r={5}
													fill={isOutOfRange ? token.colorError : item.color}
													stroke={token.colorText}
													strokeWidth={1.6}
												/>
											);
										}}
									/>
								))}
							</LineChart>
						</ResponsiveContainer>
					) : (
						<div style={{ display: 'grid', height: '100%', placeItems: 'center' }}>
							<Empty description='No numeric values in the selected rows for this date range.' />
						</div>
					)}
				</div>

				{visibleSeries.length > 0 && tableSources.length > 0 ? (
					<section style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
						<h3
							style={{
								margin: 0,
								color: token.colorTextSecondary,
								fontSize: 12,
								fontWeight: 600,
								textTransform: 'uppercase',
								letterSpacing: '0.04em',
							}}
						>
							Selected values
						</h3>
						<div className='trend-chart-table-shell'>
							<table className='trend-chart-table'>
								<thead>
									<tr>
										<th
											className='trend-chart-table-head'
											style={{
												borderColor: token.colorBorderSecondary,
												background: token.colorFillAlter,
												color: token.colorTextSecondary,
											}}
										>
											Date
										</th>
										{visibleSeries.map(item => (
											<th
												key={`selected-values-heading-${item.id}`}
												className='trend-chart-table-head'
												style={{
													borderColor: token.colorBorderSecondary,
													background: token.colorFillAlter,
													color: token.colorTextSecondary,
												}}
											>
												{item.unitLabel ? `${item.label} (${item.unitLabel})` : item.label}
											</th>
										))}
									</tr>
								</thead>
								<tbody>
									{tableSources.map((source, index) => (
										<tr
											key={`selected-values-row-${source.id}`}
											style={{
												background: index % 2 === 0 ? token.colorFillQuaternary : undefined,
											}}
										>
											<td
												className='trend-chart-table-cell'
												style={{ borderColor: token.colorBorderSecondary }}
											>
												{source.prettyDate}
											</td>
											{visibleSeries.map(item => {
												const cell = item.valuesBySourceIndex[source.index];
												return (
													<td
														key={`selected-values-${source.id}-${item.id}`}
														className='trend-chart-table-cell'
														style={{ borderColor: token.colorBorderSecondary }}
													>
														{cell?.display ?? '--'}
													</td>
												);
											})}
										</tr>
									))}
								</tbody>
							</table>
						</div>
					</section>
				) : null}
			</div>
		</ScopedVitals>
	);
});

export function VitalsControls({
	measurementFilter,
	onMeasurementFilterChange,
	availableDates,
	dateRangeValue,
	onDateRangeSliderChange,
	groupByCategory,
	onGroupByCategoryChange,
	onDownloadCsv,
	isDownloadCsvDisabled,
}: VitalsControlsProps) {
	const { token } = antdTheme.useToken();
	const sliderDates = [...availableDates].reverse();
	const maxIndex = Math.max(availableDates.length - 1, 0);
	const startIndex = Math.min(maxIndex, Math.max(0, dateRangeValue[0] ?? 0));
	const endIndex = Math.min(maxIndex, Math.max(0, dateRangeValue[1] ?? 0));
	const startDateLabel = sliderDates[startIndex] ? formatPrettyDate(sliderDates[startIndex]) : '--';
	const endDateLabel = sliderDates[endIndex] ? formatPrettyDate(sliderDates[endIndex]) : '--';

	return (
		<ScopedVitals>
			<div className='vitals-controls'>
				<label className='vitals-controls-search'>
					<Drop size={16} style={{ color: token.colorTextSecondary }} />
					<input
						value={measurementFilter}
						onChange={onMeasurementFilterChange}
						placeholder='Filter measurements'
					/>
				</label>

				<div className='vitals-controls-range'>
					<div className='vitals-controls-range-labels'>
						<span>{startDateLabel}</span>
						<span>{endDateLabel}</span>
					</div>
					<Slider
						range
						min={0}
						max={maxIndex}
						step={1}
						value={dateRangeValue}
						disabled={availableDates.length <= 1}
						onChange={value => {
							if (!Array.isArray(value) || value.length !== 2) return;
							onDateRangeSliderChange([value[0], value[1]]);
						}}
						tooltip={{
							formatter: value =>
								value === undefined ? '' : formatPrettyDate(sliderDates[value] ?? ''),
						}}
						style={{ margin: 0, width: '100%' }}
						styles={{
							rail: { background: token.colorBorderSecondary },
							track: { background: token.colorText },
							handle: { borderColor: token.colorText, background: token.colorText },
						}}
					/>
				</div>

				<label className='vitals-controls-toggle'>
					<input
						type='checkbox'
						checked={groupByCategory}
						onChange={event => onGroupByCategoryChange(event.target.checked)}
					/>
					Group by category
				</label>

				<button
					type='button'
					className='vitals-controls-button'
					onClick={onDownloadCsv}
					disabled={isDownloadCsvDisabled}
				>
					<DownloadSimple size={14} />
					CSV
				</button>
			</div>
		</ScopedVitals>
	);
}

const SelectionCheckbox = memo(function SelectionCheckbox({
	checked,
	indeterminate = false,
	disabled = false,
	onChange,
	ariaLabel,
}: SelectionCheckboxProps) {
	const { token } = antdTheme.useToken();
	const ref = useRef<HTMLInputElement | null>(null);

	useEffect(() => {
		if (!ref.current) return;
		ref.current.indeterminate = indeterminate;
	}, [indeterminate]);

	return (
		<input
			ref={ref}
			type='checkbox'
			checked={checked}
			disabled={disabled}
			aria-label={ariaLabel}
			onChange={event => onChange(event.target.checked)}
			style={{ accentColor: token.colorPrimary, borderColor: token.colorBorder }}
		/>
	);
});

const MeasurementValueCell = memo(
	function MeasurementValueCell({ cell }: { cell: MeasurementCell | undefined }) {
		const { token } = antdTheme.useToken();

		if (!cell) {
			return <span style={{ color: token.colorTextTertiary }}>--</span>;
		}

		const rangeVisualization = cell.rangeVisualization;
		const hasBand =
			rangeVisualization?.minPosition !== null && rangeVisualization?.maxPosition !== null;

		return (
			<div className='vitals-cell-value'>
				<div style={{ minHeight: 18 }}>
					<span>{cell.display}</span>
				</div>
				{rangeVisualization && cell.rangeCaption ? (
					<div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
						<div className='vitals-range-track'>
							{hasBand ? (
								<span
									className='vitals-range-band'
									style={{
										left: `${cell.rangeBandLeft}%`,
										width: `${cell.rangeBandWidth}%`,
									}}
								/>
							) : null}
							{rangeVisualization.minPosition !== null ? (
								<span
									className='vitals-range-marker'
									style={{ left: `${rangeVisualization.minPosition}%` }}
								/>
							) : null}
							{rangeVisualization.maxPosition !== null ? (
								<span
									className='vitals-range-marker'
									style={{ left: `${rangeVisualization.maxPosition}%` }}
								/>
							) : null}
							<span
								className='vitals-value-marker'
								style={{ left: `${rangeVisualization.valuePosition}%` }}
							/>
						</div>
						<span className='vitals-range-caption'>{cell.rangeCaption}</span>
					</div>
				) : null}
				{cell.flag && cell.flag !== 'normal' ? (
					<span
						className={`vitals-flag ${cell.flag === 'high' || cell.flag === 'critical' ? 'vitals-flag-danger' : 'vitals-flag-warning'}`}
					>
						{cell.flag}
					</span>
				) : null}
			</div>
		);
	},
	(prev, next) => prev.cell === next.cell,
);

type MeasurementRowProps = {
	row: VitalsRowModel;
	tableSources: SourceColumn[];
	highlightedSourceIdSet: Set<string>;
	selected: boolean;
	starred: boolean;
	tooltip: string;
	overview: MeasurementOverviewTally;
	onToggleRow: (key: string, checked: boolean) => void;
	onToggleStar: (measurementKey: string) => void;
};

const MeasurementRow = memo(
	function MeasurementRow({
		row,
		tableSources,
		highlightedSourceIdSet,
		selected,
		starred,
		tooltip,
		overview,
		onToggleRow,
		onToggleStar,
	}: MeasurementRowProps) {
		const { token } = antdTheme.useToken();
		const hasAnyCounter = overview.inRange > 0 || overview.outOfRange > 0;

		return (
			<tr className={selected ? 'vitals-row-selected' : ''}>
				<td className='vitals-cell vitals-col-select'>
					<SelectionCheckbox
						checked={selected}
						onChange={checked => onToggleRow(row.key, checked)}
						ariaLabel={`Select ${row.measurement}`}
					/>
				</td>
				<td className='vitals-cell vitals-col-measurement'>
					<div
						style={{ display: 'flex', minWidth: 0, alignItems: 'flex-start', gap: 6 }}
						title={tooltip}
					>
						<button
							type='button'
							aria-pressed={starred}
							aria-label={starred ? `Unstar ${row.measurement}` : `Star ${row.measurement}`}
							onMouseDown={event => event.preventDefault()}
							onClick={() => onToggleStar(row.key)}
							style={{
								borderColor: 'transparent',
								background: 'transparent',
								color: starred ? token.colorWarning : token.colorTextTertiary,
							}}
						>
							<Star size={14} weight={starred ? 'fill' : 'regular'} />
						</button>
						<span
							style={{
								minWidth: 0,
								lineHeight: 1.35,
								overflowWrap: 'break-word',
								whiteSpace: 'normal',
								fontWeight: starred ? 600 : undefined,
							}}
						>
							{row.measurement}
						</span>
					</div>
				</td>
				<td className='vitals-cell vitals-col-overview'>
					<div
						style={{ display: 'inline-flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}
					>
						{hasAnyCounter ? (
							<>
								{overview.inRange > 0 ? (
									<span
										style={{
											display: 'inline-flex',
											alignItems: 'center',
											gap: 4,
											color: token.colorSuccess,
											fontSize: 11,
											lineHeight: 1,
										}}
										title={`${overview.inRange} in range`}
									>
										<CheckCircle size={13} weight='fill' />
										<span style={{ color: token.colorText, fontWeight: 600 }}>
											{overview.inRange}
										</span>
									</span>
								) : null}
								{overview.outOfRange > 0 ? (
									<span
										style={{
											display: 'inline-flex',
											alignItems: 'center',
											gap: 4,
											color: token.colorError,
											fontSize: 11,
											lineHeight: 1,
										}}
										title={`${overview.outOfRange} out of range`}
									>
										<WarningCircle size={13} weight='fill' />
										<span style={{ color: token.colorText, fontWeight: 600 }}>
											{overview.outOfRange}
										</span>
									</span>
								) : null}
							</>
						) : (
							<span style={{ color: token.colorTextTertiary }}>--</span>
						)}
					</div>
				</td>
				{tableSources.map(source => (
					<td
						key={`${row.key}-${source.id}`}
						className={`vitals-cell ${highlightedSourceIdSet.has(source.id) ? 'vitals-source-filter-active' : ''}`}
					>
						<MeasurementValueCell cell={row.valuesBySourceIndex[source.index]} />
					</td>
				))}
			</tr>
		);
	},
	(prev, next) =>
		prev.row === next.row &&
		prev.tableSources === next.tableSources &&
		prev.highlightedSourceIdSet === next.highlightedSourceIdSet &&
		prev.selected === next.selected &&
		prev.starred === next.starred &&
		prev.tooltip === next.tooltip &&
		prev.overview === next.overview,
);

type CategoryRowProps = {
	row: VitalsCategoryRow;
	tableSources: SourceColumn[];
	highlightedSourceIdSet: Set<string>;
	selection: CategorySelectionState;
	onToggleCategory: (category: string, checked: boolean) => void;
};

const CategoryRow = memo(
	function CategoryRow({
		row,
		tableSources,
		highlightedSourceIdSet,
		selection,
		onToggleCategory,
	}: CategoryRowProps) {
		const { token } = antdTheme.useToken();

		return (
			<tr className='vitals-category-row'>
				<td className='vitals-cell vitals-col-select'>
					<SelectionCheckbox
						checked={selection.checked}
						indeterminate={selection.indeterminate}
						disabled={selection.disabled}
						onChange={checked => onToggleCategory(row.category, checked)}
						ariaLabel={`Select category ${row.category}`}
					/>
				</td>
				<td className='vitals-cell vitals-col-measurement'>
					<div style={{ display: 'inline-flex', minHeight: 22, alignItems: 'center', gap: 8 }}>
						<strong>{row.category}</strong>
						<span style={{ color: token.colorTextTertiary, fontSize: 11 }}>
							{row.categoryCount}
						</span>
					</div>
				</td>
				<td className='vitals-cell vitals-col-overview'>
					<div style={{ minHeight: 18 }} />
				</td>
				{tableSources.map(source => (
					<td
						key={`${row.key}-${source.id}`}
						className={`vitals-cell ${highlightedSourceIdSet.has(source.id) ? 'vitals-source-filter-active' : ''}`}
					>
						<div style={{ minHeight: 18 }} />
					</td>
				))}
			</tr>
		);
	},
	(prev, next) =>
		prev.row === next.row &&
		prev.tableSources === next.tableSources &&
		prev.highlightedSourceIdSet === next.highlightedSourceIdSet &&
		prev.selection.checked === next.selection.checked &&
		prev.selection.indeterminate === next.selection.indeterminate &&
		prev.selection.disabled === next.selection.disabled,
);

export const VitalsTable = memo(function VitalsTable({
	rows,
	tableSources,
	outOfRangeSourceFilterIdSet,
	outOfRangeMeasurementCountBySourceId,
	selectedRowKeySet,
	categorySelectionByName,
	starredMeasurementSet,
	measurementOverviewByKey,
	measurementRangesTooltipByKey,
	tableScrollX,
	tableScrollY,
	onToggleRow,
	onToggleAllRows,
	onToggleCategory,
	onToggleStar,
	onToggleOutOfRangeSourceFilter,
}: VitalsTableProps) {
	const tableShellRef = useRef<HTMLDivElement | null>(null);
	const pendingScrollTopRef = useRef<number | null>(null);

	const selectableRowKeys = useMemo(
		() =>
			rows
				.filter((row): row is VitalsRowModel => row.rowType === 'measurement')
				.map(row => row.key),
		[rows],
	);

	const selectedCount = useMemo(
		() =>
			selectableRowKeys.reduce((count, key) => (selectedRowKeySet.has(key) ? count + 1 : count), 0),
		[selectableRowKeys, selectedRowKeySet],
	);

	const allChecked = selectableRowKeys.length > 0 && selectedCount === selectableRowKeys.length;
	const someChecked = selectedCount > 0 && selectedCount < selectableRowKeys.length;

	const onToggleStarWithScrollLock = useCallback(
		(measurementKey: string) => {
			pendingScrollTopRef.current = tableShellRef.current?.scrollTop ?? null;
			onToggleStar(measurementKey);
		},
		[onToggleStar],
	);

	useLayoutEffect(() => {
		if (pendingScrollTopRef.current === null || !tableShellRef.current) {
			return;
		}
		tableShellRef.current.scrollTop = pendingScrollTopRef.current;
		pendingScrollTopRef.current = null;
	}, [rows]);

	return (
		<ScopedVitals>
			<div style={{ display: 'flex', minHeight: 0, flex: 1 }}>
				<div ref={tableShellRef} className='vitals-table-shell' style={{ maxHeight: tableScrollY }}>
					<table
						className='vitals-table'
						style={{
							minWidth: tableScrollX,
							['--selection-col-width' as string]: `${SELECTION_COLUMN_WIDTH}px`,
							['--measurement-col-width' as string]: `${MEASUREMENT_COLUMN_WIDTH}px`,
							['--overview-col-width' as string]: `${OVERVIEW_COLUMN_WIDTH}px`,
							['--source-col-width' as string]: `${SOURCE_COLUMN_WIDTH}px`,
						}}
					>
						<thead>
							<tr>
								<th className='vitals-head vitals-col-select'>
									<SelectionCheckbox
										checked={allChecked}
										indeterminate={someChecked}
										disabled={selectableRowKeys.length === 0}
										onChange={onToggleAllRows}
										ariaLabel='Select all'
									/>
								</th>
								<th className='vitals-head vitals-col-measurement'>Measurement</th>
								<th className='vitals-head vitals-col-overview'>Overview</th>
								{tableSources.map(source => {
									const isFiltered = outOfRangeSourceFilterIdSet.has(source.id);
									const outOfRangeCount = outOfRangeMeasurementCountBySourceId.get(source.id) ?? 0;

									return (
										<th
											key={source.id}
											className={`vitals-head ${isFiltered ? 'vitals-source-filter-active' : ''}`}
										>
											<div className='vitals-source-head'>
												<span>{source.prettyDate}</span>
												<button
													type='button'
													aria-label={`Filter measurements out of range in ${source.prettyDate}`}
													aria-pressed={isFiltered}
													onClick={() => onToggleOutOfRangeSourceFilter(source.id)}
													className={`vitals-source-filter-toggle ${isFiltered ? 'vitals-source-filter-toggle-active' : ''}`}
												>
													<Flag size={12} weight={isFiltered ? 'fill' : 'regular'} />
													<span>{outOfRangeCount}</span>
												</button>
											</div>
										</th>
									);
								})}
							</tr>
						</thead>
						<tbody>
							{rows.map(row => {
								if (row.rowType === 'category') {
									return (
										<CategoryRow
											key={row.key}
											row={row}
											tableSources={tableSources}
											highlightedSourceIdSet={outOfRangeSourceFilterIdSet}
											selection={
												categorySelectionByName.get(row.category) ?? {
													checked: false,
													indeterminate: false,
													disabled: true,
												}
											}
											onToggleCategory={onToggleCategory}
										/>
									);
								}

								return (
									<MeasurementRow
										key={row.key}
										row={row}
										tableSources={tableSources}
										highlightedSourceIdSet={outOfRangeSourceFilterIdSet}
										selected={selectedRowKeySet.has(row.key)}
										starred={starredMeasurementSet.has(row.key)}
										tooltip={measurementRangesTooltipByKey.get(row.key) ?? row.measurement}
										overview={
											measurementOverviewByKey.get(row.key) ?? { inRange: 0, outOfRange: 0 }
										}
										onToggleRow={onToggleRow}
										onToggleStar={onToggleStarWithScrollLock}
									/>
								);
							})}
						</tbody>
					</table>
				</div>
			</div>
		</ScopedVitals>
	);
});
