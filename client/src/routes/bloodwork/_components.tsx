import styled from '@emotion/styled'
import { CheckCircle, DownloadSimple, Drop, Flag, Star, WarningCircle } from '@phosphor-icons/react'
import { Button, Card, Checkbox, Empty, Flex, Input, Slider, Space, Table, Tag, Tooltip, Typography, theme as antdTheme } from 'antd'
import type { TableColumnsType } from 'antd'
import { type ChangeEvent, memo, useMemo } from 'react'
import { CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip as RechartsTooltip, XAxis, YAxis } from 'recharts'

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
    VitalsDisplayRow,
    VitalsRowModel,
} from './_bloodwork'
import {
    formatNormalizedYAxisTick,
    formatPrettyDate,
    MEASUREMENT_COLUMN_WIDTH,
    OVERVIEW_COLUMN_WIDTH,
    SELECTION_COLUMN_WIDTH,
    SOURCE_COLUMN_WIDTH,
} from './_bloodwork'

const OverviewGrid = styled.div`
    display: grid;
    gap: 12px;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
`

const ChangesGrid = styled.div`
    display: grid;
    gap: 12px;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
`

const ComparisonRow = styled.div`
    display: grid;
    gap: 8px;
    grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr);
`

const ComparisonValue = styled.div`
    display: flex;
    min-width: 0;
    flex-direction: column;
    gap: 2px;
`

const TableShell = styled.div`
    .bloodwork-table .ant-table-cell {
        vertical-align: top;
    }

    .bloodwork-table .ant-table-thead > tr > th {
        vertical-align: top;
    }
`

const RangeTrack = styled.div`
    position: relative;
    height: 10px;
    overflow: hidden;
    border-radius: 999px;
`

const RangeBand = styled.span`
    position: absolute;
    top: 1px;
    bottom: 1px;
    border-radius: 999px;
`

const RangeMarker = styled.span`
    position: absolute;
    top: -2px;
    bottom: -2px;
    width: 2px;
    transform: translateX(-50%);
`

const ValueMarker = styled.span`
    position: absolute;
    top: 50%;
    width: 8px;
    height: 8px;
    border-radius: 999px;
    transform: translate(-50%, -50%);
`

type CategoriesOverviewProps = {
    items: CategoryOverviewItem[]
}

type MeaningfulChangesProps = {
    items: MeaningfulChangeItem[]
}

type TrendChartProps = {
    series: ChartSeriesModel[]
    orderedSources: SourceColumn[]
    isMobile: boolean
}

type VitalsControlsProps = {
    measurementFilter: string
    onMeasurementFilterChange: (event: ChangeEvent<HTMLInputElement>) => void
    availableDates: string[]
    dateRangeValue: [number, number]
    onDateRangeSliderChange: (nextRange: [number, number]) => void
    groupByCategory: boolean
    onGroupByCategoryChange: (checked: boolean) => void
    onDownloadCsv: () => void
    isDownloadCsvDisabled: boolean
}

type VitalsTableProps = {
    rows: VitalsDisplayRow[]
    tableSources: SourceColumn[]
    outOfRangeSourceFilterIdSet: Set<string>
    outOfRangeMeasurementCountBySourceId: Map<string, number>
    selectedRowKeySet: Set<string>
    categorySelectionByName: Map<string, CategorySelectionState>
    starredMeasurementSet: Set<string>
    measurementOverviewByKey: Map<string, MeasurementOverviewTally>
    measurementRangesTooltipByKey: Map<string, string>
    tableScrollX: number
    tableScrollY: number
    onToggleRow: (key: string, checked: boolean) => void
    onToggleAllRows: (checked: boolean) => void
    onToggleCategory: (category: string, checked: boolean) => void
    onToggleStar: (measurementKey: string) => void
    onToggleOutOfRangeSourceFilter: (sourceId: string) => void
}

function formatDelta(value: number | null): string | null {
    if (value === null || !Number.isFinite(value)) {
        return null
    }
    if (value >= 1000) {
        return `${Math.round(value)}%`
    }
    if (value >= 100) {
        return `${value.toFixed(0)}%`
    }
    return `${value.toFixed(1)}%`
}

function getDirectionLabel(direction: MeaningfulChangeDirection) {
    if (direction === 'improved') return 'Improved'
    if (direction === 'worsened') return 'Worsened'
    return 'Changed'
}

function getDirectionTagStyle(token: ReturnType<typeof antdTheme.useToken>['token'], direction: MeaningfulChangeDirection) {
    if (direction === 'improved') {
        return {
            background: token.colorSuccessBg,
            borderColor: token.colorSuccessBorder,
            color: token.colorSuccess,
        }
    }
    if (direction === 'worsened') {
        return {
            background: token.colorErrorBg,
            borderColor: token.colorErrorBorder,
            color: token.colorError,
        }
    }
    return {
        background: token.colorFillAlter,
        borderColor: token.colorBorder,
        color: token.colorTextSecondary,
    }
}

function getCategoryCellStyle(args: {
    row: VitalsDisplayRow
    isFiltered?: boolean
    isSelected?: boolean
    token: ReturnType<typeof antdTheme.useToken>['token']
}) {
    if (args.row.rowType === 'category') {
        return {
            background: args.isFiltered ? args.token.colorErrorBg : args.token.colorPrimaryBg,
            verticalAlign: 'top' as const,
        }
    }

    if (args.isFiltered) {
        return {
            background: args.token.colorErrorBg,
            verticalAlign: 'top' as const,
        }
    }

    if (args.isSelected) {
        return {
            background: args.token.colorFillAlter,
            verticalAlign: 'top' as const,
        }
    }

    return {
        verticalAlign: 'top' as const,
    }
}

export function CategoriesOverview({ items }: CategoriesOverviewProps) {
    const { token } = antdTheme.useToken()

    if (items.length === 0) {
        return null
    }

    return (
        <Card
            size='small'
            title='Category overview'
            styles={{ body: { padding: 12 } }}
        >
            <OverviewGrid>
                {items.map(item => (
                    <Card
                        key={item.category}
                        size='small'
                        styles={{ body: { padding: 12 } }}
                    >
                        <Flex vertical gap={10}>
                            <Flex justify='space-between' gap={12}>
                                <Typography.Text strong>{item.category}</Typography.Text>
                                <Typography.Text type='secondary'>{item.total}</Typography.Text>
                            </Flex>

                            <Flex wrap gap={8}>
                                {item.inRange > 0 ? (
                                    <Tag
                                        style={{
                                            marginInlineEnd: 0,
                                            borderColor: token.colorSuccessBorder,
                                            background: token.colorSuccessBg,
                                            color: token.colorSuccess,
                                        }}
                                    >
                                        In range {item.inRange}
                                    </Tag>
                                ) : null}
                                {item.outOfRange > 0 ? (
                                    <Tag
                                        style={{
                                            marginInlineEnd: 0,
                                            borderColor: token.colorErrorBorder,
                                            background: token.colorErrorBg,
                                            color: token.colorError,
                                        }}
                                    >
                                        Out of range {item.outOfRange}
                                    </Tag>
                                ) : null}
                                {item.unclassified > 0 ? (
                                    <Tag
                                        style={{
                                            marginInlineEnd: 0,
                                            borderColor: token.colorBorder,
                                            background: token.colorFillAlter,
                                            color: token.colorTextSecondary,
                                        }}
                                    >
                                        Unclassified {item.unclassified}
                                    </Tag>
                                ) : null}
                            </Flex>
                        </Flex>
                    </Card>
                ))}
            </OverviewGrid>
        </Card>
    )
}

export function MeaningfulChanges({ items }: MeaningfulChangesProps) {
    const { token } = antdTheme.useToken()

    if (items.length === 0) {
        return null
    }

    const improvedCount = items.filter(item => item.direction === 'improved').length
    const worsenedCount = items.filter(item => item.direction === 'worsened').length

    return (
        <Card
            size='small'
            title='Last 6 months changes'
            extra={(
                <Typography.Text type='secondary'>
                    {improvedCount} improved · {worsenedCount} worsened · {items.length} meaningful
                </Typography.Text>
            )}
            styles={{ body: { padding: 12 } }}
        >
            <ChangesGrid>
                {items.map(item => {
                    const relativeDelta = formatDelta(item.relativeDeltaPercent)
                    const rangeDelta = formatDelta(item.normalizedRangeDeltaPercent)

                    return (
                        <Card
                            key={item.key}
                            size='small'
                            styles={{ body: { padding: 12 } }}
                        >
                            <Flex vertical gap={10}>
                                <Flex justify='space-between' align='start' gap={12}>
                                    <Flex vertical gap={2}>
                                        <Typography.Text strong>{item.measurement}</Typography.Text>
                                        <Typography.Text type='secondary'>{item.category}</Typography.Text>
                                    </Flex>

                                    <Tag
                                        style={{
                                            marginInlineEnd: 0,
                                            ...getDirectionTagStyle(token, item.direction),
                                        }}
                                    >
                                        {getDirectionLabel(item.direction)}
                                    </Tag>
                                </Flex>

                                <ComparisonRow>
                                    <ComparisonValue>
                                        <Typography.Text strong>{item.previous.display}</Typography.Text>
                                        <Typography.Text type='secondary'>{item.previous.prettyDate}</Typography.Text>
                                    </ComparisonValue>
                                    <Typography.Text type='secondary'>→</Typography.Text>
                                    <ComparisonValue>
                                        <Typography.Text strong>{item.latest.display}</Typography.Text>
                                        <Typography.Text type='secondary'>{item.latest.prettyDate}</Typography.Text>
                                    </ComparisonValue>
                                </ComparisonRow>

                                <Space size={[6, 6]} wrap>
                                    {item.reasons.map(reason => (
                                        <Tag key={`${item.key}-${reason}`} style={{ marginInlineEnd: 0 }}>
                                            {reason}
                                        </Tag>
                                    ))}
                                    {relativeDelta ? (
                                        <Tag style={{ marginInlineEnd: 0 }}>
                                            Value Δ {relativeDelta}
                                        </Tag>
                                    ) : null}
                                    {rangeDelta ? (
                                        <Tag style={{ marginInlineEnd: 0 }}>
                                            Range drift {rangeDelta}
                                        </Tag>
                                    ) : null}
                                </Space>
                            </Flex>
                        </Card>
                    )
                })}
            </ChangesGrid>
        </Card>
    )
}

export const TrendChart = memo(function TrendChart({
    series,
    orderedSources,
    isMobile,
}: TrendChartProps) {
    const { token } = antdTheme.useToken()
    const visibleSeries = useMemo(
        () =>
            series.filter(item => orderedSources.some(source => {
                const cell = item.valuesBySourceIndex[source.index]
                if (!cell) return false
                return cell.display !== '—' && cell.display !== '--' && cell.display.trim() !== ''
            })),
        [orderedSources, series],
    )

    const chartData = useMemo<TrendChartDatum[]>(
        () =>
            orderedSources.map(source => {
                const datum: TrendChartDatum = {
                    sourceId: source.id,
                    prettyDate: source.prettyDate,
                }
                for (const item of visibleSeries) {
                    datum[item.chartKey] = item.normalizedValuesBySourceIndex[source.index] ?? null
                    datum[`${item.chartKey}__out`] = item.outOfRangeBySourceIndex[source.index] ?? false
                }
                return datum
            }),
        [orderedSources, visibleSeries],
    )

    const normalizedValues = useMemo(
        () =>
            visibleSeries
                .flatMap(item => orderedSources.map(source => item.normalizedValuesBySourceIndex[source.index] ?? null))
                .filter((value): value is number => value !== null && Number.isFinite(value)),
        [orderedSources, visibleSeries],
    )

    const hasNumericData = normalizedValues.length > 0

    const yDomain = useMemo<[number, number]>(() => {
        if (!hasNumericData) {
            return [-0.2, 1.2]
        }
        const minValue = Math.min(0, ...normalizedValues)
        const maxValue = Math.max(1, ...normalizedValues)
        const spread = maxValue - minValue || 1
        const padding = Math.max(0.08 * spread, 0.12)
        return [minValue - padding, maxValue + padding]
    }, [hasNumericData, normalizedValues])

    const sourceById = useMemo(
        () => new Map(orderedSources.map(source => [source.id, source])),
        [orderedSources],
    )

    return (
        <div style={{ display: 'flex', width: '100%', flexDirection: 'column', gap: 12 }}>
            <div style={{ width: '100%', height: isMobile ? 260 : 380, minHeight: isMobile ? 220 : 320 }}>
                {hasNumericData ? (
                    <ResponsiveContainer width='100%' height='100%'>
                        <LineChart
                            data={chartData}
                            margin={isMobile
                                ? { top: 12, right: 8, left: 0, bottom: 8 }
                                : { top: 18, right: 20, left: 12, bottom: 10 }}
                        >
                            <CartesianGrid strokeDasharray='3 3' stroke='rgba(15, 23, 42, 0.16)' />
                            <XAxis
                                dataKey='sourceId'
                                tickFormatter={sourceId => sourceById.get(String(sourceId))?.prettyDate ?? String(sourceId)}
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
                                label={{ value: 'Low', position: 'insideLeft', fill: token.colorTextTertiary, fontSize: 11 }}
                            />
                            <ReferenceLine
                                y={1}
                                stroke={token.colorTextTertiary}
                                strokeDasharray='4 4'
                                label={{ value: 'High', position: 'insideLeft', fill: token.colorTextTertiary, fontSize: 11 }}
                            />
                            <RechartsTooltip
                                content={({ active, label }) => {
                                    if (!active || typeof label !== 'string') {
                                        return null
                                    }
                                    const source = sourceById.get(label)
                                    if (!source) {
                                        return null
                                    }

                                    return (
                                        <Card
                                            size='small'
                                            styles={{ body: { padding: '8px 12px' } }}
                                            style={{
                                                minWidth: 260,
                                                maxWidth: 440,
                                                borderColor: token.colorBorder,
                                                background: token.colorBgContainer,
                                                boxShadow: token.boxShadowSecondary,
                                            }}
                                        >
                                            <Flex vertical gap={6}>
                                                <Typography.Text strong>{source.prettyDate}</Typography.Text>
                                                {visibleSeries.map(item => {
                                                    const cell = item.valuesBySourceIndex[source.index]
                                                    const displayLabel = item.unitLabel
                                                        ? `${item.label} (${item.unitLabel})`
                                                        : item.label
                                                    const displayValue = cell?.display ?? '--'
                                                    const rangeLabel = cell?.rangeCaption?.trim()
                                                    const valueWithRange = rangeLabel
                                                        ? `${displayValue} (${rangeLabel})`
                                                        : displayValue
                                                    return (
                                                        <Flex key={`${label}-${item.id}`} align='center' gap={8}>
                                                            <span
                                                                style={{
                                                                    width: 10,
                                                                    height: 10,
                                                                    borderRadius: '999px',
                                                                    background: item.color,
                                                                    flex: '0 0 auto',
                                                                }}
                                                            />
                                                            <Typography.Text type='secondary' style={{ flex: 1 }}>
                                                                {displayLabel}
                                                            </Typography.Text>
                                                            <Typography.Text strong>{valueWithRange}</Typography.Text>
                                                        </Flex>
                                                    )
                                                })}
                                            </Flex>
                                        </Card>
                                    )
                                }}
                            />
                            <Legend
                                verticalAlign='bottom'
                                align='left'
                                iconSize={8}
                                formatter={value => (
                                    <span style={{ color: token.colorTextSecondary, fontSize: 11, lineHeight: 1.2 }}>
                                        {value}
                                    </span>
                                )}
                                wrapperStyle={{ paddingTop: 8, maxHeight: isMobile ? 68 : undefined, overflowY: isMobile ? 'auto' : undefined }}
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
                                            cx?: number
                                            cy?: number
                                            payload?: TrendChartDatum
                                        }
                                        if (!Number.isFinite(cx) || !Number.isFinite(cy)) {
                                            return null
                                        }
                                        const isOutOfRange = Boolean(payload?.[`${item.chartKey}__out`])
                                        return (
                                            <circle
                                                cx={cx}
                                                cy={cy}
                                                r={4}
                                                fill={isOutOfRange ? token.colorError : item.color}
                                                stroke={token.colorBgContainer}
                                                strokeWidth={1.5}
                                            />
                                        )
                                    }}
                                    activeDot={{
                                        r: 5,
                                        stroke: token.colorBgContainer,
                                        strokeWidth: 1.5,
                                    }}
                                />
                            ))}
                        </LineChart>
                    </ResponsiveContainer>
                ) : (
                    <Empty description='No numeric values in the selected rows for this date range.' />
                )}
            </div>
        </div>
    )
})

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
    const sliderDates = [...availableDates].reverse()
    const maxIndex = Math.max(availableDates.length - 1, 0)
    const startIndex = Math.min(maxIndex, Math.max(0, dateRangeValue[0] ?? 0))
    const endIndex = Math.min(maxIndex, Math.max(0, dateRangeValue[1] ?? 0))
    const startDateLabel = sliderDates[startIndex] ? formatPrettyDate(sliderDates[startIndex]) : '--'
    const endDateLabel = sliderDates[endIndex] ? formatPrettyDate(sliderDates[endIndex]) : '--'

    return (
        <Card size='small' styles={{ body: { padding: 12 } }}>
            <Flex vertical gap={12}>
                <Flex wrap gap={12} align='center'>
                    <Input
                        value={measurementFilter}
                        onChange={onMeasurementFilterChange}
                        placeholder='Filter measurements'
                        prefix={<Drop size={16} />}
                        allowClear
                        style={{ flex: 1, minWidth: 240 }}
                    />

                    <Checkbox
                        checked={groupByCategory}
                        onChange={event => onGroupByCategoryChange(event.target.checked)}
                    >
                        Group by category
                    </Checkbox>

                    <Button
                        icon={<DownloadSimple size={14} />}
                        onClick={onDownloadCsv}
                        disabled={isDownloadCsvDisabled}
                    >
                        CSV
                    </Button>
                </Flex>

                <Flex vertical gap={8}>
                    <Flex justify='space-between' gap={12}>
                        <Typography.Text type='secondary'>{startDateLabel}</Typography.Text>
                        <Typography.Text type='secondary'>{endDateLabel}</Typography.Text>
                    </Flex>

                    <Slider
                        range
                        min={0}
                        max={maxIndex}
                        step={1}
                        value={dateRangeValue}
                        disabled={availableDates.length <= 1}
                        onChange={value => {
                            if (!Array.isArray(value) || value.length !== 2) return
                            onDateRangeSliderChange([value[0], value[1]])
                        }}
                        tooltip={{ formatter: value => (value === undefined ? '' : formatPrettyDate(sliderDates[value] ?? '')) }}
                    />
                </Flex>
            </Flex>
        </Card>
    )
}

function MeasurementValueCell({ cell }: { cell: MeasurementCell | undefined }) {
    const { token } = antdTheme.useToken()

    if (!cell) {
        return <Typography.Text type='secondary'>--</Typography.Text>
    }

    const rangeVisualization = cell.rangeVisualization
    const hasBand =
        rangeVisualization?.minPosition !== null &&
        rangeVisualization?.maxPosition !== null

    return (
        <Flex vertical gap={4}>
            <Typography.Text>{cell.display}</Typography.Text>

            {rangeVisualization && cell.rangeCaption ? (
                <Flex vertical gap={4}>
                    <RangeTrack
                        style={{
                            border: `1px solid ${token.colorBorder}`,
                            background: token.colorFillSecondary,
                        }}
                    >
                        {hasBand ? (
                            <RangeBand
                                style={{
                                    left: `${cell.rangeBandLeft}%`,
                                    width: `${cell.rangeBandWidth}%`,
                                    background: token.colorPrimaryBorder,
                                }}
                            />
                        ) : null}
                        {rangeVisualization.minPosition !== null ? (
                            <RangeMarker
                                style={{
                                    left: `${rangeVisualization.minPosition}%`,
                                    background: token.colorTextSecondary,
                                }}
                            />
                        ) : null}
                        {rangeVisualization.maxPosition !== null ? (
                            <RangeMarker
                                style={{
                                    left: `${rangeVisualization.maxPosition}%`,
                                    background: token.colorTextSecondary,
                                }}
                            />
                        ) : null}
                        <ValueMarker
                            style={{
                                left: `${rangeVisualization.valuePosition}%`,
                                background: token.colorError,
                                border: `1px solid ${token.colorBgContainer}`,
                            }}
                        />
                    </RangeTrack>

                    <Typography.Text type='secondary' style={{ fontSize: 11 }}>
                        {cell.rangeCaption}
                    </Typography.Text>
                </Flex>
            ) : null}

            {cell.flag && cell.flag !== 'normal' ? (
                <Tag
                    style={{
                        width: 'fit-content',
                        marginInlineEnd: 0,
                        textTransform: 'uppercase',
                        ...(cell.flag === 'high' || cell.flag === 'critical'
                            ? {
                                borderColor: token.colorErrorBorder,
                                background: token.colorErrorBg,
                                color: token.colorError,
                            }
                            : {
                                borderColor: token.colorWarningBorder,
                                background: token.colorWarningBg,
                                color: token.colorWarning,
                            }),
                    }}
                >
                    {cell.flag}
                </Tag>
            ) : null}
        </Flex>
    )
}

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
    const { token } = antdTheme.useToken()

    const selectableRowKeys = useMemo(
        () => rows.filter((row): row is VitalsRowModel => row.rowType === 'measurement').map(row => row.key),
        [rows],
    )

    const selectedCount = useMemo(
        () => selectableRowKeys.reduce((count, key) => (selectedRowKeySet.has(key) ? count + 1 : count), 0),
        [selectableRowKeys, selectedRowKeySet],
    )

    const allChecked = selectableRowKeys.length > 0 && selectedCount === selectableRowKeys.length
    const someChecked = selectedCount > 0 && selectedCount < selectableRowKeys.length
    const measurementRowCount = selectableRowKeys.length

    const columns = useMemo<TableColumnsType<VitalsDisplayRow>>(() => {
        const sharedLeftCellStyle = (row: VitalsDisplayRow) => getCategoryCellStyle({
            row,
            isSelected: row.rowType === 'measurement' ? selectedRowKeySet.has(row.key) : false,
            token,
        })

        return [
            {
                title: (
                    <Checkbox
                        checked={allChecked}
                        indeterminate={someChecked}
                        disabled={selectableRowKeys.length === 0}
                        onChange={event => onToggleAllRows(event.target.checked)}
                    />
                ),
                key: 'selection',
                dataIndex: 'key',
                width: SELECTION_COLUMN_WIDTH,
                fixed: 'left',
                align: 'center',
                render: (_: unknown, row) => {
                    if (row.rowType === 'category') {
                        const selection = categorySelectionByName.get(row.category) ?? {
                            checked: false,
                            indeterminate: false,
                            disabled: true,
                        }

                        return (
                            <Checkbox
                                checked={selection.checked}
                                indeterminate={selection.indeterminate}
                                disabled={selection.disabled}
                                onChange={event => onToggleCategory(row.category, event.target.checked)}
                            />
                        )
                    }

                    return (
                        <Checkbox
                            checked={selectedRowKeySet.has(row.key)}
                            onChange={event => onToggleRow(row.key, event.target.checked)}
                        />
                    )
                },
                onCell: row => ({
                    style: sharedLeftCellStyle(row),
                }),
            },
            {
                title: 'Measurement',
                key: 'measurement',
                width: MEASUREMENT_COLUMN_WIDTH,
                fixed: 'left',
                render: (_: unknown, row) => {
                    if (row.rowType === 'category') {
                        return (
                            <Flex align='center' gap={8}>
                                <Typography.Text strong>{row.category}</Typography.Text>
                                <Typography.Text type='secondary'>{row.categoryCount}</Typography.Text>
                            </Flex>
                        )
                    }

                    const tooltip = measurementRangesTooltipByKey.get(row.key) ?? row.measurement

                    return (
                        <Tooltip title={tooltip} placement='topLeft'>
                            <Flex align='start' gap={6}>
                                <Button
                                    type='text'
                                    size='small'
                                    icon={<Star size={14} weight={starredMeasurementSet.has(row.key) ? 'fill' : 'regular'} />}
                                    onClick={() => onToggleStar(row.key)}
                                    style={{
                                        color: starredMeasurementSet.has(row.key) ? token.colorWarning : token.colorTextTertiary,
                                        paddingInline: 4,
                                    }}
                                />
                                <Typography.Text strong={starredMeasurementSet.has(row.key)}>
                                    {row.measurement}
                                </Typography.Text>
                            </Flex>
                        </Tooltip>
                    )
                },
                onCell: row => ({
                    style: sharedLeftCellStyle(row),
                }),
            },
            {
                title: 'Overview',
                key: 'overview',
                width: OVERVIEW_COLUMN_WIDTH,
                fixed: 'left',
                render: (_: unknown, row) => {
                    if (row.rowType === 'category') {
                        return null
                    }

                    const overview = measurementOverviewByKey.get(row.key) ?? { inRange: 0, outOfRange: 0 }
                    const hasAnyCounter = overview.inRange > 0 || overview.outOfRange > 0

                    if (!hasAnyCounter) {
                        return <Typography.Text type='secondary'>--</Typography.Text>
                    }

                    return (
                        <Flex gap={8} wrap>
                            {overview.inRange > 0 ? (
                                <Flex align='center' gap={4}>
                                    <CheckCircle size={13} weight='fill' color={token.colorSuccess} />
                                    <Typography.Text>{overview.inRange}</Typography.Text>
                                </Flex>
                            ) : null}
                            {overview.outOfRange > 0 ? (
                                <Flex align='center' gap={4}>
                                    <WarningCircle size={13} weight='fill' color={token.colorError} />
                                    <Typography.Text>{overview.outOfRange}</Typography.Text>
                                </Flex>
                            ) : null}
                        </Flex>
                    )
                },
                onCell: row => ({
                    style: sharedLeftCellStyle(row),
                }),
            },
            ...tableSources.map(source => {
                const isFiltered = outOfRangeSourceFilterIdSet.has(source.id)
                const outOfRangeCount = outOfRangeMeasurementCountBySourceId.get(source.id) ?? 0

                return {
                    title: (
                        <Flex vertical gap={6}>
                            <Typography.Text>{source.prettyDate}</Typography.Text>
                            <Button
                                size='small'
                                danger
                                type={isFiltered ? 'primary' : 'default'}
                                icon={<Flag size={12} weight={isFiltered ? 'fill' : 'regular'} />}
                                onClick={() => onToggleOutOfRangeSourceFilter(source.id)}
                            >
                                {outOfRangeCount}
                            </Button>
                        </Flex>
                    ),
                    key: source.id,
                    width: SOURCE_COLUMN_WIDTH,
                    render: (_: unknown, row: VitalsDisplayRow) => {
                        if (row.rowType === 'category') {
                            return null
                        }

                        return <MeasurementValueCell cell={row.valuesBySourceIndex[source.index]} />
                    },
                    onHeaderCell: () => ({
                        style: isFiltered
                            ? { background: token.colorErrorBgHover, verticalAlign: 'top' as const }
                            : { verticalAlign: 'top' as const },
                    }),
                    onCell: (row: VitalsDisplayRow) => ({
                        style: getCategoryCellStyle({
                            row,
                            isFiltered,
                            isSelected: row.rowType === 'measurement' ? selectedRowKeySet.has(row.key) : false,
                            token,
                        }),
                    }),
                }
            }),
        ]
    }, [
        allChecked,
        categorySelectionByName,
        measurementOverviewByKey,
        measurementRangesTooltipByKey,
        onToggleAllRows,
        onToggleCategory,
        onToggleOutOfRangeSourceFilter,
        onToggleRow,
        onToggleStar,
        outOfRangeMeasurementCountBySourceId,
        outOfRangeSourceFilterIdSet,
        selectableRowKeys.length,
        selectedRowKeySet,
        someChecked,
        starredMeasurementSet,
        tableSources,
        token,
    ])

    return (
        <Card
            size='small'
            title='Measurements'
            extra={<Typography.Text type='secondary'>{measurementRowCount} measurements</Typography.Text>}
            styles={{ body: { padding: 0 } }}
        >
            <TableShell>
                <Table<VitalsDisplayRow>
                    className='bloodwork-table'
                    rowKey='key'
                    size='small'
                    columns={columns}
                    dataSource={rows}
                    pagination={false}
                    sticky
                    scroll={{ x: tableScrollX, y: tableScrollY }}
                    locale={{
                        emptyText: <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description='No measurements found for this range' />,
                    }}
                />
            </TableShell>
        </Card>
    )
})
