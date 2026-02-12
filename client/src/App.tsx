import { useEffect, useMemo, useState, type Key } from 'react';

import styled from '@emotion/styled';
import { ChartLineUp, Drop, Flask, Pulse } from '@phosphor-icons/react';
import { Alert, Card, Empty, Input, Select, Space, Spin, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';

type BloodworkMeasurement = {
    name: string;
    originalName?: string;
    category?: string;
    value?: number | string;
    unit?: string;
    referenceRange?: {
        min?: number;
        max?: number;
    };
    flag?: 'low' | 'high' | 'normal' | 'abnormal' | 'critical' | 'unknown';
    note?: string;
};

type BloodworkLab = {
    date: string;
    labName: string;
    location?: string;
    importLocation?: string;
    weightKg?: number;
    measurements: BloodworkMeasurement[];
    notes?: string;
};

type ApiResponse = {
    items: BloodworkLab[];
};

type SourceColumn = {
    id: string;
    label: string;
    date: string;
    source: string;
};

type MeasurementCell = {
    display: string;
    numericValue: number | null;
    unit?: string;
    flag?: BloodworkMeasurement['flag'];
    note?: string;
};

type MeasurementRow = {
    key: string;
    measurement: string;
    valuesBySource: Record<string, MeasurementCell | undefined>;
};

type ChartSeries = {
    id: string;
    label: string;
    points: Array<{ sourceId: string; sourceLabel: string; xIndex: number; value: number }>;
};

const { Title, Text } = Typography;

function parseNumericValue(value: number | string | undefined): number | null {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value !== 'string') return null;

    const normalized = value.replace(',', '.').replace(/[^0-9.+-]/g, '').trim();
    if (!normalized) return null;
    const parsed = Number.parseFloat(normalized);
    return Number.isFinite(parsed) ? parsed : null;
}

function formatCell(measurement: BloodworkMeasurement): MeasurementCell {
    const valueText = measurement.value === undefined ? '' : String(measurement.value).trim();
    const unitText = measurement.unit?.trim();
    const display = [valueText, unitText].filter(Boolean).join(' ').trim() || '—';

    return {
        display,
        numericValue: parseNumericValue(measurement.value),
        unit: unitText || undefined,
        flag: measurement.flag,
        note: measurement.note?.trim() || undefined,
    };
}

function useViewport() {
    const [size, setSize] = useState({
        width: window.innerWidth,
        height: window.innerHeight,
    });

    useEffect(() => {
        const onResize = () => {
            setSize({
                width: window.innerWidth,
                height: window.innerHeight,
            });
        };

        window.addEventListener('resize', onResize);
        return () => window.removeEventListener('resize', onResize);
    }, []);

    return size;
}

function TrendChart({
    series,
    orderedSources,
}: {
    series: ChartSeries[];
    orderedSources: SourceColumn[];
}) {
    const chartWidth = 980;
    const chartHeight = 420;
    const margin = { top: 40, right: 24, bottom: 94, left: 70 };
    const innerWidth = chartWidth - margin.left - margin.right;
    const innerHeight = chartHeight - margin.top - margin.bottom;

    const allValues = series.flatMap(item => item.points.map(point => point.value));
    if (allValues.length === 0) {
        return <Empty description='Select rows and columns with numeric values to render trends.' />;
    }

    const minValue = Math.min(...allValues);
    const maxValue = Math.max(...allValues);
    const range = maxValue - minValue || 1;
    const paddedMin = minValue - range * 0.08;
    const paddedMax = maxValue + range * 0.08;

    const sourceIds = orderedSources.map(item => item.id);
    const xStep = sourceIds.length > 1 ? innerWidth / (sourceIds.length - 1) : 0;

    const getX = (sourceId: string) => {
        const index = sourceIds.indexOf(sourceId);
        const safeIndex = index < 0 ? 0 : index;
        return margin.left + safeIndex * xStep;
    };
    const getY = (value: number) =>
        margin.top + innerHeight - ((value - paddedMin) / (paddedMax - paddedMin)) * innerHeight;

    const palette = ['#0f766e', '#fb7185', '#f59e0b', '#3b82f6', '#7c3aed', '#10b981', '#e11d48'];
    const yTicks = 5;
    const yTickValues = Array.from({ length: yTicks + 1 }, (_, index) => {
        const ratio = index / yTicks;
        return paddedMax - ratio * (paddedMax - paddedMin);
    });

    return (
        <ChartShell>
            <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} role='img' aria-label='Selected vitals trends'>
                {yTickValues.map(tick => (
                    <g key={`y-${tick}`}>
                        <line
                            x1={margin.left}
                            x2={margin.left + innerWidth}
                            y1={getY(tick)}
                            y2={getY(tick)}
                            stroke='rgba(15, 23, 42, 0.12)'
                            strokeWidth={1}
                        />
                        <text
                            x={margin.left - 10}
                            y={getY(tick) + 4}
                            textAnchor='end'
                            fontSize='12'
                            fill='rgba(15, 23, 42, 0.72)'
                        >
                            {tick.toFixed(1)}
                        </text>
                    </g>
                ))}

                {orderedSources.map(item => (
                    <g key={`x-${item.id}`}>
                        <line
                            x1={getX(item.id)}
                            x2={getX(item.id)}
                            y1={margin.top}
                            y2={margin.top + innerHeight}
                            stroke='rgba(15, 23, 42, 0.08)'
                            strokeWidth={1}
                        />
                        <text
                            x={getX(item.id)}
                            y={margin.top + innerHeight + 24}
                            textAnchor='end'
                            transform={`rotate(-28 ${getX(item.id)} ${margin.top + innerHeight + 24})`}
                            fontSize='11'
                            fill='rgba(15, 23, 42, 0.72)'
                        >
                            {item.date}
                        </text>
                    </g>
                ))}

                {series.map((item, index) => {
                    const color = palette[index % palette.length];
                    const points = item.points.map(point => `${getX(point.sourceId)},${getY(point.value)}`).join(' ');

                    return (
                        <g key={item.id}>
                            <polyline
                                points={points}
                                fill='none'
                                stroke={color}
                                strokeWidth={3}
                                strokeLinecap='round'
                                strokeLinejoin='round'
                            />
                            {item.points.map(point => (
                                <circle
                                    key={`${item.id}-${point.sourceId}`}
                                    cx={getX(point.sourceId)}
                                    cy={getY(point.value)}
                                    r={4.5}
                                    fill={color}
                                    stroke='#f8fafc'
                                    strokeWidth={2}
                                />
                            ))}
                        </g>
                    );
                })}
            </svg>

            <LegendGrid>
                {series.map((item, index) => (
                    <LegendItem key={item.id}>
                        <LegendDot style={{ background: palette[index % palette.length] }} />
                        <Text>{item.label}</Text>
                    </LegendItem>
                ))}
            </LegendGrid>
        </ChartShell>
    );
}

export default function App() {
    const viewport = useViewport();
    const isMobilePortrait = viewport.width < 860 && viewport.height >= viewport.width;

    const [labs, setLabs] = useState<BloodworkLab[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [loadError, setLoadError] = useState<string | null>(null);
    const [measurementFilter, setMeasurementFilter] = useState('');
    const [selectedSourceIds, setSelectedSourceIds] = useState<string[]>([]);
    const [selectedRowKeys, setSelectedRowKeys] = useState<Key[]>([]);
    const [mobileSourceId, setMobileSourceId] = useState<string | null>(null);

    useEffect(() => {
        const controller = new AbortController();

        async function loadLabs() {
            try {
                setIsLoading(true);
                setLoadError(null);
                const response = await fetch('/api/bloodwork', { signal: controller.signal });
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                const payload = await response.json() as ApiResponse;
                setLabs(payload.items ?? []);
            } catch (error) {
                if (controller.signal.aborted) return;
                setLoadError(String(error));
            } finally {
                if (!controller.signal.aborted) {
                    setIsLoading(false);
                }
            }
        }

        loadLabs();
        return () => controller.abort();
    }, []);

    const orderedLabs = useMemo(
        () => [...labs].sort((left, right) => right.date.localeCompare(left.date)),
        [labs],
    );

    const sources = useMemo<SourceColumn[]>(
        () =>
            orderedLabs.map((lab, index) => ({
                id: `${lab.date}__${lab.labName}__${index}`,
                label: `${lab.date} · ${lab.labName}`,
                date: lab.date,
                source: lab.labName,
            })),
        [orderedLabs],
    );

    useEffect(() => {
        const sourceSet = new Set(sources.map(item => item.id));
        setSelectedSourceIds(previous => {
            if (previous.length === 0) return sources.map(item => item.id);
            const retained = previous.filter(item => sourceSet.has(item));
            return retained.length > 0 ? retained : sources.map(item => item.id);
        });
        setMobileSourceId(previous => {
            if (previous && sourceSet.has(previous)) return previous;
            return sources[0]?.id ?? null;
        });
    }, [sources]);

    const allRows = useMemo<MeasurementRow[]>(() => {
        const grouped = new Map<string, MeasurementRow>();

        orderedLabs.forEach((lab, labIndex) => {
            const sourceId = sources[labIndex]?.id;
            if (!sourceId) return;

            lab.measurements.forEach(measurement => {
                const key = measurement.name.trim().toLowerCase();
                const existing = grouped.get(key);
                if (existing) {
                    existing.valuesBySource[sourceId] = formatCell(measurement);
                    return;
                }

                grouped.set(key, {
                    key,
                    measurement: measurement.name,
                    valuesBySource: {
                        [sourceId]: formatCell(measurement),
                    },
                });
            });
        });

        return Array.from(grouped.values()).sort((left, right) => left.measurement.localeCompare(right.measurement));
    }, [orderedLabs, sources]);

    const filteredRows = useMemo(() => {
        const normalizedFilter = measurementFilter.trim().toLowerCase();
        if (!normalizedFilter) return allRows;
        return allRows.filter(row => row.measurement.toLowerCase().includes(normalizedFilter));
    }, [allRows, measurementFilter]);

    useEffect(() => {
        const availableRowIds = new Set(filteredRows.map(item => item.key));
        setSelectedRowKeys(previous => previous.filter(item => availableRowIds.has(String(item))));
    }, [filteredRows]);

    const visibleSourceIds = isMobilePortrait
        ? (mobileSourceId ? [mobileSourceId] : [])
        : selectedSourceIds;
    const visibleSources = sources.filter(item => visibleSourceIds.includes(item.id));
    const sourceOptions = sources.map(item => ({ label: item.label, value: item.id }));

    const tableColumns = useMemo<ColumnsType<MeasurementRow>>(() => {
        const measurementColumn: ColumnsType<MeasurementRow>[number] = {
            title: (
                    <Space size={6}>
                    <Flask size={17} weight='duotone' />
                    <span>Measurement</span>
                </Space>
            ),
            dataIndex: 'measurement',
            key: 'measurement',
            width: 270,
            fixed: isMobilePortrait ? false : 'left',
            render: value => <Text strong>{value}</Text>,
        };

        const sourceColumns: ColumnsType<MeasurementRow> = visibleSources.map(source => ({
            title: (
                <Space direction='vertical' size={0}>
                    <Text>{source.date}</Text>
                    <Text type='secondary'>{source.source}</Text>
                </Space>
            ),
            key: source.id,
            width: 190,
            render: (_, row) => {
                const cell = row.valuesBySource[source.id];
                if (!cell) return <Text type='secondary'>—</Text>;

                return (
                    <Space direction='vertical' size={2}>
                        <Text>{cell.display}</Text>
                        {cell.flag && cell.flag !== 'normal' && (
                            <Tag color={cell.flag === 'high' || cell.flag === 'critical' ? 'red' : 'orange'}>
                                {cell.flag}
                            </Tag>
                        )}
                    </Space>
                );
            },
        }));

        return [measurementColumn, ...sourceColumns];
    }, [isMobilePortrait, visibleSources]);

    const chartSources = sources.filter(item => selectedSourceIds.includes(item.id));
    const selectedRows = filteredRows.filter(row => selectedRowKeys.includes(row.key));
    const chartSeries = useMemo<ChartSeries[]>(
        () =>
            selectedRows
                .map(row => ({
                    id: row.key,
                    label: row.measurement,
                    points: chartSources
                        .map((source, index) => {
                            const cell = row.valuesBySource[source.id];
                            if (!cell || cell.numericValue === null) return null;
                            return {
                                sourceId: source.id,
                                sourceLabel: source.label,
                                xIndex: index,
                                value: cell.numericValue,
                            };
                        })
                        .filter((point): point is { sourceId: string; sourceLabel: string; xIndex: number; value: number } => !!point),
                }))
                .filter(series => series.points.length > 0),
        [selectedRows, chartSources],
    );

    const hasAnyData = labs.length > 0;

    return (
        <Page>
            <Backdrop />
            <Content>
                <HeaderCard>
                    <Space align='center' size={18}>
                        <IconOrb>
                            <Pulse size={26} weight='duotone' />
                        </IconOrb>
                        <div>
                            <Title level={2}>Vitals Observatory</Title>
                            <Text type='secondary'>
                                Historical bloodwork matrix with selectable source columns and live trend projections.
                            </Text>
                        </div>
                    </Space>
                </HeaderCard>

                {isLoading ? (
                    <LoadingCard>
                        <Spin size='large' />
                    </LoadingCard>
                ) : loadError ? (
                    <Alert
                        type='error'
                        showIcon
                        message='Unable to load bloodwork data'
                        description={loadError}
                    />
                ) : !hasAnyData ? (
                    <Card>
                        <Empty description='No bloodwork data found yet.' />
                    </Card>
                ) : (
                    <LayoutGrid mobile={isMobilePortrait}>
                        <PanelCard>
                            <Space direction='vertical' size={14} style={{ width: '100%' }}>
                                <ControlsRow mobile={isMobilePortrait}>
                                    <Input
                                        prefix={<Drop size={16} />}
                                        placeholder='Filter measurement names'
                                        value={measurementFilter}
                                        onChange={event => setMeasurementFilter(event.target.value)}
                                        allowClear
                                    />
                                    {!isMobilePortrait && (
                                        <Select
                                            mode='multiple'
                                            value={selectedSourceIds}
                                            options={sourceOptions}
                                            onChange={value => setSelectedSourceIds(value)}
                                            style={{ minWidth: 340 }}
                                            placeholder='Select source columns'
                                            maxTagCount='responsive'
                                        />
                                    )}
                                    {isMobilePortrait && (
                                        <Select
                                            value={mobileSourceId ?? undefined}
                                            options={sourceOptions}
                                            onChange={value => setMobileSourceId(value)}
                                            style={{ minWidth: 220 }}
                                            placeholder='Visible source'
                                        />
                                    )}
                                </ControlsRow>

                                <Table<MeasurementRow>
                                    bordered
                                    size='small'
                                    rowKey='key'
                                    dataSource={filteredRows}
                                    columns={tableColumns}
                                    pagination={{ pageSize: 16, showSizeChanger: false }}
                                    scroll={isMobilePortrait ? undefined : { x: 'max-content', y: 620 }}
                                    rowSelection={{
                                        selectedRowKeys,
                                        onChange: keys => setSelectedRowKeys(keys),
                                    }}
                                />
                            </Space>
                        </PanelCard>

                        <PanelCard>
                            <Space direction='vertical' size={12} style={{ width: '100%' }}>
                                <Space align='center' size={8}>
                                    <ChartLineUp size={18} weight='duotone' />
                                    <Text strong>Trend view</Text>
                                </Space>
                                {selectedRowKeys.length === 0 || selectedSourceIds.length === 0 ? (
                                    <Empty description='Select rows and columns to render trends.' />
                                ) : (
                                    <TrendChart
                                        series={chartSeries}
                                        orderedSources={chartSources}
                                    />
                                )}
                            </Space>
                        </PanelCard>
                    </LayoutGrid>
                )}
            </Content>
        </Page>
    );
}

const Page = styled.main`
    position: relative;
    min-height: 100vh;
    background:
        radial-gradient(circle at 14% 10%, rgba(250, 204, 21, 0.26), transparent 36%),
        radial-gradient(circle at 84% 0%, rgba(15, 118, 110, 0.23), transparent 40%),
        linear-gradient(148deg, #f8fafc 0%, #ecfeff 52%, #fff1f2 100%);
    color: #0f172a;
`;

const Backdrop = styled.div`
    position: fixed;
    inset: 0;
    pointer-events: none;
    background-image:
        linear-gradient(transparent 95%, rgba(15, 23, 42, 0.05) 96%),
        linear-gradient(90deg, transparent 95%, rgba(15, 23, 42, 0.05) 96%);
    background-size: 32px 32px;
    opacity: 0.55;
`;

const Content = styled.div`
    position: relative;
    z-index: 1;
    max-width: 1520px;
    margin: 0 auto;
    padding: 24px 22px 28px;
`;

const HeaderCard = styled(Card)`
    margin-bottom: 16px;
    border-radius: 18px;
    border: 1px solid rgba(15, 23, 42, 0.08);
    box-shadow: 0 14px 40px rgba(15, 23, 42, 0.08);

    .ant-typography {
        margin: 0;
    }
`;

const IconOrb = styled.div`
    display: grid;
    place-items: center;
    width: 52px;
    height: 52px;
    border-radius: 14px;
    color: #0f766e;
    background: linear-gradient(140deg, rgba(15, 118, 110, 0.17), rgba(251, 113, 133, 0.18));
    box-shadow: inset 0 0 0 1px rgba(15, 23, 42, 0.06);
`;

const LoadingCard = styled(Card)`
    min-height: 220px;
    display: grid;
    place-items: center;
`;

const LayoutGrid = styled.div<{ mobile: boolean }>`
    display: grid;
    grid-template-columns: ${({ mobile }) => (mobile ? '1fr' : 'minmax(0, 1.35fr) minmax(380px, 0.95fr)')};
    gap: 16px;
    align-items: start;
`;

const PanelCard = styled(Card)`
    border-radius: 18px;
    border: 1px solid rgba(15, 23, 42, 0.08);
    box-shadow: 0 12px 32px rgba(15, 23, 42, 0.08);
`;

const ControlsRow = styled.div<{ mobile: boolean }>`
    display: grid;
    grid-template-columns: ${({ mobile }) => (mobile ? '1fr' : 'minmax(220px, 1fr) auto')};
    gap: 10px;
`;

const ChartShell = styled.div`
    width: 100%;
    overflow-x: auto;
    padding-bottom: 6px;
    border-radius: 14px;
    border: 1px solid rgba(15, 23, 42, 0.08);
    background: rgba(255, 255, 255, 0.8);
`;

const LegendGrid = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    padding: 0 16px 14px;
`;

const LegendItem = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 4px 10px;
    border-radius: 999px;
    border: 1px solid rgba(15, 23, 42, 0.09);
    background: rgba(255, 255, 255, 0.9);
`;

const LegendDot = styled.span`
    width: 10px;
    height: 10px;
    border-radius: 999px;
`;
