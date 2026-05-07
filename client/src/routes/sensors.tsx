import {
	CheckCircleOutlined,
	CopyOutlined,
	DownloadOutlined,
	PlayCircleOutlined,
	WarningOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import {
	Alert,
	Button,
	Checkbox,
	DatePicker,
	Empty,
	Flex,
	Input,
	Segmented,
	Select,
	Space,
	Spin,
	Tag,
	Tooltip,
	Typography,
	message,
	theme as antdTheme,
} from 'antd';
import type { Dayjs } from 'dayjs';
import dayjs from 'dayjs';
import type { ReactNode } from 'react';
import { useEffect, useMemo, useState } from 'react';

import type { SensorRunResult } from '../utils/api';
import { useTRPC, useTRPCClient } from '../utils/trpc';

export const Route = createFileRoute('/sensors')({
	component: SensorsPage,
});

const DATE_FORMAT = 'YYYY-MM-DD';
const sensorKeys = ['labs', 'pills', 'voiceMemos', 'macrofactor', 'whoop', 'workouts'] as const;
const dependentSensorKeys = ['pills', 'voiceMemos', 'macrofactor', 'whoop', 'workouts'] as const;
const outputModes = ['json', 'text', 'csv'] as const;
const voiceMemoContentOptions = [
	{ label: 'Raw', value: 'raw' },
	{ label: 'Summary', value: 'summary' },
	{ label: 'Both', value: 'both' },
] as const;

type SensorKey = (typeof sensorKeys)[number];
type DependentSensorKey = (typeof dependentSensorKeys)[number];
type OutputMode = (typeof outputModes)[number];
type VoiceMemoContent = (typeof voiceMemoContentOptions)[number]['value'];
type RunStatus = 'idle' | 'running' | 'success' | 'error';

type RunState = {
	status: RunStatus;
	error: string | null;
	completedAt: string | null;
};

const sensorLabels = {
	labs: 'Labs',
	pills: 'Pills',
	voiceMemos: "Captain's Log",
	macrofactor: 'MacroFactor',
	whoop: 'WHOOP',
	workouts: 'Workouts',
} satisfies Record<SensorKey, string>;

const defaultRunState = Object.fromEntries(
	sensorKeys.map(key => [key, { status: 'idle', error: null, completedAt: null }]),
) as Record<SensorKey, RunState>;

function SensorsPage() {
	const { token } = antdTheme.useToken();
	const trpc = useTRPC();
	const trpcClient = useTRPCClient();
	const [messageApi, messageContextHolder] = message.useMessage();
	const [defaultStartDate, setDefaultStartDate] = useState(getDateDaysAgo(7));
	const [selectedKeys, setSelectedKeys] = useState(
		Object.fromEntries(sensorKeys.map(key => [key, true])) as Record<SensorKey, boolean>,
	);
	const [outputMode, setOutputMode] = useState<OutputMode>('text');
	const [runStates, setRunStates] = useState(defaultRunState);
	const [results, setResults] = useState<Partial<Record<SensorKey, SensorRunResult>>>({});
	const [labsConfig, setLabsConfig] = useState({
		textFilter: '',
		categories: [] as string[],
		startDate: null as string | null,
		onlyLatest: false,
	});
	const [voiceMemosConfig, setVoiceMemosConfig] = useState({
		content: 'raw' as VoiceMemoContent,
	});
	const [macrofactorConfig, setMacrofactorConfig] = useState({
		recipeDetails: false,
	});
	const [startDates, setStartDates] = useState(
		Object.fromEntries(dependentSensorKeys.map(key => [key, defaultStartDate])) as Record<
			DependentSensorKey,
			string
		>,
	);

	const configQuery = useQuery(trpc.sensors.getConfig.queryOptions());
	const defaultLabStartDate = useMemo(() => {
		const targetDate = dayjs().subtract(1, 'year').format(DATE_FORMAT);
		const sortedDates = [...(configQuery.data?.labDates ?? [])].sort((left, right) =>
			right.localeCompare(left),
		);
		return sortedDates.find(date => date <= targetDate) ?? sortedDates.at(-1) ?? null;
	}, [configQuery.data?.labDates]);
	const selectedRunKeys = sensorKeys.filter(key => selectedKeys[key]);
	const isAnySelectedRunning = selectedRunKeys.some(key => runStates[key].status === 'running');
	const isAnyRunning = sensorKeys.some(key => runStates[key].status === 'running');

	const compiledJsonText = useMemo(
		() =>
			JSON.stringify(
				{
					generatedAt: new Date().toISOString(),
					startDate: defaultStartDate,
					sections: Object.fromEntries(
						sensorKeys.flatMap((key): Array<[SensorKey, unknown]> => {
							const result = results[key];
							return result && result.json !== null ? [[key, result.json]] : [];
						}),
					),
				},
				null,
				2,
			),
		[defaultStartDate, results],
	);
	const compiledText = useMemo(
		() =>
			sensorKeys
				.map(key => {
					const result = results[key];
					if (!result?.text) {
						return null;
					}
					return `# ${result.label}\n\n${result.text.trim()}`;
				})
				.filter(Boolean)
				.join('\n\n'),
		[results],
	);
	const csvFiles = useMemo(
		() =>
			sensorKeys.flatMap(key => {
				const result = results[key];
				return result?.csvFiles ?? [];
			}),
		[results],
	);
	const outputText = outputMode === 'json' ? compiledJsonText : compiledText;

	useEffect(() => {
		if (!defaultLabStartDate) {
			return;
		}
		setLabsConfig(previous =>
			previous.startDate ? previous : { ...previous, startDate: defaultLabStartDate },
		);
	}, [defaultLabStartDate]);

	function updateDefaultStartDate(nextStartDate: string) {
		setDefaultStartDate(nextStartDate);
		setStartDates(
			Object.fromEntries(dependentSensorKeys.map(key => [key, nextStartDate])) as Record<
				DependentSensorKey,
				string
			>,
		);
	}

	async function runSensor(key: SensorKey) {
		setRunStates(previous => ({
			...previous,
			[key]: { status: 'running', error: null, completedAt: null },
		}));

		try {
			const result = await trpcClient.sensors.runExtractor.mutate({
				key,
				outputMode,
				startDate: key === 'labs' ? defaultStartDate : startDates[key as DependentSensorKey],
				labs: labsConfig,
				voiceMemos: voiceMemosConfig,
				macrofactor: macrofactorConfig,
			});
			setResults(previous => ({ ...previous, [key]: result }));
			setRunStates(previous => ({
				...previous,
				[key]: { status: 'success', error: null, completedAt: result.completedAt },
			}));
			return result;
		} catch (error) {
			const message = getErrorMessage(error);
			setRunStates(previous => ({
				...previous,
				[key]: { status: 'error', error: message, completedAt: new Date().toISOString() },
			}));
			throw error;
		}
	}

	async function runKeys(keys: SensorKey[]) {
		if (keys.length === 0) {
			messageApi.warning('Select at least one sensor.');
			return;
		}

		const settled = await Promise.allSettled(keys.map(key => runSensor(key)));
		const failedCount = settled.filter(result => result.status === 'rejected').length;
		if (failedCount > 0) {
			messageApi.error(`${failedCount} sensor${failedCount === 1 ? '' : 's'} failed.`);
			return;
		}
		messageApi.success(`Finished ${keys.length} sensor${keys.length === 1 ? '' : 's'}.`);
	}

	function copyText(value: string) {
		void navigator.clipboard.writeText(value);
		messageApi.success('Copied.');
	}

	function downloadOutput() {
		if (outputMode === 'csv') {
			if (csvFiles.length === 0) {
				messageApi.warning('Run at least one CSV sensor first.');
				return;
			}
			downloadCsvFiles(csvFiles);
			return;
		}

		const extension = outputMode === 'json' ? 'json' : 'txt';
		downloadBlob(
			new Blob([outputText], { type: outputMode === 'json' ? 'application/json' : 'text/plain' }),
			`vitals-sensors-${dateStamp()}.${extension}`,
		);
	}

	function renderDatePicker(key: DependentSensorKey) {
		return (
			<DatePicker
				value={toDayjs(startDates[key])}
				format={DATE_FORMAT}
				allowClear={false}
				onChange={value => {
					if (!value) {
						return;
					}
					setStartDates(previous => ({
						...previous,
						[key]: value.format(DATE_FORMAT),
					}));
				}}
			/>
		);
	}

	return (
		<main className='sensors-page' style={{ background: token.colorBgLayout }}>
			{messageContextHolder}
			<div className='sensors-page-inner'>
				<section
					className='sensors-toolbar'
					style={{
						background: token.colorBgContainer,
						borderColor: token.colorBorderSecondary,
					}}
				>
					<div className='sensors-toolbar-title'>
						<Typography.Title level={2}>Sensors</Typography.Title>
						<Typography.Text type='secondary'>Data extraction</Typography.Text>
					</div>

					<Space wrap>
						<DatePicker
							value={toDayjs(defaultStartDate)}
							format={DATE_FORMAT}
							allowClear={false}
							onChange={value => {
								if (value) {
									updateDefaultStartDate(value.format(DATE_FORMAT));
								}
							}}
						/>
						<Button onClick={() => updateDefaultStartDate(getDateDaysAgo(7))}>7 days</Button>
						<Button
							onClick={() =>
								updateDefaultStartDate(dayjs().subtract(1, 'month').format(DATE_FORMAT))
							}
						>
							1 month
						</Button>
						<Button
							onClick={() =>
								updateDefaultStartDate(dayjs().subtract(3, 'month').format(DATE_FORMAT))
							}
						>
							3 month
						</Button>
						<Button
							type='primary'
							icon={<PlayCircleOutlined />}
							loading={isAnySelectedRunning}
							onClick={() => void runKeys(selectedRunKeys)}
						>
							Run selected
						</Button>
					</Space>
				</section>

				<section className='sensors-section'>
					<div className='sensors-section-header'>
						<Typography.Title level={3}>Extractors</Typography.Title>
						<Typography.Text type='secondary'>{selectedRunKeys.length} selected</Typography.Text>
					</div>

					<div className='sensors-list'>
						{sensorKeys.map(key => (
							<SensorRow
								key={key}
								sensorKey={key}
								label={sensorLabels[key]}
								selected={selectedKeys[key]}
								runState={runStates[key]}
								onSelectedChange={checked =>
									setSelectedKeys(previous => ({ ...previous, [key]: checked }))
								}
								onRun={() => void runSensor(key)}
								config={
									key === 'labs' ? (
										<LabsConfig
											config={labsConfig}
											labDates={configQuery.data?.labDates ?? []}
											labCategories={configQuery.data?.labCategories ?? []}
											isLoading={configQuery.isLoading}
											onChange={setLabsConfig}
										/>
									) : key === 'voiceMemos' ? (
										<Space wrap>
											{renderDatePicker(key)}
											<Select
												value={voiceMemosConfig.content}
												style={{ width: 128 }}
												options={[...voiceMemoContentOptions]}
												onChange={content => setVoiceMemosConfig({ content })}
											/>
										</Space>
									) : key === 'macrofactor' ? (
										<Space wrap>
											{renderDatePicker(key)}
											<Checkbox
												checked={macrofactorConfig.recipeDetails}
												onChange={event =>
													setMacrofactorConfig({ recipeDetails: event.target.checked })
												}
											>
												Recipe details
											</Checkbox>
										</Space>
									) : (
										renderDatePicker(key as DependentSensorKey)
									)
								}
							/>
						))}
					</div>
				</section>

				<section className='sensors-section'>
					<div className='sensors-section-header'>
						<Typography.Title level={3}>Export</Typography.Title>
						<Space>
							<Segmented
								value={outputMode}
								options={[
									{ label: 'Text', value: 'text' },
									{ label: 'JSON', value: 'json' },
									{ label: 'CSV', value: 'csv' },
								]}
								onChange={value => setOutputMode(value as OutputMode)}
							/>
							<Button
								type='primary'
								icon={<PlayCircleOutlined />}
								loading={isAnySelectedRunning}
								onClick={() => void runKeys(selectedRunKeys)}
							>
								Start
							</Button>
							<Button
								icon={<DownloadOutlined />}
								disabled={
									outputMode === 'csv' ? csvFiles.length === 0 : outputText.trim().length === 0
								}
								onClick={downloadOutput}
							>
								Download
							</Button>
						</Space>
					</div>

					{outputMode === 'csv' ? (
						<CsvOutput files={csvFiles} onCopy={copyText} isRunning={isAnyRunning} />
					) : (
						<TextOutput
							value={outputText}
							mode={outputMode}
							isRunning={isAnyRunning}
							onCopy={copyText}
						/>
					)}
				</section>
			</div>
		</main>
	);
}

function SensorRow(props: {
	sensorKey: SensorKey;
	label: string;
	selected: boolean;
	runState: RunState;
	config: ReactNode;
	onSelectedChange: (checked: boolean) => void;
	onRun: () => void;
}) {
	const { token } = antdTheme.useToken();

	return (
		<div
			className='sensors-row'
			style={{
				background: token.colorBgContainer,
				borderColor: token.colorBorderSecondary,
			}}
		>
			<Flex align='center' justify='space-between' gap={16} wrap>
				<Space size={12}>
					<Checkbox
						checked={props.selected}
						onChange={event => props.onSelectedChange(event.target.checked)}
					>
						<Typography.Text strong>{props.label}</Typography.Text>
					</Checkbox>
					<RunStateTag state={props.runState} />
				</Space>

				<Space wrap>
					{props.config}
					<Button
						icon={<PlayCircleOutlined />}
						loading={props.runState.status === 'running'}
						onClick={props.onRun}
					>
						Run
					</Button>
				</Space>
			</Flex>

			{props.runState.error ? (
				<Alert
					type='error'
					showIcon
					className='sensors-row-error'
					message={`${props.label} failed`}
					description={
						<Typography.Paragraph copyable className='sensors-error-text'>
							{props.runState.error}
						</Typography.Paragraph>
					}
				/>
			) : null}
		</div>
	);
}

function LabsConfig(props: {
	config: {
		textFilter: string;
		categories: string[];
		startDate: string | null;
		onlyLatest: boolean;
	};
	labDates: string[];
	labCategories: Array<{
		category: string;
		count: number;
	}>;
	isLoading: boolean;
	onChange: (config: {
		textFilter: string;
		categories: string[];
		startDate: string | null;
		onlyLatest: boolean;
	}) => void;
}) {
	return (
		<Space wrap>
			<Input
				value={props.config.textFilter}
				placeholder='text filter'
				style={{ width: 220 }}
				onChange={event => props.onChange({ ...props.config, textFilter: event.target.value })}
			/>
			<Select
				mode='multiple'
				value={props.config.categories}
				placeholder='categories'
				loading={props.isLoading}
				style={{ minWidth: 240 }}
				maxTagCount='responsive'
				options={props.labCategories.map(category => ({
					label: `${category.category} (${category.count})`,
					value: category.category,
				}))}
				onChange={categories => props.onChange({ ...props.config, categories })}
			/>
			<Select
				value={props.config.startDate ?? undefined}
				placeholder='start date'
				loading={props.isLoading}
				style={{ width: 160 }}
				options={props.labDates.map(date => ({ label: date, value: date }))}
				onChange={startDate => props.onChange({ ...props.config, startDate: startDate ?? null })}
			/>
			<Tooltip title='Only the latest value for each measurement, if it was in the selected period. When off, returns all measurements taken in that period.'>
				<Checkbox
					checked={props.config.onlyLatest}
					onChange={event => props.onChange({ ...props.config, onlyLatest: event.target.checked })}
				>
					Only latest
				</Checkbox>
			</Tooltip>
		</Space>
	);
}

function RunStateTag({ state }: { state: RunState }) {
	if (state.status === 'running') {
		return (
			<Tag icon={<Spin size='small' />} color='processing'>
				Running
			</Tag>
		);
	}
	if (state.status === 'success') {
		return (
			<Tag icon={<CheckCircleOutlined />} color='success'>
				Done
			</Tag>
		);
	}
	if (state.status === 'error') {
		return (
			<Tag icon={<WarningOutlined />} color='error'>
				Error
			</Tag>
		);
	}
	return null;
}

function TextOutput(props: {
	value: string;
	mode: OutputMode;
	isRunning: boolean;
	onCopy: (value: string) => void;
}) {
	return (
		<div className='sensors-output-shell'>
			<div className='sensors-output-actions'>
				{props.isRunning ? <Spin size='small' /> : null}
				<Button
					icon={<CopyOutlined />}
					disabled={props.value.trim().length === 0}
					onClick={() => props.onCopy(props.value)}
				/>
			</div>
			<Input.TextArea
				readOnly
				value={props.value}
				placeholder={props.mode === 'json' ? '{}' : ''}
				autoSize={{ minRows: 18, maxRows: 36 }}
				className='sensors-output-textarea'
			/>
		</div>
	);
}

function CsvOutput(props: {
	files: Array<{ fileName: string; content: string }>;
	isRunning: boolean;
	onCopy: (value: string) => void;
}) {
	if (props.files.length === 0) {
		return (
			<div className='sensors-empty-output'>
				{props.isRunning ? <Spin /> : <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} />}
			</div>
		);
	}

	return (
		<div className='sensors-csv-list'>
			{props.files.map(file => (
				<div className='sensors-csv-file' key={file.fileName}>
					<Flex align='center' justify='space-between' gap={12}>
						<Typography.Text strong>{file.fileName}</Typography.Text>
						<Button icon={<CopyOutlined />} onClick={() => props.onCopy(file.content)} />
					</Flex>
					<Input.TextArea
						readOnly
						value={file.content}
						autoSize={{ minRows: 8, maxRows: 20 }}
						className='sensors-output-textarea'
					/>
				</div>
			))}
		</div>
	);
}

function toDayjs(value: string): Dayjs {
	return dayjs(value, DATE_FORMAT);
}

function getDateDaysAgo(days: number) {
	return dayjs().subtract(days, 'day').format(DATE_FORMAT);
}

function getErrorMessage(error: unknown) {
	if (error instanceof Error) {
		return error.message;
	}
	return String(error);
}

function dateStamp() {
	return dayjs().format('YYYY-MM-DD-HHmmss');
}

function downloadBlob(blob: Blob, fileName: string) {
	const url = URL.createObjectURL(blob);
	const anchor = document.createElement('a');
	anchor.href = url;
	anchor.download = fileName;
	document.body.append(anchor);
	anchor.click();
	anchor.remove();
	URL.revokeObjectURL(url);
}

function downloadCsvFiles(files: Array<{ fileName: string; content: string }>) {
	const stamp = dateStamp();
	for (const file of files) {
		downloadBlob(
			new Blob([file.content], { type: 'text/csv' }),
			`vitals-sensors-${stamp}-${file.fileName}`,
		);
	}
}
