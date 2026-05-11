import {
	CheckCircle as CheckCircleOutlined,
	Copy as CopyOutlined,
	DownloadSimple as DownloadOutlined,
	PlayCircle as PlayCircleOutlined,
	Warning as WarningOutlined,
} from '@phosphor-icons/react';
import { toast } from '@tamagui/toast/v2';
import { useQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import dayjs from 'dayjs';
import type { ReactNode } from 'react';
import { useEffect, useMemo, useState } from 'react';
import {
	Button,
	Checkbox,
	H2,
	H3,
	Input,
	Paragraph,
	Spinner,
	Text,
	XStack,
	YStack,
	useTheme,
} from 'tamagui';

import { AutoResizeTextArea } from '../components/AutoResizeTextArea';
import { TagChip } from '../components/TagChip';
import { TagInput } from '../components/TagInput';
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
	const theme = useTheme();
	const trpc = useTRPC();
	const trpcClient = useTRPCClient();
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
			toast.warning('Select at least one sensor.');
			return;
		}

		const settled = await Promise.allSettled(keys.map(key => runSensor(key)));
		const failedCount = settled.filter(result => result.status === 'rejected').length;
		if (failedCount > 0) {
			toast.error(`${failedCount} sensor${failedCount === 1 ? '' : 's'} failed.`);
			return;
		}
		toast.success(`Finished ${keys.length} sensor${keys.length === 1 ? '' : 's'}.`);
	}

	function copyText(value: string) {
		void navigator.clipboard.writeText(value);
		toast.success('Copied.');
	}

	function downloadOutput() {
		if (outputMode === 'csv') {
			if (csvFiles.length === 0) {
				toast.warning('Run at least one CSV sensor first.');
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
			<Input
				type='date'
				value={startDates[key]}
				onChange={event => {
					const value = event.target.value;
					setStartDates(previous => ({
						...previous,
						[key]: value,
					}));
				}}
			/>
		);
	}

	return (
		<main className='sensors-page' style={{ background: theme.bgLayout?.get('web') }}>
			<div className='sensors-page-inner'>
				<section
					className='sensors-toolbar'
					style={{
						background: theme.bgContainer?.get('web'),
						borderColor: theme.borderSubtle?.get('web'),
					}}
				>
					<div className='sensors-toolbar-title'>
						<H2>Sensors</H2>
						<Text color='$textMuted'>Data extraction</Text>
					</div>

					<XStack gap={8} flexWrap='wrap'>
						<Input
							type='date'
							value={defaultStartDate}
							onChange={event => updateDefaultStartDate(event.target.value)}
						/>
						<Button onPress={() => updateDefaultStartDate(getDateDaysAgo(7))}>7 days</Button>
						<Button
							onPress={() =>
								updateDefaultStartDate(dayjs().subtract(1, 'month').format(DATE_FORMAT))
							}
						>
							1 month
						</Button>
						<Button
							onPress={() =>
								updateDefaultStartDate(dayjs().subtract(3, 'month').format(DATE_FORMAT))
							}
						>
							3 month
						</Button>
						<Button
							className='app-button-primary'
							icon={<PlayCircleOutlined color='white' />}
							disabled={isAnySelectedRunning}
							backgroundColor='$primary'
							style={{ color: 'white' }}
							onPress={() => void runKeys(selectedRunKeys)}
						>
							<Text color='$white'>{isAnySelectedRunning ? 'Running...' : 'Run selected'}</Text>
						</Button>
					</XStack>
				</section>

				<section className='sensors-section'>
					<div className='sensors-section-header'>
						<H3>Extractors</H3>
						<Text color='$textMuted'>{selectedRunKeys.length} selected</Text>
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
										<XStack gap={8} flexWrap='wrap'>
											{renderDatePicker(key)}
											<NativeSelect
												value={voiceMemosConfig.content}
												options={[...voiceMemoContentOptions]}
												onChange={content => {
													if (content) {
														setVoiceMemosConfig({ content });
													}
												}}
											/>
										</XStack>
									) : key === 'macrofactor' ? (
										<XStack gap={8} flexWrap='wrap' alignItems='center'>
											{renderDatePicker(key)}
											<CheckControl
												checked={macrofactorConfig.recipeDetails}
												onCheckedChange={recipeDetails => setMacrofactorConfig({ recipeDetails })}
											>
												Recipe details
											</CheckControl>
										</XStack>
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
						<H3>Export</H3>
						<XStack gap={8} flexWrap='wrap'>
							<SegmentedControl
								value={outputMode}
								options={[
									{ label: 'Text', value: 'text' },
									{ label: 'JSON', value: 'json' },
									{ label: 'CSV', value: 'csv' },
								]}
								onChange={value => setOutputMode(value as OutputMode)}
							/>
							<Button
								className='app-button-primary'
								icon={<PlayCircleOutlined color='white' />}
								disabled={isAnySelectedRunning}
								backgroundColor='$primary'
								style={{ color: 'white' }}
								onPress={() => void runKeys(selectedRunKeys)}
							>
								<Text color='$white'>{isAnySelectedRunning ? 'Running...' : 'Start'}</Text>
							</Button>
							<Button
								icon={<DownloadOutlined />}
								disabled={
									outputMode === 'csv' ? csvFiles.length === 0 : outputText.trim().length === 0
								}
								onPress={downloadOutput}
							>
								Download
							</Button>
						</XStack>
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
	const theme = useTheme();

	return (
		<div
			className='sensors-row'
			style={{
				background: theme.bgContainer?.get('web'),
				borderColor: theme.borderSubtle?.get('web'),
			}}
		>
			<XStack alignItems='center' justifyContent='space-between' gap={16} flexWrap='wrap'>
				<XStack gap={12} alignItems='center'>
					<CheckControl checked={props.selected} onCheckedChange={props.onSelectedChange}>
						<Text fontWeight='700'>{props.label}</Text>
					</CheckControl>
					<RunStateTag state={props.runState} />
				</XStack>

				<XStack gap={8} flexWrap='wrap'>
					{props.config}
					<Button
						icon={<PlayCircleOutlined />}
						disabled={props.runState.status === 'running'}
						onPress={props.onRun}
					>
						{props.runState.status === 'running' ? 'Running...' : 'Run'}
					</Button>
				</XStack>
			</XStack>

			{props.runState.error ? (
				<InlineAlert className='sensors-row-error' kind='error' title={`${props.label} failed`}>
					<Paragraph className='sensors-error-text'>{props.runState.error}</Paragraph>
				</InlineAlert>
			) : null}
		</div>
	);
}

function CheckControl(props: {
	checked: boolean;
	children: ReactNode;
	onCheckedChange: (checked: boolean) => void;
}) {
	return (
		<XStack alignItems='center' gap={7}>
			<Checkbox
				checked={props.checked}
				onCheckedChange={value => props.onCheckedChange(Boolean(value))}
			>
				<Checkbox.Indicator />
			</Checkbox>
			<Text>{props.children}</Text>
		</XStack>
	);
}

function NativeSelect<Value extends string>(props: {
	value?: Value;
	placeholder?: string;
	disabled?: boolean;
	options: Array<{ label: ReactNode; value: Value }>;
	onChange: (value: Value | undefined) => void;
}) {
	return (
		<select
			className='native-select'
			disabled={props.disabled}
			value={props.value ?? ''}
			onChange={event => props.onChange((event.target.value || undefined) as Value | undefined)}
		>
			<option value=''>{props.placeholder ?? 'Select'}</option>
			{props.options.map(option => (
				<option key={option.value} value={option.value}>
					{option.label}
				</option>
			))}
		</select>
	);
}

function SegmentedControl<Value extends string>(props: {
	value: Value;
	options: Array<{ label: ReactNode; value: Value }>;
	onChange: (value: Value) => void;
}) {
	return (
		<div className='segmented-control'>
			{props.options.map(option => (
				<button
					type='button'
					key={option.value}
					className={option.value === props.value ? 'segmented-control-active' : ''}
					onClick={() => props.onChange(option.value)}
				>
					{option.label}
				</button>
			))}
		</div>
	);
}

function InlineAlert(props: {
	kind: 'error' | 'warning' | 'info' | 'success';
	title: ReactNode;
	children?: ReactNode;
	className?: string;
}) {
	const theme = useTheme();
	const palette = {
		error: [theme.errorBg?.get('web'), theme.errorBorder?.get('web'), theme.error?.get('web')],
		warning: [
			theme.warningBg?.get('web'),
			theme.warningBorder?.get('web'),
			theme.warning?.get('web'),
		],
		info: [theme.infoBg?.get('web'), theme.infoBorder?.get('web'), theme.infoText?.get('web')],
		success: [
			theme.successBg?.get('web'),
			theme.successBorder?.get('web'),
			theme.success?.get('web'),
		],
	}[props.kind];

	return (
		<XStack
			className={props.className}
			gap={10}
			padding={12}
			borderWidth={1}
			borderRadius={8}
			style={{ background: palette[0], borderColor: palette[1] }}
		>
			<Text fontWeight='700' style={{ color: palette[2] }}>
				●
			</Text>
			<YStack flex={1} minWidth={0}>
				<Text fontWeight='700'>{props.title}</Text>
				{props.children}
			</YStack>
		</XStack>
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
		<XStack gap={8} flexWrap='wrap' alignItems='center'>
			<Input
				value={props.config.textFilter}
				placeholder='text filter'
				style={{ width: 220 }}
				onChange={event => props.onChange({ ...props.config, textFilter: event.target.value })}
			/>
			<TagInput
				value={props.config.categories}
				placeholder='categories'
				options={props.labCategories.map(category => ({
					label: `${category.category} (${category.count})`,
					value: category.category,
				}))}
				disabled={props.isLoading}
				onChange={categories => props.onChange({ ...props.config, categories })}
			/>
			<NativeSelect
				value={props.config.startDate ?? undefined}
				placeholder='start date'
				disabled={props.isLoading}
				options={props.labDates.map(date => ({ label: date, value: date }))}
				onChange={startDate => props.onChange({ ...props.config, startDate: startDate ?? null })}
			/>
			<CheckControl
				checked={props.config.onlyLatest}
				onCheckedChange={onlyLatest => props.onChange({ ...props.config, onlyLatest })}
			>
				Only latest
			</CheckControl>
		</XStack>
	);
}

function RunStateTag({ state }: { state: RunState }) {
	if (state.status === 'running') {
		return (
			<TagChip icon={<Spinner size='small' />} color='processing'>
				Running
			</TagChip>
		);
	}
	if (state.status === 'success') {
		return (
			<TagChip icon={<CheckCircleOutlined />} color='success'>
				Done
			</TagChip>
		);
	}
	if (state.status === 'error') {
		return (
			<TagChip icon={<WarningOutlined />} color='error'>
				Error
			</TagChip>
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
				{props.isRunning ? <Spinner size='small' /> : null}
				<Button
					icon={<CopyOutlined />}
					disabled={props.value.trim().length === 0}
					onPress={() => props.onCopy(props.value)}
				/>
			</div>
			<AutoResizeTextArea
				readOnly
				value={props.value}
				placeholder={props.mode === 'json' ? '{}' : ''}
				minRows={18}
				maxRows={36}
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
				{props.isRunning ? <Spinner /> : <Text color='$textMuted'>No output yet</Text>}
			</div>
		);
	}

	return (
		<div className='sensors-csv-list'>
			{props.files.map(file => (
				<div className='sensors-csv-file' key={file.fileName}>
					<XStack alignItems='center' justifyContent='space-between' gap={12}>
						<Text fontWeight='700'>{file.fileName}</Text>
						<Button icon={<CopyOutlined />} onPress={() => props.onCopy(file.content)} />
					</XStack>
					<AutoResizeTextArea
						readOnly
						value={file.content}
						minRows={8}
						maxRows={20}
						className='sensors-output-textarea'
					/>
				</div>
			))}
		</div>
	);
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
