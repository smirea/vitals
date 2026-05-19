import type { inferRouterOutputs } from '@trpc/server';
import type { AppRouter } from 'server/trpc/index.ts';
import { ActivityIndicator, Button } from '@ant-design/react-native';
import { useQuery } from '@tanstack/react-query';
import * as FileSystem from 'expo-file-system/legacy';
import * as Sharing from 'expo-sharing';
import { useEffect, useMemo, useState } from 'react';
import { Pressable, ScrollView, Text, TextInput, View, useColorScheme } from 'react-native';
import { useTRPC, useTRPCClient } from '@/src/api/trpc';
import { BottomSheet, FloatingActionButton, IconButton } from '@/src/components/mobile-ui';
import { pageStyles } from '@/src/theme/page-styles';

type RouterOutput = inferRouterOutputs<AppRouter>;

type SensorsConfig = RouterOutput['sensors']['getConfig'];
type SensorRunResult = RouterOutput['sensors']['runExtractor'];

const sensorKeys = ['labs', 'pills', 'voiceMemos', 'macrofactor', 'whoop', 'workouts'] as const;
const dependentSensorKeys = ['pills', 'voiceMemos', 'macrofactor', 'whoop', 'workouts'] as const;
const outputModes = ['text', 'json', 'csv'] as const;
const voiceMemoContentOptions = ['raw', 'summary', 'both'] as const;

type SensorKey = (typeof sensorKeys)[number];
type DependentSensorKey = (typeof dependentSensorKeys)[number];
type OutputMode = (typeof outputModes)[number];
type VoiceMemoContent = (typeof voiceMemoContentOptions)[number];
type RunStatus = 'idle' | 'running' | 'success' | 'error';

type RunState = {
	status: RunStatus;
	error: string | null;
	completedAt: string | null;
};

type LabsConfig = {
	textFilter: string;
	categories: string[];
	startDate: string | null;
	onlyLatest: boolean;
};

type VoiceMemosConfig = {
	content: VoiceMemoContent;
};

type MacrofactorConfig = {
	recipeDetails: boolean;
};

const sensorLabels = {
	labs: 'Labs',
	pills: 'Pills',
	voiceMemos: "Captain's Log",
	macrofactor: 'MacroFactor',
	whoop: 'WHOOP',
	workouts: 'Workouts',
} satisfies Record<SensorKey, string>;

function createDefaultRunStates() {
	return Object.fromEntries(
		sensorKeys.map(key => [key, { status: 'idle', error: null, completedAt: null }]),
	) as Record<SensorKey, RunState>;
}

function getDateDaysAgo(days: number) {
	const date = new Date();
	date.setDate(date.getDate() - days);
	return formatDateInput(date);
}

function getDateMonthsAgo(months: number) {
	const date = new Date();
	date.setMonth(date.getMonth() - months);
	return formatDateInput(date);
}

function formatDateInput(date: Date) {
	const year = String(date.getFullYear());
	const month = String(date.getMonth() + 1).padStart(2, '0');
	const day = String(date.getDate()).padStart(2, '0');
	return `${year}-${month}-${day}`;
}

function isDateInput(value: string) {
	return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function getDefaultLabStartDate(config: SensorsConfig | undefined) {
	const targetDate = getDateMonthsAgo(12);
	const sortedDates = [...(config?.labDates ?? [])].sort((left, right) =>
		right.localeCompare(left),
	);
	return sortedDates.find(date => date <= targetDate) ?? sortedDates.at(-1) ?? null;
}

function buildCompiledJsonText(
	defaultStartDate: string,
	results: Partial<Record<SensorKey, SensorRunResult>>,
) {
	return JSON.stringify(
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
	);
}

function buildCompiledText(results: Partial<Record<SensorKey, SensorRunResult>>) {
	return sensorKeys
		.map(key => {
			const result = results[key];
			if (!result?.text) return null;
			return `# ${result.label}\n\n${result.text.trim()}`;
		})
		.filter(Boolean)
		.join('\n\n');
}

function getCsvFiles(results: Partial<Record<SensorKey, SensorRunResult>>) {
	return sensorKeys.flatMap(key => results[key]?.csvFiles ?? []);
}

function dateStamp() {
	const now = new Date();
	const date = formatDateInput(now);
	const hours = String(now.getHours()).padStart(2, '0');
	const minutes = String(now.getMinutes()).padStart(2, '0');
	const seconds = String(now.getSeconds()).padStart(2, '0');
	return `${date}-${hours}${minutes}${seconds}`;
}

function formatCompletedAt(value: string | null) {
	if (!value) return '';
	return new Date(value).toLocaleTimeString('en-US', {
		hour: 'numeric',
		minute: '2-digit',
	});
}

function parseCategoryText(value: string) {
	const seen = new Set<string>();
	const categories: string[] = [];
	for (const rawCategory of value.split(',')) {
		const category = rawCategory.trim();
		const key = category.toLocaleLowerCase();
		if (!category || seen.has(key)) continue;
		seen.add(key);
		categories.push(category);
	}
	return categories;
}

export default function SensorsScreen() {
	const trpc = useTRPC();
	const trpcClient = useTRPCClient();
	const isDark = useColorScheme() === 'dark';
	const styles = sensorsStyles(isDark);
	const sharedStyles = pageStyles(isDark);
	const [notice, setNotice] = useState<string | null>(null);
	const [defaultStartDate, setDefaultStartDate] = useState(getDateDaysAgo(7));
	const [selectedKeys, setSelectedKeys] = useState(
		Object.fromEntries(sensorKeys.map(key => [key, true])) as Record<SensorKey, boolean>,
	);
	const [outputMode, setOutputMode] = useState<OutputMode>('text');
	const [runStates, setRunStates] = useState(createDefaultRunStates);
	const [results, setResults] = useState<Partial<Record<SensorKey, SensorRunResult>>>({});
	const [configSensor, setConfigSensor] = useState<SensorKey | null>(null);
	const [labsConfig, setLabsConfig] = useState<LabsConfig>({
		textFilter: '',
		categories: [],
		startDate: null,
		onlyLatest: false,
	});
	const [voiceMemosConfig, setVoiceMemosConfig] = useState<VoiceMemosConfig>({
		content: 'raw',
	});
	const [macrofactorConfig, setMacrofactorConfig] = useState<MacrofactorConfig>({
		recipeDetails: false,
	});
	const [startDates, setStartDates] = useState(
		Object.fromEntries(dependentSensorKeys.map(key => [key, defaultStartDate])) as Record<
			DependentSensorKey,
			string
		>,
	);

	const configQuery = useQuery(trpc.sensors.getConfig.queryOptions());
	const selectedRunKeys = sensorKeys.filter(key => selectedKeys[key]);
	const isAnySelectedRunning = selectedRunKeys.some(key => runStates[key].status === 'running');
	const isAnyRunning = sensorKeys.some(key => runStates[key].status === 'running');
	const compiledJsonText = useMemo(
		() => buildCompiledJsonText(defaultStartDate, results),
		[defaultStartDate, results],
	);
	const compiledText = useMemo(() => buildCompiledText(results), [results]);
	const csvFiles = useMemo(() => getCsvFiles(results), [results]);
	const outputText = outputMode === 'json' ? compiledJsonText : compiledText;
	const defaultLabStartDate = useMemo(
		() => getDefaultLabStartDate(configQuery.data),
		[configQuery.data],
	);

	useEffect(() => {
		if (!defaultLabStartDate) return;
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
		const startDate = key === 'labs' ? defaultStartDate : startDates[key as DependentSensorKey];
		if (!isDateInput(startDate)) {
			throw new Error(`${sensorLabels[key]} start date must use YYYY-MM-DD.`);
		}

		setRunStates(previous => ({
			...previous,
			[key]: { status: 'running', error: null, completedAt: null },
		}));

		try {
			const result = await trpcClient.sensors.runExtractor.mutate({
				key,
				outputMode,
				startDate,
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
			const message = error instanceof Error ? error.message : String(error);
			setRunStates(previous => ({
				...previous,
				[key]: { status: 'error', error: message, completedAt: new Date().toISOString() },
			}));
			throw error;
		}
	}

	async function runKeys(keys: SensorKey[]) {
		if (keys.length === 0) {
			setNotice('Select at least one sensor.');
			return;
		}

		const settled = await Promise.allSettled(keys.map(key => runSensor(key)));
		const failedCount = settled.filter(result => result.status === 'rejected').length;
		setNotice(
			failedCount > 0
				? `${failedCount} sensor${failedCount === 1 ? '' : 's'} failed.`
				: `Finished ${keys.length} sensor${keys.length === 1 ? '' : 's'}.`,
		);
	}

	async function shareOutput() {
		try {
			if (outputMode === 'csv') {
				if (csvFiles.length === 0) throw new Error('Run at least one CSV sensor first.');
				await writeAndShareFile({
					fileName: `vitals-sensors-${dateStamp()}-csv.txt`,
					content: csvFiles
						.map(file => `# ${file.fileName}\n${file.content.trimEnd()}`)
						.join('\n\n'),
					mimeType: 'text/plain',
				});
				return;
			}

			if (!outputText.trim()) throw new Error('Run at least one sensor first.');
			await writeAndShareFile({
				fileName: `vitals-sensors-${dateStamp()}.${outputMode === 'json' ? 'json' : 'txt'}`,
				content: outputText,
				mimeType: outputMode === 'json' ? 'application/json' : 'text/plain',
			});
		} catch (error) {
			setNotice(error instanceof Error ? error.message : String(error));
		}
	}

	async function shareCsvFile(file: { fileName: string; content: string }) {
		try {
			await writeAndShareFile({
				fileName: `vitals-sensors-${dateStamp()}-${file.fileName}`,
				content: file.content,
				mimeType: 'text/csv',
			});
		} catch (error) {
			setNotice(error instanceof Error ? error.message : String(error));
		}
	}

	async function writeAndShareFile(args: { fileName: string; content: string; mimeType: string }) {
		if (!FileSystem.documentDirectory) throw new Error('Document directory is unavailable.');
		if (!(await Sharing.isAvailableAsync())) throw new Error('Sharing is unavailable.');
		const fileUri = `${FileSystem.documentDirectory}${args.fileName}`;
		await FileSystem.writeAsStringAsync(fileUri, args.content, {
			encoding: FileSystem.EncodingType.UTF8,
		});
		await Sharing.shareAsync(fileUri, {
			mimeType: args.mimeType,
			dialogTitle: args.fileName,
		});
	}

	if (configQuery.isLoading) {
		return (
			<View style={styles.loadingScreen}>
				<ActivityIndicator animating text='Loading sensors...' />
			</View>
		);
	}

	if (configQuery.error) {
		return (
			<ScrollView
				contentInsetAdjustmentBehavior='automatic'
				contentContainerStyle={sharedStyles.page}
			>
				<Text selectable style={sharedStyles.errorText}>
					{configQuery.error.message}
				</Text>
			</ScrollView>
		);
	}

	return (
		<>
			<ScrollView
				contentInsetAdjustmentBehavior='automatic'
				contentContainerStyle={sharedStyles.page}
			>
				{notice ? (
					<Pressable onPress={() => setNotice(null)} style={styles.notice}>
						<Text selectable style={styles.noticeText}>
							{notice}
						</Text>
					</Pressable>
				) : null}

				<View style={styles.controlPanel}>
					<View style={styles.rowBetween}>
						<View>
							<Text style={styles.statValue}>{selectedRunKeys.length}</Text>
							<Text style={styles.muted}>selected sensors</Text>
						</View>
						<Button
							size='small'
							onPress={() => void shareOutput()}
							disabled={
								outputMode === 'csv' ? csvFiles.length === 0 : outputText.trim().length === 0
							}
						>
							Share
						</Button>
					</View>
					<TextInput
						value={defaultStartDate}
						placeholder='YYYY-MM-DD'
						placeholderTextColor={styles.placeholder.color}
						style={styles.compactInput}
						onChangeText={updateDefaultStartDate}
					/>
					<View style={styles.chipRow}>
						<Chip
							label='7 days'
							onPress={() => updateDefaultStartDate(getDateDaysAgo(7))}
							styles={styles}
						/>
						<Chip
							label='1 month'
							onPress={() => updateDefaultStartDate(getDateMonthsAgo(1))}
							styles={styles}
						/>
						<Chip
							label='3 months'
							onPress={() => updateDefaultStartDate(getDateMonthsAgo(3))}
							styles={styles}
						/>
					</View>
					<View style={styles.segment}>
						{outputModes.map(mode => (
							<SegmentButton
								key={mode}
								label={mode.toUpperCase()}
								active={outputMode === mode}
								onPress={() => setOutputMode(mode)}
								styles={styles}
							/>
						))}
					</View>
				</View>

				<View style={styles.stack}>
					{sensorKeys.map(key => (
						<SensorCard
							key={key}
							sensorKey={key}
							selected={selectedKeys[key]}
							runState={runStates[key]}
							startDate={key === 'labs' ? defaultStartDate : startDates[key as DependentSensorKey]}
							labsConfig={labsConfig}
							voiceMemosConfig={voiceMemosConfig}
							macrofactorConfig={macrofactorConfig}
							onSelectedChange={selected =>
								setSelectedKeys(previous => ({ ...previous, [key]: selected }))
							}
							onRun={() => void runSensor(key).catch(error => setNotice(error.message))}
							onConfigure={() => setConfigSensor(key)}
							styles={styles}
						/>
					))}
				</View>

				<View style={styles.outputCard}>
					<View style={styles.rowBetween}>
						<Text style={styles.sectionTitle}>Output</Text>
						<Text style={styles.muted}>{outputMode.toUpperCase()}</Text>
					</View>
					{outputMode === 'csv' ? (
						<CsvOutput files={csvFiles} onShare={shareCsvFile} styles={styles} />
					) : (
						<TextOutput value={outputText} isRunning={isAnyRunning} styles={styles} />
					)}
				</View>
			</ScrollView>
			<FloatingActionButton
				icon='play.circle.fill'
				label='Run'
				onPress={() => void runKeys(selectedRunKeys)}
				loading={isAnySelectedRunning}
			/>
			<SensorConfigSheet
				sensorKey={configSensor}
				defaultStartDate={defaultStartDate}
				startDates={startDates}
				labsConfig={labsConfig}
				voiceMemosConfig={voiceMemosConfig}
				macrofactorConfig={macrofactorConfig}
				labDates={configQuery.data?.labDates ?? []}
				labCategories={configQuery.data?.labCategories ?? []}
				onClose={() => setConfigSensor(null)}
				onStartDateChange={(key, startDate) =>
					setStartDates(previous => ({ ...previous, [key]: startDate }))
				}
				onLabsConfigChange={setLabsConfig}
				onVoiceMemosConfigChange={setVoiceMemosConfig}
				onMacrofactorConfigChange={setMacrofactorConfig}
				styles={styles}
			/>
		</>
	);
}

function SensorCard({
	sensorKey,
	selected,
	runState,
	startDate,
	labsConfig,
	voiceMemosConfig,
	macrofactorConfig,
	onSelectedChange,
	onRun,
	onConfigure,
	styles,
}: {
	sensorKey: SensorKey;
	selected: boolean;
	runState: RunState;
	startDate: string;
	labsConfig: LabsConfig;
	voiceMemosConfig: VoiceMemosConfig;
	macrofactorConfig: MacrofactorConfig;
	onSelectedChange: (selected: boolean) => void;
	onRun: () => void;
	onConfigure: () => void;
	styles: ReturnType<typeof sensorsStyles>;
}) {
	const configSummary = getSensorConfigSummary({
		sensorKey,
		startDate,
		labsConfig,
		voiceMemosConfig,
		macrofactorConfig,
	});

	return (
		<View style={styles.sensorCard}>
			<View style={styles.rowBetween}>
				<Pressable onPress={() => onSelectedChange(!selected)} style={styles.sensorTitleRow}>
					<View style={[styles.checkbox, selected && styles.checkboxActive]}>
						{selected ? <View style={styles.checkboxDot} /> : null}
					</View>
					<View style={{ flex: 1 }}>
						<Text style={styles.cardTitle}>{sensorLabels[sensorKey]}</Text>
						<Text style={styles.muted} numberOfLines={2}>
							{configSummary}
						</Text>
					</View>
				</Pressable>
				<View style={styles.inline}>
					<IconButton
						icon='gearshape'
						label={`${sensorLabels[sensorKey]} settings`}
						onPress={onConfigure}
					/>
					<Button size='small' onPress={onRun} loading={runState.status === 'running'}>
						Run
					</Button>
				</View>
			</View>
			<RunStatePill state={runState} styles={styles} />
			{runState.error ? (
				<Text selectable style={styles.errorText}>
					{runState.error}
				</Text>
			) : null}
		</View>
	);
}

function SensorConfigSheet({
	sensorKey,
	defaultStartDate,
	startDates,
	labsConfig,
	voiceMemosConfig,
	macrofactorConfig,
	labDates,
	labCategories,
	onClose,
	onStartDateChange,
	onLabsConfigChange,
	onVoiceMemosConfigChange,
	onMacrofactorConfigChange,
	styles,
}: {
	sensorKey: SensorKey | null;
	defaultStartDate: string;
	startDates: Record<DependentSensorKey, string>;
	labsConfig: LabsConfig;
	voiceMemosConfig: VoiceMemosConfig;
	macrofactorConfig: MacrofactorConfig;
	labDates: string[];
	labCategories: Array<{ category: string; count: number }>;
	onClose: () => void;
	onStartDateChange: (key: DependentSensorKey, startDate: string) => void;
	onLabsConfigChange: (config: LabsConfig) => void;
	onVoiceMemosConfigChange: (config: VoiceMemosConfig) => void;
	onMacrofactorConfigChange: (config: MacrofactorConfig) => void;
	styles: ReturnType<typeof sensorsStyles>;
}) {
	const dependentKey = sensorKey && sensorKey !== 'labs' ? (sensorKey as DependentSensorKey) : null;

	return (
		<BottomSheet
			visible={sensorKey !== null}
			title={sensorKey ? `${sensorLabels[sensorKey]} settings` : 'Settings'}
			onClose={onClose}
		>
			{sensorKey === 'labs' ? (
				<LabsConfigView
					config={labsConfig}
					labDates={labDates}
					labCategories={labCategories}
					onChange={onLabsConfigChange}
					styles={styles}
				/>
			) : dependentKey ? (
				<View style={styles.stack}>
					<Text style={styles.fieldLabel}>Start date</Text>
					<TextInput
						value={startDates[dependentKey]}
						placeholder='YYYY-MM-DD'
						placeholderTextColor={styles.placeholder.color}
						style={styles.input}
						onChangeText={startDate => onStartDateChange(dependentKey, startDate)}
					/>
					<Text style={styles.muted}>Default is {defaultStartDate}.</Text>
					{sensorKey === 'voiceMemos' ? (
						<View style={styles.chipRow}>
							{voiceMemoContentOptions.map(content => (
								<Chip
									key={content}
									label={content}
									active={voiceMemosConfig.content === content}
									onPress={() => onVoiceMemosConfigChange({ content })}
									styles={styles}
								/>
							))}
						</View>
					) : null}
					{sensorKey === 'macrofactor' ? (
						<Chip
							label='Recipe details'
							active={macrofactorConfig.recipeDetails}
							onPress={() =>
								onMacrofactorConfigChange({
									recipeDetails: !macrofactorConfig.recipeDetails,
								})
							}
							styles={styles}
						/>
					) : null}
				</View>
			) : null}
		</BottomSheet>
	);
}

function getSensorConfigSummary({
	sensorKey,
	startDate,
	labsConfig,
	voiceMemosConfig,
	macrofactorConfig,
}: {
	sensorKey: SensorKey;
	startDate: string;
	labsConfig: LabsConfig;
	voiceMemosConfig: VoiceMemosConfig;
	macrofactorConfig: MacrofactorConfig;
}) {
	if (sensorKey === 'labs') {
		const parts = [
			labsConfig.startDate ?? startDate,
			labsConfig.onlyLatest ? 'latest only' : null,
			labsConfig.categories.length ? `${labsConfig.categories.length} categories` : null,
			labsConfig.textFilter.trim() ? 'filtered' : null,
		].filter(Boolean);
		return parts.join(' - ');
	}
	if (sensorKey === 'voiceMemos') return `${startDate} - ${voiceMemosConfig.content}`;
	if (sensorKey === 'macrofactor') {
		return `${startDate}${macrofactorConfig.recipeDetails ? ' - recipes' : ''}`;
	}
	return startDate;
}

function LabsConfigView({
	config,
	labDates,
	labCategories,
	onChange,
	styles,
}: {
	config: LabsConfig;
	labDates: string[];
	labCategories: Array<{ category: string; count: number }>;
	onChange: (config: LabsConfig) => void;
	styles: ReturnType<typeof sensorsStyles>;
}) {
	return (
		<View style={styles.stack}>
			<Text style={styles.fieldLabel}>Lab text filter</Text>
			<TextInput
				value={config.textFilter}
				placeholder='measurement, category, lab'
				placeholderTextColor={styles.placeholder.color}
				style={styles.input}
				onChangeText={textFilter => onChange({ ...config, textFilter })}
			/>
			<Text style={styles.fieldLabel}>Lab start date</Text>
			<TextInput
				value={config.startDate ?? ''}
				placeholder='YYYY-MM-DD'
				placeholderTextColor={styles.placeholder.color}
				style={styles.input}
				onChangeText={startDate => onChange({ ...config, startDate: startDate || null })}
			/>
			{labDates.length > 0 ? (
				<ScrollView horizontal showsHorizontalScrollIndicator={false}>
					<View style={styles.chipRow}>
						{labDates.slice(0, 8).map(date => (
							<Chip
								key={date}
								label={date}
								onPress={() => onChange({ ...config, startDate: date })}
								styles={styles}
							/>
						))}
					</View>
				</ScrollView>
			) : null}
			<Text style={styles.fieldLabel}>Categories</Text>
			<TextInput
				value={config.categories.join(', ')}
				placeholder='comma-separated'
				placeholderTextColor={styles.placeholder.color}
				style={styles.input}
				onChangeText={value => onChange({ ...config, categories: parseCategoryText(value) })}
			/>
			{labCategories.length > 0 ? (
				<ScrollView horizontal showsHorizontalScrollIndicator={false}>
					<View style={styles.chipRow}>
						{labCategories.slice(0, 16).map(category => (
							<Chip
								key={category.category}
								label={`${category.category} (${category.count})`}
								active={config.categories.includes(category.category)}
								onPress={() =>
									onChange({
										...config,
										categories: toggleCategory(config.categories, category.category),
									})
								}
								styles={styles}
							/>
						))}
					</View>
				</ScrollView>
			) : null}
			<Chip
				label='Only latest'
				active={config.onlyLatest}
				onPress={() => onChange({ ...config, onlyLatest: !config.onlyLatest })}
				styles={styles}
			/>
		</View>
	);
}

function RunStatePill({
	state,
	styles,
}: {
	state: RunState;
	styles: ReturnType<typeof sensorsStyles>;
}) {
	if (state.status === 'idle') return null;
	const label =
		state.status === 'running'
			? 'Running'
			: state.status === 'success'
				? `Done ${formatCompletedAt(state.completedAt)}`
				: `Error ${formatCompletedAt(state.completedAt)}`;
	return (
		<View
			style={[
				styles.statusPill,
				state.status === 'success' && styles.statusSuccess,
				state.status === 'error' && styles.statusError,
			]}
		>
			<Text
				style={[
					styles.statusText,
					state.status === 'success' && styles.statusSuccessText,
					state.status === 'error' && styles.statusErrorText,
				]}
			>
				{label}
			</Text>
		</View>
	);
}

function TextOutput({
	value,
	isRunning,
	styles,
}: {
	value: string;
	isRunning: boolean;
	styles: ReturnType<typeof sensorsStyles>;
}) {
	if (!value.trim()) {
		return <Text style={styles.muted}>{isRunning ? 'Running...' : 'No output yet.'}</Text>;
	}

	return (
		<TextInput
			value={value}
			editable={false}
			multiline
			style={[styles.input, styles.outputInput]}
		/>
	);
}

function CsvOutput({
	files,
	onShare,
	styles,
}: {
	files: Array<{ fileName: string; content: string }>;
	onShare: (file: { fileName: string; content: string }) => void;
	styles: ReturnType<typeof sensorsStyles>;
}) {
	if (files.length === 0) {
		return <Text style={styles.muted}>No CSV files yet.</Text>;
	}

	return (
		<View style={styles.stack}>
			{files.map(file => (
				<View key={file.fileName} style={styles.csvFile}>
					<View style={styles.rowBetween}>
						<Text style={styles.cardTitle}>{file.fileName}</Text>
						<Button size='small' onPress={() => onShare(file)}>
							Share
						</Button>
					</View>
					<TextInput
						value={file.content}
						editable={false}
						multiline
						style={[styles.input, styles.csvInput]}
					/>
				</View>
			))}
		</View>
	);
}

function SegmentButton({
	label,
	active,
	onPress,
	styles,
}: {
	label: string;
	active: boolean;
	onPress: () => void;
	styles: ReturnType<typeof sensorsStyles>;
}) {
	return (
		<Pressable
			onPress={onPress}
			style={[styles.segmentButton, active && styles.segmentButtonActive]}
		>
			<Text style={active ? styles.segmentButtonActiveText : styles.segmentButtonText}>
				{label}
			</Text>
		</Pressable>
	);
}

function Chip({
	label,
	active = false,
	onPress,
	styles,
}: {
	label: string;
	active?: boolean;
	onPress: () => void;
	styles: ReturnType<typeof sensorsStyles>;
}) {
	return (
		<Pressable
			onPress={onPress}
			style={({ pressed }) => [styles.chip, active && styles.chipActive, pressed && styles.pressed]}
		>
			<Text style={active ? styles.chipActiveText : styles.chipText}>{label}</Text>
		</Pressable>
	);
}

function toggleCategory(categories: string[], category: string) {
	return categories.includes(category)
		? categories.filter(value => value !== category)
		: [...categories, category];
}

function sensorsStyles(isDark: boolean) {
	const text = isDark ? '#f9fafb' : '#111827';
	const muted = isDark ? '#a1a1aa' : '#71717a';
	const bg = isDark ? '#0f172a' : '#f6f7f9';
	const border = isDark ? '#27272a' : '#e5e7eb';
	const surface = isDark ? '#111827' : '#fff';

	return {
		loadingScreen: {
			alignItems: 'center' as const,
			backgroundColor: bg,
			flex: 1,
			justifyContent: 'center' as const,
		},
		headerRow: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 12,
			justifyContent: 'space-between' as const,
		},
		stack: {
			gap: 12,
		},
		controlPanel: {
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 14,
			borderWidth: 1,
			gap: 10,
			padding: 12,
		},
		statValue: {
			color: text,
			fontSize: 26,
			fontVariant: ['tabular-nums'] as ['tabular-nums'],
			fontWeight: '800' as const,
		},
		rowBetween: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 10,
			justifyContent: 'space-between' as const,
		},
		inline: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			gap: 8,
		},
		notice: {
			backgroundColor: isDark ? '#102a43' : '#e6f4ff',
			borderColor: '#91caff',
			borderRadius: 8,
			borderWidth: 1,
			padding: 10,
		},
		noticeText: {
			color: text,
			fontSize: 14,
		},
		title: {
			color: text,
			fontSize: 24,
			fontWeight: '800' as const,
		},
		sectionTitle: {
			color: muted,
			fontSize: 13,
			fontWeight: '700' as const,
			textTransform: 'uppercase' as const,
		},
		cardTitle: {
			color: text,
			flex: 1,
			fontSize: 15,
			fontWeight: '700' as const,
		},
		muted: {
			color: muted,
			fontSize: 12,
		},
		errorText: {
			color: '#cf1322',
			fontSize: 13,
			lineHeight: 19,
		},
		fieldLabel: {
			color: muted,
			fontSize: 12,
			fontWeight: '700' as const,
			textTransform: 'uppercase' as const,
		},
		input: {
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 8,
			borderWidth: 1,
			color: text,
			fontSize: 15,
			paddingHorizontal: 10,
			paddingVertical: 8,
		},
		compactInput: {
			backgroundColor: isDark ? '#0f172a' : '#f6f7f9',
			borderColor: border,
			borderRadius: 10,
			borderWidth: 1,
			color: text,
			fontSize: 15,
			paddingHorizontal: 10,
			paddingVertical: 8,
		},
		outputInput: {
			maxHeight: 420,
			minHeight: 260,
			textAlignVertical: 'top' as const,
		},
		csvInput: {
			maxHeight: 220,
			minHeight: 140,
			textAlignVertical: 'top' as const,
		},
		placeholder: {
			color: muted,
		},
		chipRow: {
			alignItems: 'center' as const,
			flexDirection: 'row' as const,
			flexWrap: 'wrap' as const,
			gap: 6,
		},
		chip: {
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 999,
			borderWidth: 1,
			paddingHorizontal: 10,
			paddingVertical: 6,
		},
		chipActive: {
			backgroundColor: '#1677ff',
			borderColor: '#1677ff',
		},
		chipText: {
			color: muted,
			fontSize: 12,
			fontWeight: '600' as const,
		},
		chipActiveText: {
			color: '#fff',
			fontSize: 12,
			fontWeight: '700' as const,
		},
		pressed: {
			opacity: 0.75,
		},
		segment: {
			backgroundColor: isDark ? '#1f2937' : '#e5e7eb',
			borderRadius: 8,
			flexDirection: 'row' as const,
			gap: 4,
			padding: 4,
		},
		segmentButton: {
			borderRadius: 7,
			flex: 1,
			paddingVertical: 8,
		},
		segmentButtonActive: {
			backgroundColor: '#1677ff',
		},
		segmentButtonText: {
			color: muted,
			fontSize: 12,
			fontWeight: '700' as const,
			textAlign: 'center' as const,
		},
		segmentButtonActiveText: {
			color: '#fff',
			fontSize: 12,
			fontWeight: '700' as const,
			textAlign: 'center' as const,
		},
		sensorTitleRow: {
			alignItems: 'center' as const,
			flex: 1,
			flexDirection: 'row' as const,
			gap: 10,
		},
		sensorCard: {
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 14,
			borderWidth: 1,
			gap: 10,
			padding: 12,
		},
		checkbox: {
			alignItems: 'center' as const,
			borderColor: border,
			borderRadius: 5,
			borderWidth: 1,
			height: 22,
			justifyContent: 'center' as const,
			width: 22,
		},
		checkboxActive: {
			backgroundColor: '#1677ff',
			borderColor: '#1677ff',
		},
		checkboxDot: {
			backgroundColor: '#fff',
			borderRadius: 4,
			height: 8,
			width: 8,
		},
		statusPill: {
			alignSelf: 'flex-start' as const,
			backgroundColor: isDark ? '#1f2937' : '#f4f4f5',
			borderRadius: 999,
			paddingHorizontal: 10,
			paddingVertical: 5,
		},
		statusSuccess: {
			backgroundColor: isDark ? '#12361f' : '#f6ffed',
		},
		statusError: {
			backgroundColor: isDark ? '#431418' : '#fff1f0',
		},
		statusText: {
			color: muted,
			fontSize: 12,
			fontWeight: '700' as const,
		},
		statusSuccessText: {
			color: '#237804',
		},
		statusErrorText: {
			color: '#cf1322',
		},
		csvFile: {
			gap: 10,
		},
		outputCard: {
			backgroundColor: surface,
			borderColor: border,
			borderRadius: 14,
			borderWidth: 1,
			gap: 10,
			padding: 12,
		},
	};
}
