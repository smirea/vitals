import {
	ActivityIndicator,
	Button,
	Card,
	Checkbox,
	Modal,
	Switch,
	Tag,
} from '@ant-design/react-native';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import * as DocumentPicker from 'expo-document-picker';
import * as FileSystem from 'expo-file-system/legacy';
import * as Sharing from 'expo-sharing';
import { SymbolView, type SymbolViewProps } from 'expo-symbols';
import { useMemo, useState } from 'react';
import {
	Alert,
	Linking,
	Pressable,
	ScrollView,
	Text,
	TextInput,
	View,
	useColorScheme,
} from 'react-native';

import { API_BASE_URL, useTRPC } from '@/src/api/trpc';
import { BottomSheet, FloatingActionButton, IconButton } from '@/src/components/mobile-ui';
import {
	buildCsv,
	buildMeasurementRows,
	getCategoryOverview,
	getMeaningfulChanges,
	getPreviewRows,
	getSources,
	type LabMeasurementRow,
	type LabsDashboard,
	type LabsImportDocument,
	type PreviewRow,
	type SourceColumn,
} from '@/src/features/labs/model';
import { pageStyles } from '@/src/theme/page-styles';

type ActiveSection = 'overview' | 'documents';

export function LabsScreen() {
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const isDark = useColorScheme() === 'dark';
	const styles = labStyles(isDark);
	const sharedStyles = pageStyles(isDark);
	const [activeSection, setActiveSection] = useState<ActiveSection>('overview');
	const [searchText, setSearchText] = useState('');
	const [selectedSourceIds, setSelectedSourceIds] = useState<Set<string>>(new Set());
	const [groupByCategory, setGroupByCategory] = useState(true);
	const [starredKeys, setStarredKeys] = useState<Set<string>>(new Set());
	const [selectedRow, setSelectedRow] = useState<LabMeasurementRow | null>(null);
	const [previewDocument, setPreviewDocument] = useState<LabsImportDocument | null>(null);
	const [showFlaggedPreviewOnly, setShowFlaggedPreviewOnly] = useState(false);
	const [selectedDocumentIds, setSelectedDocumentIds] = useState<Set<number>>(new Set());
	const [notice, setNotice] = useState<string | null>(null);

	const dashboardQuery = useQuery(trpc.labs.getDashboard.queryOptions());
	const documentsQuery = useQuery(trpc.labs.listDocuments.queryOptions());
	const dashboard = dashboardQuery.data;
	const documents = documentsQuery.data ?? [];
	const sources = useMemo(() => (dashboard ? getSources(dashboard.documents) : []), [dashboard]);
	const activeSourceIds = useMemo(() => {
		if (selectedSourceIds.size > 0) return selectedSourceIds;
		return new Set(sources.map(source => source.id));
	}, [selectedSourceIds, sources]);
	const rows = useMemo(() => (dashboard ? buildMeasurementRows(dashboard) : []), [dashboard]);
	const filteredRows = useMemo(
		() =>
			rows
				.map(row => ({
					...row,
					values: row.values.filter(value => activeSourceIds.has(value.sourceId)),
				}))
				.filter(row => row.values.length > 0)
				.filter(row => row.searchText.includes(searchText.trim().toLowerCase())),
		[activeSourceIds, rows, searchText],
	);
	const visibleRows = useMemo(
		() =>
			[...filteredRows].sort((left, right) => {
				const starredDelta = Number(starredKeys.has(right.key)) - Number(starredKeys.has(left.key));
				return (
					starredDelta ||
					left.category.localeCompare(right.category) ||
					left.name.localeCompare(right.name)
				);
			}),
		[filteredRows, starredKeys],
	);
	const categoryOverview = useMemo(() => getCategoryOverview(rows), [rows]);
	const meaningfulChanges = useMemo(() => getMeaningfulChanges(rows), [rows]);
	const groupedRows = useMemo(
		() => groupRows(visibleRows, groupByCategory),
		[visibleRows, groupByCategory],
	);
	const selectedDocuments = useMemo(
		() => documents.filter(document => selectedDocumentIds.has(document.id)),
		[documents, selectedDocumentIds],
	);
	const hasSelectedDocumentGroup = selectedDocuments.some(document => Boolean(document.group));

	const invalidateLabs = async () => {
		await queryClient.invalidateQueries({ queryKey: [['labs']] });
	};
	const uploadDocumentsMutation = useMutation({
		...trpc.labs.uploadDocuments.mutationOptions(),
		onSuccess: async data => {
			await invalidateLabs();
			const queued = data.documents.filter(document => !document.deduplicated).length;
			const skipped = data.documents.length - queued;
			setNotice(
				`Queued ${queued} PDF${queued === 1 ? '' : 's'}${skipped ? `, skipped ${skipped} duplicate${skipped === 1 ? '' : 's'}` : ''}.`,
			);
		},
		onError: error => setNotice(error.message),
	});
	const updateDocumentsMutation = useMutation({
		...trpc.table.labDocuments.updateMany.mutationOptions(),
		onSuccess: async data => {
			await invalidateLabs();
			setSelectedDocumentIds(new Set());
			setNotice(`${data.updatedCount} document${data.updatedCount === 1 ? '' : 's'} updated.`);
		},
		onError: error => setNotice(error.message),
	});
	const deleteDocumentMutation = useMutation({
		...trpc.table.labDocuments.deleteMany.mutationOptions(),
		onSuccess: async data => {
			await invalidateLabs();
			setNotice(`${data.deletedCount} document${data.deletedCount === 1 ? '' : 's'} deleted.`);
		},
		onError: error => setNotice(error.message),
	});
	const retryDocumentMutation = useMutation({
		...trpc.labs.retryDocument.mutationOptions(),
		onSuccess: async () => {
			await invalidateLabs();
			setNotice('Document queued for retry.');
		},
		onError: error => setNotice(error.message),
	});
	const reprocessDocumentMutation = useMutation({
		...trpc.labs.reprocessDocument.mutationOptions(),
		onSuccess: async () => {
			await invalidateLabs();
			setNotice('Document queued for reprocess.');
		},
		onError: error => setNotice(error.message),
	});

	const onToggleSource = (sourceId: string) => {
		const allIds = sources.map(source => source.id);
		setSelectedSourceIds(previous => {
			const next = new Set(previous.size > 0 ? previous : allIds);
			if (next.has(sourceId)) {
				next.delete(sourceId);
			} else {
				next.add(sourceId);
			}
			return next.size === allIds.length ? new Set() : next;
		});
	};
	const onToggleDocument = (documentId: number) => {
		setSelectedDocumentIds(previous => {
			const next = new Set(previous);
			if (next.has(documentId)) {
				next.delete(documentId);
			} else {
				next.add(documentId);
			}
			return next;
		});
	};
	const onToggleStar = (key: string) => {
		setStarredKeys(previous => {
			const next = new Set(previous);
			if (next.has(key)) {
				next.delete(key);
			} else {
				next.add(key);
			}
			return next;
		});
	};

	const onImportDocuments = async () => {
		const result = await DocumentPicker.getDocumentAsync({
			type: 'application/pdf',
			copyToCacheDirectory: true,
			multiple: true,
		});
		if (result.canceled) return;

		await uploadDocumentsMutation.mutateAsync({
			files: await Promise.all(
				result.assets.map(async asset => ({
					fileName: asset.name,
					mimeType: asset.mimeType || 'application/pdf',
					dataBase64: await FileSystem.readAsStringAsync(asset.uri, {
						encoding: FileSystem.EncodingType.Base64,
					}),
				})),
			),
		});
	};
	const onGroupDocuments = async () => {
		if (selectedDocuments.length < 2) return;
		await updateDocumentsMutation.mutateAsync({
			where: [
				{ column: 'id', operator: 'in', value: selectedDocuments.map(document => document.id) },
			],
			values: { group: `mobile_${Date.now()}` },
		});
	};
	const onClearGroup = async () => {
		const groupedDocumentIds = selectedDocuments
			.filter(document => Boolean(document.group))
			.map(document => document.id);
		if (groupedDocumentIds.length === 0) return;
		await updateDocumentsMutation.mutateAsync({
			where: [{ column: 'id', operator: 'in', value: groupedDocumentIds }],
			values: { group: null },
		});
	};
	const onDeleteDocument = (document: LabsImportDocument) => {
		Alert.alert('Delete imported file?', document.fileName, [
			{ text: 'Cancel', style: 'cancel' },
			{
				text: 'Delete',
				style: 'destructive',
				onPress: () => {
					void deleteDocumentMutation.mutateAsync({
						where: [{ column: 'id', operator: 'eq', value: document.id }],
					});
				},
			},
		]);
	};
	const onShareCsv = async () => {
		if (!FileSystem.documentDirectory) throw new Error('Document directory is unavailable.');
		if (!(await Sharing.isAvailableAsync())) throw new Error('Sharing is unavailable.');
		const fileUri = `${FileSystem.documentDirectory}vitals-labs-${new Date().toISOString().slice(0, 10)}.csv`;
		await FileSystem.writeAsStringAsync(fileUri, buildCsv(visibleRows, sources), {
			encoding: FileSystem.EncodingType.UTF8,
		});
		await Sharing.shareAsync(fileUri, {
			mimeType: 'text/csv',
			UTI: 'public.comma-separated-values-text',
			dialogTitle: 'Share labs CSV',
		});
	};

	if (dashboardQuery.isLoading || documentsQuery.isLoading) {
		return (
			<View style={styles.loadingScreen}>
				<ActivityIndicator animating text='Loading labs...' />
			</View>
		);
	}

	const error = dashboardQuery.error ?? documentsQuery.error;
	if (error) {
		return (
			<ScrollView
				contentInsetAdjustmentBehavior='automatic'
				contentContainerStyle={sharedStyles.page}
			>
				<Text selectable style={sharedStyles.errorText}>
					{error.message}
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
				<View style={styles.segment}>
					<SegmentButton
						active={activeSection === 'overview'}
						onPress={() => setActiveSection('overview')}
					>
						Overview
					</SegmentButton>
					<SegmentButton
						active={activeSection === 'documents'}
						onPress={() => setActiveSection('documents')}
					>
						Documents
					</SegmentButton>
				</View>

				{notice ? (
					<Pressable onPress={() => setNotice(null)} style={styles.notice}>
						<Text selectable style={styles.noticeText}>
							{notice}
						</Text>
					</Pressable>
				) : null}

				{activeSection === 'overview' ? (
					<LabsOverview
						categoryOverview={categoryOverview}
						meaningfulChanges={meaningfulChanges}
						sources={sources}
						activeSourceIds={activeSourceIds}
						selectedSourceIds={selectedSourceIds}
						onToggleSource={onToggleSource}
						onClearSources={() => setSelectedSourceIds(new Set())}
						searchText={searchText}
						onSearchTextChange={setSearchText}
						groupByCategory={groupByCategory}
						onGroupByCategoryChange={setGroupByCategory}
						groupedRows={groupedRows}
						starredKeys={starredKeys}
						onToggleStar={onToggleStar}
						onSelectRow={setSelectedRow}
						onShareCsv={() => {
							void onShareCsv().catch(error => setNotice(error.message));
						}}
						styles={styles}
					/>
				) : (
					<LabsDocuments
						documents={documents}
						selectedDocumentIds={selectedDocumentIds}
						selectedDocuments={selectedDocuments}
						hasSelectedDocumentGroup={hasSelectedDocumentGroup}
						isMutating={
							uploadDocumentsMutation.isPending ||
							updateDocumentsMutation.isPending ||
							deleteDocumentMutation.isPending ||
							retryDocumentMutation.isPending ||
							reprocessDocumentMutation.isPending
						}
						onGroupDocuments={() => {
							void onGroupDocuments();
						}}
						onClearGroup={() => {
							void onClearGroup();
						}}
						onToggleDocument={onToggleDocument}
						onPreviewDocument={document => {
							setPreviewDocument(document);
							setShowFlaggedPreviewOnly(false);
						}}
						onOpenPdf={document => {
							void Linking.openURL(`${API_BASE_URL}/labs/documents/${document.id}/pdf`);
						}}
						onRetryDocument={document => {
							void retryDocumentMutation.mutateAsync({ documentId: document.id });
						}}
						onReprocessDocument={document => {
							void reprocessDocumentMutation.mutateAsync({ documentId: document.id });
						}}
						onDeleteDocument={onDeleteDocument}
						styles={styles}
					/>
				)}
			</ScrollView>
			<FloatingActionButton
				icon={activeSection === 'overview' ? 'square.and.arrow.up' : 'doc.badge.plus'}
				label={activeSection === 'overview' ? 'CSV' : 'Import'}
				onPress={() => {
					if (activeSection === 'overview') {
						void onShareCsv().catch(error => setNotice(error.message));
						return;
					}
					void onImportDocuments().catch(error => setNotice(error.message));
				}}
				loading={uploadDocumentsMutation.isPending}
			/>

			<MeasurementModal row={selectedRow} onClose={() => setSelectedRow(null)} styles={styles} />
			<DocumentPreviewModal
				document={previewDocument}
				dashboard={dashboard}
				showFlaggedOnly={showFlaggedPreviewOnly}
				onShowFlaggedOnlyChange={setShowFlaggedPreviewOnly}
				onClose={() => setPreviewDocument(null)}
				onOpenPdf={document => {
					void Linking.openURL(`${API_BASE_URL}/labs/documents/${document.id}/pdf`);
				}}
				styles={styles}
			/>
		</>
	);
}

function LabsOverview({
	categoryOverview,
	meaningfulChanges,
	sources,
	activeSourceIds,
	selectedSourceIds,
	onToggleSource,
	onClearSources,
	searchText,
	onSearchTextChange,
	groupByCategory,
	onGroupByCategoryChange,
	groupedRows,
	starredKeys,
	onToggleStar,
	onSelectRow,
	onShareCsv,
	styles,
}: {
	categoryOverview: ReturnType<typeof getCategoryOverview>;
	meaningfulChanges: ReturnType<typeof getMeaningfulChanges>;
	sources: SourceColumn[];
	activeSourceIds: Set<string>;
	selectedSourceIds: Set<string>;
	onToggleSource: (sourceId: string) => void;
	onClearSources: () => void;
	searchText: string;
	onSearchTextChange: (value: string) => void;
	groupByCategory: boolean;
	onGroupByCategoryChange: (value: boolean) => void;
	groupedRows: Array<{ category: string; rows: LabMeasurementRow[] }>;
	starredKeys: Set<string>;
	onToggleStar: (key: string) => void;
	onSelectRow: (row: LabMeasurementRow) => void;
	onShareCsv: () => void;
	styles: ReturnType<typeof labStyles>;
}) {
	return (
		<View style={styles.stack}>
			<HorizontalSection title='Categories'>
				{categoryOverview.map(category => (
					<View key={category.category} style={styles.categoryPill}>
						<View style={styles.rowBetween}>
							<Text style={styles.categoryTitle}>{category.category}</Text>
							<Text style={styles.muted}>{category.total}</Text>
						</View>
						<View style={styles.tallyRow}>
							<Tally color='#52c41a' count={category.inRange} />
							<Tally color='#ff4d4f' count={category.outOfRange} />
							<Tally color='#8c8c8c' count={category.unclassified} />
						</View>
					</View>
				))}
			</HorizontalSection>

			<HorizontalSection title='Last 6 months'>
				{meaningfulChanges.map(change => (
					<Card key={change.key} style={styles.changeCard}>
						<Card.Body>
							<View style={styles.rowBetween}>
								<Text style={styles.cardTitle} numberOfLines={1}>
									{change.measurement}
								</Text>
								<StatusTag status={change.direction} />
							</View>
							<Text style={styles.muted}>{change.category}</Text>
							<Text style={styles.changeLine} numberOfLines={2}>
								{change.previous.display}
								{' -> '}
								{change.latest.display}
							</Text>
							<Text style={styles.muted}>
								{change.previous.prettyDate} to {change.latest.prettyDate}
								{change.deltaPercent === null ? '' : `, ${Math.round(change.deltaPercent)}%`}
							</Text>
						</Card.Body>
					</Card>
				))}
			</HorizontalSection>

			<Card full>
				<Card.Body>
					<View style={styles.stack}>
						<TextInput
							value={searchText}
							placeholder='Filter measurements'
							onChangeText={onSearchTextChange}
							placeholderTextColor={styles.searchPlaceholder.color}
							style={styles.searchInput}
						/>
						<View style={styles.rowBetween}>
							<View style={styles.inline}>
								<Switch checked={groupByCategory} onChange={onGroupByCategoryChange} />
								<Text style={styles.body}>Group by category</Text>
							</View>
							<Button size='small' onPress={onShareCsv}>
								CSV
							</Button>
						</View>
					</View>
				</Card.Body>
			</Card>

			<HorizontalSection title='Sources'>
				<Pressable
					onPress={onClearSources}
					style={[styles.sourceChip, selectedSourceIds.size === 0 && styles.sourceChipActive]}
				>
					<Text
						style={
							selectedSourceIds.size === 0 ? styles.sourceChipActiveText : styles.sourceChipText
						}
					>
						All
					</Text>
				</Pressable>
				{sources.map(source => (
					<Pressable
						key={source.id}
						onPress={() => onToggleSource(source.id)}
						style={[styles.sourceChip, activeSourceIds.has(source.id) && styles.sourceChipActive]}
					>
						<Text
							style={
								activeSourceIds.has(source.id) ? styles.sourceChipActiveText : styles.sourceChipText
							}
						>
							{source.prettyDate}
						</Text>
					</Pressable>
				))}
			</HorizontalSection>

			{groupedRows.map(group => (
				<View key={group.category} style={styles.stack}>
					{groupByCategory ? <Text style={styles.sectionLabel}>{group.category}</Text> : null}
					{group.rows.map(row => (
						<MeasurementRow
							key={row.key}
							row={row}
							isStarred={starredKeys.has(row.key)}
							onToggleStar={() => onToggleStar(row.key)}
							onPress={() => onSelectRow(row)}
							styles={styles}
						/>
					))}
				</View>
			))}
		</View>
	);
}

function LabsDocuments({
	documents,
	selectedDocumentIds,
	selectedDocuments,
	hasSelectedDocumentGroup,
	isMutating,
	onGroupDocuments,
	onClearGroup,
	onToggleDocument,
	onPreviewDocument,
	onOpenPdf,
	onRetryDocument,
	onReprocessDocument,
	onDeleteDocument,
	styles,
}: {
	documents: LabsImportDocument[];
	selectedDocumentIds: Set<number>;
	selectedDocuments: LabsImportDocument[];
	hasSelectedDocumentGroup: boolean;
	isMutating: boolean;
	onGroupDocuments: () => void;
	onClearGroup: () => void;
	onToggleDocument: (documentId: number) => void;
	onPreviewDocument: (document: LabsImportDocument) => void;
	onOpenPdf: (document: LabsImportDocument) => void;
	onRetryDocument: (document: LabsImportDocument) => void;
	onReprocessDocument: (document: LabsImportDocument) => void;
	onDeleteDocument: (document: LabsImportDocument) => void;
	styles: ReturnType<typeof labStyles>;
}) {
	const [actionDocument, setActionDocument] = useState<LabsImportDocument | null>(null);

	return (
		<>
			<View style={styles.stack}>
				{selectedDocuments.length > 0 ? (
					<View style={styles.selectionBar}>
						<Text style={styles.body}>{selectedDocuments.length} selected</Text>
						<Button
							size='small'
							onPress={onGroupDocuments}
							disabled={selectedDocuments.length < 2 || isMutating}
						>
							Group
						</Button>
						<Button
							size='small'
							onPress={onClearGroup}
							disabled={!hasSelectedDocumentGroup || isMutating}
						>
							Clear group
						</Button>
					</View>
				) : null}

				{documents.map(document => (
					<View key={document.id} style={styles.documentCard}>
						<View style={styles.documentCardRow}>
							<Checkbox
								checked={selectedDocumentIds.has(document.id)}
								onChange={() => onToggleDocument(document.id)}
							/>
							<Pressable style={styles.documentMain} onPress={() => onPreviewDocument(document)}>
								<View style={styles.inline}>
									<StatusTag status={document.status} />
									{document.group ? <Tag small>Grouped</Tag> : null}
								</View>
								<Text style={styles.documentTitle} numberOfLines={2}>
									{document.fileName}
								</Text>
								<Text style={styles.muted} numberOfLines={2}>
									{document.date ?? document.queuedAt.slice(0, 10)}
									{document.labName ? `, ${document.labName}` : ''}
									{document.statusText ? `, ${document.statusText}` : ''}
								</Text>
								{document.status === 'failed' && document.lastError ? (
									<Text selectable style={styles.errorText} numberOfLines={4}>
										{document.lastError}
									</Text>
								) : null}
							</Pressable>
							<IconButton
								icon='ellipsis'
								label='Document actions'
								onPress={() => setActionDocument(document)}
							/>
						</View>
					</View>
				))}
			</View>
			<BottomSheet
				visible={Boolean(actionDocument)}
				title='Document actions'
				onClose={() => setActionDocument(null)}
			>
				{actionDocument ? (
					<View style={styles.stack}>
						<View style={styles.inline}>
							<StatusTag status={actionDocument.status} />
							{actionDocument.group ? <Tag small>Grouped</Tag> : null}
						</View>
						<Text style={styles.documentTitle}>{actionDocument.fileName}</Text>
						<Text style={styles.muted}>
							{actionDocument.date ?? actionDocument.queuedAt.slice(0, 10)}
							{actionDocument.labName ? `, ${actionDocument.labName}` : ''}
							{actionDocument.statusText ? `, ${actionDocument.statusText}` : ''}
						</Text>
						<Button
							onPress={() => {
								onOpenPdf(actionDocument);
								setActionDocument(null);
							}}
						>
							PDF
						</Button>
						{actionDocument.status === 'failed' ? (
							<Button
								onPress={() => {
									onRetryDocument(actionDocument);
									setActionDocument(null);
								}}
								disabled={isMutating}
							>
								Retry
							</Button>
						) : null}
						{actionDocument.status === 'completed' ? (
							<Button
								onPress={() => {
									onReprocessDocument(actionDocument);
									setActionDocument(null);
								}}
								disabled={isMutating}
							>
								Reprocess
							</Button>
						) : null}
						<Button
							onPress={() => {
								onDeleteDocument(actionDocument);
								setActionDocument(null);
							}}
							disabled={isMutating}
						>
							Delete
						</Button>
					</View>
				) : null}
			</BottomSheet>
		</>
	);
}

function MeasurementRow({
	row,
	isStarred,
	onToggleStar,
	onPress,
	styles,
}: {
	row: LabMeasurementRow;
	isStarred: boolean;
	onToggleStar: () => void;
	onPress: () => void;
	styles: ReturnType<typeof labStyles>;
}) {
	const latest = row.values[0] ?? row.latest;

	return (
		<Pressable onPress={onPress}>
			<Card full>
				<Card.Body>
					<View style={styles.measurementRow}>
						<Pressable onPress={onToggleStar} hitSlop={12}>
							<SymbolIcon
								name={isStarred ? 'star.fill' : 'star'}
								color={isStarred ? '#faad14' : '#8c8c8c'}
							/>
						</Pressable>
						<View style={styles.measurementMain}>
							<View style={styles.rowBetween}>
								<Text style={styles.measurementTitle} numberOfLines={1}>
									{row.name}
								</Text>
								{latest ? <ValueStatusTag status={latest.status} /> : null}
							</View>
							<Text style={styles.muted} numberOfLines={1}>
								{row.category}
							</Text>
							{latest ? (
								<View style={styles.rowBetween}>
									<Text style={styles.valueText}>{latest.display}</Text>
									<Text style={styles.muted}>{latest.prettyDate}</Text>
								</View>
							) : null}
							{latest?.rangeCaption ? (
								<Text style={styles.muted}>{latest.rangeCaption}</Text>
							) : null}
						</View>
					</View>
				</Card.Body>
			</Card>
		</Pressable>
	);
}

function MeasurementModal({
	row,
	onClose,
	styles,
}: {
	row: LabMeasurementRow | null;
	onClose: () => void;
	styles: ReturnType<typeof labStyles>;
}) {
	return (
		<Modal
			visible={row !== null}
			title={row?.name ?? 'Measurement'}
			transparent
			animationType='slide-up'
			onClose={onClose}
			closable
			footer={[{ text: 'Done', onPress: onClose }]}
		>
			<ScrollView style={styles.modalScroll}>
				{row ? (
					<View style={styles.stack}>
						<Text style={styles.muted}>{row.category}</Text>
						{row.values.map(value => (
							<View key={`${value.sourceId}:${value.documentId}`} style={styles.timelineRow}>
								<View style={styles.timelineDot} />
								<View style={styles.timelineContent}>
									<View style={styles.rowBetween}>
										<Text style={styles.cardTitle}>{value.display}</Text>
										<ValueStatusTag status={value.status} />
									</View>
									<Text style={styles.muted}>{value.prettyDate}</Text>
									{value.rangeCaption ? (
										<Text style={styles.muted}>{value.rangeCaption}</Text>
									) : null}
									{value.note ? <Text style={styles.muted}>{value.note}</Text> : null}
								</View>
							</View>
						))}
					</View>
				) : null}
			</ScrollView>
		</Modal>
	);
}

function DocumentPreviewModal({
	document,
	dashboard,
	showFlaggedOnly,
	onShowFlaggedOnlyChange,
	onClose,
	onOpenPdf,
	styles,
}: {
	document: LabsImportDocument | null;
	dashboard: LabsDashboard | undefined;
	showFlaggedOnly: boolean;
	onShowFlaggedOnlyChange: (value: boolean) => void;
	onClose: () => void;
	onOpenPdf: (document: LabsImportDocument) => void;
	styles: ReturnType<typeof labStyles>;
}) {
	const previewRows = useMemo(
		() => (document && dashboard ? getPreviewRows({ documentId: document.id, dashboard }) : []),
		[dashboard, document],
	);
	const flaggedCount = previewRows.filter(row => row.hasIssue).length;
	const visibleRows = showFlaggedOnly ? previewRows.filter(row => row.hasIssue) : previewRows;

	return (
		<Modal
			visible={document !== null}
			title={document?.fileName ?? 'Document'}
			transparent
			animationType='slide-up'
			onClose={onClose}
			closable
			footer={[
				{ text: 'PDF', onPress: () => (document ? onOpenPdf(document) : undefined) },
				{ text: 'Done', onPress: onClose },
			]}
		>
			<ScrollView style={styles.modalScroll}>
				<View style={styles.stack}>
					<View style={styles.rowBetween}>
						<Text style={styles.cardTitle}>Parsed Values ({visibleRows.length})</Text>
						<View style={styles.inline}>
							<Checkbox
								checked={showFlaggedOnly}
								onChange={() => onShowFlaggedOnlyChange(!showFlaggedOnly)}
							/>
							<Text style={styles.body}>Flagged ({flaggedCount})</Text>
						</View>
					</View>
					{visibleRows.map(row => (
						<PreviewValueRow key={row.id} row={row} styles={styles} />
					))}
				</View>
			</ScrollView>
		</Modal>
	);
}

function PreviewValueRow({
	row,
	styles,
}: {
	row: PreviewRow;
	styles: ReturnType<typeof labStyles>;
}) {
	return (
		<View style={styles.previewRow}>
			<View style={styles.rowBetween}>
				<View style={styles.previewName}>
					<Text style={styles.cardTitle} numberOfLines={2}>
						{row.name}
					</Text>
					{row.issueLabel ? (
						<Text style={styles.warningText} numberOfLines={1}>
							{row.issueLabel}
						</Text>
					) : null}
				</View>
				<View style={styles.previewValue}>
					<Text style={styles.valueText}>{row.valueText}</Text>
					{row.rangeText ? <Text style={styles.muted}>ref {row.rangeText}</Text> : null}
				</View>
			</View>
			{row.note ? <Text style={styles.muted}>{row.note}</Text> : null}
		</View>
	);
}

function HorizontalSection({ title, children }: { title: string; children: React.ReactNode }) {
	return (
		<View style={{ gap: 8 }}>
			<Text
				style={{ color: '#71717a', fontSize: 13, fontWeight: '700', textTransform: 'uppercase' }}
			>
				{title}
			</Text>
			<ScrollView
				horizontal
				showsHorizontalScrollIndicator={false}
				contentContainerStyle={{ gap: 10 }}
			>
				{children}
			</ScrollView>
		</View>
	);
}

function SegmentButton({
	active,
	children,
	onPress,
}: {
	active: boolean;
	children: string;
	onPress: () => void;
}) {
	return (
		<Pressable
			onPress={onPress}
			style={{
				backgroundColor: active ? '#1677ff' : 'transparent',
				borderRadius: 7,
				flex: 1,
				paddingVertical: 8,
			}}
		>
			<Text
				style={{
					color: active ? '#fff' : '#71717a',
					fontSize: 14,
					fontWeight: '700',
					textAlign: 'center',
				}}
			>
				{children}
			</Text>
		</Pressable>
	);
}

function Tally({ color, count }: { color: string; count: number }) {
	if (count === 0) return null;
	return (
		<View style={{ alignItems: 'center', flexDirection: 'row', gap: 4 }}>
			<View style={{ backgroundColor: color, borderRadius: 999, height: 9, width: 28 }} />
			<Text style={{ color: '#71717a', fontSize: 12 }}>{count}</Text>
		</View>
	);
}

function StatusTag({ status }: { status: string }) {
	const color =
		status === 'completed' || status === 'improved'
			? '#52c41a'
			: status === 'failed' || status === 'worsened'
				? '#ff4d4f'
				: status === 'processing'
					? '#1677ff'
					: '#8c8c8c';
	return (
		<Tag small selected styles={{ wrap: { borderColor: color }, text: { color } }}>
			{status}
		</Tag>
	);
}

function ValueStatusTag({ status }: { status: 'in-range' | 'out-of-range' | 'unclassified' }) {
	if (status === 'in-range') return <StatusTag status='in range' />;
	if (status === 'out-of-range') return <StatusTag status='out' />;
	return <StatusTag status='no ref' />;
}

function SymbolIcon({ name, color }: { name: SymbolViewProps['name']; color: string }) {
	return (
		<SymbolView
			name={name}
			size={20}
			tintColor={color}
			weight='semibold'
			fallback={<Text style={{ color, fontSize: 16 }}>{name === 'star.fill' ? '*' : '+'}</Text>}
		/>
	);
}

function groupRows(rows: LabMeasurementRow[], shouldGroup: boolean) {
	if (!shouldGroup) return [{ category: 'Measurements', rows }];
	const groups = new Map<string, LabMeasurementRow[]>();
	for (const row of rows) {
		const existing = groups.get(row.category);
		if (existing) {
			existing.push(row);
		} else {
			groups.set(row.category, [row]);
		}
	}
	return Array.from(groups.entries()).map(([category, groupRows]) => ({
		category,
		rows: groupRows,
	}));
}

function labStyles(isDark: boolean) {
	const text = isDark ? '#f9fafb' : '#111827';
	const muted = isDark ? '#a1a1aa' : '#71717a';
	const bg = isDark ? '#0f172a' : '#f6f7f9';
	const border = isDark ? '#27272a' : '#e5e7eb';

	return {
		loadingScreen: {
			alignItems: 'center' as const,
			backgroundColor: bg,
			flex: 1,
			justifyContent: 'center' as const,
		},
		stack: {
			gap: 12,
		},
		segment: {
			backgroundColor: isDark ? '#1f2937' : '#e5e7eb',
			borderRadius: 8,
			flexDirection: 'row' as const,
			gap: 4,
			padding: 4,
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
		body: {
			color: text,
			fontSize: 14,
		},
		muted: {
			color: muted,
			fontSize: 12,
		},
		errorText: {
			color: '#cf1322',
			fontSize: 13,
		},
		sectionLabel: {
			color: muted,
			fontSize: 13,
			fontWeight: '700' as const,
			textTransform: 'uppercase' as const,
		},
		categoryPill: {
			backgroundColor: isDark ? '#111827' : '#fff',
			borderColor: border,
			borderRadius: 8,
			borderWidth: 1,
			gap: 8,
			padding: 10,
			width: 210,
		},
		categoryTitle: {
			color: text,
			fontSize: 13,
			fontWeight: '700' as const,
		},
		tallyRow: {
			flexDirection: 'row' as const,
			gap: 10,
		},
		changeCard: {
			width: 260,
		},
		cardTitle: {
			color: text,
			fontSize: 14,
			fontWeight: '700' as const,
		},
		changeLine: {
			color: text,
			fontSize: 14,
			fontWeight: '700' as const,
			marginTop: 6,
		},
		sourceChip: {
			backgroundColor: isDark ? '#111827' : '#fff',
			borderColor: border,
			borderRadius: 999,
			borderWidth: 1,
			paddingHorizontal: 12,
			paddingVertical: 8,
		},
		sourceChipActive: {
			backgroundColor: '#1677ff',
			borderColor: '#1677ff',
		},
		sourceChipText: {
			color: muted,
			fontSize: 13,
			fontWeight: '600' as const,
		},
		sourceChipActiveText: {
			color: '#fff',
			fontSize: 13,
			fontWeight: '700' as const,
		},
		searchInput: {
			backgroundColor: isDark ? '#111827' : '#fff',
			borderColor: border,
			borderRadius: 8,
			borderWidth: 1,
			color: text,
			fontSize: 16,
			paddingHorizontal: 12,
			paddingVertical: 10,
		},
		searchPlaceholder: {
			color: muted,
		},
		measurementRow: {
			alignItems: 'flex-start' as const,
			flexDirection: 'row' as const,
			gap: 12,
		},
		measurementMain: {
			flex: 1,
			gap: 4,
			minWidth: 0,
		},
		measurementTitle: {
			color: text,
			flex: 1,
			fontSize: 16,
			fontWeight: '700' as const,
		},
		valueText: {
			color: text,
			fontSize: 15,
			fontWeight: '700' as const,
		},
		selectionBar: {
			alignItems: 'center' as const,
			backgroundColor: isDark ? '#111827' : '#fff',
			borderColor: border,
			borderRadius: 14,
			borderWidth: 1,
			flexDirection: 'row' as const,
			flexWrap: 'wrap' as const,
			gap: 8,
			padding: 10,
		},
		documentCard: {
			backgroundColor: isDark ? '#111827' : '#fff',
			borderColor: border,
			borderRadius: 14,
			borderWidth: 1,
			padding: 12,
		},
		documentCardRow: {
			alignItems: 'flex-start' as const,
			flexDirection: 'row' as const,
			gap: 10,
		},
		documentMain: {
			flex: 1,
			gap: 6,
			minWidth: 0,
		},
		documentTitle: {
			color: text,
			fontSize: 15,
			fontWeight: '700' as const,
		},
		documentButtonRow: {
			flexDirection: 'row' as const,
			flexWrap: 'wrap' as const,
			gap: 8,
			justifyContent: 'flex-end' as const,
			marginTop: 10,
		},
		modalScroll: {
			maxHeight: 460,
		},
		timelineRow: {
			flexDirection: 'row' as const,
			gap: 10,
		},
		timelineDot: {
			backgroundColor: '#1677ff',
			borderRadius: 999,
			height: 9,
			marginTop: 6,
			width: 9,
		},
		timelineContent: {
			borderBottomColor: border,
			borderBottomWidth: 1,
			flex: 1,
			gap: 3,
			paddingBottom: 10,
		},
		previewRow: {
			borderBottomColor: border,
			borderBottomWidth: 1,
			gap: 4,
			paddingVertical: 10,
		},
		previewName: {
			flex: 1,
			gap: 3,
			minWidth: 0,
		},
		previewValue: {
			alignItems: 'flex-end' as const,
			maxWidth: 140,
		},
		warningText: {
			color: '#d48806',
			fontSize: 12,
			fontWeight: '700' as const,
		},
	};
}
