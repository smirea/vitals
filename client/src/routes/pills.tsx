import {
    ArrowDownOutlined,
    ArrowUpOutlined,
    DeleteOutlined,
    EditOutlined,
    PlusOutlined,
    TagOutlined,
    UploadOutlined,
} from '@ant-design/icons'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { createFileRoute } from '@tanstack/react-router'
import {
    AutoComplete,
    Badge,
    Button,
    Card,
    DatePicker,
    Divider,
    Drawer,
    Empty,
    Form,
    Image,
    Input,
    Popover,
    Select,
    Space,
    Spin,
    Tag,
    Table,
    Typography,
    Upload,
    message,
    theme as antdTheme,
} from 'antd'
import type { FormInstance, TableColumnsType } from 'antd'
import type { UploadChangeParam, UploadFile, UploadProps } from 'antd/es/upload/interface'
import { formatDistanceToNow, parseISO } from 'date-fns'
import dayjs from 'dayjs'
import type { Key } from 'react'
import { useDeferredValue, useEffect, useMemo, useState } from 'react'
import { z } from 'zod'

import type {
    PillComponent,
    PillExtractionResult,
    PillImage,
    PillPeriod,
    PillRecord,
} from '../utils/api'
import { PageNav } from '../components/PageNav'
import { useTRPC } from '../utils/trpc'

const pillsSearchSchema = z.object({
    edit: z.coerce.number().int().positive().optional(),
})

const DATE_FORMAT = 'YYYY-MM-DD'
const IMAGE_TILE_SIZE = 104
const { Dragger } = Upload

export const Route = createFileRoute('/pills')({
    validateSearch: search => pillsSearchSchema.parse(search),
    component: PillsRouteComponent,
})

type PillTiming = 'morning' | 'afternoon' | 'evening' | 'random'

type PillImageFormValue = {
    id?: number
    uid: string
    fileName: string
    dataUrl: string
}

type PillComponentFormValue = {
    name: string
    value: string
    unit: string
}

type PillPeriodFormValue = {
    id?: number
    startDate?: string
    endDate?: string
    valueOverride?: string
    unitOverride?: string
    timing?: PillTiming
    tagNames?: string[]
}

type PillFormValues = {
    id?: number
    name: string
    value: string
    unit: string
    url: string
    note: string
    images: PillImageFormValue[]
    components: PillComponentFormValue[]
    periods: PillPeriodFormValue[]
}

type ActivePillRow = {
    pill: PillRecord
    activePeriod: PillPeriod
}

type PastPillRow = {
    key: string
    pill: PillRecord
    period: PillPeriod
}

type NotTrackedPillRow = {
    pill: PillRecord
}

const timingOptions = [
    { label: 'Morning', value: 'morning' },
    { label: 'Afternoon', value: 'afternoon' },
    { label: 'Evening', value: 'evening' },
    { label: 'Random', value: 'random' },
] as const

function PillsRouteComponent() {
    const search = Route.useSearch()
    const navigate = Route.useNavigate()
    const editPillId = search.edit ?? null
    const onEditPillChange = (nextEditPillId: number | null) => navigate({
        search: nextEditPillId ? { edit: nextEditPillId } : {},
    })

    const { token } = antdTheme.useToken()
    const trpc = useTRPC()
    const queryClient = useQueryClient()
    const [form] = Form.useForm<PillFormValues>()
    const [isCreateDrawerOpen, setIsCreateDrawerOpen] = useState(false)
    const [pillQuery, setPillQuery] = useState('')
    const [hydratedEditPillId, setHydratedEditPillId] = useState<number | null>(null)
    const [deletingPeriodId, setDeletingPeriodId] = useState<number | null>(null)
    const [isSaveDisabled, setIsSaveDisabled] = useState(true)
    const [expandedComponentRowKeys, setExpandedComponentRowKeys] = useState<string[]>([])
    const [openPeriodTagEditorKey, setOpenPeriodTagEditorKey] = useState<string | null>(null)
    const deferredPillQuery = useDeferredValue(pillQuery)
    const watchedFormValues = Form.useWatch([], form) as PillFormValues | undefined
    const watchedImages = (Form.useWatch('images', form) ?? []) as PillImageFormValue[]
    const watchedDefaultValue = Form.useWatch('value', form) ?? ''
    const watchedDefaultUnit = Form.useWatch('unit', form) ?? ''
    const watchedPeriods = (Form.useWatch('periods', form) ?? []) as PillPeriodFormValue[]
    const isEditMode = editPillId !== null
    const isDrawerRequestedOpen = isCreateDrawerOpen || isEditMode

    const dashboardQuery = useQuery(trpc.pills.getDashboard.queryOptions())
    const tagsQuery = useQuery(trpc.tags.list.queryOptions())
    const searchQuery = useQuery({
        ...trpc.pills.search.queryOptions({
            query: deferredPillQuery,
            limit: 8,
        }),
        enabled: isDrawerRequestedOpen,
    })

    const extractionMutation = useMutation({
        ...trpc.pills.extractFromImages.mutationOptions(),
        onSuccess: extraction => {
            if (!extraction.detected) {
                message.info(
                    extraction.extractionNotes ??
                    'No pill or supplement label was confidently detected in the uploaded images.',
                )
                return
            }

            form.setFieldsValue(extractionToFormPatch(extraction))
            message.success(`Filled pill details from images using ${extraction.model}.`)
        },
        onError: error => {
            message.error(error.message)
        },
    })
    const isParsingImages = extractionMutation.isPending

    const upsertMutation = useMutation({
        ...trpc.pills.upsert.mutationOptions(),
        onSuccess: () => {
            void queryClient.invalidateQueries()
            setIsCreateDrawerOpen(false)
            setOpenPeriodTagEditorKey(null)
            onEditPillChange(null)
            message.success('Pill saved.')
        },
        onError: error => {
            message.error(error.message)
        },
    })

    const deletePeriodMutation = useMutation({
        ...trpc.table.pillPeriods.deleteMany.mutationOptions(),
        onSuccess: async () => {
            await queryClient.invalidateQueries()
        },
        onError: error => {
            message.error(error.message)
        },
        onSettled: () => {
            setDeletingPeriodId(null)
        },
    })

    const dashboard = dashboardQuery.data
    const availableTags = tagsQuery.data ?? []
    const searchResults = (searchQuery.data ?? []) as PillRecord[]

    const autocompleteOptions = useMemo(
        () => searchResults.map(result => ({
            label: result.name,
            value: result.name,
            pill: result,
        })),
        [searchResults],
    )
    const tagAutocompleteOptions = useMemo(
        () => availableTags.map(tag => ({
            label: tag.name,
            value: tag.name,
        })),
        [availableTags],
    )

    const allPills = dashboard?.pills ?? []
    const activePills = dashboard?.activePills ?? []
    const pastPills = dashboard?.pastPills ?? []
    const editedPill = useMemo(
        () => allPills.find(pill => pill.id === editPillId) ?? null,
        [allPills, editPillId],
    )
    const isDrawerOpen =
        isCreateDrawerOpen || (editPillId !== null && (dashboardQuery.isLoading || editedPill !== null))
    const activeRows = useMemo<ActivePillRow[]>(
        () => activePills
            .map(pill => ({
                pill,
                activePeriod: getActivePeriod(pill),
            }))
            .filter((row): row is ActivePillRow => row.activePeriod !== null),
        [activePills],
    )
    const pastRows = useMemo<PastPillRow[]>(
        () => pastPills
            .flatMap(pill => pill.periods
                .filter(period => period.endDate)
                .map(period => ({
                    key: `${pill.id}-${period.id}`,
                    pill,
                    period,
                })))
            .sort((left, right) => right.period.startDate.localeCompare(left.period.startDate)),
        [pastPills],
    )
    const notTrackedRows = useMemo<NotTrackedPillRow[]>(
        () => allPills
            .filter(pill => pill.periods.length === 0)
            .map(pill => ({ pill })),
        [allPills],
    )

    useEffect(() => {
        let isCancelled = false

        const syncSaveDisabledState = async () => {
            const hasErrors = await hasFormErrors(form)
            if (!isCancelled) {
                setIsSaveDisabled(hasErrors)
            }
        }

        void syncSaveDisabledState()

        return () => {
            isCancelled = true
        }
    }, [form, watchedFormValues])

    useEffect(() => {
        if (watchedPeriods.length === 0) {
            return
        }

        const latestDefaults = getNewPeriodDefaults({
            canonicalValue: watchedDefaultValue,
            canonicalUnit: watchedDefaultUnit,
            periods: watchedPeriods,
        })

        const nextPeriods = watchedPeriods.map(period => ({
            ...period,
            valueOverride: period.valueOverride?.trim() ? period.valueOverride : latestDefaults.value,
            unitOverride: period.unitOverride?.trim() ? period.unitOverride : latestDefaults.unit,
            timing: period.timing ?? latestDefaults.timing,
        }))

        const hasChanged = nextPeriods.some(
            (period, index) =>
                period.valueOverride !== watchedPeriods[index]?.valueOverride ||
                period.unitOverride !== watchedPeriods[index]?.unitOverride ||
                period.timing !== watchedPeriods[index]?.timing,
        )

        if (hasChanged) {
            form.setFieldValue('periods', nextPeriods)
        }
    }, [form, watchedDefaultUnit, watchedDefaultValue, watchedPeriods])

    useEffect(() => {
        if (!isDrawerOpen) {
            return
        }

        const onPaste = (event: ClipboardEvent) => {
            const pastedText = event.clipboardData?.getData('text')?.trim() ?? ''
            if (isValidPastedUrl(pastedText)) {
                event.preventDefault()
                form.setFieldValue('url', pastedText)
                return
            }

            const pastedFiles = getPastedImageFiles(event)
            if (pastedFiles.length === 0) {
                return
            }

            event.preventDefault()

            void (async () => {
                try {
                    const nextImages = await filesToFormImages(pastedFiles)
                    const currentImages = (form.getFieldValue('images') ?? []) as PillImageFormValue[]
                    form.setFieldValue('images', [...currentImages, ...nextImages])
                    message.success(
                        pastedFiles.length === 1
                            ? 'Pasted 1 image.'
                            : `Pasted ${pastedFiles.length} images.`,
                    )
                } catch (error) {
                    message.error(error instanceof Error ? error.message : 'Unable to process pasted images.')
                }
            })()
        }

        window.addEventListener('paste', onPaste)

        return () => {
            window.removeEventListener('paste', onPaste)
        }
    }, [form, isDrawerOpen])

    function resetPillForm() {
        form.resetFields()
        form.setFieldsValue({
            ...createEmptyFormValues(),
            id: undefined,
        })
    }

    useEffect(() => {
        if (editPillId === null) {
            setHydratedEditPillId(null)
            return
        }

        if (dashboardQuery.isLoading) {
            return
        }

        if (hydratedEditPillId === editPillId) {
            return
        }

        if (!editedPill) {
            setPillQuery('')
            setHydratedEditPillId(null)
            onEditPillChange(null)
            resetPillForm()
            return
        }

        setPillQuery(editedPill.name)
        form.setFieldsValue(pillToFormValues(editedPill))
        setHydratedEditPillId(editPillId)
    }, [dashboardQuery.isLoading, editPillId, editedPill, form, hydratedEditPillId, onEditPillChange])

    function openNewPillDrawer() {
        setIsCreateDrawerOpen(true)
        setHydratedEditPillId(null)
        setPillQuery('')
        setOpenPeriodTagEditorKey(null)
        onEditPillChange(null)
        resetPillForm()
    }

    function openExistingPillDrawer(pill: PillRecord) {
        setIsCreateDrawerOpen(false)
        setHydratedEditPillId(null)
        setOpenPeriodTagEditorKey(null)
        onEditPillChange(pill.id)
    }

    function handleCloseDrawer() {
        setIsCreateDrawerOpen(false)
        setOpenPeriodTagEditorKey(null)
        onEditPillChange(null)
    }

    function toggleExpandedComponentsRow(rowKey: string) {
        setExpandedComponentRowKeys(currentKeys =>
            currentKeys.includes(rowKey)
                ? currentKeys.filter(currentKey => currentKey !== rowKey)
                : [...currentKeys, rowKey],
        )
    }

    function handleAutocompleteSelect(_: string, option: { pill?: PillRecord }) {
        if (!option.pill) {
            return
        }

        form.setFieldsValue(
            pillToFormValues(option.pill, {
                appendNewPeriod: true,
            }),
        )
    }

    async function syncImagesFromUpload(event: UploadChangeParam<UploadFile<any>>) {
        try {
            const nextImages = await Promise.all(
                event.fileList.map(async file => {
                    if (typeof file.url === 'string' && !file.originFileObj) {
                        return {
                            uid: file.uid,
                            fileName: file.name,
                            dataUrl: file.url,
                        } satisfies PillImageFormValue
                    }

                    const originalFile = file.originFileObj
                    if (!originalFile) {
                        throw new Error(`Unable to process ${file.name}.`)
                    }

                    return {
                        uid: file.uid,
                        fileName: originalFile.name,
                        dataUrl: await readFileAsDataUrl(originalFile),
                        id:
                            typeof file.uid === 'string' && file.uid.startsWith('saved-')
                                ? Number(file.uid.replace('saved-', '')) || undefined
                                : undefined,
                    } satisfies PillImageFormValue
                }),
            )

            form.setFieldValue('images', nextImages)
        } catch (error) {
            message.error(error instanceof Error ? error.message : 'Unable to process uploaded images.')
        }
    }

    async function handleParseImages() {
        if (watchedImages.length === 0) {
            return
        }

        await extractionMutation.mutateAsync({
            images: getImagePayload(watchedImages),
        })
    }

    const uploadProps: UploadProps = {
        accept: 'image/*',
        beforeUpload: () => false,
        disabled: isParsingImages,
        multiple: true,
        fileList: buildUploadFileList(watchedImages),
        showUploadList: false,
        onChange: info => {
            void syncImagesFromUpload(info)
        },
        onRemove: file => {
            form.setFieldValue('images', removeImageByUid(watchedImages, file.uid))
            return true
        },
    }

    async function handleSubmit(values: PillFormValues) {
        const submittedPeriods = ((form.getFieldValue('periods') ?? values.periods) as PillPeriodFormValue[])
            .map(period => ({
                ...period,
                tagNames: normalizeTagNames(period.tagNames ?? []),
            }))

        await upsertMutation.mutateAsync({
            id: values.id,
            name: values.name,
            value: values.value,
            unit: values.unit,
            url: values.url,
            note: values.note,
            images: getImagePayload(values.images),
            components: values.components,
            periods: submittedPeriods,
        })
    }

    async function handleDeleteSavedPeriod(
        periodId: number,
        fieldIndex: number,
        remove: (index: number | number[]) => void,
    ) {
        setOpenPeriodTagEditorKey(null)
        setDeletingPeriodId(periodId)

        try {
            const result = await deletePeriodMutation.mutateAsync({
                where: [
                    {
                        column: 'id',
                        operator: 'eq',
                        value: periodId,
                    },
                ],
            })

            remove(fieldIndex)

            const remainingPeriods = (form.getFieldValue('periods') ?? []) as PillPeriodFormValue[]
            if (remainingPeriods.length === 0 && !isEditMode) {
                form.setFieldValue('periods', [
                    createBlankPeriod(
                        getNewPeriodDefaults({
                            canonicalValue: form.getFieldValue('value') ?? '',
                            canonicalUnit: form.getFieldValue('unit') ?? '',
                            periods: [],
                        }),
                    ),
                ])
            }

            message.success(
                result.deletedCount === 1
                    ? 'Date range deleted.'
                    : `${result.deletedCount} date ranges deleted.`,
            )
        } catch {}
    }

    function handlePeriodTagNamesChange(fieldIndex: number, values: string[]) {
        form.setFieldValue(['periods', fieldIndex, 'tagNames'], normalizeTagNames(values))
    }

    function renderPeriodTagEditor(fieldIndex: number) {
        const rowValue = watchedPeriods[fieldIndex]
        const currentTagNames = rowValue?.tagNames ?? []
        const editorKey = getPeriodTagEditorKey(fieldIndex, rowValue)

        return (
            <Popover
                trigger='click'
                open={openPeriodTagEditorKey === editorKey}
                onOpenChange={open => {
                    setOpenPeriodTagEditorKey(open ? editorKey : null)
                }}
                placement='leftTop'
                destroyOnHidden
                content={(
                    <Space direction='vertical' size={8} style={{ width: 280 }}>
                        <Typography.Text strong>Tags</Typography.Text>
                        <Select
                            mode='tags'
                            value={currentTagNames}
                            options={tagAutocompleteOptions}
                            placeholder='Type to attach or create tags'
                            style={{ width: '100%' }}
                            getPopupContainer={trigger => trigger.parentElement ?? document.body}
                            onChange={values => {
                                handlePeriodTagNamesChange(fieldIndex, values)
                            }}
                            tokenSeparators={[',']}
                        />
                        <div>
                            {currentTagNames.length > 0 ? (
                                <Space size={[4, 4]} wrap>
                                    {currentTagNames.map(tagName => {
                                        const tagRecord = availableTags.find(tag => tag.name === tagName)

                                        return (
                                            <Tag key={tagName} color={tagRecord?.color}>
                                                {tagName}
                                            </Tag>
                                        )
                                    })}
                                </Space>
                            ) : (
                                <Typography.Text type='secondary'>
                                    No tags attached to this date range yet.
                                </Typography.Text>
                            )}
                        </div>
                    </Space>
                )}
            >
                <Badge count={currentTagNames.length} size='small' offset={[-2, 2]}>
                    <Button
                        size='small'
                        icon={<TagOutlined />}
                        aria-label='Edit date range tags'
                        title={formatPeriodTagButtonTitle(currentTagNames)}
                    />
                </Badge>
            </Popover>
        )
    }

    function renderPillNameCell(pill: PillRecord, tagNames: string[]) {
        return (
            <Space direction='vertical' size={4}>
                <Button
                    type='link'
                    size='small'
                    className='pills-link-button'
                    icon={<EditOutlined />}
                    onClick={() => openExistingPillDrawer(pill)}
                >
                    {pill.name}
                </Button>

                {tagNames.length > 0 ? (
                    <Space size={[4, 4]} wrap>
                        {tagNames.map(tagName => {
                            const tagRecord = availableTags.find(tag => tag.name === tagName)

                            return (
                                <Tag key={`${pill.id}-${tagName}`} color={tagRecord?.color}>
                                    {tagName}
                                </Tag>
                            )
                        })}
                    </Space>
                ) : null}
            </Space>
        )
    }

    const activeColumns: TableColumnsType<ActivePillRow> = [
        {
            title: 'Pill',
            key: 'name',
            render: (_: unknown, row: ActivePillRow) => renderPillNameCell(
                row.pill,
                row.activePeriod.tags.map(tag => tag.name),
            ),
        },
        {
            title: 'Amount',
            key: 'amount',
            render: (_: unknown, row: ActivePillRow) => (
                <Typography.Text>
                    {formatServing(
                        row.activePeriod.valueOverride ?? row.pill.value,
                        row.activePeriod.unitOverride ?? row.pill.unit,
                    )}
                </Typography.Text>
            ),
        },
        {
            title: 'Timing',
            key: 'timing',
            render: (_: unknown, row: ActivePillRow) => (
                <Typography.Text>{formatTiming(row.activePeriod.timing)}</Typography.Text>
            ),
        },
        {
            title: 'Started',
            key: 'started',
            render: (_: unknown, row: ActivePillRow) => (
                <Typography.Text>{formatRelativeDate(row.activePeriod.startDate)}</Typography.Text>
            ),
        },
        {
            title: 'Components',
            key: 'components',
            render: (_: unknown, row: ActivePillRow) => renderComponents(row.pill.components),
            onCell: row =>
                row.pill.components.length > 0
                    ? {
                        onClick: () => {
                            toggleExpandedComponentsRow(String(row.pill.id))
                        },
                        style: { cursor: 'pointer' },
                    }
                    : {},
        },
        {
            title: 'Images',
            key: 'images',
            render: (_: unknown, row: ActivePillRow) => renderImages(row.pill.images),
        },
    ]

    const pastColumns: TableColumnsType<PastPillRow> = [
        {
            title: 'Pill',
            key: 'name',
            render: (_: unknown, row: PastPillRow) => renderPillNameCell(
                row.pill,
                row.period.tags.map(tag => tag.name),
            ),
        },
        {
            title: 'Amount',
            key: 'amount',
            render: (_: unknown, row: PastPillRow) => (
                <Typography.Text>
                    {formatServing(row.period.valueOverride ?? row.pill.value, row.period.unitOverride ?? row.pill.unit)}
                </Typography.Text>
            ),
        },
        {
            title: 'Timing',
            key: 'timing',
            render: (_: unknown, row: PastPillRow) => (
                <Typography.Text>{formatTiming(row.period.timing)}</Typography.Text>
            ),
        },
        {
            title: 'Period',
            key: 'period',
            render: (_: unknown, row: PastPillRow) => (
                <Typography.Text>{formatPeriodRange(row.period)}</Typography.Text>
            ),
        },
        {
            title: 'Components',
            key: 'components',
            render: (_: unknown, row: PastPillRow) => renderComponents(row.pill.components),
            onCell: row =>
                row.pill.components.length > 0
                    ? {
                        onClick: () => {
                            toggleExpandedComponentsRow(row.key)
                        },
                        style: { cursor: 'pointer' },
                    }
                    : {},
        },
        {
            title: 'Images',
            key: 'images',
            render: (_: unknown, row: PastPillRow) => renderImages(row.pill.images),
        },
    ]

    const notTrackedColumns: TableColumnsType<NotTrackedPillRow> = [
        {
            title: 'Pill',
            key: 'name',
            render: (_: unknown, row: NotTrackedPillRow) => renderPillNameCell(row.pill, []),
        },
        {
            title: 'Amount',
            key: 'amount',
            render: (_: unknown, row: NotTrackedPillRow) => (
                <Typography.Text>{formatServing(row.pill.value, row.pill.unit)}</Typography.Text>
            ),
        },
        {
            title: 'Timing',
            key: 'timing',
            render: () => <Typography.Text />,
        },
        {
            title: 'Components',
            key: 'components',
            render: (_: unknown, row: NotTrackedPillRow) => renderComponents(row.pill.components),
            onCell: row =>
                row.pill.components.length > 0
                    ? {
                        onClick: () => {
                            toggleExpandedComponentsRow(String(row.pill.id))
                        },
                        style: { cursor: 'pointer' },
                    }
                    : {},
        },
        {
            title: 'Images',
            key: 'images',
            render: (_: unknown, row: NotTrackedPillRow) => renderImages(row.pill.images),
        },
    ]

    const activeTableExpandable = {
        expandedRowKeys: expandedComponentRowKeys,
        expandedRowRender: (row: ActivePillRow) => renderExpandedComponents(row.pill.components),
        onExpandedRowsChange: (keys: readonly Key[]) => {
            setExpandedComponentRowKeys(keys.map(key => String(key)))
        },
        rowExpandable: (row: ActivePillRow) => row.pill.components.length > 0,
        showExpandColumn: false,
    }

    const pastTableExpandable = {
        expandedRowKeys: expandedComponentRowKeys,
        expandedRowRender: (row: PastPillRow) => renderExpandedComponents(row.pill.components),
        onExpandedRowsChange: (keys: readonly Key[]) => {
            setExpandedComponentRowKeys(keys.map(key => String(key)))
        },
        rowExpandable: (row: PastPillRow) => row.pill.components.length > 0,
        showExpandColumn: false,
    }

    const notTrackedTableExpandable = {
        expandedRowKeys: expandedComponentRowKeys,
        expandedRowRender: (row: NotTrackedPillRow) => renderExpandedComponents(row.pill.components),
        onExpandedRowsChange: (keys: readonly Key[]) => {
            setExpandedComponentRowKeys(keys.map(key => String(key)))
        },
        rowExpandable: (row: NotTrackedPillRow) => row.pill.components.length > 0,
        showExpandColumn: false,
    }

    return (
        <main
            className='pills-page'
            style={{ background: token.colorBgLayout }}
        >
            <PageNav
                title='Pills'
                actions={(
                    <Button type='primary' size='large' icon={<PlusOutlined />} onClick={openNewPillDrawer}>
                        Log pill
                    </Button>
                )}
            />

            <div className='pills-page-inner'>
                <Card
                    title='Active pills'
                    extra={<Typography.Text type='secondary'>{activePills.length} rows</Typography.Text>}
                >
                    <Table
                        rowKey={row => String(row.pill.id)}
                        size='small'
                        columns={activeColumns}
                        dataSource={activeRows}
                        loading={dashboardQuery.isLoading}
                        pagination={false}
                        expandable={activeTableExpandable}
                        locale={{
                            emptyText: (
                                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description='No active pills yet' />
                            ),
                        }}
                    />
                </Card>

                <Card
                    title='Not tracked yet'
                    extra={<Typography.Text type='secondary'>{notTrackedRows.length} rows</Typography.Text>}
                >
                    <Table
                        rowKey={row => String(row.pill.id)}
                        size='small'
                        columns={notTrackedColumns}
                        dataSource={notTrackedRows}
                        loading={dashboardQuery.isLoading}
                        pagination={false}
                        expandable={notTrackedTableExpandable}
                        locale={{
                            emptyText: (
                                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description='No untracked pills yet' />
                            ),
                        }}
                    />
                </Card>

                <Card
                    title='Past pills'
                    extra={<Typography.Text type='secondary'>{pastRows.length} rows</Typography.Text>}
                >
                    <Table
                        rowKey='key'
                        size='small'
                        columns={pastColumns}
                        dataSource={pastRows}
                        loading={dashboardQuery.isLoading}
                        pagination={false}
                        expandable={pastTableExpandable}
                        locale={{
                            emptyText: (
                                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description='No past pills yet' />
                            ),
                        }}
                    />
                </Card>
            </div>

            <Drawer
                title={isEditMode ? 'Edit pill' : 'Log pill'}
                placement='right'
                width={920}
                open={isDrawerOpen}
                onClose={handleCloseDrawer}
                afterOpenChange={open => {
                    if (open) {
                        return
                    }

                    setPillQuery('')
                    setHydratedEditPillId(null)
                    setOpenPeriodTagEditorKey(null)
                    resetPillForm()
                }}
                destroyOnHidden={false}
                styles={{
                    body: {
                        padding: 16,
                    },
                }}
                extra={(
                    <Space>
                        <Button onClick={handleCloseDrawer}>Cancel</Button>
                        <Button
                            type='primary'
                            loading={upsertMutation.isPending}
                            disabled={isSaveDisabled}
                            onClick={() => void form.submit()}
                        >
                            Save
                        </Button>
                    </Space>
                )}
            >
                <Form<PillFormValues>
                    form={form}
                    layout='vertical'
                    initialValues={createEmptyFormValues()}
                    onFinish={values => {
                        void handleSubmit(values)
                    }}
                >
                    <Form.Item name='id' hidden>
                        <Input />
                    </Form.Item>

                    <div className='pills-primary-fields'>
                        <Form.Item
                            label='Pill name'
                            name='name'
                            rules={[{ required: true, message: 'Enter a pill name.' }]}
                        >
                            <AutoComplete
                                options={autocompleteOptions}
                                onSearch={value => setPillQuery(value)}
                                onSelect={handleAutocompleteSelect}
                                onChange={value => {
                                    setPillQuery(value)

                                    if (value !== form.getFieldValue('name')) {
                                        form.setFieldValue('name', value)
                                    }

                                    const currentId = form.getFieldValue('id')
                                    const selectedPill = searchResults.find(result => result.id === currentId)
                                    if (selectedPill && selectedPill.name !== value) {
                                        form.setFieldValue('id', undefined)
                                    }
                                }}
                                placeholder='Start typing to reuse a canonical pill'
                                filterOption={false}
                            />
                        </Form.Item>

                        <Form.Item
                            label='Default value'
                            name='value'
                            rules={[{ required: true, message: 'Enter the default pill value.' }]}
                        >
                            <Input placeholder='e.g. 2' />
                        </Form.Item>

                        <Form.Item
                            label='Default unit'
                            name='unit'
                            rules={[{ required: true, message: 'Enter the default pill unit.' }]}
                        >
                            <Input placeholder='e.g. capsules' />
                        </Form.Item>
                    </div>

                    <Form.Item label='URL' name='url'>
                        <Input placeholder='Optional product URL' />
                    </Form.Item>

                    <Form.Item label='Note' name='note'>
                        <Input.TextArea
                            rows={3}
                            placeholder='Optional note about the pill, brand, or reason for taking it'
                        />
                    </Form.Item>

                    <div className='pills-images-header'>
                        <Divider className='pills-images-divider'>Images</Divider>

                        {watchedImages.length > 0 ? (
                            <Button
                                size='small'
                                loading={isParsingImages}
                                disabled={isParsingImages}
                                onClick={() => {
                                    void handleParseImages()
                                }}
                            >
                                Parse images
                            </Button>
                        ) : null}
                    </div>

                    <Form.Item name='images' hidden>
                        <Input />
                    </Form.Item>

                    <Image.PreviewGroup>
                        <div className='pills-images-grid'>
                            {watchedImages.map(image => (
                                <div
                                    key={image.uid}
                                    className='pills-image-tile'
                                    style={{
                                        border: '1px solid var(--ant-color-border-secondary)',
                                        background: 'var(--ant-color-bg-container)',
                                    }}
                                >
                                    <Image
                                        src={image.dataUrl}
                                        alt={image.fileName}
                                        width={IMAGE_TILE_SIZE}
                                        height={IMAGE_TILE_SIZE}
                                        style={{ objectFit: 'cover' }}
                                    />
                                    <Button
                                        size='small'
                                        danger
                                        icon={<DeleteOutlined />}
                                        onClick={() => {
                                            form.setFieldValue('images', removeImageByUid(watchedImages, image.uid))
                                        }}
                                        className='pills-image-remove'
                                    />
                                </div>
                            ))}

                            <Dragger
                                {...uploadProps}
                                className='pills-upload-tile'
                                style={{
                                    width: IMAGE_TILE_SIZE,
                                    height: IMAGE_TILE_SIZE,
                                }}
                            >
                                <div className='pills-upload-inner'>
                                    <UploadOutlined style={{ fontSize: 20 }} />
                                    <Typography.Text
                                        className='pills-upload-label'
                                        type='secondary'
                                    >
                                        Drop or upload
                                    </Typography.Text>
                                </div>
                            </Dragger>
                        </div>
                    </Image.PreviewGroup>

                    {isParsingImages ? (
                        <div
                            className='pills-parsing-banner'
                            style={{
                                borderColor: token.colorInfoBorder,
                                background: token.colorInfoBg,
                                color: token.colorInfoText,
                            }}
                        >
                            <Spin size='small' />
                            <Typography.Text className='pills-parsing-banner-text'>
                                Parsing uploaded images and filling the form…
                            </Typography.Text>
                        </div>
                    ) : null}

                    <Divider>Date Ranges</Divider>

                    <Form.List name='periods'>
                        {(fields, { add, remove }) => (
                            <Space direction='vertical' size={8} className='pills-list-space'>
                                <Table
                                    size='small'
                                    pagination={false}
                                    rowKey='key'
                                    dataSource={fields}
                                    scroll={{ x: 860 }}
                                    columns={[
                                        {
                                            title: 'Start',
                                            width: 150,
                                            render: (_: unknown, field) => (
                                                <>
                                                    <Form.Item name={[field.name, 'id']} hidden>
                                                        <Input />
                                                    </Form.Item>
                                                    <Form.Item
                                                        name={[field.name, 'startDate']}
                                                        rules={[{ required: true, message: 'Required' }]}
                                                        style={{ marginBottom: 0 }}
                                                        getValueFromEvent={(_, dateString) =>
                                                            Array.isArray(dateString) ? dateString[0] : dateString
                                                        }
                                                        getValueProps={value => ({
                                                            value: value ? dayjs(value, DATE_FORMAT) : null,
                                                        })}
                                                    >
                                                        <DatePicker style={{ width: '100%' }} format={DATE_FORMAT} />
                                                    </Form.Item>
                                                </>
                                            ),
                                        },
                                        {
                                            title: 'End',
                                            width: 150,
                                            render: (_: unknown, field) => (
                                                <Form.Item
                                                    name={[field.name, 'endDate']}
                                                    style={{ marginBottom: 0 }}
                                                    getValueFromEvent={(_, dateString) =>
                                                        Array.isArray(dateString) ? dateString[0] : dateString
                                                    }
                                                    getValueProps={value => ({
                                                        value: value ? dayjs(value, DATE_FORMAT) : null,
                                                    })}
                                                >
                                                    <DatePicker style={{ width: '100%' }} format={DATE_FORMAT} allowClear />
                                                </Form.Item>
                                            ),
                                        },
                                        {
                                            title: 'Value',
                                            width: 110,
                                            render: (_: unknown, field) => (
                                                <Form.Item
                                                    name={[field.name, 'valueOverride']}
                                                    rules={[{ required: true, message: 'Required' }]}
                                                    style={{ marginBottom: 0 }}
                                                >
                                                    <Input placeholder='Amount' />
                                                </Form.Item>
                                            ),
                                        },
                                        {
                                            title: 'Unit',
                                            width: 120,
                                            render: (_: unknown, field) => (
                                                <Form.Item
                                                    name={[field.name, 'unitOverride']}
                                                    rules={[{ required: true, message: 'Required' }]}
                                                    style={{ marginBottom: 0 }}
                                                >
                                                    <Input placeholder='Unit' />
                                                </Form.Item>
                                            ),
                                        },
                                        {
                                            title: 'Timing',
                                            width: 140,
                                            render: (_: unknown, field) => (
                                                <Form.Item
                                                    name={[field.name, 'timing']}
                                                    rules={[{ required: true, message: 'Required' }]}
                                                    style={{ marginBottom: 0 }}
                                                >
                                                    <Select
                                                        options={timingOptions as unknown as { label: string; value: string }[]}
                                                    />
                                                </Form.Item>
                                            ),
                                        },
                                        {
                                            width: 170,
                                            render: (_: unknown, field) => {
                                                const rowValue = watchedPeriods[field.name] as
                                                    | PillPeriodFormValue
                                                    | undefined
                                                const isSavedRow = Boolean(rowValue?.id)

                                                return (
                                                    <Space size='small'>
                                                        <Form.Item
                                                            name={[field.name, 'tagNames']}
                                                            hidden
                                                            style={{ marginBottom: 0 }}
                                                            getValueProps={value => ({
                                                                value: value ?? [],
                                                            })}
                                                        >
                                                            <Select mode='multiple' />
                                                        </Form.Item>

                                                        {renderPeriodTagEditor(field.name)}

                                                        <Button
                                                            size='small'
                                                            danger
                                                            icon={<DeleteOutlined />}
                                                            loading={isSavedRow && deletingPeriodId === rowValue?.id}
                                                            onClick={() => {
                                                                if (isSavedRow && rowValue?.id) {
                                                                    void handleDeleteSavedPeriod(rowValue.id, field.name, remove)
                                                                    return
                                                                }

                                                                remove(field.name)
                                                                setOpenPeriodTagEditorKey(null)
                                                            }}
                                                        />
                                                    </Space>
                                                )
                                            },
                                        },
                                    ]}
                                />

                                <Button
                                    size='small'
                                    icon={<PlusOutlined />}
                                    onClick={() =>
                                        add(
                                            createBlankPeriod(
                                                getNewPeriodDefaults({
                                                    canonicalValue: form.getFieldValue('value') ?? '',
                                                    canonicalUnit: form.getFieldValue('unit') ?? '',
                                                    periods: (form.getFieldValue('periods') ?? []) as PillPeriodFormValue[],
                                                }),
                                            ),
                                        )
                                    }
                                >
                                    Add another range
                                </Button>
                            </Space>
                        )}
                    </Form.List>

                    <Divider>Supplement Facts</Divider>

                    <Form.List name='components'>
                        {(fields, { add, remove }) => (
                            <Space direction='vertical' size={8} className='pills-list-space'>
                                <Table
                                    size='small'
                                    pagination={false}
                                    rowKey='key'
                                    dataSource={fields}
                                    scroll={{ x: 760 }}
                                    columns={[
                                        {
                                            title: 'Name',
                                            render: (_: unknown, field) => (
                                                <Form.Item name={[field.name, 'name']} style={{ marginBottom: 0 }}>
                                                    <Input placeholder='Vitamin D3' />
                                                </Form.Item>
                                            ),
                                        },
                                        {
                                            title: 'Value',
                                            width: 130,
                                            render: (_: unknown, field) => (
                                                <Form.Item name={[field.name, 'value']} style={{ marginBottom: 0 }}>
                                                    <Input placeholder='125' />
                                                </Form.Item>
                                            ),
                                        },
                                        {
                                            title: 'Unit',
                                            width: 140,
                                            render: (_: unknown, field) => (
                                                <Form.Item name={[field.name, 'unit']} style={{ marginBottom: 0 }}>
                                                    <Input placeholder='mcg' />
                                                </Form.Item>
                                            ),
                                        },
                                        {
                                            title: 'Action',
                                            width: 124,
                                            render: (_: unknown, field) => (
                                                <Space size='small'>
                                                    <Button
                                                        size='small'
                                                        icon={<ArrowUpOutlined />}
                                                        onClick={() => add(getComponentInsertValue(), field.name)}
                                                        aria-label='Insert component before'
                                                        title='Insert component before'
                                                    />
                                                    <Button
                                                        size='small'
                                                        icon={<ArrowDownOutlined />}
                                                        onClick={() => add(getComponentInsertValue(), field.name + 1)}
                                                        aria-label='Insert component after'
                                                        title='Insert component after'
                                                    />
                                                    <Button
                                                        size='small'
                                                        danger
                                                        icon={<DeleteOutlined />}
                                                        onClick={() => remove(field.name)}
                                                        aria-label='Delete component'
                                                        title='Delete component'
                                                    />
                                                </Space>
                                            ),
                                        },
                                    ]}
                                />

                                <Button
                                    size='small'
                                    icon={<PlusOutlined />}
                                    onClick={() => add(getComponentInsertValue())}
                                >
                                    Add component
                                </Button>
                            </Space>
                        )}
                    </Form.List>
                </Form>
            </Drawer>
        </main>
    )
}

function getTodayDateString() {
    return dayjs().format(DATE_FORMAT)
}

function createBlankPeriod(defaults?: {
    value?: string
    unit?: string
    timing?: PillTiming
}): PillPeriodFormValue {
    return {
        startDate: getTodayDateString(),
        endDate: '',
        valueOverride: defaults?.value ?? '',
        unitOverride: defaults?.unit ?? '',
        timing: defaults?.timing ?? 'random',
        tagNames: [],
    }
}

function getLatestTrackedPeriodDefaults(periods: PillPeriodFormValue[]) {
    const periodsWithStartDate = periods.filter(period => period.startDate)
    if (periodsWithStartDate.length === 0) {
        return null
    }

    const latestPeriod = [...periodsWithStartDate]
        .sort((left, right) => (left.startDate ?? '').localeCompare(right.startDate ?? ''))
        .at(-1)

    if (!latestPeriod) {
        return null
    }

    return {
        value: latestPeriod.valueOverride?.trim() ?? '',
        unit: latestPeriod.unitOverride?.trim() ?? '',
        timing: latestPeriod.timing ?? 'random',
    }
}

function getNewPeriodDefaults(args: {
    canonicalValue?: string
    canonicalUnit?: string
    periods?: PillPeriodFormValue[]
}) {
    const latestTrackedPeriodDefaults = getLatestTrackedPeriodDefaults(args.periods ?? [])

    return {
        value: latestTrackedPeriodDefaults?.value || args.canonicalValue?.trim() || '',
        unit: latestTrackedPeriodDefaults?.unit || args.canonicalUnit?.trim() || '',
        timing: latestTrackedPeriodDefaults?.timing || 'random',
    }
}

function getPillPeriodFormValues(pill: PillRecord) {
    return pill.periods.map(period => ({
        id: period.id,
        startDate: period.startDate,
        endDate: period.endDate ?? '',
        valueOverride: period.valueOverride ?? pill.value ?? '',
        unitOverride: period.unitOverride ?? pill.unit ?? '',
        timing: period.timing ?? 'random',
        tagNames: period.tags.map(tag => tag.name),
    }))
}

function createEmptyFormValues(): PillFormValues {
    return {
        name: '',
        value: '',
        unit: '',
        url: '',
        note: '',
        images: [],
        components: [{ name: '', value: '', unit: '' }],
        periods: [createBlankPeriod({ value: '', unit: '', timing: 'random' })],
    }
}

function getPeriodTagEditorKey(fieldIndex: number, period: PillPeriodFormValue | undefined) {
    return period?.id ? `period-${period.id}` : `draft-${fieldIndex}`
}

function normalizeTagNames(values: string[]) {
    const namesByKey = new Map<string, string>()

    for (const rawValue of values) {
        const value = rawValue.trim()
        if (!value) {
            continue
        }

        const key = value.toLocaleLowerCase()
        if (!namesByKey.has(key)) {
            namesByKey.set(key, value)
        }
    }

    return [...namesByKey.values()]
}

function formatPeriodTagButtonTitle(tagNames: string[]) {
    if (tagNames.length === 0) {
        return 'Edit date range tags'
    }

    return `Tags: ${tagNames.join(', ')}`
}

function formatServing(value?: string | null, unit?: string | null) {
    const text = [value?.trim(), unit?.trim()].filter(Boolean).join(' ')
    return text || 'Not set'
}

function formatTiming(timing?: PillTiming | null) {
    if (!timing || timing === 'random') {
        return ''
    }

    return timing.charAt(0).toUpperCase() + timing.slice(1)
}

function getActivePeriod(pill: PillRecord) {
    const today = getTodayDateString()

    return [...pill.periods]
        .filter(period => !period.endDate || period.endDate > today)
        .sort((left, right) => left.startDate.localeCompare(right.startDate))
        .at(-1) ?? null
}

function formatPeriodRange(period: Pick<PillPeriod, 'startDate' | 'endDate'>) {
    return period.endDate ? `${period.startDate} to ${period.endDate}` : `${period.startDate} to ongoing`
}

function createImageUid(
    image: Pick<PillImageFormValue, 'id' | 'fileName' | 'dataUrl'>,
    index: number,
) {
    return image.id
        ? `saved-${image.id}`
        : `image-${index}-${image.fileName}-${image.dataUrl.length}`
}

function pillToFormValues(
    pill: PillRecord,
    options?: {
        appendNewPeriod?: boolean
    },
): PillFormValues {
    const periodFormValues = getPillPeriodFormValues(pill)
    const shouldAppendNewPeriod = options?.appendNewPeriod ?? false

    return {
        id: pill.id,
        name: pill.name,
        value: pill.value ?? '',
        unit: pill.unit ?? '',
        url: pill.url ?? '',
        note: pill.note ?? '',
        images: pill.images.map((image, index) => ({
            id: image.id,
            uid: createImageUid(
                {
                    id: image.id,
                    fileName: image.fileName,
                    dataUrl: image.dataUrl,
                },
                index,
            ),
            fileName: image.fileName,
            dataUrl: image.dataUrl,
        })),
        components:
            pill.components.length > 0
                ? pill.components.map(component => ({
                    name: component.name,
                    value: component.value ?? '',
                    unit: component.unit ?? '',
                }))
                : [{ name: '', value: '', unit: '' }],
        periods: shouldAppendNewPeriod
            ? [
                ...periodFormValues,
                createBlankPeriod(
                    getNewPeriodDefaults({
                        canonicalValue: pill.value ?? '',
                        canonicalUnit: pill.unit ?? '',
                        periods: periodFormValues,
                    }),
                ),
            ]
            : periodFormValues,
    }
}

function extractionToFormPatch(extraction: PillExtractionResult) {
    return {
        name: extraction.name ?? '',
        value: extraction.value ?? '',
        unit: extraction.unit ?? '',
        note: extraction.note ?? '',
        components:
            extraction.components.length > 0
                ? extraction.components.map(component => ({
                    name: component.name,
                    value: component.value ?? '',
                    unit: component.unit ?? '',
                }))
                : [{ name: '', value: '', unit: '' }],
    } satisfies Partial<PillFormValues>
}

function buildUploadFileList(images: PillImageFormValue[]): UploadFile[] {
    return images.map(image => ({
        uid: image.uid,
        name: image.fileName,
        status: 'done',
        url: image.dataUrl,
    }))
}

function getImagePayload(images: PillImageFormValue[]) {
    return images.map(image => ({
        fileName: image.fileName,
        dataUrl: image.dataUrl,
    }))
}

function removeImageByUid(images: PillImageFormValue[], uid: string) {
    return images.filter(image => image.uid !== uid)
}

function getComponentInsertValue() {
    return { name: '', value: '', unit: '' }
}

async function hasFormErrors(form: FormInstance<PillFormValues>) {
    try {
        await form.validateFields({
            validateOnly: true,
            recursive: true,
        })
        return false
    } catch {
        return true
    }
}

function readFileAsDataUrl(file: File) {
    return new Promise<string>((resolve, reject) => {
        const reader = new FileReader()
        reader.onload = () => {
            if (typeof reader.result !== 'string') {
                reject(new Error(`Unable to read ${file.name}.`))
                return
            }

            resolve(reader.result)
        }
        reader.onerror = () => reject(reader.error ?? new Error(`Unable to read ${file.name}.`))
        reader.readAsDataURL(file)
    })
}

function buildPastedImageFileName(file: File, index: number) {
    if (file.name.trim()) {
        return file.name
    }

    const fileExtension = file.type.split('/')[1] || 'png'
    return `pasted-image-${Date.now()}-${index}.${fileExtension}`
}

async function filesToFormImages(files: File[]) {
    return Promise.all(
        files.map(async (file, index) => ({
            uid: `pasted-${Date.now()}-${index}-${Math.random().toString(36).slice(2, 10)}`,
            fileName: buildPastedImageFileName(file, index),
            dataUrl: await readFileAsDataUrl(file),
        }) satisfies PillImageFormValue),
    )
}

function getPastedImageFiles(event: ClipboardEvent) {
    const clipboardItems = Array.from(event.clipboardData?.items ?? [])
        .filter(item => item.kind === 'file' && item.type.startsWith('image/'))
        .map(item => item.getAsFile())
        .filter((file): file is File => file !== null)

    if (clipboardItems.length > 0) {
        return clipboardItems
    }

    return Array.from(event.clipboardData?.files ?? []).filter(file =>
        file.type.startsWith('image/'),
    )
}

function isValidPastedUrl(value: string) {
    if (!value) {
        return false
    }

    try {
        const url = new URL(value)
        return url.protocol === 'http:' || url.protocol === 'https:'
    } catch {
        return false
    }
}

function renderComponents(components: PillComponent[]) {
    if (components.length === 0) {
        return <Typography.Text type='secondary'>No components</Typography.Text>
    }

    const [firstComponent, ...remainingComponents] = components

    return (
        <Space direction='vertical' size={0}>
            <Typography.Text>
                {firstComponent.name}: {formatServing(firstComponent.value, firstComponent.unit)}
            </Typography.Text>
            {remainingComponents.length > 0 ? (
                <Typography.Text type='secondary'>+{remainingComponents.length} more</Typography.Text>
            ) : null}
        </Space>
    )
}

function renderExpandedComponents(components: PillComponent[]) {
    return (
        <Table
            size='small'
            pagination={false}
            rowKey='id'
            dataSource={components}
            columns={[
                {
                    title: 'Name',
                    dataIndex: 'name',
                    key: 'name',
                },
                {
                    title: 'Value',
                    key: 'value',
                    width: 180,
                    render: (_: unknown, component: PillComponent) =>
                        formatServing(component.value, component.unit),
                },
            ]}
        />
    )
}

function renderImages(images: PillImage[]) {
    if (images.length === 0) {
        return <Typography.Text type='secondary'>No images</Typography.Text>
    }

    return (
        <Image.PreviewGroup>
            <div className='pills-image-list'>
                {images.map(image => (
                    <Image
                        key={image.id}
                        src={image.dataUrl}
                        alt={image.fileName}
                        width={44}
                        height={44}
                        className='pills-image-preview'
                    />
                ))}
            </div>
        </Image.PreviewGroup>
    )
}

function formatRelativeDate(value: string) {
    try {
        return formatDistanceToNow(parseISO(value), { addSuffix: true })
    } catch {
        return value
    }
}
