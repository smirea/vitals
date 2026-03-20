import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { createFileRoute } from '@tanstack/react-router'
import { Button, Card, ColorPicker, Form, Input, Space, Table, Tag, Typography, message } from 'antd'
import type { TableColumnsType } from 'antd'
import { formatDistanceToNow, parseISO } from 'date-fns'
import { useState } from 'react'

import type { TagRecord } from '../utils/api'
import { PageNav } from '../components/PageNav'
import { useTRPC } from '../utils/trpc'
import { TAG_COLOR_PRESETS } from '../../../shared/constants.ts'

type TagFormValues = {
    name: string
    note: string
}

export const Route = createFileRoute('/tags')({
    component: TagsRouteComponent,
})

function TagsRouteComponent() {
    const trpc = useTRPC()
    const queryClient = useQueryClient()
    const [form] = Form.useForm<TagFormValues>()
    const [selectedColor, setSelectedColor] = useState<string>(TAG_COLOR_PRESETS[0])
    const [editingTagId, setEditingTagId] = useState<number | null>(null)

    const tagsQuery = useQuery(trpc.tags.list.queryOptions())
    const createTagMutation = useMutation({
        ...trpc.tags.create.mutationOptions(),
        onSuccess: async () => {
            await queryClient.invalidateQueries()
            resetTagForm()
            message.success('Tag created.')
        },
        onError: error => {
            message.error(error.message)
        },
    })
    const updateTagMutation = useMutation({
        ...trpc.tags.update.mutationOptions(),
        onSuccess: async () => {
            await queryClient.invalidateQueries()
            resetTagForm()
            message.success('Tag updated.')
        },
        onError: error => {
            message.error(error.message)
        },
    })

    async function handleSubmit(values: TagFormValues) {
        if (editingTagId === null) {
            await createTagMutation.mutateAsync({
                name: values.name,
                color: selectedColor,
                note: values.note,
            })
            return
        }

        await updateTagMutation.mutateAsync({
            id: editingTagId,
            name: values.name,
            color: selectedColor,
            note: values.note,
        })
    }

    function resetTagForm() {
        form.resetFields()
        setSelectedColor(TAG_COLOR_PRESETS[0])
        setEditingTagId(null)
    }

    function handleEditTag(tag: TagRecord) {
        setEditingTagId(tag.id)
        setSelectedColor(tag.color)
        form.setFieldsValue({
            name: tag.name,
            note: tag.note ?? '',
        })
    }

    const columns: TableColumnsType<TagRecord> = [
        {
            title: 'Tag',
            key: 'tag',
            render: (_: unknown, row: TagRecord) => (
                <Space direction='vertical' size={2}>
                    <Tag
                        color={row.color}
                        style={{ cursor: 'pointer' }}
                        onClick={() => {
                            handleEditTag(row)
                        }}
                    >
                        {row.name}
                    </Tag>
                    {row.note ? (
                        <Typography.Text type='secondary'>{row.note}</Typography.Text>
                    ) : null}
                </Space>
            ),
        },
        {
            title: 'Created',
            dataIndex: 'createdDate',
            key: 'createdDate',
            render: (createdDate: string) => (
                <Space direction='vertical' size={0}>
                    <Typography.Text>{formatCreatedDate(createdDate)}</Typography.Text>
                    <Typography.Text type='secondary'>{createdDate}</Typography.Text>
                </Space>
            ),
        },
        {
            title: 'Pill ranges',
            key: 'pillPeriods',
            align: 'right',
            render: (_: unknown, row: TagRecord) => row.attachmentCounts.pillPeriods,
        },
    ]

    return (
        <main className='pills-page'>
            <PageNav title='Tags' />

            <div className='pills-page-inner'>
                <Card
                    title={editingTagId === null ? 'Create tag' : 'Edit tag'}
                    extra={editingTagId === null ? null : (
                        <Button onClick={resetTagForm}>
                            Cancel
                        </Button>
                    )}
                >
                    <Form<TagFormValues>
                        form={form}
                        layout='vertical'
                        initialValues={{
                            name: '',
                            note: '',
                        }}
                        onFinish={values => {
                            void handleSubmit(values)
                        }}
                    >
                        <div
                            style={{
                                display: 'grid',
                                gap: 12,
                                gridTemplateColumns: 'minmax(180px, 220px) 160px minmax(260px, 1fr) auto',
                                alignItems: 'start',
                            }}
                        >
                            <Form.Item
                                label='Name'
                                name='name'
                                rules={[{ required: true, message: 'Enter a tag name.' }]}
                            >
                                <Input placeholder='Travel' />
                            </Form.Item>

                            <Form.Item label='Color' required>
                                <ColorPicker
                                    value={selectedColor}
                                    disabledAlpha
                                    presets={[
                                        {
                                            label: 'Preset colors',
                                            colors: [...TAG_COLOR_PRESETS],
                                        },
                                    ]}
                                    onChange={value => {
                                        setSelectedColor(value.toHexString())
                                    }}
                                    showText
                                />
                            </Form.Item>

                            <Form.Item label='Note' name='note'>
                                <Input.TextArea
                                    placeholder='Optional note about when or why this tag is useful'
                                    autoSize={{ minRows: 1, maxRows: 4 }}
                                />
                            </Form.Item>

                            <Form.Item label=' '>
                                <Space>
                                    <Button
                                        type='primary'
                                        htmlType='submit'
                                        loading={createTagMutation.isPending || updateTagMutation.isPending}
                                    >
                                        {editingTagId === null ? 'Create tag' : 'Save tag'}
                                    </Button>

                                    {editingTagId !== null ? (
                                        <Button onClick={resetTagForm}>
                                            Cancel
                                        </Button>
                                    ) : null}
                                </Space>
                            </Form.Item>
                        </div>
                    </Form>
                </Card>

                <Card
                    title='All tags'
                    extra={<Typography.Text type='secondary'>{tagsQuery.data?.length ?? 0} rows</Typography.Text>}
                >
                    <Table
                        rowKey={row => String(row.id)}
                        size='small'
                        pagination={false}
                        loading={tagsQuery.isLoading}
                        columns={columns}
                        dataSource={tagsQuery.data ?? []}
                        onRow={row => ({
                            onClick: () => {
                                handleEditTag(row)
                            },
                            style: { cursor: 'pointer' },
                        })}
                    />
                </Card>
            </div>
        </main>
    )
}

function formatCreatedDate(value: string) {
    try {
        return formatDistanceToNow(parseISO(value), { addSuffix: true })
    } catch {
        return value
    }
}
