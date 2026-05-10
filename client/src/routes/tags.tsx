import { toast } from '@tamagui/toast/v2';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { formatDistanceToNow, parseISO } from 'date-fns';
import { useMemo, useState } from 'react';
import { Button, Card, Input, Text, XStack, YStack } from 'tamagui';

import { TAG_COLOR_PRESETS } from '../../../shared/constants.ts';
import { AutoResizeTextArea } from '../components/AutoResizeTextArea';
import { DataTable, type DataColumn } from '../components/DataTable';
import { FormField } from '../components/FormField';
import { PageNav } from '../components/PageNav';
import { TagChip } from '../components/TagChip';
import type { TagRecord } from '../utils/api';
import { useTRPC } from '../utils/trpc';

type TagFormValues = {
	name: string;
	note: string;
};

const emptyTagForm: TagFormValues = {
	name: '',
	note: '',
};

export const Route = createFileRoute('/tags')({
	component: TagsRouteComponent,
});

function TagsRouteComponent() {
	const trpc = useTRPC();
	const queryClient = useQueryClient();
	const [formValues, setFormValues] = useState<TagFormValues>(emptyTagForm);
	const [selectedColor, setSelectedColor] = useState<string>(TAG_COLOR_PRESETS[0]);
	const [editingTagId, setEditingTagId] = useState<number | null>(null);

	const tagsQuery = useQuery(trpc.tags.list.queryOptions());
	const createTagMutation = useMutation({
		...trpc.tags.create.mutationOptions(),
		onSuccess: async () => {
			await queryClient.invalidateQueries({ queryKey: [['tags']] });
			await queryClient.invalidateQueries({ queryKey: [['pills']] });
			resetTagForm();
			toast.success('Tag created.');
		},
		onError: error => {
			toast.error(error.message);
		},
	});
	const updateTagMutation = useMutation({
		...trpc.tags.update.mutationOptions(),
		onSuccess: async () => {
			await queryClient.invalidateQueries({ queryKey: [['tags']] });
			await queryClient.invalidateQueries({ queryKey: [['pills']] });
			resetTagForm();
			toast.success('Tag updated.');
		},
		onError: error => {
			toast.error(error.message);
		},
	});

	const isSaving = createTagMutation.isPending || updateTagMutation.isPending;

	const columns = useMemo<Array<DataColumn<TagRecord>>>(
		() => [
			{
				key: 'tag',
				header: 'Tag',
				cell: row => (
					<YStack gap={2}>
						<TagChip
							color={row.color}
							style={{ cursor: 'pointer' }}
							onPress={() => handleEditTag(row)}
						>
							{row.name}
						</TagChip>
						{row.note ? <Text color='$textMuted'>{row.note}</Text> : null}
					</YStack>
				),
			},
			{
				key: 'created',
				header: 'Created',
				cell: row => (
					<YStack gap={0}>
						<Text>{formatCreatedDate(row.createdDate)}</Text>
						<Text color='$textMuted'>{row.createdDate}</Text>
					</YStack>
				),
			},
			{
				key: 'pillPeriods',
				header: 'Pill ranges',
				align: 'right',
				cell: row => row.attachmentCounts.pillPeriods,
			},
		],
		[],
	);

	function updateFormValue<Key extends keyof TagFormValues>(key: Key, value: TagFormValues[Key]) {
		setFormValues(current => ({ ...current, [key]: value }));
	}

	async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
		event.preventDefault();
		const name = formValues.name.trim();
		if (!name) {
			toast.error('Enter a tag name.');
			return;
		}

		if (editingTagId === null) {
			await createTagMutation.mutateAsync({
				name,
				color: selectedColor,
				note: formValues.note,
			});
			return;
		}

		await updateTagMutation.mutateAsync({
			id: editingTagId,
			name,
			color: selectedColor,
			note: formValues.note,
		});
	}

	function resetTagForm() {
		setFormValues(emptyTagForm);
		setSelectedColor(TAG_COLOR_PRESETS[0]);
		setEditingTagId(null);
	}

	function handleEditTag(tag: TagRecord) {
		setEditingTagId(tag.id);
		setSelectedColor(tag.color);
		setFormValues({
			name: tag.name,
			note: tag.note ?? '',
		});
	}

	return (
		<main className='pills-page'>
			<PageNav title='Tags' />

			<div className='pills-page-inner'>
				<Card bg='$bgContainer' borderColor='$borderSubtle' borderWidth={1}>
					<XStack
						alignItems='center'
						justifyContent='space-between'
						gap={12}
						padding={12}
						borderBottomWidth={1}
						borderBottomColor='$borderSubtle'
					>
						<Text fontWeight='700'>{editingTagId === null ? 'Create tag' : 'Edit tag'}</Text>
						{editingTagId === null ? null : <Button onPress={resetTagForm}>Cancel</Button>}
					</XStack>

					<form onSubmit={event => void handleSubmit(event)}>
						<div className='tag-form-grid'>
							<FormField label='Name' required>
								<Input
									value={formValues.name}
									placeholder='Travel'
									onChange={event => updateFormValue('name', event.target.value)}
								/>
							</FormField>

							<FormField label='Color' required>
								<Input
									type='color'
									value={selectedColor}
									onChange={event => setSelectedColor(event.target.value)}
									className='tag-color-input'
								/>
							</FormField>

							<FormField label='Note'>
								<AutoResizeTextArea
									value={formValues.note}
									placeholder='Optional note about when or why this tag is useful'
									minRows={1}
									maxRows={4}
									onChange={event =>
										updateFormValue('note', (event.target as unknown as HTMLTextAreaElement).value)
									}
								/>
							</FormField>

							<FormField label=' '>
								<XStack gap={8}>
									<Button
										type='submit'
										disabled={isSaving}
										backgroundColor='$primary'
										style={{ color: 'white' }}
									>
										{isSaving ? 'Saving...' : editingTagId === null ? 'Create tag' : 'Save tag'}
									</Button>

									{editingTagId !== null ? <Button onPress={resetTagForm}>Cancel</Button> : null}
								</XStack>
							</FormField>
						</div>
					</form>
				</Card>

				<Card bg='$bgContainer' borderColor='$borderSubtle' borderWidth={1}>
					<XStack
						alignItems='center'
						justifyContent='space-between'
						gap={12}
						padding={12}
						borderBottomWidth={1}
						borderBottomColor='$borderSubtle'
					>
						<Text fontWeight='700'>All tags</Text>
						<Text color='$textMuted'>{tagsQuery.data?.length ?? 0} rows</Text>
					</XStack>

					<YStack padding={16}>
						<DataTable
							getRowKey={row => row.id}
							loading={tagsQuery.isLoading}
							columns={columns}
							rows={tagsQuery.data ?? []}
							getRowProps={row => ({
								onClick: () => handleEditTag(row),
								style: { cursor: 'pointer' },
							})}
						/>
					</YStack>
				</Card>
			</div>
		</main>
	);
}

function formatCreatedDate(value: string) {
	try {
		return formatDistanceToNow(parseISO(value), { addSuffix: true });
	} catch {
		return value;
	}
}
