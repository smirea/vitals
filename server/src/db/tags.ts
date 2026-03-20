import { asc, eq } from 'drizzle-orm'
import { z } from 'zod'

import type { VitalsDatabase } from 'server/db/client.ts'
import { pillPeriodTags, tags, type TagRow } from 'server/db/schema.ts'
import { TAG_COLOR_PRESETS } from '../../../shared/constants.ts'

type TagsReadDb = Pick<VitalsDatabase, 'select'>
type TagsWriteDb = Pick<VitalsDatabase, 'select' | 'insert' | 'update'>

function getRandomTagColor() {
    return TAG_COLOR_PRESETS[Math.floor(Math.random() * TAG_COLOR_PRESETS.length)]
}

export const tagCreateInputSchema = z.object({
    name: z.string().trim().min(1),
    color: z.string().trim().min(1),
    note: z.string().trim().optional().default(''),
})

export const tagUpdateInputSchema = tagCreateInputSchema.extend({
    id: z.number().int().positive(),
})

function normalizeOptionalText(value: string | null | undefined) {
    const trimmed = value?.trim() ?? ''
    return trimmed.length > 0 ? trimmed : null
}

function normalizeTagNames(tagNames: string[]) {
    const namesByKey = new Map<string, string>()

    for (const rawName of tagNames) {
        const name = rawName.trim()
        if (!name) {
            continue
        }

        const key = name.toLocaleLowerCase()
        if (!namesByKey.has(key)) {
            namesByKey.set(key, name)
        }
    }

    return [...namesByKey.values()]
}

function normalizeTagWriteError(error: unknown, name: string) {
    if (error instanceof Error && error.message.includes('UNIQUE constraint failed: tags.name')) {
        return new Error(`A tag named '${name}' already exists.`)
    }

    return error
}

export function listTags(db: TagsReadDb) {
    const tagRows = db
        .select()
        .from(tags)
        .orderBy(asc(tags.name), asc(tags.id))
        .all()
    const tagLinkRows = db
        .select({
            tagId: pillPeriodTags.tagId,
        })
        .from(pillPeriodTags)
        .all()

    const pillPeriodCounts = new Map<number, number>()
    for (const row of tagLinkRows) {
        pillPeriodCounts.set(row.tagId, (pillPeriodCounts.get(row.tagId) ?? 0) + 1)
    }

    return tagRows.map(row => ({
        ...row,
        attachmentCounts: {
            pillPeriods: pillPeriodCounts.get(row.id) ?? 0,
        },
    }))
}

export function createTag(db: TagsWriteDb, input: z.infer<typeof tagCreateInputSchema>) {
    const name = input.name.trim()
    const existingTag = getExistingTagByName(db, name)

    if (existingTag) {
        throw new Error(`A tag named '${existingTag.name}' already exists.`)
    }

    try {
        const insertedTag = db
            .insert(tags)
            .values({
                name,
                color: input.color.trim(),
                note: normalizeOptionalText(input.note),
                createdDate: new Date().toISOString(),
            })
            .returning()
            .get()

        return insertedTag
    } catch (error) {
        throw normalizeTagWriteError(error, name)
    }
}

export function updateTag(db: TagsWriteDb, input: z.infer<typeof tagUpdateInputSchema>) {
    const name = input.name.trim()
    const existingTag = db
        .select()
        .from(tags)
        .orderBy(asc(tags.name), asc(tags.id))
        .all()
        .find(tag => tag.id === input.id)

    if (!existingTag) {
        throw new Error(`Tag ${input.id} does not exist.`)
    }

    const conflictingTag = getExistingTagByName(db, name)
    if (conflictingTag && conflictingTag.id !== input.id) {
        throw new Error(`A tag named '${conflictingTag.name}' already exists.`)
    }

    try {
        const updatedTag = db
            .update(tags)
            .set({
                name,
                color: input.color.trim(),
                note: normalizeOptionalText(input.note),
            })
            .where(eq(tags.id, input.id))
            .returning()
            .get()

        return updatedTag
    } catch (error) {
        throw normalizeTagWriteError(error, name)
    }
}

export function ensureTagsByNames(db: TagsWriteDb, inputNames: string[]) {
    const names = normalizeTagNames(inputNames)
    if (names.length === 0) {
        return [] satisfies TagRow[]
    }

    const existingTags = db
        .select()
        .from(tags)
        .orderBy(asc(tags.name), asc(tags.id))
        .all()
    const tagsByName = new Map(existingTags.map(tag => [tag.name.toLocaleLowerCase(), tag]))
    const resolvedTags: TagRow[] = []

    for (const name of names) {
        const normalizedName = name.toLocaleLowerCase()
        const existingTag = tagsByName.get(normalizedName)
        if (existingTag) {
            resolvedTags.push(existingTag)
            continue
        }

        const insertedTag = db
            .insert(tags)
            .values({
                name,
                color: getRandomTagColor(),
                note: null,
                createdDate: new Date().toISOString(),
            })
            .returning()
            .get()

        tagsByName.set(normalizedName, insertedTag)
        resolvedTags.push(insertedTag)
    }

    return resolvedTags
}

function getExistingTagByName(db: TagsReadDb, name: string) {
    return db
        .select()
        .from(tags)
        .orderBy(asc(tags.name), asc(tags.id))
        .all()
        .find(tag => tag.name.toLocaleLowerCase() === name.toLocaleLowerCase()) ?? null
}
