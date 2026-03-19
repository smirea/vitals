import {
    and,
    asc,
    count,
    count as countAll,
    desc,
    eq,
    getTableColumns,
    gt,
    gte,
    inArray,
    isNotNull,
    isNull,
    like,
    lt,
    lte,
    type AnyColumn,
} from 'drizzle-orm';
import type { AnySQLiteTable } from 'drizzle-orm/sqlite-core';
import { z } from 'zod';

import { appTables } from 'server/db/schema.ts';
import { createRouter, publicProcedure } from 'server/trpc/shared.ts';

type TableColumns<TTable extends AnySQLiteTable> = TTable['_']['columns'];
type TableColumnName<TTable extends AnySQLiteTable> = keyof TableColumns<TTable> & string;
type ColumnScalar<TColumn extends AnyColumn> = Exclude<TColumn['_']['data'], null | undefined>;

type TableFilterConditionForColumn<
    TColumnName extends string,
    TColumn extends AnyColumn,
> =
    | {
        column: TColumnName;
        operator: 'eq';
        value: ColumnScalar<TColumn>;
    }
    | {
        column: TColumnName;
        operator: 'in';
        value: ColumnScalar<TColumn>[];
    }
    | {
        column: TColumnName;
        operator: 'isNull';
        value: boolean;
    }
    | (
        ColumnScalar<TColumn> extends string | number | Date
            ? {
                column: TColumnName;
                operator: 'gt' | 'gte' | 'lt' | 'lte';
                value: ColumnScalar<TColumn>;
            }
            : never
    )
    | (
        ColumnScalar<TColumn> extends string
            ? {
                column: TColumnName;
                operator: 'like';
                value: string;
            }
            : never
    );

type TableFilterCondition<TTable extends AnySQLiteTable> = {
    [TColumnName in TableColumnName<TTable>]: TableFilterConditionForColumn<
        TColumnName,
        TableColumns<TTable>[TColumnName]
    >;
}[TableColumnName<TTable>];

type TableQueryInput<TTable extends AnySQLiteTable> = {
    where?: TableFilterCondition<TTable>[];
    orderBy?: Array<{
        column: TableColumnName<TTable>;
        direction?: 'asc' | 'desc';
    }>;
    limit?: number;
    offset?: number;
};

function getColumnValueSchema(column: AnyColumn): z.ZodTypeAny {
    switch (column.dataType) {
        case 'number':
            return z.number().finite();
        case 'boolean':
            return z.boolean();
        case 'string':
        case 'date':
            return z.string();
        default:
            return z.string();
    }
}

function supportsOrderedComparisons(column: AnyColumn) {
    return column.dataType === 'number' || column.dataType === 'string' || column.dataType === 'date';
}

function supportsLike(column: AnyColumn) {
    return column.dataType === 'string';
}

function buildWhereSchema<TTable extends AnySQLiteTable>(table: TTable): z.ZodType<TableFilterCondition<TTable>[]> {
    const columns = getTableColumns(table);
    const conditionSchemas = Object.entries(columns).flatMap(([columnName, column]) => {
        const valueSchema = getColumnValueSchema(column);
        const schemas: z.ZodTypeAny[] = [
            z.object({
                column: z.literal(columnName),
                operator: z.literal('eq'),
                value: valueSchema,
            }),
            z.object({
                column: z.literal(columnName),
                operator: z.literal('in'),
                value: z.array(valueSchema).min(1),
            }),
            z.object({
                column: z.literal(columnName),
                operator: z.literal('isNull'),
                value: z.boolean(),
            }),
        ];

        if (supportsOrderedComparisons(column)) {
            schemas.push(
                z.object({
                    column: z.literal(columnName),
                    operator: z.literal('gt'),
                    value: valueSchema,
                }),
                z.object({
                    column: z.literal(columnName),
                    operator: z.literal('gte'),
                    value: valueSchema,
                }),
                z.object({
                    column: z.literal(columnName),
                    operator: z.literal('lt'),
                    value: valueSchema,
                }),
                z.object({
                    column: z.literal(columnName),
                    operator: z.literal('lte'),
                    value: valueSchema,
                }),
            );
        }

        if (supportsLike(column)) {
            schemas.push(z.object({
                column: z.literal(columnName),
                operator: z.literal('like'),
                value: z.string().min(1),
            }));
        }

        return schemas;
    });

    const [firstSchema, ...restSchemas] = conditionSchemas;
    if (!firstSchema) {
        return z.array(z.never()) as z.ZodType<TableFilterCondition<TTable>[]>;
    }

    const conditionSchema = restSchemas.length > 0
        ? z.union([firstSchema, ...restSchemas] as [typeof firstSchema, ...typeof restSchemas])
        : firstSchema;

    return z.array(conditionSchema).max(100) as z.ZodType<TableFilterCondition<TTable>[]>;
}

function buildOrderBySchema<TTable extends AnySQLiteTable>(table: TTable): z.ZodType<TableQueryInput<TTable>['orderBy']> {
    const columns = Object.keys(getTableColumns(table));
    const columnSchema = z.enum(columns as [string, ...string[]]);

    return z.array(z.object({
        column: columnSchema,
        direction: z.enum(['asc', 'desc']).optional(),
    })).max(20) as z.ZodType<TableQueryInput<TTable>['orderBy']>;
}

function buildTableQuerySchema<TTable extends AnySQLiteTable>(table: TTable): z.ZodType<TableQueryInput<TTable>> {
    return z.object({
        where: buildWhereSchema(table).optional(),
        orderBy: buildOrderBySchema(table).optional(),
        limit: z.number().int().positive().optional(),
        offset: z.number().int().nonnegative().optional(),
    }) as z.ZodType<TableQueryInput<TTable>>;
}

function buildDeleteSchema<TTable extends AnySQLiteTable>(table: TTable) {
    const whereSchema = buildWhereSchema(table) as unknown as z.ZodArray<z.ZodTypeAny>;

    return z.object({
        where: whereSchema.min(1),
    }) as z.ZodType<{ where: TableFilterCondition<TTable>[] }>;
}

function buildWhereClause<TTable extends AnySQLiteTable>(
    table: TTable,
    where: TableFilterCondition<TTable>[] | undefined,
) {
    if (!where?.length) {
        return undefined;
    }

    const columns = getTableColumns(table);
    const conditions = where.map(item => {
        const column = columns[item.column];

        switch (item.operator) {
            case 'eq':
                return eq(column, item.value as never);
            case 'in':
                return inArray(column, item.value as never[]);
            case 'isNull':
                return item.value ? isNull(column) : isNotNull(column);
            case 'gt':
                return gt(column, item.value as never);
            case 'gte':
                return gte(column, item.value as never);
            case 'lt':
                return lt(column, item.value as never);
            case 'lte':
                return lte(column, item.value as never);
            case 'like':
                return like(column, item.value);
        }
    });

    return conditions.length === 1 ? conditions[0] : and(...conditions);
}

function buildOrderBy<TTable extends AnySQLiteTable>(
    table: TTable,
    orderBy: TableQueryInput<TTable>['orderBy'],
) {
    if (!orderBy?.length) {
        return [];
    }

    const columns = getTableColumns(table);
    return orderBy.map(item => (
        item.direction === 'desc'
            ? desc(columns[item.column])
            : asc(columns[item.column])
    ));
}

function createTableAccessRouter<TTable extends AnySQLiteTable>(table: TTable) {
    const querySchema = buildTableQuerySchema(table);
    const deleteSchema = buildDeleteSchema(table);

    return createRouter({
        findMany: publicProcedure
            .input(querySchema.optional())
            .query(({ ctx, input }) => {
                const safeInput = input ?? {};
                const whereClause = buildWhereClause(table, safeInput.where);
                const orderBy = buildOrderBy(table, safeInput.orderBy);
                const limit = Math.min(safeInput.limit ?? 100, 1000);
                const offset = safeInput.offset ?? 0;

                let query = ctx.db.select().from(table).$dynamic();

                if (whereClause) {
                    query = query.where(whereClause);
                }
                if (orderBy.length > 0) {
                    query = query.orderBy(...orderBy);
                }
                if (offset > 0) {
                    query = query.offset(offset);
                }

                return query.limit(limit).all();
            }),
        findOne: publicProcedure
            .input(querySchema.optional())
            .query(({ ctx, input }) => {
                const safeInput = input ?? {};
                const whereClause = buildWhereClause(table, safeInput.where);
                const orderBy = buildOrderBy(table, safeInput.orderBy);
                const offset = safeInput.offset ?? 0;

                let query = ctx.db.select().from(table).$dynamic();

                if (whereClause) {
                    query = query.where(whereClause);
                }
                if (orderBy.length > 0) {
                    query = query.orderBy(...orderBy);
                }
                if (offset > 0) {
                    query = query.offset(offset);
                }

                return query.limit(1).get() ?? null;
            }),
        count: publicProcedure
            .input(querySchema.optional())
            .query(({ ctx, input }) => {
                const whereClause = buildWhereClause(table, input?.where);

                let query = ctx.db.select({
                    value: count(),
                }).from(table).$dynamic();

                if (whereClause) {
                    query = query.where(whereClause);
                }

                return query.get()?.value ?? 0;
            }),
        deleteMany: publicProcedure
            .input(deleteSchema)
            .mutation(({ ctx, input }) => {
                const whereClause = buildWhereClause(table, input.where);
                if (!whereClause) {
                    throw new Error('deleteMany requires at least one where condition.');
                }

                const deletedCount = ctx.db.select({
                    value: countAll(),
                }).from(table).where(whereClause).get()?.value ?? 0;

                ctx.db.delete(table).where(whereClause).run();

                return {
                    deletedCount,
                };
            }),
    });
}

const tableRouters = Object.fromEntries(
    Object.entries(appTables).map(([tableName, table]) => [tableName, createTableAccessRouter(table)]),
) as {
    [TTableName in keyof typeof appTables]: ReturnType<
        typeof createTableAccessRouter<(typeof appTables)[TTableName]>
    >;
};

export const tableRouter = createRouter(tableRouters);
