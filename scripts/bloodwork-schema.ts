import { z } from 'zod';

const ISO_DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;

function toInteger(value: string, fieldName: string): number {
    const parsed = Number.parseInt(value, 10);
    if (Number.isNaN(parsed)) {
        throw new Error(`Invalid ${fieldName}: ${value}`);
    }
    return parsed;
}

function isValidCalendarDate(year: number, month: number, day: number): boolean {
    if (month < 1 || month > 12) return false;
    if (day < 1 || day > 31) return false;
    const date = new Date(Date.UTC(year, month - 1, day));
    return (
        date.getUTCFullYear() === year &&
        date.getUTCMonth() === month - 1 &&
        date.getUTCDate() === day
    );
}

export function normalizeIsoDate(rawDate: string): string {
    const value = rawDate.trim();
    if (!value) {
        throw new Error('Date is empty');
    }

    if (ISO_DATE_REGEX.test(value)) {
        const [year, month, day] = value.split('-').map(part => toInteger(part, 'date part'));
        if (!isValidCalendarDate(year, month, day)) {
            throw new Error(`Invalid ISO date: ${value}`);
        }
        return value;
    }

    const yearFirst = value.match(/^(\d{4})[./-](\d{1,2})[./-](\d{1,2})$/);
    if (yearFirst) {
        const year = toInteger(yearFirst[1], 'year');
        const month = toInteger(yearFirst[2], 'month');
        const day = toInteger(yearFirst[3], 'day');
        if (!isValidCalendarDate(year, month, day)) {
            throw new Error(`Invalid date: ${value}`);
        }
        return `${year.toString().padStart(4, '0')}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
    }

    const dayFirst = value.match(/^(\d{1,2})[./-](\d{1,2})[./-](\d{4})$/);
    if (dayFirst) {
        const day = toInteger(dayFirst[1], 'day');
        const month = toInteger(dayFirst[2], 'month');
        const year = toInteger(dayFirst[3], 'year');
        if (!isValidCalendarDate(year, month, day)) {
            throw new Error(`Invalid date: ${value}`);
        }
        return `${year.toString().padStart(4, '0')}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
    }

    const parsedTimestamp = Date.parse(value);
    if (!Number.isNaN(parsedTimestamp)) {
        return new Date(parsedTimestamp).toISOString().slice(0, 10);
    }

    throw new Error(`Could not parse date: ${value}`);
}

export const bloodworkMeasurementFlagSchema = z.enum([
    'low',
    'high',
    'normal',
    'abnormal',
    'critical',
    'unknown',
]);

export const bloodworkReferenceRangeSchema = z
    .object({
        lower: z.number().finite().optional(),
        upper: z.number().finite().optional(),
        text: z.string().trim().min(1).optional(),
    })
    .refine(
        value => value.lower !== undefined || value.upper !== undefined || value.text !== undefined,
        { message: 'referenceRange must contain at least one field' },
    );

export const bloodworkMeasurementSchema = z.object({
    name: z.string().trim().min(1),
    originalName: z.string().trim().min(1).optional(),
    category: z.string().trim().min(1).optional(),
    value: z.union([z.number().finite(), z.string().trim().min(1)]).optional(),
    unit: z.string().trim().min(1).optional(),
    referenceRange: bloodworkReferenceRangeSchema.optional(),
    flag: bloodworkMeasurementFlagSchema.optional(),
    notes: z.string().trim().min(1).optional(),
});

export const bloodworkLabSchema = z.object({
    date: z.string().trim().min(1).transform(normalizeIsoDate),
    labName: z.string().trim().min(1),
    location: z.string().trim().min(1).optional(),
    importLocation: z.string().trim().min(1).optional(),
    importLocationIsInferred: z.boolean().optional(),
    weightKg: z.number().positive().finite().optional(),
    measurements: z.array(bloodworkMeasurementSchema).min(1),
    notes: z.string().trim().min(1).optional(),
});

export type BloodworkMeasurement = z.infer<typeof bloodworkMeasurementSchema>;
export type BloodworkLab = z.infer<typeof bloodworkLabSchema>;

export function slugifyForPath(value: string): string {
    const stripped = value
        .normalize('NFKD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
    return stripped || 'unknown-lab';
}

export function buildBloodworkFileName(input: Pick<BloodworkLab, 'date' | 'labName'>): string {
    return `bloodwork_${normalizeIsoDate(input.date)}_${slugifyForPath(input.labName)}.json`;
}

export function buildBloodworkS3Key(
    input: Pick<BloodworkLab, 'date' | 'labName'>,
    prefix = 'vitals',
): string {
    const normalizedPrefix = prefix.replace(/^\/+|\/+$/g, '');
    if (!normalizedPrefix) {
        return buildBloodworkFileName(input);
    }
    return `${normalizedPrefix}/${buildBloodworkFileName(input)}`;
}
