import { ArrowsDownUp, TrendDown, TrendUp } from '@phosphor-icons/react';

import type { MeasurementChangeModel } from '../types';
import { formatPrettyDate } from '../utils';

type RecentChangesProps = {
    items: MeasurementChangeModel[];
};

function getDeltaLabel(change: MeasurementChangeModel): string {
    if (change.delta === null) {
        return 'Updated';
    }

    const sign = change.delta > 0 ? '+' : change.delta < 0 ? '-' : '';
    const deltaMagnitude = Math.abs(change.delta).toFixed(2).replace(/\.?0+$/, '');
    const deltaValue = `${sign}${deltaMagnitude}`;
    if (change.deltaRatio === null) {
        return `\u0394 ${deltaValue}`;
    }

    const ratioPercent = Math.round(change.deltaRatio * 100);
    return `\u0394 ${deltaValue} (${sign}${ratioPercent}%)`;
}

function getRangeCaption(value: string): string {
    return value.trim() || 'No reference range';
}

export function RecentChanges({ items }: RecentChangesProps) {
    return (
        <section className='vitals-summary-card'>
            <div className='vitals-summary-head vitals-summary-head-tight'>
                <h2>Changes</h2>
                <span className='vitals-summary-caption-chip'>{items.length} found</span>
            </div>
            <p className='vitals-summary-caption'>Latest measurement compared with the prior result on record.</p>

            {items.length === 0 ? (
                <div className='vitals-changes-empty'>No meaningful shifts found across the latest two measurements.</div>
            ) : (
                <div className='vitals-changes-grid'>
                    {items.map(change => {
                        const TrendIcon = change.direction === 'up'
                            ? TrendUp
                            : change.direction === 'down'
                                ? TrendDown
                                : ArrowsDownUp;

                        return (
                            <article key={change.key} className='vitals-change-card'>
                                <header className='vitals-change-header'>
                                    <div className='vitals-change-title'>
                                        <h3>{change.measurement}</h3>
                                        <span>{change.category}</span>
                                    </div>
                                    <div className='vitals-change-delta'>
                                        <TrendIcon size={14} weight='bold' />
                                        <span>{getDeltaLabel(change)}</span>
                                    </div>
                                </header>

                                <div className='vitals-change-comparison'>
                                    <section className='vitals-change-side vitals-change-side-before'>
                                        <span className='vitals-change-side-label'>Before</span>
                                        <span className='vitals-change-date'>{formatPrettyDate(change.previousSource.date)}</span>
                                        <strong>{change.previousCell.display}</strong>
                                        <span className='vitals-change-range'>{getRangeCaption(change.previousCell.rangeCaption)}</span>
                                    </section>

                                    <section className='vitals-change-side vitals-change-side-after'>
                                        <span className='vitals-change-side-label'>After</span>
                                        <span className='vitals-change-date'>{formatPrettyDate(change.latestSource.date)}</span>
                                        <strong>{change.latestCell.display}</strong>
                                        <span className='vitals-change-range'>{getRangeCaption(change.latestCell.rangeCaption)}</span>
                                    </section>
                                </div>
                            </article>
                        );
                    })}
                </div>
            )}
        </section>
    );
}
