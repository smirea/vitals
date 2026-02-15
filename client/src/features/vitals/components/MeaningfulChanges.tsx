import type { MeaningfulChangeItem } from '../types';

type MeaningfulChangesProps = {
    items: MeaningfulChangeItem[];
};

function formatDelta(value: number | null): string | null {
    if (value === null || !Number.isFinite(value)) {
        return null;
    }
    if (value >= 1000) {
        return `${Math.round(value)}%`;
    }
    if (value >= 100) {
        return `${value.toFixed(0)}%`;
    }
    return `${value.toFixed(1)}%`;
}

export function MeaningfulChanges({ items }: MeaningfulChangesProps) {
    if (items.length === 0) {
        return null;
    }

    const improvedCount = items.filter(item => item.direction === 'improved').length;
    const worsenedCount = items.filter(item => item.direction === 'worsened').length;

    return (
        <section className='vitals-meaningful-changes'>
            <div className='vitals-meaningful-changes-header'>
                <h2>Last 6 months changes</h2>
                <p>{improvedCount} improved · {worsenedCount} worsened · {items.length} meaningful</p>
            </div>

            <div className='vitals-meaningful-changes-list'>
                {items.map(item => {
                    const relativeDelta = formatDelta(item.relativeDeltaPercent);
                    const rangeDelta = formatDelta(item.normalizedRangeDeltaPercent);
                    const directionLabel = item.direction === 'improved'
                        ? 'Improved'
                        : item.direction === 'worsened'
                            ? 'Worsened'
                            : 'Changed';

                    return (
                        <article key={item.key} className='vitals-meaningful-change-item'>
                            <div className='vitals-meaningful-change-title-row'>
                                <div className='vitals-meaningful-change-titles'>
                                    <h3>{item.measurement}</h3>
                                    <span>{item.category}</span>
                                </div>
                                <span className={`vitals-meaningful-change-direction vitals-meaningful-change-direction-${item.direction}`}>
                                    {directionLabel}
                                </span>
                            </div>

                            <div className='vitals-meaningful-change-values'>
                                <div>
                                    <span>Before</span>
                                    <strong>{item.previous.display}</strong>
                                    <small>{item.previous.prettyDate}</small>
                                </div>
                                <div className='vitals-meaningful-change-arrow' aria-hidden>→</div>
                                <div>
                                    <span>Latest</span>
                                    <strong>{item.latest.display}</strong>
                                    <small>{item.latest.prettyDate}</small>
                                </div>
                            </div>

                            <div className='vitals-meaningful-change-meta'>
                                {item.reasons.map(reason => (
                                    <span key={`${item.key}-${reason}`} className='vitals-meaningful-change-reason'>{reason}</span>
                                ))}
                                {relativeDelta && <span className='vitals-meaningful-change-delta'>Value Δ {relativeDelta}</span>}
                                {rangeDelta && <span className='vitals-meaningful-change-delta'>Range drift {rangeDelta}</span>}
                            </div>
                        </article>
                    );
                })}
            </div>
        </section>
    );
}
