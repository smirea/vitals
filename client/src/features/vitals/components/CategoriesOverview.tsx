import type { CategoryOverviewItem } from '../types';

type CategoriesOverviewProps = {
    items: CategoryOverviewItem[];
};

function toPercent(value: number, total: number): number {
    if (total <= 0 || value <= 0) {
        return 0;
    }
    return (value / total) * 100;
}

export function CategoriesOverview({ items }: CategoriesOverviewProps) {
    if (items.length === 0) {
        return null;
    }

    return (
        <section className='vitals-category-overview'>
            <div className='vitals-category-overview-head'>
                <strong>Category Overview</strong>
                <span>Latest value in last 6 months</span>
            </div>

            <div className='vitals-category-overview-grid'>
                {items.map(item => (
                    <article key={item.category} className='vitals-category-overview-item'>
                        <div className='vitals-category-overview-row'>
                            <h3>{item.category}</h3>
                            <span>{item.total}</span>
                        </div>

                        <div className='vitals-category-overview-track' role='presentation'>
                            {item.inRange > 0 && (
                                <span
                                    className='vitals-category-overview-segment vitals-category-overview-segment-in-range'
                                    style={{ width: `${toPercent(item.inRange, item.total)}%` }}
                                />
                            )}
                            {item.outOfRange > 0 && (
                                <span
                                    className='vitals-category-overview-segment vitals-category-overview-segment-out-of-range'
                                    style={{ width: `${toPercent(item.outOfRange, item.total)}%` }}
                                />
                            )}
                            {item.unclassified > 0 && (
                                <span
                                    className='vitals-category-overview-segment vitals-category-overview-segment-unclassified'
                                    style={{ width: `${toPercent(item.unclassified, item.total)}%` }}
                                />
                            )}
                        </div>

                        <div className='vitals-category-overview-legend'>
                            {item.inRange > 0 && <span className='vitals-category-overview-legend-in-range'>In range {item.inRange}</span>}
                            {item.outOfRange > 0 && <span className='vitals-category-overview-legend-out-of-range'>Out of range {item.outOfRange}</span>}
                            {item.unclassified > 0 && <span className='vitals-category-overview-legend-unclassified'>Unclassified {item.unclassified}</span>}
                        </div>
                    </article>
                ))}
            </div>
        </section>
    );
}
