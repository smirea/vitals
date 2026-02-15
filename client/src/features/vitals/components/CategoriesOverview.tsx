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

function toPillWidthPercent(value: number, total: number): number {
    const percentage = toPercent(value, total);
    if (percentage <= 0) {
        return 0;
    }
    return Math.max(8, percentage);
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

                        <div className='vitals-category-overview-pills' role='presentation'>
                            {item.inRange > 0 && (
                                <span className='vitals-category-overview-pill-group'>
                                    <span
                                        className='vitals-category-overview-pill vitals-category-overview-pill-in-range'
                                        style={{ width: `${toPillWidthPercent(item.inRange, item.total)}%` }}
                                    />
                                    <span className='vitals-category-overview-pill-count'>{item.inRange}</span>
                                </span>
                            )}
                            {item.outOfRange > 0 && (
                                <span className='vitals-category-overview-pill-group'>
                                    <span
                                        className='vitals-category-overview-pill vitals-category-overview-pill-out-of-range'
                                        style={{ width: `${toPillWidthPercent(item.outOfRange, item.total)}%` }}
                                    />
                                    <span className='vitals-category-overview-pill-count'>{item.outOfRange}</span>
                                </span>
                            )}
                            {item.unclassified > 0 && (
                                <span className='vitals-category-overview-pill-group'>
                                    <span
                                        className='vitals-category-overview-pill vitals-category-overview-pill-unclassified'
                                        style={{ width: `${toPillWidthPercent(item.unclassified, item.total)}%` }}
                                    />
                                    <span className='vitals-category-overview-pill-count'>{item.unclassified}</span>
                                </span>
                            )}
                        </div>
                    </article>
                ))}
            </div>
        </section>
    );
}
