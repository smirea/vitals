import type { CategoryOverviewItem } from '../types';

type CategoriesOverviewProps = {
    items: CategoryOverviewItem[];
};

const MAX_PILL_WIDTH_PX = 140;

function toPillWidth(value: number, maxTotal: number): number {
    if (value <= 0 || maxTotal <= 0) {
        return 0;
    }
    return Math.max(8, Math.round((value / maxTotal) * MAX_PILL_WIDTH_PX));
}

export function CategoriesOverview({ items }: CategoriesOverviewProps) {
    if (items.length === 0) {
        return null;
    }

    const maxTotal = Math.max(...items.map(item => item.total), 1);

    return (
        <section className='vitals-category-overview'>
            <div className='vitals-category-overview-grid'>
                {items.map(item => {
                    const statuses = [
                        {
                            key: 'in-range',
                            count: item.inRange,
                            className: 'vitals-category-overview-pill-in-range',
                        },
                        {
                            key: 'out-of-range',
                            count: item.outOfRange,
                            className: 'vitals-category-overview-pill-out-of-range',
                        },
                        {
                            key: 'unclassified',
                            count: item.unclassified,
                            className: 'vitals-category-overview-pill-unclassified',
                        },
                    ].filter(status => status.count > 0);

                    return (
                        <article key={item.category} className='vitals-category-overview-item'>
                            <div className='vitals-category-overview-row'>
                                <h3>{item.category}</h3>
                                <span>{item.total}</span>
                            </div>

                            <div className='vitals-category-overview-pills' role='presentation'>
                                {statuses.map(status => (
                                    <span key={`${item.category}-${status.key}`} className='vitals-category-overview-pill-group'>
                                        <span
                                            className={`vitals-category-overview-pill ${status.className}`}
                                            style={{ width: `${toPillWidth(status.count, maxTotal)}px` }}
                                        />
                                        <span className='vitals-category-overview-pill-count'>{status.count}</span>
                                    </span>
                                ))}
                            </div>
                        </article>
                    );
                })}
            </div>
        </section>
    );
}
