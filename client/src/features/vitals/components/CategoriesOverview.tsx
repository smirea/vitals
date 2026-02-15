import type { CategoryOverviewItem } from '../types';

type CategoriesOverviewProps = {
    items: CategoryOverviewItem[];
};

function toSharePercent(value: number, maxTotal: number): number {
    if (value <= 0 || maxTotal <= 0) {
        return 0;
    }
    return (value / maxTotal) * 100;
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
                            sharePercent: toSharePercent(item.inRange, maxTotal),
                        },
                        {
                            key: 'out-of-range',
                            count: item.outOfRange,
                            className: 'vitals-category-overview-pill-out-of-range',
                            sharePercent: toSharePercent(item.outOfRange, maxTotal),
                        },
                        {
                            key: 'unclassified',
                            count: item.unclassified,
                            className: 'vitals-category-overview-pill-unclassified',
                            sharePercent: toSharePercent(item.unclassified, maxTotal),
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
                                    <span
                                        key={`${item.category}-${status.key}`}
                                        className='vitals-category-overview-pill-group'
                                        style={{ width: `calc(var(--vitals-overview-section-min-width) + ${status.sharePercent}%)` }}
                                    >
                                        <span
                                            className={`vitals-category-overview-pill ${status.className}`}
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
