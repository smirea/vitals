import {
    AppleLogo,
    ArrowRight,
    Bone,
    Brain,
    Circle,
    Dna,
    Drop,
    Fire,
    Flask,
    Heart,
    Leaf,
    Lightning,
    Nut,
    Pulse,
    Stethoscope,
    TestTube,
    Watch,
    WaveSine,
} from '@phosphor-icons/react';

import type { CategoryOverviewModel } from '../types';

type CategoriesOverviewProps = {
    items: CategoryOverviewModel[];
    onViewAll: () => void;
};

function getCategoryIcon(category: string) {
    const normalized = category.toLowerCase();
    if (normalized.includes('blood') || normalized.includes('iron')) return Drop;
    if (normalized.includes('heart') || normalized.includes('cardio') || normalized.includes('lipid')) return Heart;
    if (normalized.includes('brain')) return Brain;
    if (normalized.includes('bone')) return Bone;
    if (normalized.includes('immune') || normalized.includes('inflammation') || normalized.includes('auto')) return Pulse;
    if (normalized.includes('liver')) return Leaf;
    if (normalized.includes('kidney') || normalized.includes('urine')) return TestTube;
    if (normalized.includes('thyroid') || normalized.includes('hormone') || normalized.includes('age')) return Dna;
    if (normalized.includes('electrolyte') || normalized.includes('metabolic') || normalized.includes('glucose')) return Lightning;
    if (normalized.includes('daily')) return Watch;
    if (normalized.includes('nutrient') || normalized.includes('vitamin')) return AppleLogo;
    if (normalized.includes('stress') || normalized.includes('toxin')) return Flask;
    if (normalized.includes('food') || normalized.includes('allerg')) return Nut;
    if (normalized.includes('cancer')) return Fire;
    if (normalized.includes('health') || normalized.includes('other')) return Stethoscope;
    if (normalized.includes('regulation')) return WaveSine;
    return Circle;
}

function getSegmentWidth(value: number, total: number): number {
    if (total <= 0 || value <= 0) {
        return 0;
    }
    return (value / total) * 100;
}

export function CategoriesOverview({
    items,
    onViewAll,
}: CategoriesOverviewProps) {
    return (
        <section className='vitals-summary-card'>
            <div className='vitals-summary-head'>
                <h2>Categories</h2>
                <button type='button' onClick={onViewAll} className='vitals-summary-link'>
                    View all
                    <ArrowRight size={15} />
                </button>
            </div>
            <p className='vitals-summary-caption'>Latest reading per measurement from the last 6 months.</p>

            <div className='vitals-overview-grid'>
                {items.map(item => {
                    const Icon = getCategoryIcon(item.category);
                    const inRangeWidth = getSegmentWidth(item.inRangeCount, item.totalCount);
                    const outOfRangeWidth = getSegmentWidth(item.outOfRangeCount, item.totalCount);
                    const unclassifiedWidth = getSegmentWidth(item.unclassifiedCount, item.totalCount);

                    return (
                        <article key={item.category} className='vitals-overview-item'>
                            <div className='vitals-overview-icon'>
                                <Icon size={32} weight='regular' />
                            </div>

                            <div className='vitals-overview-body'>
                                <div className='vitals-overview-title-row'>
                                    <h3>{item.category}</h3>
                                </div>

                                <div className='vitals-overview-meter'>
                                    <div className='vitals-overview-track' role='presentation'>
                                        {item.inRangeCount > 0 && (
                                            <span className='vitals-overview-segment vitals-overview-segment-in-range' style={{ width: `${inRangeWidth}%` }} />
                                        )}
                                        {item.outOfRangeCount > 0 && (
                                            <span className='vitals-overview-segment vitals-overview-segment-out-of-range' style={{ width: `${outOfRangeWidth}%` }} />
                                        )}
                                        {item.unclassifiedCount > 0 && (
                                            <span className='vitals-overview-segment vitals-overview-segment-unclassified' style={{ width: `${unclassifiedWidth}%` }} />
                                        )}
                                    </div>

                                    <div className='vitals-overview-tally'>
                                        {item.inRangeCount > 0 && (
                                            <span className='vitals-overview-tally-item'>
                                                <span className='vitals-overview-dot vitals-overview-dot-in-range' />
                                                {item.inRangeCount}
                                            </span>
                                        )}
                                        {item.outOfRangeCount > 0 && (
                                            <span className='vitals-overview-tally-item'>
                                                <span className='vitals-overview-dot vitals-overview-dot-out-of-range' />
                                                {item.outOfRangeCount}
                                            </span>
                                        )}
                                        {item.unclassifiedCount > 0 && (
                                            <span className='vitals-overview-tally-item'>
                                                <span className='vitals-overview-dot vitals-overview-dot-unclassified' />
                                                {item.unclassifiedCount}
                                            </span>
                                        )}
                                    </div>
                                </div>
                            </div>

                            <div className='vitals-overview-total'>
                                <span>{item.totalCount}</span>
                                <span className='vitals-overview-active-dot' />
                            </div>
                        </article>
                    );
                })}
            </div>
        </section>
    );
}
