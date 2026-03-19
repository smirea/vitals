export type MeasurementFlag = 'low' | 'high' | 'normal' | 'abnormal' | 'critical' | 'unknown';

export type BloodworkMeasurementRecord = {
    key: string;
    name: string;
    category: string | null;
    valueText: string | null;
    valueNumeric: number | null;
    unit: string | null;
    referenceRangeMin: number | null;
    referenceRangeMax: number | null;
    flag: MeasurementFlag | null;
    note: string | null;
};

export type SourceColumn = {
    id: string;
    reportId: number;
    date: string;
    prettyDate: string;
    index: number;
};

export type MeasurementCell = {
    display: string;
    numericValue: number | null;
    rangeMin: number | null;
    rangeMax: number | null;
    rangeCaption: string;
    rangeVisualization: {
        minPosition: number | null;
        maxPosition: number | null;
        valuePosition: number;
    } | null;
    rangeBandLeft: number;
    rangeBandWidth: number;
    unit?: string;
    flag?: MeasurementFlag;
    note?: string;
};

export type VitalsRowModel = {
    rowType: 'measurement';
    key: string;
    measurement: string;
    category: string;
    measurementSearchText: string;
    categorySearchText: string;
    valuesBySourceIndex: Array<MeasurementCell | undefined>;
};

export type VitalsCategoryRow = {
    rowType: 'category';
    key: string;
    category: string;
    categoryCount: number;
};

export type VitalsDisplayRow = VitalsRowModel | VitalsCategoryRow;

export type CategorySelectionState = {
    checked: boolean;
    indeterminate: boolean;
    disabled: boolean;
};

export type MeasurementOverviewTally = {
    inRange: number;
    outOfRange: number;
};

export type CategoryOverviewItem = {
    category: string;
    inRange: number;
    outOfRange: number;
    unclassified: number;
    total: number;
};

export type MeasurementRangeStatus = 'in-range' | 'out-of-range' | 'unclassified';

export type MeaningfulChangeDirection = 'improved' | 'worsened' | 'changed';

export type MeaningfulChangeItem = {
    key: string;
    measurement: string;
    category: string;
    direction: MeaningfulChangeDirection;
    score: number;
    reasons: string[];
    relativeDeltaPercent: number | null;
    normalizedRangeDeltaPercent: number | null;
    latest: {
        date: string;
        prettyDate: string;
        display: string;
        status: MeasurementRangeStatus;
    };
    previous: {
        date: string;
        prettyDate: string;
        display: string;
        status: MeasurementRangeStatus;
    };
};

export type ChartSeriesModel = {
    id: string;
    chartKey: string;
    label: string;
    color: string;
    valuesBySourceIndex: Array<MeasurementCell | undefined>;
    normalizedValuesBySourceIndex: Array<number | null>;
    outOfRangeBySourceIndex: boolean[];
    unitLabel?: string;
};

export type TrendChartDatum = {
    sourceId: string;
    prettyDate: string;
    [key: string]: string | number | boolean | null;
};

export type SelectionState = {
    selectedRowKeys: string[];
    selectedRowKeySet: Set<string>;
};

export type VitalsViewModel = {
    sources: SourceColumn[];
    visibleSources: SourceColumn[];
    chartSources: SourceColumn[];
    tableSources: SourceColumn[];
    allMeasurementRows: VitalsRowModel[];
    filteredMeasurementRows: VitalsRowModel[];
    tableRows: VitalsDisplayRow[];
    measurementKeysByCategory: Map<string, string[]>;
    categorySelectionByName: Map<string, CategorySelectionState>;
    selectedRows: VitalsRowModel[];
    measurementOverviewByKey: Map<string, MeasurementOverviewTally>;
    measurementRangesTooltipByKey: Map<string, string>;
    chartSeries: ChartSeriesModel[];
};
