export type BloodworkMeasurement = {
    name: string;
    originalName?: string;
    category?: string;
    value?: number | string;
    unit?: string;
    referenceRange?: {
        min?: number;
        max?: number;
    };
    flag?: 'low' | 'high' | 'normal' | 'abnormal' | 'critical' | 'unknown';
    note?: string;
};

export type BloodworkLab = {
    date: string;
    labName: string;
    location?: string;
    importLocation?: string;
    importLocationIsInferred?: boolean;
    weightKg?: number;
    measurements: BloodworkMeasurement[];
    notes?: string;
};

export type ApiResponse = {
    items: BloodworkLab[];
};

export type SourceColumn = {
    id: string;
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
    flag?: BloodworkMeasurement['flag'];
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
