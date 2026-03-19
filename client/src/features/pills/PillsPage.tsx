import {
  ArrowDownOutlined,
  ArrowUpOutlined,
  DeleteOutlined,
  EditOutlined,
  PlusOutlined,
  UploadOutlined,
} from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AutoComplete,
  Button,
  Card,
  DatePicker,
  Divider,
  Drawer,
  Empty,
  Form,
  Image,
  Input,
  Select,
  Space,
  Spin,
  Statistic,
  Table,
  Tag,
  Typography,
  Upload,
  message,
} from "antd";
import type { FormInstance, TableColumnsType } from "antd";
import type { UploadChangeParam, UploadFile, UploadProps } from "antd/es/upload/interface";
import dayjs from "dayjs";
import type { Key } from "react";
import { useDeferredValue, useEffect, useMemo, useState } from "react";

import { useTRPC } from "../../lib/trpc";
import type {
  PillComponent,
  PillExtractionResult,
  PillImage,
  PillPeriod,
  PillRecord,
} from "../vitals/api";

const DATE_FORMAT = "YYYY-MM-DD";
const { Dragger } = Upload;
type PillTiming = "morning" | "afternoon" | "evening" | "random";

type PillImageFormValue = {
  id?: number;
  uid: string;
  fileName: string;
  dataUrl: string;
};

type PillComponentFormValue = {
  name: string;
  value: string;
  unit: string;
};

type PillPeriodFormValue = {
  id?: number;
  startDate?: string;
  endDate?: string;
  valueOverride?: string;
  unitOverride?: string;
  timing?: PillTiming;
};

type PillFormValues = {
  id?: number;
  name: string;
  value: string;
  unit: string;
  note: string;
  images: PillImageFormValue[];
  components: PillComponentFormValue[];
  periods: PillPeriodFormValue[];
};

const timingOptions = [
  { label: "Morning", value: "morning" },
  { label: "Afternoon", value: "afternoon" },
  { label: "Evening", value: "evening" },
  { label: "Random", value: "random" },
] as const;

function getTodayDateString() {
  return dayjs().format(DATE_FORMAT);
}

function createBlankPeriod(defaults?: {
  value?: string;
  unit?: string;
  timing?: PillTiming;
}): PillPeriodFormValue {
  return {
    startDate: getTodayDateString(),
    endDate: "",
    valueOverride: defaults?.value ?? "",
    unitOverride: defaults?.unit ?? "",
    timing: defaults?.timing ?? "random",
  };
}

function getLatestTrackedPeriodDefaults(periods: PillPeriodFormValue[]) {
  const periodsWithStartDate = periods.filter((period) => period.startDate);
  if (periodsWithStartDate.length === 0) {
    return null;
  }

  const latestPeriod = [...periodsWithStartDate]
    .sort((left, right) => (left.startDate ?? "").localeCompare(right.startDate ?? ""))
    .at(-1);

  if (!latestPeriod) {
    return null;
  }

  return {
    value: latestPeriod.valueOverride?.trim() ?? "",
    unit: latestPeriod.unitOverride?.trim() ?? "",
    timing: latestPeriod.timing ?? "random",
  };
}

function getNewPeriodDefaults(args: {
  canonicalValue?: string;
  canonicalUnit?: string;
  periods?: PillPeriodFormValue[];
}) {
  const latestTrackedPeriodDefaults = getLatestTrackedPeriodDefaults(args.periods ?? []);

  return {
    value: latestTrackedPeriodDefaults?.value || args.canonicalValue?.trim() || "",
    unit: latestTrackedPeriodDefaults?.unit || args.canonicalUnit?.trim() || "",
    timing: latestTrackedPeriodDefaults?.timing || "random",
  };
}

function getPillPeriodFormValues(pill: PillRecord) {
  return pill.periods.map((period) => ({
    id: period.id,
    startDate: period.startDate,
    endDate: period.endDate ?? "",
    valueOverride: period.valueOverride ?? pill.value ?? "",
    unitOverride: period.unitOverride ?? pill.unit ?? "",
    timing: period.timing ?? "random",
  }));
}

function createEmptyFormValues(): PillFormValues {
  return {
    name: "",
    value: "",
    unit: "",
    note: "",
    images: [],
    components: [{ name: "", value: "", unit: "" }],
    periods: [createBlankPeriod({ value: "", unit: "", timing: "random" })],
  };
}

function formatServing(value?: string | null, unit?: string | null) {
  const text = [value?.trim(), unit?.trim()].filter(Boolean).join(" ");
  return text || "Not set";
}

function formatPeriodLabel(
  period: Pick<PillPeriod, "startDate" | "endDate" | "valueOverride" | "unitOverride" | "timing">,
) {
  const rangeText = period.endDate
    ? `${period.startDate} to ${period.endDate}`
    : `${period.startDate} to ongoing`;
  const overrideText =
    period.valueOverride || period.unitOverride
      ? ` (${formatServing(period.valueOverride, period.unitOverride)})`
      : "";
  const timingText = period.timing ? ` · ${period.timing}` : "";

  return `${rangeText}${timingText}${overrideText}`;
}

function createImageUid(
  image: Pick<PillImageFormValue, "id" | "fileName" | "dataUrl">,
  index: number,
) {
  return image.id
    ? `saved-${image.id}`
    : `image-${index}-${image.fileName}-${image.dataUrl.length}`;
}

function pillToFormValues(pill: PillRecord): PillFormValues {
  const periodFormValues = getPillPeriodFormValues(pill);

  return {
    id: pill.id,
    name: pill.name,
    value: pill.value ?? "",
    unit: pill.unit ?? "",
    note: pill.note ?? "",
    images: pill.images.map((image, index) => ({
      id: image.id,
      uid: createImageUid(
        {
          id: image.id,
          fileName: image.fileName,
          dataUrl: image.dataUrl,
        },
        index,
      ),
      fileName: image.fileName,
      dataUrl: image.dataUrl,
    })),
    components:
      pill.components.length > 0
        ? pill.components.map((component) => ({
            name: component.name,
            value: component.value ?? "",
            unit: component.unit ?? "",
          }))
        : [{ name: "", value: "", unit: "" }],
    periods: [
      ...periodFormValues,
      createBlankPeriod(
        getNewPeriodDefaults({
          canonicalValue: pill.value ?? "",
          canonicalUnit: pill.unit ?? "",
          periods: periodFormValues,
        }),
      ),
    ],
  };
}

function extractionToFormPatch(extraction: PillExtractionResult) {
  return {
    name: extraction.name ?? "",
    value: extraction.value ?? "",
    unit: extraction.unit ?? "",
    note: extraction.note ?? "",
    components:
      extraction.components.length > 0
        ? extraction.components.map((component) => ({
            name: component.name,
            value: component.value ?? "",
            unit: component.unit ?? "",
          }))
        : [{ name: "", value: "", unit: "" }],
  } satisfies Partial<PillFormValues>;
}

function buildUploadFileList(images: PillImageFormValue[]): UploadFile[] {
  return images.map((image) => ({
    uid: image.uid,
    name: image.fileName,
    status: "done",
    url: image.dataUrl,
  }));
}

function getImagePayload(images: PillImageFormValue[]) {
  return images.map((image) => ({
    fileName: image.fileName,
    dataUrl: image.dataUrl,
  }));
}

function getComponentInsertValue() {
  return { name: "", value: "", unit: "" };
}

async function hasFormErrors(form: FormInstance<PillFormValues>) {
  try {
    await form.validateFields({
      validateOnly: true,
      recursive: true,
    });
    return false;
  } catch {
    return true;
  }
}

function readFileAsDataUrl(file: File) {
  return new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      if (typeof reader.result !== "string") {
        reject(new Error(`Unable to read ${file.name}.`));
        return;
      }

      resolve(reader.result);
    };
    reader.onerror = () => reject(reader.error ?? new Error(`Unable to read ${file.name}.`));
    reader.readAsDataURL(file);
  });
}

function renderComponents(components: PillComponent[]) {
  if (components.length === 0) {
    return <Typography.Text type="secondary">No components</Typography.Text>;
  }

  const [firstComponent, ...remainingComponents] = components;

  return (
    <Space direction="vertical" size={0}>
      <Typography.Text>
        {firstComponent.name}: {formatServing(firstComponent.value, firstComponent.unit)}
      </Typography.Text>
      {remainingComponents.length > 0 ? (
        <Typography.Text type="secondary">+{remainingComponents.length} more</Typography.Text>
      ) : null}
    </Space>
  );
}

function renderExpandedComponents(components: PillComponent[]) {
  return (
    <Table
      size="small"
      pagination={false}
      rowKey="id"
      dataSource={components}
      columns={[
        {
          title: "Name",
          dataIndex: "name",
          key: "name",
        },
        {
          title: "Value",
          key: "value",
          width: 180,
          render: (_: unknown, component: PillComponent) =>
            formatServing(component.value, component.unit),
        },
      ]}
    />
  );
}

function renderPeriods(periods: PillPeriod[]) {
  if (periods.length === 0) {
    return <Typography.Text type="secondary">No date ranges</Typography.Text>;
  }

  return (
    <Space direction="vertical" size={4}>
      {periods.map((period) => (
        <Tag
          key={period.id}
          color={period.endDate ? "default" : "green"}
          className="me-0 whitespace-normal py-1"
        >
          {formatPeriodLabel(period)}
        </Tag>
      ))}
    </Space>
  );
}

function renderImages(images: PillImage[]) {
  if (images.length === 0) {
    return <Typography.Text type="secondary">No images</Typography.Text>;
  }

  return (
    <Image.PreviewGroup>
      <div className="flex flex-wrap items-center gap-2">
        {images.map((image) => (
          <Image
            key={image.id}
            src={image.dataUrl}
            alt={image.fileName}
            width={44}
            height={44}
            className="rounded-lg object-cover"
          />
        ))}
      </div>
    </Image.PreviewGroup>
  );
}

export function PillsPage() {
  const trpc = useTRPC();
  const queryClient = useQueryClient();
  const [form] = Form.useForm<PillFormValues>();
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);
  const [pillQuery, setPillQuery] = useState("");
  const [isHydratingExistingPill, setIsHydratingExistingPill] = useState(false);
  const [deletingPeriodId, setDeletingPeriodId] = useState<number | null>(null);
  const [isSaveDisabled, setIsSaveDisabled] = useState(true);
  const [expandedComponentRowKeys, setExpandedComponentRowKeys] = useState<number[]>([]);
  const deferredPillQuery = useDeferredValue(pillQuery);
  const watchedImages = (Form.useWatch("images", form) ?? []) as PillImageFormValue[];
  const watchedDefaultValue = Form.useWatch("value", form) ?? "";
  const watchedDefaultUnit = Form.useWatch("unit", form) ?? "";
  const watchedPeriods = (Form.useWatch("periods", form) ?? []) as PillPeriodFormValue[];

  const dashboardQuery = useQuery(trpc.pills.getDashboard.queryOptions());
  const searchQuery = useQuery({
    ...trpc.pills.search.queryOptions({
      query: deferredPillQuery,
      limit: 8,
    }),
    enabled: isDrawerOpen,
  });

  const extractionMutation = useMutation({
    ...trpc.pills.extractFromImages.mutationOptions(),
    onSuccess: (extraction) => {
      if (!extraction.detected) {
        message.info(
          extraction.extractionNotes ??
            "No pill or supplement label was confidently detected in the uploaded images.",
        );
        return;
      }

      form.setFieldsValue(extractionToFormPatch(extraction));
      message.success(`Filled pill details from images using ${extraction.model}.`);
    },
    onError: (error) => {
      message.error(error.message);
    },
  });
  const isParsingImages = extractionMutation.isPending;

  const upsertMutation = useMutation({
    ...trpc.pills.upsert.mutationOptions(),
    onSuccess: () => {
      void queryClient.invalidateQueries();
      setIsDrawerOpen(false);
      setPillQuery("");
      form.setFieldsValue(createEmptyFormValues());
      message.success("Pill saved.");
    },
    onError: (error) => {
      message.error(error.message);
    },
  });

  const deletePeriodMutation = useMutation({
    ...trpc.table.pillPeriods.deleteMany.mutationOptions(),
    onSuccess: async () => {
      await queryClient.invalidateQueries();
    },
    onError: (error) => {
      message.error(error.message);
    },
    onSettled: () => {
      setDeletingPeriodId(null);
    },
  });

  const dashboard = dashboardQuery.data;
  const searchResults = (searchQuery.data ?? []) as PillRecord[];

  const autocompleteOptions = useMemo(
    () =>
      searchResults.map((result) => ({
        label: result.name,
        value: result.name,
        pill: result,
      })),
    [searchResults],
  );

  const activePills = dashboard?.activePills ?? [];
  const pastPills = dashboard?.pastPills ?? [];
  const totals = dashboard?.totals ?? { all: 0, active: 0, past: 0 };

  useEffect(() => {
    let isCancelled = false;

    const syncSaveDisabledState = async () => {
      const hasErrors = await hasFormErrors(form);
      if (!isCancelled) {
        setIsSaveDisabled(hasErrors);
      }
    };

    void syncSaveDisabledState();

    return () => {
      isCancelled = true;
    };
  }, [form, watchedDefaultUnit, watchedDefaultValue, watchedImages, watchedPeriods]);

  useEffect(() => {
    if (watchedPeriods.length === 0) {
      return;
    }

    const latestDefaults = getNewPeriodDefaults({
      canonicalValue: watchedDefaultValue,
      canonicalUnit: watchedDefaultUnit,
      periods: watchedPeriods,
    });

    const nextPeriods = watchedPeriods.map((period) => ({
      ...period,
      valueOverride: period.valueOverride?.trim() ? period.valueOverride : latestDefaults.value,
      unitOverride: period.unitOverride?.trim() ? period.unitOverride : latestDefaults.unit,
      timing: period.timing ?? latestDefaults.timing,
    }));

    const hasChanged = nextPeriods.some(
      (period, index) =>
        period.valueOverride !== watchedPeriods[index]?.valueOverride ||
        period.unitOverride !== watchedPeriods[index]?.unitOverride ||
        period.timing !== watchedPeriods[index]?.timing,
    );

    if (hasChanged) {
      form.setFieldValue("periods", nextPeriods);
    }
  }, [form, watchedDefaultUnit, watchedDefaultValue, watchedPeriods]);

  function openNewPillDrawer() {
    setIsDrawerOpen(true);
    setIsHydratingExistingPill(false);
    setPillQuery("");
    form.setFieldsValue(createEmptyFormValues());
  }

  function openExistingPillDrawer(pill: PillRecord) {
    setIsDrawerOpen(true);
    setIsHydratingExistingPill(true);
    setPillQuery(pill.name);
    form.setFieldsValue(pillToFormValues(pill));
  }

  function handleCloseDrawer() {
    setIsDrawerOpen(false);
    setPillQuery("");
    setIsHydratingExistingPill(false);
  }

  function toggleExpandedComponentsRow(pillId: number) {
    setExpandedComponentRowKeys((currentKeys) =>
      currentKeys.includes(pillId)
        ? currentKeys.filter((currentKey) => currentKey !== pillId)
        : [...currentKeys, pillId],
    );
  }

  function handleAutocompleteSelect(_: string, option: { pill?: PillRecord }) {
    if (!option.pill) {
      return;
    }

    setIsHydratingExistingPill(true);
    form.setFieldsValue(pillToFormValues(option.pill));
  }

  async function syncImagesFromUpload(event: UploadChangeParam<UploadFile<any>>) {
    try {
      const nextImages = await Promise.all(
        event.fileList.map(async (file) => {
          if (typeof file.url === "string" && !file.originFileObj) {
            return {
              uid: file.uid,
              fileName: file.name,
              dataUrl: file.url,
            } satisfies PillImageFormValue;
          }

          const originalFile = file.originFileObj;
          if (!originalFile) {
            throw new Error(`Unable to process ${file.name}.`);
          }

          return {
            uid: file.uid,
            fileName: originalFile.name,
            dataUrl: await readFileAsDataUrl(originalFile),
            id:
              typeof file.uid === "string" && file.uid.startsWith("saved-")
                ? Number(file.uid.replace("saved-", "")) || undefined
                : undefined,
          } satisfies PillImageFormValue;
        }),
      );

      form.setFieldValue("images", nextImages);
    } catch (error) {
      message.error(error instanceof Error ? error.message : "Unable to process uploaded images.");
    }
  }

  async function handleParseImages() {
    if (watchedImages.length === 0) {
      return;
    }

    await extractionMutation.mutateAsync({
      images: getImagePayload(watchedImages),
    });
  }

  const uploadProps: UploadProps = {
    accept: "image/*",
    beforeUpload: () => false,
    disabled: isParsingImages,
    listType: "picture-card",
    multiple: true,
    fileList: buildUploadFileList(watchedImages),
    onChange: (info) => {
      void syncImagesFromUpload(info);
    },
    onRemove: (file) => {
      const nextImages = watchedImages.filter((image) => image.uid !== file.uid);
      form.setFieldValue("images", nextImages);
      return true;
    },
  };

  async function handleSubmit(values: PillFormValues) {
    await upsertMutation.mutateAsync({
      id: values.id,
      name: values.name,
      value: values.value,
      unit: values.unit,
      note: values.note,
      images: getImagePayload(values.images),
      components: values.components,
      periods: values.periods,
    });
  }

  async function handleDeleteSavedPeriod(
    periodId: number,
    fieldIndex: number,
    remove: (index: number | number[]) => void,
  ) {
    setDeletingPeriodId(periodId);

    try {
      const result = await deletePeriodMutation.mutateAsync({
        where: [
          {
            column: "id",
            operator: "eq",
            value: periodId,
          },
        ],
      });

      remove(fieldIndex);

      const remainingPeriods = (form.getFieldValue("periods") ?? []) as PillPeriodFormValue[];
      if (remainingPeriods.length === 0) {
        form.setFieldValue("periods", [
          createBlankPeriod(
            getNewPeriodDefaults({
              canonicalValue: form.getFieldValue("value") ?? "",
              canonicalUnit: form.getFieldValue("unit") ?? "",
              periods: [],
            }),
          ),
        ]);
      }

      message.success(
        result.deletedCount === 1
          ? "Date range deleted."
          : `${result.deletedCount} date ranges deleted.`,
      );
    } catch {}
  }

  const columns: TableColumnsType<PillRecord> = [
    {
      title: "Pill",
      key: "name",
      width: 220,
      render: (_: unknown, pill: PillRecord) => (
        <div className="space-y-2">
          <Button
            type="link"
            size="small"
            className="!px-0 !font-semibold"
            icon={<EditOutlined />}
            onClick={() => openExistingPillDrawer(pill)}
          >
            {pill.name}
          </Button>
          <div className="flex flex-wrap gap-1.5">
            {pill.images.length > 0 ? (
              <Tag className="me-0 rounded-full">Images {pill.images.length}</Tag>
            ) : null}
            <Tag
              color={pill.periods.some((period) => !period.endDate) ? "green" : "default"}
              className="me-0 rounded-full"
            >
              {pill.periods.length} ranges
            </Tag>
          </div>
        </div>
      ),
    },
    {
      title: "Default Serving",
      key: "serving",
      width: 160,
      render: (_: unknown, pill: PillRecord) => (
        <Typography.Text>{formatServing(pill.value, pill.unit)}</Typography.Text>
      ),
    },
    {
      title: "Components",
      key: "components",
      width: 360,
      render: (_: unknown, pill: PillRecord) => renderComponents(pill.components),
      onCell: (pill) =>
        pill.components.length > 0
          ? {
              onClick: () => {
                toggleExpandedComponentsRow(pill.id);
              },
              style: { cursor: "pointer" },
            }
          : {},
    },
    {
      title: "Date Ranges",
      key: "periods",
      width: 320,
      render: (_: unknown, pill: PillRecord) => renderPeriods(pill.periods),
    },
    {
      title: "Note",
      key: "note",
      width: 260,
      render: (_: unknown, pill: PillRecord) =>
        pill.note ? (
          <Typography.Paragraph
            ellipsis={{ rows: 4, expandable: true, symbol: "more" }}
            className="!mb-0 whitespace-pre-wrap"
          >
            {pill.note}
          </Typography.Paragraph>
        ) : (
          <Typography.Text type="secondary">No note</Typography.Text>
        ),
    },
    {
      title: "Images",
      key: "images",
      width: 220,
      render: (_: unknown, pill: PillRecord) => renderImages(pill.images),
    },
  ];

  const tableExpandable = {
    expandedRowKeys: expandedComponentRowKeys,
    expandedRowRender: (pill: PillRecord) => renderExpandedComponents(pill.components),
    onExpandedRowsChange: (keys: readonly Key[]) => {
      setExpandedComponentRowKeys(keys.map((key) => Number(key)));
    },
    rowExpandable: (pill: PillRecord) => pill.components.length > 0,
    showExpandColumn: false,
  };

  return (
    <main className="h-full overflow-auto bg-slate-100 p-6">
      <div className="mx-auto flex max-w-[1800px] flex-col gap-6">
        <Card className="shadow-sm">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
            <div className="space-y-2">
              <Typography.Title level={2} className="!mb-0">
                Pills
              </Typography.Title>
              <Typography.Paragraph className="!mb-0 max-w-3xl text-slate-600">
                Log canonical pills, track every period they were taken, and keep the supplement
                facts plus label images together.
              </Typography.Paragraph>
            </div>

            <Button type="primary" size="large" icon={<PlusOutlined />} onClick={openNewPillDrawer}>
              Log pill
            </Button>
          </div>

          <Divider className="!my-5" />

          <div className="grid gap-4 md:grid-cols-3">
            <Card size="small" className="bg-slate-50">
              <Statistic title="All Pills" value={totals.all} />
            </Card>
            <Card size="small" className="bg-emerald-50">
              <Statistic title="Active Pills" value={totals.active} />
            </Card>
            <Card size="small" className="bg-slate-50">
              <Statistic title="Past Pills" value={totals.past} />
            </Card>
          </div>
        </Card>

        <Card
          title="Active pills"
          className="shadow-sm"
          extra={<Typography.Text type="secondary">{activePills.length} rows</Typography.Text>}
        >
          <Table
            rowKey="id"
            size="small"
            columns={columns}
            dataSource={activePills}
            loading={dashboardQuery.isLoading}
            pagination={false}
            scroll={{ x: 1500 }}
            expandable={tableExpandable}
            locale={{
              emptyText: (
                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No active pills yet" />
              ),
            }}
          />
        </Card>

        <Card
          title="Past pills"
          className="shadow-sm"
          extra={<Typography.Text type="secondary">{pastPills.length} rows</Typography.Text>}
        >
          <Table
            rowKey="id"
            size="small"
            columns={columns}
            dataSource={pastPills}
            loading={dashboardQuery.isLoading}
            pagination={false}
            scroll={{ x: 1500 }}
            expandable={tableExpandable}
            locale={{
              emptyText: (
                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No past pills yet" />
              ),
            }}
          />
        </Card>
      </div>

      <Drawer
        title={isHydratingExistingPill ? "Edit pill" : "Log pill"}
        placement="right"
        width={920}
        open={isDrawerOpen}
        onClose={handleCloseDrawer}
        destroyOnHidden={false}
        styles={{
          body: {
            padding: 16,
          },
        }}
        extra={
          <Space>
            <Button onClick={handleCloseDrawer}>Cancel</Button>
            <Button
              type="primary"
              loading={upsertMutation.isPending}
              disabled={isSaveDisabled}
              onClick={() => void form.submit()}
            >
              Save
            </Button>
          </Space>
        }
      >
        <Form<PillFormValues>
          form={form}
          layout="vertical"
          initialValues={createEmptyFormValues()}
          onFinish={(values) => {
            void handleSubmit(values);
          }}
        >
          <Form.Item name="id" hidden>
            <Input />
          </Form.Item>

          <div className="grid gap-3 md:grid-cols-2">
            <Form.Item
              label="Pill name"
              name="name"
              rules={[{ required: true, message: "Enter a pill name." }]}
              className="md:col-span-2"
            >
              <AutoComplete
                options={autocompleteOptions}
                onSearch={(value) => setPillQuery(value)}
                onSelect={handleAutocompleteSelect}
                onChange={(value) => {
                  setPillQuery(value);

                  if (value !== form.getFieldValue("name")) {
                    form.setFieldValue("name", value);
                  }

                  const currentId = form.getFieldValue("id");
                  const selectedPill = searchResults.find((result) => result.id === currentId);
                  if (selectedPill && selectedPill.name !== value) {
                    form.setFieldValue("id", undefined);
                    setIsHydratingExistingPill(false);
                  }
                }}
                placeholder="Start typing to reuse a canonical pill"
                filterOption={false}
              />
            </Form.Item>

            <Form.Item
              label="Default value"
              name="value"
              rules={[{ required: true, message: "Enter the default pill value." }]}
            >
              <Input placeholder="e.g. 2" />
            </Form.Item>

            <Form.Item
              label="Default unit"
              name="unit"
              rules={[{ required: true, message: "Enter the default pill unit." }]}
            >
              <Input placeholder="e.g. capsules" />
            </Form.Item>
          </div>

          <Form.Item label="Note" name="note">
            <Input.TextArea
              rows={3}
              placeholder="Optional note about the pill, brand, or reason for taking it"
            />
          </Form.Item>

          <div className="mb-3 flex items-center justify-between gap-3">
            <Divider className="!my-0 !min-w-0 !flex-1">Images</Divider>

            {watchedImages.length > 0 ? (
              <Button
                size="small"
                loading={isParsingImages}
                disabled={isParsingImages}
                onClick={() => {
                  void handleParseImages();
                }}
              >
                Parse images
              </Button>
            ) : null}
          </div>

          <Form.Item name="images" hidden>
            <Input />
          </Form.Item>

          <Dragger {...uploadProps} className="bg-slate-50">
            <p className="ant-upload-drag-icon">
              <UploadOutlined />
            </p>
            <Typography.Text className="block text-sm font-medium text-slate-800">
              Drag images here or click to upload
            </Typography.Text>
            <Typography.Text type="secondary" className="text-xs">
              All selected images are sent together for pill-label extraction.
            </Typography.Text>
          </Dragger>

          {isParsingImages ? (
            <div className="mt-2 flex items-center gap-2 rounded-lg border border-sky-200 bg-sky-50 px-3 py-2 text-sm text-sky-700">
              <Spin size="small" />
              <Typography.Text className="!text-sky-700">
                Parsing uploaded images and filling the form…
              </Typography.Text>
            </div>
          ) : null}

          <Divider>Date Ranges</Divider>

          <Form.List name="periods">
            {(fields, { add, remove }) => (
              <Space direction="vertical" size={8} className="flex">
                <Table
                  size="small"
                  pagination={false}
                  rowKey="key"
                  dataSource={fields}
                  scroll={{ x: 860 }}
                  columns={[
                    {
                      title: "Start",
                      width: 150,
                      render: (_: unknown, field) => (
                        <>
                          <Form.Item name={[field.name, "id"]} hidden>
                            <Input />
                          </Form.Item>
                          <Form.Item
                            name={[field.name, "startDate"]}
                            rules={[{ required: true, message: "Required" }]}
                            style={{ marginBottom: 0 }}
                            getValueFromEvent={(_, dateString) =>
                              Array.isArray(dateString) ? dateString[0] : dateString
                            }
                            getValueProps={(value) => ({
                              value: value ? dayjs(value, DATE_FORMAT) : null,
                            })}
                          >
                            <DatePicker style={{ width: "100%" }} format={DATE_FORMAT} />
                          </Form.Item>
                        </>
                      ),
                    },
                    {
                      title: "End",
                      width: 150,
                      render: (_: unknown, field) => (
                        <Form.Item
                          name={[field.name, "endDate"]}
                          style={{ marginBottom: 0 }}
                          getValueFromEvent={(_, dateString) =>
                            Array.isArray(dateString) ? dateString[0] : dateString
                          }
                          getValueProps={(value) => ({
                            value: value ? dayjs(value, DATE_FORMAT) : null,
                          })}
                        >
                          <DatePicker style={{ width: "100%" }} format={DATE_FORMAT} allowClear />
                        </Form.Item>
                      ),
                    },
                    {
                      title: "Value",
                      width: 110,
                      render: (_: unknown, field) => (
                        <Form.Item
                          name={[field.name, "valueOverride"]}
                          rules={[{ required: true, message: "Required" }]}
                          style={{ marginBottom: 0 }}
                        >
                          <Input placeholder="Amount" />
                        </Form.Item>
                      ),
                    },
                    {
                      title: "Unit",
                      width: 120,
                      render: (_: unknown, field) => (
                        <Form.Item
                          name={[field.name, "unitOverride"]}
                          rules={[{ required: true, message: "Required" }]}
                          style={{ marginBottom: 0 }}
                        >
                          <Input placeholder="Unit" />
                        </Form.Item>
                      ),
                    },
                    {
                      title: "Timing",
                      width: 140,
                      render: (_: unknown, field) => (
                        <Form.Item
                          name={[field.name, "timing"]}
                          rules={[{ required: true, message: "Required" }]}
                          style={{ marginBottom: 0 }}
                        >
                          <Select
                            options={timingOptions as unknown as { label: string; value: string }[]}
                          />
                        </Form.Item>
                      ),
                    },
                    {
                      width: 110,
                      render: (_: unknown, field) => {
                        const rowValue = form.getFieldValue(["periods", field.name]) as
                          | PillPeriodFormValue
                          | undefined;
                        const isSavedRow = Boolean(rowValue?.id);

                        return (
                          <Button
                            size="small"
                            danger
                            icon={<DeleteOutlined />}
                            loading={isSavedRow && deletingPeriodId === rowValue?.id}
                            onClick={() => {
                              if (isSavedRow && rowValue?.id) {
                                void handleDeleteSavedPeriod(rowValue.id, field.name, remove);
                                return;
                              }

                              remove(field.name);
                            }}
                          />
                        );
                      },
                    },
                  ]}
                />

                <Button
                  size="small"
                  icon={<PlusOutlined />}
                  onClick={() =>
                    add(
                      createBlankPeriod(
                        getNewPeriodDefaults({
                          canonicalValue: form.getFieldValue("value") ?? "",
                          canonicalUnit: form.getFieldValue("unit") ?? "",
                          periods: (form.getFieldValue("periods") ?? []) as PillPeriodFormValue[],
                        }),
                      ),
                    )
                  }
                >
                  Add another range
                </Button>
              </Space>
            )}
          </Form.List>

          <Divider>Supplement Facts</Divider>

          <Form.List name="components">
            {(fields, { add, remove }) => (
              <Space direction="vertical" size={8} className="flex">
                <Table
                  size="small"
                  pagination={false}
                  rowKey="key"
                  dataSource={fields}
                  scroll={{ x: 760 }}
                  columns={[
                    {
                      title: "Name",
                      render: (_: unknown, field) => (
                        <Form.Item name={[field.name, "name"]} style={{ marginBottom: 0 }}>
                          <Input placeholder="Vitamin D3" />
                        </Form.Item>
                      ),
                    },
                    {
                      title: "Value",
                      width: 130,
                      render: (_: unknown, field) => (
                        <Form.Item name={[field.name, "value"]} style={{ marginBottom: 0 }}>
                          <Input placeholder="125" />
                        </Form.Item>
                      ),
                    },
                    {
                      title: "Unit",
                      width: 140,
                      render: (_: unknown, field) => (
                        <Form.Item name={[field.name, "unit"]} style={{ marginBottom: 0 }}>
                          <Input placeholder="mcg" />
                        </Form.Item>
                      ),
                    },
                    {
                      title: "Action",
                      width: 124,
                      render: (_: unknown, field) => (
                        <Space size="small">
                          <Button
                            size="small"
                            icon={<ArrowUpOutlined />}
                            onClick={() => add(getComponentInsertValue(), field.name)}
                            aria-label="Insert component before"
                            title="Insert component before"
                          />
                          <Button
                            size="small"
                            icon={<ArrowDownOutlined />}
                            onClick={() => add(getComponentInsertValue(), field.name + 1)}
                            aria-label="Insert component after"
                            title="Insert component after"
                          />
                          <Button
                            size="small"
                            danger
                            icon={<DeleteOutlined />}
                            onClick={() => remove(field.name)}
                            aria-label="Delete component"
                            title="Delete component"
                          />
                        </Space>
                      ),
                    },
                  ]}
                />

                <Button
                  size="small"
                  icon={<PlusOutlined />}
                  onClick={() => add(getComponentInsertValue())}
                >
                  Add component
                </Button>
              </Space>
            )}
          </Form.List>
        </Form>
      </Drawer>
    </main>
  );
}
