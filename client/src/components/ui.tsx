import dayjs, { type Dayjs } from 'dayjs';
import React, {
	type CSSProperties,
	type HTMLAttributes,
	type Key,
	type ReactElement,
	type ReactNode,
	cloneElement,
	createContext,
	isValidElement,
	useContext,
	useEffect,
	useMemo,
	useRef,
	useState,
	useSyncExternalStore,
} from 'react';
import {
	Button as TButton,
	Card as TCard,
	Checkbox as TCheckbox,
	Spinner as TSpinner,
	Text as TText,
} from 'tamagui';

import useAppContext from '../hooks/useAppContext';

type NamePath = string | number | Array<string | number>;
type SizeName = 'small' | 'middle' | 'large';
type FieldRule = {
	required?: boolean;
	message?: string;
};

export type FormInstance<T extends object = any> = {
	getFieldValue: (name: NamePath) => any;
	setFieldValue: (name: NamePath, value: any) => void;
	setFieldsValue: (values: Partial<T>) => void;
	resetFields: () => void;
	validateFields: (_options?: unknown) => Promise<T>;
	submit: () => void;
	__getAll: () => T;
	__subscribe: (listener: () => void) => () => void;
	__registerField: (name: NamePath, rules?: FieldRule[]) => () => void;
	__setInitialValues: (values: Partial<T>) => void;
	__setSubmit: (handler: (values: T) => void) => void;
};

export type TableColumnsType<T> = Array<TableColumn<T>>;

export type TableColumn<T> = {
	title?: ReactNode;
	dataIndex?: NamePath;
	key?: Key;
	width?: number | string;
	align?: 'left' | 'center' | 'right';
	render?: (value: any, row: T, index: number) => ReactNode;
	onCell?: (row: T, index: number) => HTMLAttributes<HTMLTableCellElement>;
};

export type UploadFile<T = any> = {
	uid: string;
	name: string;
	status?: string;
	url?: string;
	originFileObj?: File;
	response?: T;
};

export type UploadChangeParam<T extends UploadFile = UploadFile> = {
	fileList: T[];
	file: T;
};

export type UploadProps = {
	accept?: string;
	beforeUpload?: (file: File) => boolean;
	disabled?: boolean;
	multiple?: boolean;
	fileList?: UploadFile[];
	showUploadList?: boolean;
	onChange?: (info: UploadChangeParam<UploadFile<any>>) => void;
	onRemove?: (file: UploadFile) => boolean | Promise<boolean>;
	className?: string;
	style?: CSSProperties;
	children?: ReactNode;
};

type TokenSet = {
	colorBgLayout: string;
	colorBgContainer: string;
	colorBorder: string;
	colorBorderSecondary: string;
	colorText: string;
	colorTextSecondary: string;
	colorTextTertiary: string;
	colorFillAlter: string;
	colorFillSecondary: string;
	colorFillTertiary: string;
	colorFillQuaternary: string;
	colorPrimary: string;
	colorPrimaryBg: string;
	colorPrimaryBgHover: string;
	colorPrimaryBorder: string;
	colorInfoBg: string;
	colorInfoBorder: string;
	colorInfoText: string;
	colorSuccess: string;
	colorSuccessBg: string;
	colorSuccessBorder: string;
	colorError: string;
	colorErrorBg: string;
	colorErrorBgHover: string;
	colorErrorBorder: string;
	colorWarning: string;
	colorWarningBg: string;
	colorWarningBorder: string;
	colorWhite: string;
	boxShadowSecondary: string;
};

const lightTokens: TokenSet = {
	colorBgLayout: '#f5f7fb',
	colorBgContainer: '#ffffff',
	colorBorder: '#d7dce5',
	colorBorderSecondary: '#e8ebf1',
	colorText: '#151922',
	colorTextSecondary: '#5f6878',
	colorTextTertiary: '#8b93a1',
	colorFillAlter: '#f4f6fa',
	colorFillSecondary: '#e9edf4',
	colorFillTertiary: '#f0f3f8',
	colorFillQuaternary: '#f8f9fc',
	colorPrimary: '#2563eb',
	colorPrimaryBg: '#eaf1ff',
	colorPrimaryBgHover: '#dbe8ff',
	colorPrimaryBorder: '#9fbcff',
	colorInfoBg: '#eaf1ff',
	colorInfoBorder: '#aac2f7',
	colorInfoText: '#1d4ed8',
	colorSuccess: '#15803d',
	colorSuccessBg: '#e9f8ef',
	colorSuccessBorder: '#9ed8b6',
	colorError: '#dc2626',
	colorErrorBg: '#fff0f0',
	colorErrorBgHover: '#ffe0e0',
	colorErrorBorder: '#f4a7a7',
	colorWarning: '#b45309',
	colorWarningBg: '#fff7e6',
	colorWarningBorder: '#f3c977',
	colorWhite: '#ffffff',
	boxShadowSecondary: '0 12px 28px rgba(15, 23, 42, 0.14)',
};

const darkTokens: TokenSet = {
	colorBgLayout: '#0d1117',
	colorBgContainer: '#151b23',
	colorBorder: '#343b46',
	colorBorderSecondary: '#242c37',
	colorText: '#e8edf4',
	colorTextSecondary: '#a8b3c2',
	colorTextTertiary: '#788391',
	colorFillAlter: '#1b2330',
	colorFillSecondary: '#253041',
	colorFillTertiary: '#202938',
	colorFillQuaternary: '#111721',
	colorPrimary: '#78a5ff',
	colorPrimaryBg: '#14223b',
	colorPrimaryBgHover: '#1b3156',
	colorPrimaryBorder: '#365d9e',
	colorInfoBg: '#14223b',
	colorInfoBorder: '#365d9e',
	colorInfoText: '#a9c5ff',
	colorSuccess: '#65d086',
	colorSuccessBg: '#10281a',
	colorSuccessBorder: '#27663d',
	colorError: '#ff8585',
	colorErrorBg: '#321717',
	colorErrorBgHover: '#461f1f',
	colorErrorBorder: '#773030',
	colorWarning: '#f0bf5b',
	colorWarningBg: '#30240d',
	colorWarningBorder: '#7c5917',
	colorWhite: '#ffffff',
	boxShadowSecondary: '0 18px 34px rgba(0, 0, 0, 0.42)',
};

export const theme = {
	useToken() {
		const { theme: appTheme } = useAppContext();
		return { token: appTheme === 'dark' ? darkTokens : lightTokens };
	},
};

type ToastKind = 'success' | 'error' | 'info' | 'warning';

type Toast = {
	id: number;
	kind: ToastKind;
	text: string;
};

const toastListeners = new Set<(toast: Toast) => void>();
let toastId = 0;

function emitToast(kind: ToastKind, text: ReactNode) {
	const normalized = typeof text === 'string' ? text : String(text ?? '');
	const toast = { id: ++toastId, kind, text: normalized };
	for (const listener of toastListeners) listener(toast);
}

function useMessageApi() {
	return useMemo(
		() => ({
			success: (text: ReactNode) => emitToast('success', text),
			error: (text: ReactNode) => emitToast('error', text),
			info: (text: ReactNode) => emitToast('info', text),
			warning: (text: ReactNode) => emitToast('warning', text),
		}),
		[],
	);
}

export const message = {
	success: (text: ReactNode) => emitToast('success', text),
	error: (text: ReactNode) => emitToast('error', text),
	info: (text: ReactNode) => emitToast('info', text),
	warning: (text: ReactNode) => emitToast('warning', text),
	useMessage() {
		return [useMessageApi(), null] as const;
	},
};

export function MessageViewport() {
	const { token } = theme.useToken();
	const [toasts, setToasts] = useState<Toast[]>([]);

	useEffect(() => {
		const listener = (toast: Toast) => {
			setToasts(current => [...current, toast]);
			window.setTimeout(() => {
				setToasts(current => current.filter(item => item.id !== toast.id));
			}, 3200);
		};
		toastListeners.add(listener);
		return () => {
			toastListeners.delete(listener);
		};
	}, []);

	const colorByKind: Record<ToastKind, string> = {
		success: token.colorSuccess,
		error: token.colorError,
		info: token.colorInfoText,
		warning: token.colorWarning,
	};

	return (
		<div className='ui-toast-viewport'>
			{toasts.map(toast => (
				<div
					key={toast.id}
					className='ui-toast'
					style={{
						background: token.colorBgContainer,
						borderColor: token.colorBorder,
						boxShadow: token.boxShadowSecondary,
					}}
				>
					<span style={{ color: colorByKind[toast.kind], fontWeight: 700 }}>●</span>
					<span>{toast.text}</span>
				</div>
			))}
		</div>
	);
}

function toPath(name: NamePath | undefined): Array<string | number> {
	if (name === undefined) return [];
	return Array.isArray(name) ? name : [name];
}

function getAtPath(source: any, name: NamePath | undefined): any {
	const path = toPath(name);
	let value = source;
	for (const part of path) {
		if (value == null) return undefined;
		value = value[part as keyof typeof value];
	}
	return value;
}

function setAtPath(source: any, name: NamePath, value: any): any {
	const path = toPath(name);
	if (path.length === 0) return value;
	const [head, ...rest] = path;
	const clone = Array.isArray(source) ? [...source] : { ...source };
	if (rest.length === 0) {
		clone[head as keyof typeof clone] = value;
		return clone;
	}
	clone[head as keyof typeof clone] = setAtPath(source?.[head as keyof typeof source], rest, value);
	return clone;
}

function deepMerge<T>(base: T, patch: any): T {
	if (Array.isArray(patch)) return patch as T;
	if (typeof patch !== 'object' || patch === null) return patch as T;
	const output: any = Array.isArray(base) ? [...base] : { ...(base as any) };
	for (const [key, value] of Object.entries(patch)) {
		output[key] = deepMerge(output[key], value);
	}
	return output;
}

function cloneValue<T>(value: T): T {
	if (typeof structuredClone === 'function') {
		return structuredClone(value);
	}
	return JSON.parse(JSON.stringify(value));
}

function isEmptyRequiredValue(value: unknown) {
	return (
		value === undefined ||
		value === null ||
		value === '' ||
		(Array.isArray(value) && value.length === 0)
	);
}

function createFormInstance<T extends object>(): FormInstance<T> {
	let values = {} as T;
	let initialValues = {} as T;
	let submitHandler: ((values: T) => void) | null = null;
	const listeners = new Set<() => void>();
	const fields = new Map<string, { name: NamePath; rules?: FieldRule[] }>();

	const notify = () => {
		for (const listener of listeners) listener();
	};

	const form: FormInstance<T> = {
		getFieldValue(name) {
			return getAtPath(values, name);
		},
		setFieldValue(name, value) {
			values = setAtPath(values, name, value);
			notify();
		},
		setFieldsValue(nextValues) {
			values = deepMerge(values, nextValues);
			notify();
		},
		resetFields() {
			values = cloneValue(initialValues);
			notify();
		},
		async validateFields() {
			const errors: string[] = [];
			for (const field of fields.values()) {
				if (!field.rules?.some(rule => rule.required)) continue;
				const value = getAtPath(values, field.name);
				if (isEmptyRequiredValue(value)) {
					errors.push(field.rules.find(rule => rule.required)?.message ?? 'Required');
				}
			}
			if (errors.length > 0) {
				throw new Error(errors[0]);
			}
			return values;
		},
		submit() {
			void form.validateFields().then(validValues => {
				submitHandler?.(validValues);
			});
		},
		__getAll() {
			return values;
		},
		__subscribe(listener) {
			listeners.add(listener);
			return () => listeners.delete(listener);
		},
		__registerField(name, rules) {
			const key = JSON.stringify(toPath(name));
			fields.set(key, { name, rules });
			return () => {
				fields.delete(key);
			};
		},
		__setInitialValues(nextValues) {
			initialValues = cloneValue(nextValues as T);
			values = deepMerge(cloneValue(nextValues as T), values);
			notify();
		},
		__setSubmit(handler) {
			submitHandler = handler;
		},
	};

	return form;
}

const FormContext = createContext<FormInstance | null>(null);

type FormProps<T extends object> = {
	form: FormInstance<T>;
	initialValues?: Partial<T>;
	onFinish?: (values: T) => void;
	children?: ReactNode;
	className?: string;
	style?: CSSProperties;
	layout?: 'vertical' | 'horizontal';
};

function FormRoot<T extends object>({
	form,
	initialValues,
	onFinish,
	children,
	className,
	style,
}: FormProps<T>) {
	const didInitializeRef = useRef(false);

	useEffect(() => {
		if (didInitializeRef.current) return;
		didInitializeRef.current = true;
		form.__setInitialValues(initialValues ?? {});
	}, [form, initialValues]);

	useEffect(() => {
		form.__setSubmit(values => onFinish?.(values as T));
	}, [form, onFinish]);

	return (
		<FormContext.Provider value={form}>
			<form
				className={className}
				style={style}
				onSubmit={event => {
					event.preventDefault();
					form.submit();
				}}
			>
				{children}
			</form>
		</FormContext.Provider>
	);
}

function useForm<T extends object>(): [FormInstance<T>] {
	const ref = useRef<FormInstance<T> | null>(null);
	if (!ref.current) {
		ref.current = createFormInstance<T>();
	}
	return [ref.current];
}

function useWatch(name: NamePath | undefined, form: FormInstance) {
	return useSyncExternalStore(
		form.__subscribe,
		() => (name === undefined ? form.__getAll() : getAtPath(form.__getAll(), name)),
		() => undefined,
	);
}

type FormItemProps = {
	name?: NamePath;
	label?: ReactNode;
	required?: boolean;
	rules?: FieldRule[];
	children?: ReactNode;
	hidden?: boolean;
	style?: CSSProperties;
	getValueProps?: (value: any) => Record<string, unknown>;
	layout?: string;
	labelCol?: unknown;
	wrapperCol?: unknown;
	labelAlign?: string;
};

function FormItem({ name, label, required, rules, children, hidden, style }: FormItemProps) {
	const form = useContext(FormContext);
	const value = form && name !== undefined ? useWatch(name, form) : undefined;
	const mergedRules = useMemo(() => {
		if (!required) return rules;
		return [{ required: true }, ...(rules ?? [])];
	}, [required, rules]);

	useEffect(() => {
		if (!form || name === undefined) return;
		return form.__registerField(name, mergedRules);
	}, [form, name, mergedRules]);

	if (hidden) {
		return null;
	}

	const child =
		form && name !== undefined && isValidElement(children)
			? cloneElement(children as ReactElement<any>, {
					value: value ?? '',
					onChange: (next: any) => {
						const nextValue = next?.target ? next.target.value : next;
						form.setFieldValue(name, nextValue);
						(children as ReactElement<any>).props.onChange?.(next);
					},
				})
			: children;

	return (
		<label className='ui-form-item' style={style}>
			{label ? (
				<span className='ui-form-label'>
					{label}
					{required || rules?.some(rule => rule.required) ? <span> *</span> : null}
				</span>
			) : null}
			{child}
		</label>
	);
}

type FormListField = { key: number; name: number };

function FormList({
	name,
	children,
}: {
	name: NamePath;
	children: (
		fields: FormListField[],
		operations: {
			add: (value: any, index?: number) => void;
			remove: (index: number | number[]) => void;
		},
	) => ReactNode;
}) {
	const form = useContext(FormContext);
	if (!form) throw new Error('Form.List must be used inside Form.');
	const values = (useWatch(name, form) ?? []) as any[];
	const fields = values.map((_value, index) => ({ key: index, name: index }));

	const operations = useMemo(
		() => ({
			add(value: any, index?: number) {
				const next = [...values];
				const insertionIndex = index === undefined ? next.length : index;
				next.splice(insertionIndex, 0, value);
				form.setFieldValue(name, next);
			},
			remove(index: number | number[]) {
				const removeSet = new Set(Array.isArray(index) ? index : [index]);
				form.setFieldValue(
					name,
					values.filter((_value, itemIndex) => !removeSet.has(itemIndex)),
				);
			},
		}),
		[form, name, values],
	);

	return <>{children(fields, operations)}</>;
}

export const Form = Object.assign(FormRoot, {
	useForm,
	useWatch,
	Item: FormItem,
	List: FormList,
});

function sizeToHeight(size?: SizeName) {
	if (size === 'small') return 28;
	if (size === 'large') return 42;
	return 34;
}

export function Button({
	children,
	type,
	danger,
	loading,
	disabled,
	icon,
	htmlType,
	size,
	className,
	style,
	onClick,
	...rest
}: {
	children?: ReactNode;
	type?: 'primary' | 'default' | 'link' | 'text';
	danger?: boolean;
	loading?: boolean;
	disabled?: boolean;
	icon?: ReactNode;
	htmlType?: 'button' | 'submit' | 'reset';
	size?: SizeName;
	className?: string;
	style?: CSSProperties;
	onClick?: React.MouseEventHandler<HTMLButtonElement>;
	[key: string]: any;
}) {
	const { token } = theme.useToken();
	const isPrimary = type === 'primary';
	const isLink = type === 'link' || type === 'text';

	return (
		<TButton
			asChild={false}
			type={htmlType ?? 'button'}
			disabled={disabled || loading}
			className={className}
			style={
				{
					height: sizeToHeight(size),
					borderRadius: 6,
					paddingInline: isLink ? 0 : size === 'large' ? 16 : 12,
					background: isLink
						? 'transparent'
						: isPrimary
							? danger
								? token.colorError
								: token.colorPrimary
							: token.colorBgContainer,
					borderColor: isLink ? 'transparent' : danger ? token.colorErrorBorder : token.colorBorder,
					color: isPrimary ? token.colorWhite : danger ? token.colorError : token.colorText,
					borderWidth: isLink ? 0 : 1,
					borderStyle: 'solid',
					...style,
				} as any
			}
			onClick={onClick as any}
			{...rest}
		>
			<span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
				{loading ? <TSpinner size='small' /> : icon}
				{children ? <TText color='inherit'>{children}</TText> : null}
			</span>
		</TButton>
	);
}

export function Card({
	title,
	extra,
	children,
	className,
	style,
	styles,
}: {
	title?: ReactNode;
	extra?: ReactNode;
	children?: ReactNode;
	className?: string;
	style?: CSSProperties;
	size?: SizeName;
	styles?: { body?: CSSProperties };
}) {
	const { token } = theme.useToken();

	return (
		<TCard
			className={className}
			style={
				{
					background: token.colorBgContainer,
					border: `1px solid ${token.colorBorderSecondary}`,
					borderRadius: 8,
					overflow: 'hidden',
					...style,
				} as any
			}
		>
			{title || extra ? (
				<div
					style={{
						display: 'flex',
						alignItems: 'center',
						justifyContent: 'space-between',
						gap: 12,
						padding: 12,
						borderBottom: `1px solid ${token.colorBorderSecondary}`,
					}}
				>
					<TText fontWeight='700'>{title}</TText>
					{extra}
				</div>
			) : null}
			<div className='ui-card-body' style={styles?.body}>
				{children}
			</div>
		</TCard>
	);
}

function typographyColor(type: string | undefined, token: TokenSet) {
	if (type === 'secondary') return token.colorTextSecondary;
	if (type === 'danger') return token.colorError;
	return token.colorText;
}

function TextPrimitive({
	children,
	type,
	strong,
	className,
	style,
	...rest
}: {
	children?: ReactNode;
	type?: 'secondary' | 'danger';
	strong?: boolean;
	className?: string;
	style?: CSSProperties;
	[key: string]: any;
}) {
	const { token } = theme.useToken();
	return (
		<span
			className={className}
			style={{
				color: typographyColor(type, token),
				fontWeight: strong ? 700 : undefined,
				...style,
			}}
			{...rest}
		>
			{children}
		</span>
	);
}

function Title({
	level = 1,
	children,
	className,
	style,
}: {
	level?: 1 | 2 | 3 | 4 | 5;
	children?: ReactNode;
	className?: string;
	style?: CSSProperties;
}) {
	const Tag = `h${level}` as React.ElementType;
	return (
		<Tag className={className} style={style as any}>
			{children}
		</Tag>
	);
}

function Paragraph({
	children,
	type,
	copyable,
	className,
	style,
	ellipsis,
	onClick,
}: {
	children?: ReactNode;
	type?: 'secondary' | 'danger';
	copyable?: boolean;
	className?: string;
	style?: CSSProperties;
	ellipsis?: { rows: number };
	onClick?: () => void;
}) {
	const { token } = theme.useToken();
	const ellipsisStyle: CSSProperties | undefined = ellipsis
		? {
				display: '-webkit-box',
				WebkitLineClamp: ellipsis.rows,
				WebkitBoxOrient: 'vertical' as any,
				overflow: 'hidden',
			}
		: undefined;

	return (
		<p
			className={className}
			style={{ color: typographyColor(type, token), margin: 0, ...ellipsisStyle, ...style }}
			onClick={onClick}
		>
			{children}
			{copyable ? (
				<button
					type='button'
					className='ui-inline-copy'
					onClick={() => void navigator.clipboard.writeText(String(children ?? ''))}
				>
					Copy
				</button>
			) : null}
		</p>
	);
}

function LinkText({
	children,
	href,
	target,
	rel,
	className,
	style,
}: {
	children?: ReactNode;
	href?: string;
	target?: string;
	rel?: string;
	className?: string;
	style?: CSSProperties;
}) {
	const { token } = theme.useToken();
	return (
		<a
			href={href}
			target={target}
			rel={rel}
			className={className}
			style={{ color: token.colorPrimary, textDecoration: 'none', ...style }}
		>
			{children}
		</a>
	);
}

export const Typography = {
	Text: TextPrimitive,
	Title,
	Paragraph,
	Link: LinkText,
};

export function Space({
	children,
	direction = 'horizontal',
	size = 8,
	wrap,
	align,
	className,
	style,
}: {
	children?: ReactNode;
	direction?: 'horizontal' | 'vertical';
	size?: number | string | [number, number];
	wrap?: boolean;
	align?: CSSProperties['alignItems'];
	className?: string;
	style?: CSSProperties;
}) {
	const gap = Array.isArray(size)
		? `${size[1]}px ${size[0]}px`
		: typeof size === 'number'
			? size
			: size;
	return (
		<div
			className={className}
			style={{
				display: 'flex',
				flexDirection: direction === 'vertical' ? 'column' : 'row',
				flexWrap: wrap ? 'wrap' : undefined,
				alignItems: align ?? (direction === 'vertical' ? 'stretch' : 'center'),
				gap,
				...style,
			}}
		>
			{children}
		</div>
	);
}

Space.Compact = function SpaceCompact({
	children,
	style,
}: {
	children?: ReactNode;
	style?: CSSProperties;
}) {
	return (
		<div className='ui-space-compact' style={style}>
			{children}
		</div>
	);
};

export function Flex({
	children,
	vertical,
	align,
	justify,
	gap,
	wrap,
	className,
	style,
}: {
	children?: ReactNode;
	vertical?: boolean;
	align?: CSSProperties['alignItems'];
	justify?: CSSProperties['justifyContent'];
	gap?: number | string;
	wrap?: boolean;
	className?: string;
	style?: CSSProperties;
}) {
	return (
		<div
			className={className}
			style={{
				display: 'flex',
				flexDirection: vertical ? 'column' : 'row',
				alignItems: align,
				justifyContent: justify,
				gap,
				flexWrap: wrap ? 'wrap' : undefined,
				...style,
			}}
		>
			{children}
		</div>
	);
}

export function Alert({
	type = 'info',
	message,
	description,
	showIcon,
	closable,
	onClose,
	className,
}: {
	type?: 'success' | 'info' | 'warning' | 'error';
	message?: ReactNode;
	description?: ReactNode;
	showIcon?: boolean;
	closable?: boolean;
	onClose?: () => void;
	className?: string;
}) {
	const { token } = theme.useToken();
	const palette = {
		success: [token.colorSuccessBg, token.colorSuccessBorder, token.colorSuccess],
		info: [token.colorInfoBg, token.colorInfoBorder, token.colorInfoText],
		warning: [token.colorWarningBg, token.colorWarningBorder, token.colorWarning],
		error: [token.colorErrorBg, token.colorErrorBorder, token.colorError],
	}[type];

	return (
		<div
			className={className}
			style={{
				display: 'flex',
				gap: 10,
				border: `1px solid ${palette[1]}`,
				borderRadius: 8,
				background: palette[0],
				color: token.colorText,
				padding: 12,
			}}
		>
			{showIcon ? <span style={{ color: palette[2], fontWeight: 700 }}>●</span> : null}
			<div style={{ minWidth: 0, flex: 1 }}>
				{message ? <div style={{ fontWeight: 700 }}>{message}</div> : null}
				{description ? <div style={{ marginTop: message ? 4 : 0 }}>{description}</div> : null}
			</div>
			{closable ? (
				<button type='button' className='ui-icon-button' onClick={onClose}>
					×
				</button>
			) : null}
		</div>
	);
}

export function Empty({
	description,
}: {
	description?: ReactNode;
	image?: unknown;
	style?: CSSProperties;
}) {
	const { token } = theme.useToken();
	return (
		<div className='ui-empty'>
			<span style={{ color: token.colorTextTertiary }}>No data</span>
			{description ? <span style={{ color: token.colorTextSecondary }}>{description}</span> : null}
		</div>
	);
}

Empty.PRESENTED_IMAGE_SIMPLE = 'simple';

export function Spin({ size }: { size?: SizeName }) {
	return <TSpinner size={size === 'large' ? 'large' : 'small'} />;
}

function readInputValue(event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) {
	return event.target.value;
}

type TextAreaAutoSize = boolean | { minRows?: number; maxRows?: number };

function normalizeTextAreaAutoSize(autoSize?: TextAreaAutoSize) {
	if (!autoSize) return undefined;
	return autoSize === true ? {} : autoSize;
}

function readPixelValue(value: string) {
	return Number.parseFloat(value) || 0;
}

function resizeTextAreaHeight(element: HTMLTextAreaElement, autoSize?: TextAreaAutoSize) {
	const config = normalizeTextAreaAutoSize(autoSize);
	if (!config) return;

	const styles = window.getComputedStyle(element);
	const lineHeight =
		readPixelValue(styles.lineHeight) || readPixelValue(styles.fontSize) * 1.2 || 24;
	const verticalPadding = readPixelValue(styles.paddingTop) + readPixelValue(styles.paddingBottom);
	const verticalBorder =
		styles.boxSizing === 'border-box'
			? readPixelValue(styles.borderTopWidth) + readPixelValue(styles.borderBottomWidth)
			: 0;
	const minHeight = config.minRows
		? config.minRows * lineHeight + verticalPadding + verticalBorder
		: 0;
	const maxHeight = config.maxRows
		? config.maxRows * lineHeight + verticalPadding + verticalBorder
		: Number.POSITIVE_INFINITY;

	element.style.height = 'auto';
	const nextHeight = Math.max(
		minHeight,
		Math.min(element.scrollHeight + verticalBorder, maxHeight),
	);
	element.style.height = `${nextHeight}px`;
	element.style.overflowY = element.scrollHeight + verticalBorder > maxHeight ? 'auto' : 'hidden';
}

function InputRoot(props: React.InputHTMLAttributes<HTMLInputElement>) {
	const { token } = theme.useToken();
	return (
		<input
			{...props}
			className={['ui-input', props.className].filter(Boolean).join(' ')}
			style={
				{
					background: token.colorBgContainer,
					borderColor: token.colorBorder,
					color: token.colorText,
					...props.style,
				} as CSSProperties
			}
		/>
	);
}

function TextArea({
	autoSize,
	onChange,
	onInput,
	rows,
	style,
	...props
}: React.TextareaHTMLAttributes<HTMLTextAreaElement> & {
	autoSize?: TextAreaAutoSize;
}) {
	const { token } = theme.useToken();
	const ref = useRef<HTMLTextAreaElement>(null);
	const autoSizeConfig = normalizeTextAreaAutoSize(autoSize);

	useEffect(() => {
		if (!ref.current) return;
		resizeTextAreaHeight(ref.current, autoSize);
	}, [autoSize, props.value, props.defaultValue]);

	return (
		<textarea
			{...props}
			ref={ref}
			rows={autoSizeConfig?.minRows ?? rows}
			className={['ui-input', props.className].filter(Boolean).join(' ')}
			onChange={event => {
				onChange?.(event);
				resizeTextAreaHeight(event.currentTarget, autoSize);
			}}
			onInput={event => {
				onInput?.(event);
				resizeTextAreaHeight(event.currentTarget, autoSize);
			}}
			style={{
				resize: autoSizeConfig ? 'none' : undefined,
				background: token.colorBgContainer,
				borderColor: token.colorBorder,
				color: token.colorText,
				...style,
			}}
		/>
	);
}

export const Input = Object.assign(InputRoot, {
	TextArea,
});

export function InputNumber({
	value,
	onChange,
	...props
}: Omit<React.InputHTMLAttributes<HTMLInputElement>, 'onChange'> & {
	value?: number | string;
	onChange?: (value: number | null) => void;
}) {
	return (
		<InputRoot
			{...props}
			type='number'
			value={value ?? ''}
			onChange={event => {
				const next = readInputValue(event);
				onChange?.(next === '' ? null : Number(next));
			}}
		/>
	);
}

export function Checkbox({
	checked,
	onChange,
	children,
	disabled,
	className,
	style,
}: {
	checked?: boolean;
	onChange?: (event: { target: { checked: boolean } }) => void;
	children?: ReactNode;
	disabled?: boolean;
	className?: string;
	style?: CSSProperties;
}) {
	return (
		<label className={['ui-checkbox-label', className].filter(Boolean).join(' ')} style={style}>
			<TCheckbox
				checked={checked}
				disabled={disabled}
				onCheckedChange={next => onChange?.({ target: { checked: Boolean(next) } })}
			>
				<TCheckbox.Indicator />
			</TCheckbox>
			{children ? <span>{children}</span> : null}
		</label>
	);
}

Checkbox.Group = function CheckboxGroup({
	options,
	value = [],
	onChange,
	className,
}: {
	options: Array<{ label: ReactNode; value: string }>;
	value?: string[];
	onChange?: (value: string[]) => void;
	className?: string;
}) {
	return (
		<div className={['ui-checkbox-group', className].filter(Boolean).join(' ')}>
			{options.map(option => (
				<Checkbox
					key={option.value}
					checked={value.includes(option.value)}
					onChange={event => {
						const next = event.target.checked
							? [...value, option.value]
							: value.filter(item => item !== option.value);
						onChange?.(next);
					}}
				>
					{option.label}
				</Checkbox>
			))}
		</div>
	);
};

export function Switch({
	checked,
	onChange,
	checkedChildren,
	unCheckedChildren,
	size,
	disabled,
	...rest
}: {
	checked?: boolean;
	onChange?: (checked: boolean) => void;
	checkedChildren?: ReactNode;
	unCheckedChildren?: ReactNode;
	size?: SizeName;
	disabled?: boolean;
	[key: string]: any;
}) {
	return (
		<button
			type='button'
			className={`ui-switch ${checked ? 'ui-switch-checked' : ''} ${size === 'small' ? 'ui-switch-small' : ''}`}
			disabled={disabled}
			onClick={() => onChange?.(!checked)}
			{...rest}
		>
			<span className='ui-switch-thumb' />
			<span className='ui-switch-copy'>{checked ? checkedChildren : unCheckedChildren}</span>
		</button>
	);
}

type SelectOption = { label: ReactNode; value: string };

export function Select({
	value,
	options = [],
	mode,
	placeholder,
	disabled,
	loading,
	onChange,
	style,
	className,
	allowClear,
}: {
	value?: string | string[];
	options?: SelectOption[];
	mode?: 'multiple' | 'tags';
	placeholder?: string;
	disabled?: boolean;
	loading?: boolean;
	onChange?: (value: any) => void;
	style?: CSSProperties;
	size?: SizeName;
	className?: string;
	tokenSeparators?: string[];
	maxTagCount?: number | 'responsive';
	maxTagPlaceholder?: ReactNode;
	tagRender?: (props: {
		label: ReactNode;
		value: string;
		closable: boolean;
		onClose: () => void;
	}) => ReactNode;
	allowClear?: boolean;
	getPopupContainer?: (node: HTMLElement) => HTMLElement;
}) {
	const [draft, setDraft] = useState('');
	const values = Array.isArray(value) ? value : value ? [value] : [];

	if (mode === 'multiple' || mode === 'tags') {
		function addValue(raw: string) {
			const next = raw.trim();
			if (!next || values.includes(next)) return;
			onChange?.([...values, next]);
			setDraft('');
		}

		return (
			<div className={['ui-select-tags', className].filter(Boolean).join(' ')} style={style}>
				{values.map(item => {
					const option = options.find(candidate => candidate.value === item);
					return (
						<Tag
							key={item}
							closable
							onClose={() => onChange?.(values.filter(value => value !== item))}
						>
							{option?.label ?? item}
						</Tag>
					);
				})}
				<input
					disabled={disabled || loading}
					value={draft}
					placeholder={values.length === 0 ? placeholder : undefined}
					list={`select-options-${placeholder ?? 'values'}`}
					onChange={event => {
						const next = event.target.value;
						if (next.includes(',')) {
							next.split(',').forEach(addValue);
							return;
						}
						setDraft(next);
					}}
					onKeyDown={event => {
						if (event.key === 'Enter') {
							event.preventDefault();
							addValue(draft);
						}
						if (event.key === 'Backspace' && !draft && values.length > 0) {
							onChange?.(values.slice(0, -1));
						}
					}}
					onBlur={() => {
						if (mode === 'tags') addValue(draft);
					}}
				/>
				<datalist id={`select-options-${placeholder ?? 'values'}`}>
					{options.map(option => (
						<option key={option.value} value={option.value} />
					))}
				</datalist>
			</div>
		);
	}

	return (
		<select
			className={['ui-input', className].filter(Boolean).join(' ')}
			disabled={disabled || loading}
			value={(value as string | undefined) ?? ''}
			style={style}
			onChange={event => onChange?.(event.target.value || undefined)}
		>
			<option value=''>{placeholder ?? (allowClear ? '' : 'Select')}</option>
			{options.map(option => (
				<option key={option.value} value={option.value}>
					{option.label}
				</option>
			))}
		</select>
	);
}

export function AutoComplete({
	value,
	options = [],
	onSearch,
	onSelect,
	onChange,
	placeholder,
}: {
	value?: string;
	options?: Array<SelectOption & { pill?: unknown }>;
	onSearch?: (value: string) => void;
	onSelect?: (value: string, option: any) => void;
	onChange?: (value: string) => void;
	placeholder?: string;
	filterOption?: boolean;
}) {
	const listId = 'autocomplete-options';
	return (
		<>
			<Input
				value={value ?? ''}
				placeholder={placeholder}
				list={listId}
				onChange={event => {
					const next = event.target.value;
					onChange?.(next);
					onSearch?.(next);
					const selected = options.find(option => option.value === next);
					if (selected) {
						onSelect?.(next, selected);
					}
				}}
			/>
			<datalist id={listId}>
				{options.map(option => (
					<option key={option.value} value={option.value} />
				))}
			</datalist>
		</>
	);
}

export function Tag({
	children,
	color,
	icon,
	closable,
	onClose,
	className,
	style,
	onClick,
}: {
	children?: ReactNode;
	color?: string;
	icon?: ReactNode;
	closable?: boolean;
	onClose?: () => void;
	className?: string;
	style?: CSSProperties;
	onClick?: () => void;
}) {
	const { token } = theme.useToken();
	const knownColor =
		color === 'success'
			? token.colorSuccess
			: color === 'error'
				? token.colorError
				: color === 'processing'
					? token.colorPrimary
					: color === 'default'
						? token.colorTextSecondary
						: color;
	return (
		<span
			className={['ui-tag', className].filter(Boolean).join(' ')}
			style={{
				borderColor: knownColor ?? token.colorBorder,
				background: knownColor ? `${knownColor}22` : token.colorFillAlter,
				color: knownColor ?? token.colorText,
				...style,
			}}
			onClick={onClick}
		>
			{icon}
			{children}
			{closable ? (
				<button
					type='button'
					onClick={event => {
						event.stopPropagation();
						onClose?.();
					}}
				>
					×
				</button>
			) : null}
		</span>
	);
}

export function Badge({
	count,
	children,
}: {
	count?: number;
	size?: SizeName;
	offset?: [number, number];
	children?: ReactNode;
}) {
	return (
		<span className='ui-badge'>
			{children}
			{count ? <span className='ui-badge-count'>{count}</span> : null}
		</span>
	);
}

export function Divider({ children, className }: { children?: ReactNode; className?: string }) {
	return (
		<div className={['ui-divider', className].filter(Boolean).join(' ')}>
			<span>{children}</span>
		</div>
	);
}

export function Tooltip({ children }: { title?: ReactNode; children?: ReactNode }) {
	return <>{children}</>;
}

export function Popover({
	children,
	content,
	open,
	onOpenChange,
}: {
	children?: ReactNode;
	content?: ReactNode;
	trigger?: string;
	open?: boolean;
	onOpenChange?: (open: boolean) => void;
	placement?: string;
	destroyOnHidden?: boolean;
}) {
	return (
		<span className='ui-popover-shell'>
			<span onClick={() => onOpenChange?.(!open)}>{children}</span>
			{open ? <div className='ui-popover-content'>{content}</div> : null}
		</span>
	);
}

export function Popconfirm({
	children,
	title,
	description,
	onConfirm,
	disabled,
}: {
	children: ReactElement;
	title?: ReactNode;
	description?: ReactNode;
	okText?: string;
	okButtonProps?: unknown;
	onConfirm?: () => void;
	disabled?: boolean;
}) {
	if (!isValidElement(children)) return children;
	const child = children as ReactElement<any>;
	return cloneElement(child, {
		onClick: (event: React.MouseEvent) => {
			child.props.onClick?.(event);
			if (disabled) return;
			const text = [title, description].filter(Boolean).join('\n\n');
			if (window.confirm(text || 'Continue?')) {
				onConfirm?.();
			}
		},
	});
}

export function Table<T>({
	columns,
	dataSource,
	rowKey,
	loading,
	scroll,
	expandable,
	onRow,
	className,
}: {
	columns: TableColumnsType<T>;
	dataSource: T[];
	rowKey: keyof T | ((row: T) => Key);
	size?: SizeName;
	pagination?: false;
	loading?: boolean;
	scroll?: { x?: number | string };
	expandable?: {
		expandedRowKeys?: readonly Key[];
		expandedRowRender?: (row: T) => ReactNode;
		onExpandedRowsChange?: (keys: readonly Key[]) => void;
		rowExpandable?: (row: T) => boolean;
		showExpandColumn?: boolean;
	};
	onRow?: (row: T) => HTMLAttributes<HTMLTableRowElement>;
	className?: string;
	tableLayout?: CSSProperties['tableLayout'];
}) {
	const { token } = theme.useToken();
	const getKey = (row: T) => (typeof rowKey === 'function' ? rowKey(row) : (row[rowKey] as Key));
	const expandedKeys = expandable?.expandedRowKeys?.map(String) ?? [];
	const showExpandColumn = Boolean(expandable && expandable.showExpandColumn !== false);

	function toggleRow(key: Key) {
		const keyText = String(key);
		const next = expandedKeys.includes(keyText)
			? expandedKeys.filter(item => item !== keyText)
			: [...expandedKeys, keyText];
		expandable?.onExpandedRowsChange?.(next);
	}

	return (
		<div className='ui-table-shell' style={{ overflowX: scroll?.x ? 'auto' : undefined }}>
			<table
				className={['ui-table', className].filter(Boolean).join(' ')}
				style={{ minWidth: scroll?.x, borderColor: token.colorBorderSecondary }}
			>
				<thead>
					<tr>
						{showExpandColumn ? <th style={{ width: 34 }} /> : null}
						{columns.map((column, index) => (
							<th
								key={String(column.key ?? column.dataIndex ?? index)}
								style={{
									width: column.width,
									textAlign: column.align,
									color: token.colorTextSecondary,
									background: token.colorFillAlter,
									borderColor: token.colorBorderSecondary,
								}}
							>
								{column.title}
							</th>
						))}
					</tr>
				</thead>
				<tbody>
					{loading ? (
						<tr>
							<td colSpan={columns.length + (showExpandColumn ? 1 : 0)}>
								<div className='ui-table-empty'>
									<Spin />
								</div>
							</td>
						</tr>
					) : dataSource.length === 0 ? (
						<tr>
							<td colSpan={columns.length + (showExpandColumn ? 1 : 0)}>
								<div className='ui-table-empty'>No rows</div>
							</td>
						</tr>
					) : (
						dataSource.map((row, rowIndex) => {
							const key = getKey(row);
							const keyText = String(key);
							const rowProps = onRow?.(row) ?? {};
							const canExpand = expandable?.rowExpandable?.(row) ?? Boolean(expandable);
							const isExpanded = expandedKeys.includes(keyText);

							return (
								<React.Fragment key={keyText}>
									<tr {...rowProps}>
										{showExpandColumn ? (
											<td style={{ width: 34 }}>
												{canExpand ? (
													<button
														type='button'
														className='ui-icon-button'
														onClick={() => toggleRow(key)}
													>
														{isExpanded ? '−' : '+'}
													</button>
												) : null}
											</td>
										) : null}
										{columns.map((column, columnIndex) => {
											const value = column.dataIndex ? getAtPath(row, column.dataIndex) : undefined;
											const cellProps = column.onCell?.(row, rowIndex) ?? {};
											return (
												<td
													key={String(column.key ?? column.dataIndex ?? columnIndex)}
													{...cellProps}
													style={{
														textAlign: column.align,
														borderColor: token.colorBorderSecondary,
														...cellProps.style,
													}}
												>
													{column.render
														? column.render(value, row, rowIndex)
														: (value as ReactNode)}
												</td>
											);
										})}
									</tr>
									{isExpanded && expandable?.expandedRowRender ? (
										<tr>
											<td colSpan={columns.length + (showExpandColumn ? 1 : 0)}>
												{expandable.expandedRowRender(row)}
											</td>
										</tr>
									) : null}
								</React.Fragment>
							);
						})
					)}
				</tbody>
			</table>
		</div>
	);
}

function DatePickerRoot({
	value,
	format = 'YYYY-MM-DD',
	onChange,
	allowClear,
}: {
	value?: Dayjs | null;
	format?: string;
	onChange?: (value: Dayjs | null, dateString: string) => void;
	allowClear?: boolean;
}) {
	return (
		<Input
			type='date'
			value={value?.isValid() ? value.format('YYYY-MM-DD') : ''}
			onChange={event => {
				const next = event.target.value;
				if (!next && allowClear) {
					onChange?.(null, '');
					return;
				}
				const parsed = dayjs(next);
				onChange?.(parsed, parsed.format(format));
			}}
		/>
	);
}

function RangePicker({
	value,
	onChange,
	format = 'YYYY-MM-DD',
}: {
	value?: [Dayjs | null, Dayjs | null] | null;
	onChange?: (values: [Dayjs | null, Dayjs | null], dateStrings: [string, string]) => void;
	format?: string;
	allowEmpty?: [boolean, boolean];
	style?: CSSProperties;
}) {
	const [start, end] = value ?? [null, null];
	function emit(nextStart: string, nextEnd: string) {
		const values: [Dayjs | null, Dayjs | null] = [
			nextStart ? dayjs(nextStart) : null,
			nextEnd ? dayjs(nextEnd) : null,
		];
		onChange?.(values, [
			values[0]?.isValid() ? values[0].format(format) : '',
			values[1]?.isValid() ? values[1].format(format) : '',
		]);
	}

	return (
		<div className='ui-date-range'>
			<Input
				type='date'
				value={start?.isValid() ? start.format('YYYY-MM-DD') : ''}
				onChange={event => emit(event.target.value, end?.format('YYYY-MM-DD') ?? '')}
			/>
			<Input
				type='date'
				value={end?.isValid() ? end.format('YYYY-MM-DD') : ''}
				onChange={event => emit(start?.format('YYYY-MM-DD') ?? '', event.target.value)}
			/>
		</div>
	);
}

export const DatePicker = Object.assign(DatePickerRoot, {
	RangePicker,
});

export function ColorPicker({
	value,
	onChange,
}: {
	value?: string;
	disabledAlpha?: boolean;
	presets?: unknown[];
	onChange?: (value: { toHexString: () => string }) => void;
	showText?: boolean;
}) {
	return (
		<Input
			type='color'
			value={value ?? '#2563eb'}
			onChange={event => {
				const next = event.target.value;
				onChange?.({ toHexString: () => next });
			}}
			style={{ width: 52, padding: 4 }}
		/>
	);
}

export function Drawer({
	open,
	onClose,
	title,
	extra,
	children,
	width = 720,
	destroyOnHidden = true,
	afterOpenChange,
	styles,
}: {
	open?: boolean;
	onClose?: () => void;
	title?: ReactNode;
	extra?: ReactNode;
	children?: ReactNode;
	width?: number;
	placement?: string;
	destroyOnHidden?: boolean;
	afterOpenChange?: (open: boolean) => void;
	styles?: { body?: CSSProperties };
}) {
	const afterOpenChangeRef = useRef(afterOpenChange);
	const previousOpenRef = useRef<boolean | undefined>(undefined);

	useEffect(() => {
		afterOpenChangeRef.current = afterOpenChange;
	}, [afterOpenChange]);

	useEffect(() => {
		const nextOpen = Boolean(open);
		if (previousOpenRef.current === nextOpen) {
			return;
		}

		previousOpenRef.current = nextOpen;
		afterOpenChangeRef.current?.(nextOpen);
	}, [open]);

	if (!open && destroyOnHidden) return null;

	return (
		<div className='ui-drawer-root' style={{ display: open ? 'block' : 'none' }}>
			<div className='ui-drawer-mask' onClick={onClose} />
			<aside className='ui-drawer' style={{ width }}>
				<header className='ui-drawer-header'>
					<strong>{title}</strong>
					<div className='ui-drawer-extra'>{extra}</div>
					<button type='button' className='ui-icon-button' onClick={onClose}>
						×
					</button>
				</header>
				<div className='ui-drawer-body' style={styles?.body}>
					{children}
				</div>
			</aside>
		</div>
	);
}

export function Tabs({
	activeKey,
	onChange,
	items,
}: {
	activeKey?: string;
	onChange?: (key: string) => void;
	items: Array<{ key: string; label: ReactNode; children: ReactNode }>;
}) {
	const activeItem = items.find(item => item.key === activeKey) ?? items[0];
	return (
		<div className='ui-tabs'>
			<div className='ui-tabs-list'>
				{items.map(item => (
					<button
						type='button'
						key={item.key}
						className={item.key === activeItem?.key ? 'ui-tabs-active' : ''}
						onClick={() => onChange?.(item.key)}
					>
						{item.label}
					</button>
				))}
			</div>
			<div className='ui-tabs-panel'>{activeItem?.children}</div>
		</div>
	);
}

function SplitterPanel({ children }: { children?: ReactNode; defaultSize?: string; min?: number }) {
	return <>{children}</>;
}

export function Splitter({
	children,
	style,
}: {
	children?: ReactNode;
	style?: CSSProperties;
	styles?: unknown;
}) {
	const panels = React.Children.toArray(children) as ReactElement<any>[];
	return (
		<div className='ui-splitter' style={style}>
			{panels.map((panel, index) => (
				<div
					key={index}
					className='ui-splitter-panel'
					style={{
						flexBasis: panel.props.defaultSize ?? undefined,
						minWidth: panel.props.min,
					}}
				>
					{panel.props.children}
				</div>
			))}
		</div>
	);
}

Splitter.Panel = SplitterPanel;

export function Segmented({
	value,
	options,
	onChange,
}: {
	value?: string;
	options: Array<{ label: ReactNode; value: string }>;
	onChange?: (value: string) => void;
}) {
	return (
		<div className='ui-segmented'>
			{options.map(option => (
				<button
					type='button'
					key={option.value}
					className={option.value === value ? 'ui-segmented-active' : ''}
					onClick={() => onChange?.(option.value)}
				>
					{option.label}
				</button>
			))}
		</div>
	);
}

export function Slider({
	value,
	min = 0,
	max = 100,
	step = 1,
	disabled,
	onChange,
	style,
}: {
	range?: boolean;
	value?: [number, number];
	min?: number;
	max?: number;
	step?: number;
	disabled?: boolean;
	onChange?: (value: [number, number]) => void;
	style?: CSSProperties;
	tooltip?: unknown;
	styles?: unknown;
}) {
	const [left, right] = value ?? [min, max];
	return (
		<div className='ui-range-slider' style={style}>
			<input
				type='range'
				min={min}
				max={max}
				step={step}
				disabled={disabled}
				value={left}
				onChange={event => onChange?.([Number(event.target.value), right])}
			/>
			<input
				type='range'
				min={min}
				max={max}
				step={step}
				disabled={disabled}
				value={right}
				onChange={event => onChange?.([left, Number(event.target.value)])}
			/>
		</div>
	);
}

export const Image = Object.assign(
	function ImageRoot({
		src,
		alt,
		width,
		height,
		className,
		style,
	}: {
		src?: string;
		alt?: string;
		width?: number;
		height?: number;
		className?: string;
		style?: CSSProperties;
	}) {
		return (
			<img
				src={src}
				alt={alt}
				width={width}
				height={height}
				className={className}
				style={style}
				onClick={() => src && window.open(src, '_blank', 'noopener,noreferrer')}
			/>
		);
	},
	{
		PreviewGroup({ children }: { children?: ReactNode }) {
			return <>{children}</>;
		},
	},
);

function UploadDragger({
	children,
	accept,
	multiple,
	disabled,
	fileList = [],
	onChange,
	className,
	style,
}: UploadProps) {
	const inputRef = useRef<HTMLInputElement | null>(null);

	function handleFiles(files: FileList | null) {
		const selectedFiles = Array.from(files ?? []);
		if (selectedFiles.length === 0) return;
		const uploads = selectedFiles.map(file => ({
			uid: `${Date.now()}-${file.name}-${Math.random().toString(36).slice(2)}`,
			name: file.name,
			originFileObj: file,
		}));
		onChange?.({ file: uploads[0], fileList: [...fileList, ...uploads] });
	}

	return (
		<div
			className={className}
			style={style}
			onClick={() => inputRef.current?.click()}
			onDragOver={event => event.preventDefault()}
			onDrop={event => {
				event.preventDefault();
				handleFiles(event.dataTransfer.files);
			}}
		>
			<input
				ref={inputRef}
				type='file'
				accept={accept}
				multiple={multiple}
				disabled={disabled}
				hidden
				onChange={event => {
					handleFiles(event.target.files);
					event.target.value = '';
				}}
			/>
			{children}
		</div>
	);
}

export const Upload = Object.assign(
	function UploadRoot({ children }: UploadProps) {
		return <>{children}</>;
	},
	{
		Dragger: UploadDragger,
	},
);
