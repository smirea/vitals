import type { CSSProperties, ReactNode } from 'react';
import { useRef } from 'react';

type FileDropzoneProps = {
	accept?: string;
	multiple?: boolean;
	disabled?: boolean;
	children: ReactNode;
	className?: string;
	style?: CSSProperties;
	onFiles: (files: File[]) => void;
};

export function FileDropzone({
	accept,
	multiple,
	disabled,
	children,
	className,
	style,
	onFiles,
}: FileDropzoneProps) {
	const inputRef = useRef<HTMLInputElement | null>(null);

	function emitFiles(files: FileList | null) {
		const selectedFiles = Array.from(files ?? []);
		if (selectedFiles.length > 0) {
			onFiles(selectedFiles);
		}
	}

	return (
		<div
			className={className}
			style={style}
			onClick={() => {
				if (!disabled) {
					inputRef.current?.click();
				}
			}}
			onDragOver={event => {
				event.preventDefault();
			}}
			onDrop={event => {
				event.preventDefault();
				if (!disabled) {
					emitFiles(event.dataTransfer.files);
				}
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
					emitFiles(event.target.files);
					event.target.value = '';
				}}
			/>
			{children}
		</div>
	);
}
