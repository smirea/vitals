const INDENT_REG = /^\s*/;
const SUFFIX_SPACE_REG = /\s+$/;

/**
 * Template function that strips indentation and supports array interpolation.
 */
export function textBlock(list: TemplateStringsArray, ...args: any[]): string {
	const parts: string[] = [];

	for (let i = 0; i < list.length; ++i) {
		const rawPrefix = list[i] ?? 'undefined';
		const hasArg = i < args.length;

		parts.push(rawPrefix);

		if (hasArg) {
			const arg = args[i];
			const blockMatch = rawPrefix.match(/\n([ \t]*)$/);

			if (blockMatch) {
				// Block-style interpolation (after newline)
				const indent = blockMatch[1];

				if (Array.isArray(arg)) {
					if (arg.length > 0) {
						parts.push(arg.map(item => String(item)).join('\n' + indent));
					} else {
						// Remove the trailing newline+indent for empty arrays
						parts[parts.length - 1] = rawPrefix.slice(0, rawPrefix.length - blockMatch[0].length);
					}
				} else {
					const argStr = String(arg);
					if (argStr.includes('\n')) {
						const argLines = argStr.split('\n');
						const indentedArg = argLines
							.map((line, idx) => (idx === 0 ? line : line.length > 0 ? indent + line : line))
							.join('\n');
						parts.push(indentedArg);
					} else {
						parts.push(argStr);
					}
				}
			} else {
				// Inline interpolation - find current line indentation for multiline strings
				const lines = rawPrefix.split('\n');
				const lastLine = lines[lines.length - 1]!;
				const currentLineIndent = lastLine.match(/^([ \t]*)/)?.[1] || '';

				if (Array.isArray(arg)) {
					// Arrays in inline position should be joined with commas
					parts.push(arg.map(item => String(item)).join(','));
				} else {
					const argStr = String(arg);
					if (argStr.includes('\n')) {
						// Multiline strings in inline position should be indented to current line level
						const argLines = argStr.split('\n');
						const indentedArg = argLines
							.map((line, idx) =>
								idx === 0 ? line : line.length > 0 ? currentLineIndent + line : line,
							)
							.join('\n');
						parts.push(indentedArg);
					} else {
						parts.push(argStr);
					}
				}
			}
		}
	}

	const raw = parts.join('');
	const lines = raw.split('\n');

	// Trim leading/trailing empty lines
	while (lines.length && lines[0]!.trim() === '') lines.shift();
	while (lines.length && lines[lines.length - 1]!.trim() === '') lines.pop();

	// Find minimum indent (excluding fully-blank lines)
	let minIndent: string | null = null;
	for (const line of lines) {
		const [indent] = line.match(INDENT_REG)!;
		if (indent.length === line.length) continue;
		if (minIndent === null || indent.length < minIndent.length) {
			minIndent = indent;
			if (minIndent.length === 0) break;
		}
	}

	const stripIndent = new RegExp(`^${minIndent || ''}`);
	return lines.map(line => line.replace(stripIndent, '').replace(SUFFIX_SPACE_REG, '')).join('\n');
}
