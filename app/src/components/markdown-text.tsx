import type { ReactNode } from 'react';
import { Text, type StyleProp, type TextStyle } from 'react-native';
import * as SimpleMarkdown from 'simple-markdown';

type MarkdownNode = SimpleMarkdown.SingleASTNode | SimpleMarkdown.SingleASTNode[];

export function MarkdownText({
	value,
	numberOfLines,
	style,
	compact = false,
}: {
	value: string;
	numberOfLines?: number;
	style?: StyleProp<TextStyle>;
	compact?: boolean;
}) {
	const nodes = SimpleMarkdown.defaultBlockParse(value.trim());
	return (
		<Text style={style} numberOfLines={numberOfLines} ellipsizeMode='tail'>
			{renderNodes(nodes, 'md', compact)}
		</Text>
	);
}

function renderNodes(nodes: MarkdownNode, keyPrefix = 'md', compact = false): ReactNode {
	return renderNodeList(nodes, keyPrefix, compact, compact ? ' ' : '\n');
}

function renderInlineNodes(nodes: MarkdownNode, keyPrefix: string, compact: boolean): ReactNode {
	return renderNodeList(nodes, keyPrefix, compact, '');
}

function renderNodeList(
	nodes: MarkdownNode,
	keyPrefix: string,
	compact: boolean,
	separator: string,
): ReactNode {
	if (Array.isArray(nodes)) {
		return nodes.flatMap((node, index) => {
			const rendered = renderNode(node, `${keyPrefix}-${index}`, compact);
			return index === 0 || !separator ? [rendered] : [separator, rendered];
		});
	}
	return renderNode(nodes, keyPrefix, compact);
}

function renderNode(node: SimpleMarkdown.SingleASTNode, key: string, compact: boolean): ReactNode {
	switch (node.type) {
		case 'text':
			return String(node.content ?? '');
		case 'newline':
		case 'br':
			return compact ? ' ' : '\n';
		case 'paragraph':
			return <Text key={key}>{renderInlineNodes(node.content ?? [], key, compact)}</Text>;
		case 'strong':
			return (
				<Text key={key} style={{ fontWeight: '800' }}>
					{renderInlineNodes(node.content ?? [], key, compact)}
				</Text>
			);
		case 'em':
			return (
				<Text key={key} style={{ fontStyle: 'italic' }}>
					{renderInlineNodes(node.content ?? [], key, compact)}
				</Text>
			);
		case 'inlineCode':
			return (
				<Text key={key} style={{ fontFamily: 'Menlo', fontSize: 12 }}>
					{String(node.content ?? '')}
				</Text>
			);
		case 'list': {
			const items = Array.isArray(node.items) ? node.items : [];
			const start = typeof node.start === 'number' ? node.start : 1;
			return items.flatMap((item, index) => [
				index === 0 ? '' : compact ? ' ' : '\n',
				<Text key={`${key}-${index}`}>
					{compact ? '' : node.ordered ? `${start + index}. ` : '- '}
					{renderInlineNodes(item, `${key}-${index}`, compact)}
				</Text>,
			]);
		}
		case 'link':
			return (
				<Text key={key} style={{ color: '#1677ff', fontWeight: '700' }}>
					{renderInlineNodes(node.content ?? [], key, compact)}
				</Text>
			);
		default:
			if (node.content)
				return <Text key={key}>{renderInlineNodes(node.content, key, compact)}</Text>;
			return null;
	}
}
