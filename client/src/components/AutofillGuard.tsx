import { useEffect } from 'react';

const FIELD_SELECTOR = 'input, textarea, select';
const FORM_SELECTOR = 'form';

function applyAutocompleteBlock(element: Element) {
	if (element instanceof HTMLFormElement) {
		element.setAttribute('autocomplete', 'off');
		element.setAttribute('data-1p-ignore', 'true');
		element.setAttribute('data-lpignore', 'true');
		element.setAttribute('data-form-type', 'other');
		return;
	}

	if (
		element instanceof HTMLInputElement ||
		element instanceof HTMLTextAreaElement ||
		element instanceof HTMLSelectElement
	) {
		element.autocomplete = 'off';
		element.setAttribute('autocomplete', 'off');
		element.setAttribute('data-1p-ignore', 'true');
		element.setAttribute('data-lpignore', 'true');
		element.setAttribute('data-form-type', 'other');
	}
}

function applyWithin(root: ParentNode) {
	if (root instanceof Element && (root.matches(FORM_SELECTOR) || root.matches(FIELD_SELECTOR))) {
		applyAutocompleteBlock(root);
	}

	root.querySelectorAll(FORM_SELECTOR).forEach(applyAutocompleteBlock);
	root.querySelectorAll(FIELD_SELECTOR).forEach(applyAutocompleteBlock);
}

export function AutofillGuard() {
	useEffect(() => {
		applyWithin(document);

		const observer = new MutationObserver(mutations => {
			for (const mutation of mutations) {
				mutation.addedNodes.forEach(node => {
					if (node instanceof Element) {
						applyWithin(node);
					}
				});
			}
		});

		observer.observe(document.body, {
			childList: true,
			subtree: true,
		});

		const handleFocusIn = (event: FocusEvent) => {
			const target = event.target;
			if (!(target instanceof Element)) {
				return;
			}

			applyWithin(target);

			const form = target.closest(FORM_SELECTOR);
			if (form) {
				applyAutocompleteBlock(form);
			}
		};

		document.addEventListener('focusin', handleFocusIn, true);

		return () => {
			observer.disconnect();
			document.removeEventListener('focusin', handleFocusIn, true);
		};
	}, []);

	return null;
}
