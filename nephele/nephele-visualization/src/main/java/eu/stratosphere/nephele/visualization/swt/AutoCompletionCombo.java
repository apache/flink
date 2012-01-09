/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.visualization.swt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;

/**
 * The class implements a combo box with auto-completion features.
 * 
 * @author warneke
 */
public final class AutoCompletionCombo extends Composite implements KeyListener, SelectionListener {

	/**
	 * The list of possible suggestions for the auto-completion.
	 */
	private final ArrayList<String> suggestions;

	/**
	 * The internal SWT combo box.
	 */
	private final Combo combo;

	/**
	 * Constructs a new auto-completion combo box.
	 * 
	 * @param parent
	 *        the parent composite
	 * @param style
	 *        the style of the combo box
	 * @param suggestions
	 *        a list of suggestions for the auto-completion
	 */
	public AutoCompletionCombo(final Composite parent, final int style, final List<String> suggestions) {
		super(parent, style);

		// First, sort the suggestions
		this.suggestions = new ArrayList<String>(suggestions);
		Collections.sort(this.suggestions);

		setLayout(new FillLayout());

		this.combo = new Combo(this, style);
		this.combo.addKeyListener(this);
		this.combo.addSelectionListener(this);

		// Add the suggestions
		for (final String suggestion : this.suggestions) {
			this.combo.add(suggestion);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void keyPressed(final KeyEvent arg0) {

		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void keyReleased(final KeyEvent arg0) {

		if (arg0.character == 0) {
			return;
		}

		final String text = this.combo.getText();
		if (text == null) {
			return;
		}

		final List<String> matchingSuggestions = updateSuggestions(text);

		final int length = text.length();

		if (arg0.character == SWT.BS) {

			this.combo.setText(text);
			this.combo.setSelection(new Point(length, length));

		} else {

			if (length != 0) {
				Point selection;
				if (matchingSuggestions.isEmpty()) {
					this.combo.setText(text);
					selection = new Point(length, length);
				} else {
					final String suggestion = matchingSuggestions.get(0);
					this.combo.setText(suggestion);
					selection = new Point(length, suggestion.length());
				}

				this.combo.setSelection(selection);
			}
		}
	}

	/**
	 * Computes an update of the combo box's suggestions. All suggestions have the entered text as a prefix.
	 * 
	 * @param text
	 *        the text entered in the combo box's text field so far
	 * @return the list of suggestions, possibly empty
	 */
	private List<String> updateSuggestions(final String text) {

		final ArrayList<String> matchingSuggestions = new ArrayList<String>(this.suggestions.size());
		for (final String suggestion : this.suggestions) {

			if (suggestion.startsWith(text)) {
				matchingSuggestions.add(suggestion);
			}
		}

		this.combo.removeAll();
		for (final String suggestion : matchingSuggestions) {
			this.combo.add(suggestion);
		}

		return matchingSuggestions;
	}

	/**
	 * Returns the text from the text field of the combo box.
	 * 
	 * @return the text from the text field of the combo box
	 */
	public String getText() {

		return this.combo.getText();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void widgetDefaultSelected(final SelectionEvent arg0) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void widgetSelected(final SelectionEvent arg0) {

		final String text = this.combo.getText();
		updateSuggestions(text);
		this.combo.setText(text);
	}

	/**
	 * Adds a {@link KeyListener} to the combo box.
	 * 
	 * @param keyListener
	 *        the key listener object to be added
	 */
	public void addKeyListener(final KeyListener keyListener) {

		this.combo.addKeyListener(keyListener);
	}
}
