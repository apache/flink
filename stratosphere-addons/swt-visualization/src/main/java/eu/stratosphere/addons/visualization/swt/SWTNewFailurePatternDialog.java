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

package eu.stratosphere.addons.visualization.swt;

import java.util.HashSet;
import java.util.Set;

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;

/**
 * This class implements a dialogue for creating a new failure pattern.
 * 
 * @author warneke
 */
public final class SWTNewFailurePatternDialog {

	/**
	 * The width of the dialog.
	 */
	private static final int WIDTH = 300;

	/**
	 * The height of the dialog.
	 */
	private static final int HEIGHT = 100;

	/**
	 * The shell for this dialog.
	 */
	private final Shell shell;

	/**
	 * The auto-completion combo box.
	 */
	private final AutoCompletionCombo input;

	/**
	 * The taken names which must not be accepted as names for the new pattern.
	 */
	private final Set<String> takenNames;

	/**
	 * The return value of the <code>showDialog</code> method.
	 */
	private String returnValue = null;

	/**
	 * Constructs a new dialog for creating a new failure pattern.
	 * 
	 * @param parent
	 *        the parent of this dialog
	 * @param nameSuggestions
	 *        name suggestions to be displayed inside auto-completion combo box
	 * @param takenNames
	 *        names that are already taken and must not be accepted as input
	 */
	public SWTNewFailurePatternDialog(final Shell parent, final Set<String> nameSuggestions,
			final Set<String> takenNames) {

		this.takenNames = takenNames;

		this.shell = new Shell(parent);
		this.shell.setSize(WIDTH, HEIGHT);
		this.shell.setText("Add New Failure Pattern");
		this.shell.setLayout(new GridLayout(1, false));

		GridData gridData = new GridData();
		gridData.horizontalAlignment = GridData.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.grabExcessVerticalSpace = false;

		// Remove the taken names from
		final Set<String> nameSugg = new HashSet<String>(nameSuggestions);
		nameSugg.removeAll(takenNames);

		this.input = new AutoCompletionCombo(this.shell, SWT.NONE, nameSugg);
		this.input.setLayoutData(gridData);
		this.input.addKeyListener(new KeyAdapter() {

			@Override
			public void keyReleased(final KeyEvent arg0) {

				if (arg0.character != SWT.CR) {
					return;
				}

				if (isInputValid()) {
					returnValue = input.getText();
					shell.dispose();
				}
			}
		});

		gridData = new GridData();
		gridData.horizontalAlignment = SWT.RIGHT;
		gridData.verticalAlignment = SWT.BOTTOM;
		gridData.grabExcessHorizontalSpace = true;
		gridData.grabExcessVerticalSpace = true;

		final Composite buttonComposite = new Composite(this.shell, SWT.RIGHT_TO_LEFT);
		final RowLayout rowLayout = new RowLayout(SWT.HORIZONTAL);
		rowLayout.marginBottom = 0;
		rowLayout.marginHeight = 0;
		rowLayout.marginLeft = 0;
		rowLayout.marginRight = 0;
		rowLayout.marginTop = 0;
		rowLayout.marginWidth = 0;
		rowLayout.pack = false;

		buttonComposite.setLayoutData(gridData);
		buttonComposite.setLayout(rowLayout);

		final Button ok = new Button(buttonComposite, SWT.PUSH);
		ok.setText("OK");
		ok.addListener(SWT.Selection, new Listener() {

			@Override
			public void handleEvent(final Event arg0) {

				if (isInputValid()) {
					returnValue = input.getText();
					shell.dispose();
				}
			}
		});

		final Button cancel = new Button(buttonComposite, SWT.PUSH);
		cancel.setText("Cancel");
		cancel.addListener(SWT.Selection, new Listener() {

			@Override
			public void handleEvent(final Event arg0) {

				returnValue = null;
				shell.dispose();
			}
		});
	}

	/**
	 * Checks whether the input is valid and displays an error message box if not.
	 * 
	 * @return <code>true</code> if the input is valid, <code>false</code> otherwise
	 */
	private boolean isInputValid() {

		final String text = this.input.getText();
		if (text.isEmpty()) {
			final MessageBox messageBox = new MessageBox(this.shell, SWT.ICON_ERROR);

			messageBox.setText("Invalid Input");
			messageBox.setMessage("Name for failure pattern must not be empty.");
			messageBox.open();

			this.input.setFocus();

			return false;
		}

		if (this.takenNames.contains(text)) {

			final MessageBox messageBox = new MessageBox(this.shell, SWT.ICON_ERROR);

			messageBox.setText("Invalid Input");
			messageBox.setMessage("The chosen name is already used by another loaded failure pattern.");
			messageBox.open();

			this.input.setFocus();

			return false;

		}

		return true;
	}

	/**
	 * Opens the dialog.
	 * 
	 * @return the name for the new failure pattern or <code>null</code> if the user has canceled the dialog
	 */
	public String showDialog() {

		this.shell.open();

		final Display display = this.shell.getDisplay();

		while (!this.shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}

		return this.returnValue;
	}
}
