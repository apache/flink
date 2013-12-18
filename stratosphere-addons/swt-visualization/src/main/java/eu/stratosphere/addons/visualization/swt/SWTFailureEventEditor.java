/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.addons.visualization.swt;

import java.util.Set;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public final class SWTFailureEventEditor {

	/**
	 * The width of the dialog.
	 */
	private static final int WIDTH = 300;

	/**
	 * The height of the dialog.
	 */
	private static final int HEIGHT = 200;

	/**
	 * The shell for this dialog.
	 */
	private final Shell shell;

	/**
	 * The auto-completion combo box for the name.
	 */
	private final AutoCompletionCombo name;

	/**
	 * The text field for the interval.
	 */
	private final Text interval;

	/**
	 * The radio button for task failure events.
	 */
	private final Button taskFailureButton;

	/**
	 * The radio button for the instance failure events.
	 */
	private final Button instanceFailureButton;

	/**
	 * The return value of the editor.
	 */
	private AbstractFailureEvent returnValue = null;

	public SWTFailureEventEditor(final Shell parent, final Set<String> nameSuggestions,
			final AbstractFailureEvent failureEvent) {

		this.shell = new Shell(parent);
		this.shell.setSize(WIDTH, HEIGHT);

		// Determine the correct title for the window
		String title = null;
		if (failureEvent == null) {
			title = "Add Failure Event";
		} else {
			title = "Edit Failure Event";
		}

		this.shell.setText(title);
		this.shell.setLayout(new GridLayout(2, false));

		final GridData labelGridData = new GridData();
		labelGridData.horizontalAlignment = GridData.BEGINNING;
		labelGridData.grabExcessHorizontalSpace = false;
		labelGridData.grabExcessVerticalSpace = false;
		labelGridData.widthHint = 50;
		labelGridData.minimumWidth = 50;

		final Label nameLabel = new Label(this.shell, SWT.NONE);
		nameLabel.setText("Name:");
		nameLabel.setLayoutData(labelGridData);

		final GridData fieldGridData = new GridData();
		fieldGridData.horizontalAlignment = GridData.FILL;
		fieldGridData.grabExcessHorizontalSpace = true;
		fieldGridData.grabExcessVerticalSpace = false;

		this.name = new AutoCompletionCombo(this.shell, SWT.NONE, nameSuggestions);
		this.name.setLayoutData(fieldGridData);
		this.name.addKeyListener(new KeyAdapter() {

			@Override
			public void keyReleased(final KeyEvent arg0) {

				if (arg0.character != SWT.CR) {
					return;
				}

				if (isInputValid()) {
					returnValue = assembleReturnValue();
					shell.dispose();
				}
			}
		});
		if (failureEvent != null) {
			this.name.setText(failureEvent.getName());
		}

		final Label intervalLabel = new Label(this.shell, SWT.NONE);
		intervalLabel.setText("Interval:");
		intervalLabel.setLayoutData(labelGridData);

		this.interval = new Text(this.shell, SWT.SINGLE | SWT.BORDER);
		this.interval.setLayoutData(fieldGridData);
		this.interval.addKeyListener(new KeyAdapter() {

			@Override
			public void keyReleased(final KeyEvent arg0) {

				if (arg0.character != SWT.CR) {
					return;
				}

				if (isInputValid()) {
					returnValue = assembleReturnValue();
					shell.dispose();
				}
			}
		});
		if (failureEvent != null) {
			this.interval.setText(Integer.toString(failureEvent.getInterval()));
		}

		final GridData groupGridData = new GridData();
		groupGridData.horizontalAlignment = GridData.FILL;
		groupGridData.grabExcessHorizontalSpace = true;
		groupGridData.grabExcessVerticalSpace = false;
		groupGridData.horizontalSpan = 2;

		final Group typeGroup = new Group(this.shell, SWT.BORDER);
		typeGroup.setText("Event type");
		typeGroup.setLayoutData(groupGridData);
		typeGroup.setLayout(new GridLayout(1, true));

		this.taskFailureButton = new Button(typeGroup, SWT.RADIO);
		this.taskFailureButton.setText("Task failure");

		this.instanceFailureButton = new Button(typeGroup, SWT.RADIO);
		this.instanceFailureButton.setText("Instance failure");

		if (failureEvent == null) {
			this.taskFailureButton.setSelection(true);
			this.instanceFailureButton.setSelection(false);
		} else {
			if (failureEvent instanceof VertexFailureEvent) {
				this.taskFailureButton.setSelection(true);
				this.instanceFailureButton.setSelection(false);
			} else {
				this.taskFailureButton.setSelection(false);
				this.instanceFailureButton.setSelection(true);
			}
		}

		final GridData buttonGridData = new GridData();
		buttonGridData.horizontalAlignment = SWT.RIGHT;
		buttonGridData.verticalAlignment = SWT.BOTTOM;
		buttonGridData.grabExcessHorizontalSpace = true;
		buttonGridData.grabExcessVerticalSpace = true;
		buttonGridData.horizontalSpan = 2;

		final Composite buttonComposite = new Composite(this.shell, SWT.RIGHT_TO_LEFT);
		final RowLayout rowLayout = new RowLayout(SWT.HORIZONTAL);
		rowLayout.marginBottom = 0;
		rowLayout.marginHeight = 0;
		rowLayout.marginLeft = 0;
		rowLayout.marginRight = 0;
		rowLayout.marginTop = 0;
		rowLayout.marginWidth = 0;
		rowLayout.pack = false;

		buttonComposite.setLayoutData(buttonGridData);
		buttonComposite.setLayout(rowLayout);

		final Button ok = new Button(buttonComposite, SWT.PUSH);
		ok.setText("OK");
		ok.addListener(SWT.Selection, new Listener() {

			@Override
			public void handleEvent(final Event arg0) {

				if (isInputValid()) {

					returnValue = assembleReturnValue();
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
	 * Constructs the dialog's return value from the current dialog settings.
	 * 
	 * @return the dialog's return value
	 */
	private AbstractFailureEvent assembleReturnValue() {

		final String n = this.name.getText();
		final int iv = Integer.parseInt(this.interval.getText());

		if (this.taskFailureButton.getSelection()) {
			return new VertexFailureEvent(n, iv);
		} else {
			return new InstanceFailureEvent(n, iv);
		}
	}

	/**
	 * Checks whether the input is valid and displays an error message box if not.
	 * 
	 * @return <code>true</code> if the input is valid, <code>false</code> otherwise
	 */
	private boolean isInputValid() {

		if (this.name.getText().isEmpty()) {

			final MessageBox messageBox = new MessageBox(this.shell, SWT.ICON_ERROR);
			messageBox.setText("Invalid Input");
			messageBox.setMessage("Name must not be empty.");
			messageBox.open();
			this.name.setFocus();

			return false;
		}

		final String intervalString = this.interval.getText();
		if (intervalString.isEmpty()) {

			final MessageBox messageBox = new MessageBox(this.shell, SWT.ICON_ERROR);
			messageBox.setText("Invalid Input");
			messageBox.setMessage("Interval must not be empty");
			messageBox.open();
			this.interval.setFocus();

			return false;
		}

		// Try parsing the interval number
		int interval = -1;
		try {
			interval = Integer.parseInt(intervalString);
		} catch (NumberFormatException nfe) {

			final MessageBox messageBox = new MessageBox(this.shell, SWT.ICON_ERROR);
			messageBox.setText("Invalid Input");
			messageBox.setMessage("Given interval is not an integer number");
			messageBox.open();
			this.interval.setFocus();

			return false;
		}

		if (interval <= 0) {

			final MessageBox messageBox = new MessageBox(this.shell, SWT.ICON_ERROR);
			messageBox.setText("Invalid Input");
			messageBox.setMessage("Given interval is must be greater than 0");
			messageBox.open();
			this.interval.setFocus();

			return false;
		}

		return true;
	}

	/**
	 * Opens the dialog.
	 * 
	 * @return the new {@link AbstractFailureEvent} created with the help of the dialog.
	 */
	public AbstractFailureEvent showDialog() {

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
