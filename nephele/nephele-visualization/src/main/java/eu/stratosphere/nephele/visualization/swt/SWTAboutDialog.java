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

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Shell;

public class SWTAboutDialog {

	private final Color white;

	private static final String ABOUT_TEXT = "This software is part of the Stratosphere project, \u00a9 2010-2011\r\n\r\n";

	private static final String URL_TEXT = "For more information, please visit <A>http://www.stratosphere.eu/</A>.\r\n";

	private final Shell shell;

	public SWTAboutDialog(final Shell parentShell) {

		this.shell = new Shell(parentShell);

		this.white = new Color(parentShell.getDisplay(), 255, 255, 255);

		this.shell.setText("About Nephele Job Visualization");
		this.shell.setSize(400, 220);
		this.shell.setBackground(this.white);

		final GridLayout gridLayout = new GridLayout(1, false);
		gridLayout.marginTop = 0;
		gridLayout.marginBottom = 0;
		gridLayout.marginLeft = 0;
		gridLayout.marginRight = 0;
		gridLayout.marginWidth = 0;
		gridLayout.marginHeight = 0;

		this.shell.setLayout(gridLayout);

		new LogoCanvas(this.shell, SWT.NONE).setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

		final Composite composite = new Composite(this.shell, SWT.NONE);
		composite.setLayoutData(new GridData(GridData.FILL_HORIZONTAL | GridData.FILL_VERTICAL));

		final GridLayout innerGridLayout = new GridLayout(1, false);
		innerGridLayout.marginTop = 10;
		innerGridLayout.marginBottom = 10;
		innerGridLayout.marginLeft = 10;
		innerGridLayout.marginRight = 10;
		innerGridLayout.marginWidth = 0;
		innerGridLayout.marginHeight = 0;
		composite.setLayout(innerGridLayout);
		composite.setBackground(this.white);

		final Label aboutLabel = new Label(composite, SWT.LEFT);
		aboutLabel.setBackground(this.white);
		aboutLabel.setText(ABOUT_TEXT);

		final Link websiteLink = new Link(composite, SWT.LEFT);
		websiteLink.setText(URL_TEXT);
		websiteLink.setBackground(this.white);
		websiteLink.addMouseListener(new MouseAdapter() {

			@Override
			public void mouseDown(final MouseEvent arg0) {
				Program.launch("http://www.stratosphere.eu/");
			}

		});

		/*
		 * final Text licenseText = new Text(this.shell, SWT.MULTI | SWT.READ_ONLY | SWT.LEFT | SWT.V_SCROLL);
		 * GridData licenseTextLayout = new GridData(GridData.FILL_HORIZONTAL);
		 * licenseTextLayout.horizontalSpan = 10;
		 * licenseTextLayout.horizontalIndent = 10;
		 * licenseText.setEditable(false);
		 * licenseText.setEnabled(false);
		 * licenseText.setText(ABOUT_TEXT);
		 * licenseText.setLayoutData(licenseTextLayout);
		 */

		final Button button = new Button(composite, SWT.PUSH);
		final GridData buttonLayout = new GridData();
		buttonLayout.minimumWidth = 100;
		buttonLayout.grabExcessHorizontalSpace = true;
		buttonLayout.horizontalAlignment = SWT.CENTER;
		buttonLayout.verticalAlignment = SWT.BOTTOM;
		button.setLayoutData(buttonLayout);
		button.setText("OK");
		button.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				shell.close();
			}
		});

	}

	public void open() {

		this.shell.open();
		final Display display = this.shell.getDisplay();

		while (!this.shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
	}
}
