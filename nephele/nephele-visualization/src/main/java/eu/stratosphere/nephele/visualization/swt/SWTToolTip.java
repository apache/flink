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
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public abstract class SWTToolTip {

	protected static final int ICONSIZE = 20;

	private final Shell shell;

	private final Label titleLabel;

	private static final int OFFSET = 20;

	public SWTToolTip(Shell parent, int x, int y) {

		this.shell = new Shell(parent, SWT.TOOL | SWT.ON_TOP);
		final GridLayout gridLayout = new GridLayout(1, false);

		this.shell.setLayout(gridLayout);

		this.titleLabel = new Label(this.shell, SWT.NONE);
		this.titleLabel.setFont(FontScheme.getToolTipTitleFont(parent.getDisplay()));

		this.titleLabel.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
	}

	protected void finishInstantiation(int x, int y, int width) {

		final Rectangle displayBounds = this.shell.getDisplay().getPrimaryMonitor().getBounds();

		if ((x + OFFSET + this.shell.getBounds().width) > (displayBounds.x + displayBounds.width)) {
			x -= (this.shell.getBounds().width + OFFSET);
		} else {
			x += OFFSET;
		}

		this.shell.setLocation(x, y + OFFSET);

		this.shell.setVisible(true);
		this.shell.open();
	}

	public void dispose() {
		this.shell.dispose();
	}

	public boolean isDisposed() {
		return this.shell.isDisposed();
	}

	public void move(int x, int y) {

		final Rectangle displayBounds = this.shell.getDisplay().getPrimaryMonitor().getBounds();

		if ((x + OFFSET + this.shell.getBounds().width) > (displayBounds.x + displayBounds.width)) {
			x -= (this.shell.getBounds().width + OFFSET);
		} else {
			x += OFFSET;
		}

		this.shell.setLocation(x, y + OFFSET);
	}

	protected Shell getShell() {
		return this.shell;
	}

	protected void setTitle(String title) {
		this.titleLabel.setText(title);
	}

	public abstract void updateView();

	protected Composite createWarningComposite(String text, int imageType) {

		final Image image = getShell().getDisplay().getSystemImage(imageType);

		final Composite composite = new Composite(getShell(), SWT.NONE);
		final GridLayout gridLayout = new GridLayout(2, false);
		composite.setLayout(gridLayout);
		Composite imageCanvas = new SWTImageCanvas(composite, SWT.NONE, image);
		imageCanvas.setLayoutData(new GridData(ICONSIZE, ICONSIZE));

		final Label label = new Label(composite, SWT.NONE);
		label.setLayoutData(new GridData(GridData.CENTER, GridData.CENTER, true, true));
		label.setText(text);

		return composite;
	}
}
