/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.Date;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class implements a tooltip for Nephele jobs.
 * 
 * @author warneke
 */
public class SWTJobToolTip extends SWTToolTip {

	public SWTJobToolTip(final Shell parent, final String jobName, final JobID jobID, final long submissionTimestamp,
			final int x, final int y) {

		super(parent, x, y);

		setTitle(jobName);

		final Color backgroundColor = getShell().getBackground();
		final Color foregroundColor = getShell().getForeground();

		final Composite tableComposite = new Composite(getShell(), SWT.NONE);
		final GridLayout tableGridLayout = new GridLayout(2, false);
		tableGridLayout.marginHeight = 0;
		tableGridLayout.marginLeft = 0;
		tableComposite.setLayout(tableGridLayout);
		tableComposite.setBackground(backgroundColor);
		tableComposite.setForeground(foregroundColor);

		final Label jobIDTextLabel = new Label(tableComposite, SWT.NONE);
		jobIDTextLabel.setBackground(backgroundColor);
		jobIDTextLabel.setForeground(foregroundColor);
		jobIDTextLabel.setText("Job ID:");

		final Label jobIDLabel = new Label(tableComposite, SWT.NONE);
		jobIDLabel.setBackground(backgroundColor);
		jobIDLabel.setForeground(foregroundColor);
		jobIDLabel.setText(jobID.toString());

		final Label submissionDateTextLabel = new Label(tableComposite, SWT.NONE);
		submissionDateTextLabel.setBackground(backgroundColor);
		submissionDateTextLabel.setForeground(foregroundColor);
		submissionDateTextLabel.setText("Submitted:");

		final Label submissionDateLabel = new Label(tableComposite, SWT.NONE);
		submissionDateLabel.setBackground(backgroundColor);
		submissionDateLabel.setForeground(foregroundColor);
		submissionDateLabel.setText(new Date(submissionTimestamp).toString());

		finishInstantiation(x, y, 100, true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void updateView() {
		// Nothing to do here
	}

}
