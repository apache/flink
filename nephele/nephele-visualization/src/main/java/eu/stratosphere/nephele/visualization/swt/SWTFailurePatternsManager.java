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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Tree;

import eu.stratosphere.nephele.jobgraph.JobID;

public final class SWTFailurePatternsManager implements SelectionListener {

	private static final Log LOG = LogFactory.getLog(SWTFailurePatternsManager.class);

	private static final int WIDTH = 800;

	private static final int HEIGHT = 400;

	private final Shell shell;

	private final Tree jobTree;

	private final CTabFolder jobTabFolder;

	private final CTabItem taskFailurePatternsTab;

	private final CTabItem instanceFailurePatternsTab;

	private final Map<String, JobFailurePattern> failurePatterns = new HashMap<String, JobFailurePattern>();

	SWTFailurePatternsManager(final Shell parent) {

		// Set size
		this.shell = new Shell(parent);
		this.shell.setSize(WIDTH, HEIGHT);
		this.shell.setText("Manage Outage Patterns");

		this.shell.setLayout(new GridLayout(2, false));

		final SashForm horizontalSash = new SashForm(this.shell, SWT.HORIZONTAL);
		horizontalSash.setLayoutData(new GridData(GridData.FILL_BOTH));

		final Group jobGroup = new Group(horizontalSash, SWT.NONE);
		jobGroup.setText("Job Failure Patterns");
		jobGroup.setLayout(new FillLayout());

		this.jobTree = new Tree(jobGroup, SWT.SINGLE | SWT.BORDER);
		this.jobTree.addSelectionListener(this);

		this.jobTabFolder = new CTabFolder(horizontalSash, SWT.TOP);
		this.jobTabFolder.addSelectionListener(this);
		this.jobTabFolder.setVisible(false);

		this.taskFailurePatternsTab = new CTabItem(this.jobTabFolder, SWT.NONE);

		this.instanceFailurePatternsTab = new CTabItem(this.jobTabFolder, SWT.NONE);

		this.jobTabFolder.setSelection(this.taskFailurePatternsTab);

		horizontalSash.setWeights(new int[] { 2, 8 });

		this.taskFailurePatternsTab.setText("Task Failure Patterns");
		this.instanceFailurePatternsTab.setText("Instance Failure Patterns");

	}

	public void open() {

		this.shell.open();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void widgetDefaultSelected(final SelectionEvent arg0) {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void widgetSelected(final SelectionEvent arg0) {
		// TODO Auto-generated method stub

	}

	public void startFailurePattern(final JobID jobID, final String jobName, final long referenceTime) {

		final JobFailurePattern failurePattern = this.failurePatterns.get(jobName.toLowerCase());
		if (failurePattern == null) {
			LOG.info("No failure pattern for job " + jobName);
		}

		final JobFailurePatternExecutor executor = new JobFailurePatternExecutor(this.shell.getDisplay(), jobID,
			jobName, failurePattern);

		executor.start(referenceTime);
	}
}
