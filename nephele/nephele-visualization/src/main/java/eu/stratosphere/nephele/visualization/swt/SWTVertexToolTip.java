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
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.experimental.chart.swt.ChartComposite;

import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;

public class SWTVertexToolTip extends SWTToolTip {

	private final static String WARNINGTEXT = "Nephele has identified this task as a CPU bottleneck";

	private final Label instanceTypeLabel;

	private final Label instanceIDLabel;

	private final Label executionStateLabel;

	private final Label checkpointStateLabel;

	private Composite warningComposite;

	private final ManagementVertex managementVertex;

	private final ChartComposite threadChart;

	private final static int WIDTH = 400;

	public SWTVertexToolTip(Shell parent, final SWTToolTipCommandReceiver commandReceiver,
			ManagementVertex managementVertex, int x, int y) {
		super(parent, x, y);

		this.managementVertex = managementVertex;

		final VertexVisualizationData vertexVisualizationData = (VertexVisualizationData) managementVertex
			.getAttachment();

		int height;

		// Set the title
		final String taskName = managementVertex.getName() + " (" + (managementVertex.getIndexInGroup() + 1) + " of "
			+ managementVertex.getNumberOfVerticesInGroup() + ")";
		setTitle(taskName);

		// Only create chart if profiling is enabled
		if (vertexVisualizationData.isProfilingEnabledForJob()) {
			this.threadChart = createThreadChart(vertexVisualizationData);
			this.threadChart.setLayoutData(new GridData(GridData.FILL_BOTH));
			height = 240; // should be 265 when cancel button is enabled
		} else {
			this.threadChart = null;
			height = 125;
		}

		final Composite tableComposite = new Composite(getShell(), SWT.NONE);
		tableComposite.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
		final GridLayout tableGridLayout = new GridLayout(3, false);
		tableGridLayout.marginHeight = 0;
		tableGridLayout.marginLeft = 0;
		tableComposite.setLayout(tableGridLayout);

		final GridData gridData1 = new GridData();
		gridData1.horizontalSpan = 2;
		gridData1.grabExcessHorizontalSpace = true;
		gridData1.widthHint = 200;

		final GridData gridData2 = new GridData();
		gridData2.grabExcessHorizontalSpace = true;

		// Instance type
		new Label(tableComposite, SWT.NONE).setText("Instance type:");
		this.instanceTypeLabel = new Label(tableComposite, SWT.NONE);
		this.instanceTypeLabel.setText(this.managementVertex.getInstanceType());
		this.instanceTypeLabel.setLayoutData(gridData1);

		// Instance ID
		new Label(tableComposite, SWT.NONE).setText("Instance ID:");
		this.instanceIDLabel = new Label(tableComposite, SWT.NONE);
		this.instanceIDLabel.setText(this.managementVertex.getInstanceName());
		this.instanceIDLabel.setLayoutData(gridData2);

		final Button switchToInstanceButton = new Button(tableComposite, SWT.PUSH);
		switchToInstanceButton.setText("Switch to instance...");
		switchToInstanceButton.setEnabled(vertexVisualizationData.isProfilingEnabledForJob());
		switchToInstanceButton.setVisible(false);

		/*
		 * final String instanceName = this.managementVertex.getInstanceName();
		 * switchToInstanceButton.addListener(SWT.Selection, new Listener() {
		 * @Override
		 * public void handleEvent(Event arg0) {
		 * commandReceiver.switchToInstance(instanceName);
		 * }
		 * });
		 */

		// Execution state
		new Label(tableComposite, SWT.NONE).setText("Execution state:");
		this.executionStateLabel = new Label(tableComposite, SWT.NONE);
		this.executionStateLabel.setText(this.managementVertex.getExecutionState().toString());
		this.executionStateLabel.setLayoutData(gridData1);

		// Checkpoint state
		new Label(tableComposite, SWT.NONE).setText("Checkpoint state:");
		this.checkpointStateLabel = new Label(tableComposite, SWT.NONE);
		this.checkpointStateLabel.setText(this.managementVertex.getCheckpointState().toString());
		this.checkpointStateLabel.setLayoutData(gridData1);

		final ManagementGroupVertex groupVertex = this.managementVertex.getGroupVertex();
		final GroupVertexVisualizationData groupVertexVisualizationData = (GroupVertexVisualizationData) groupVertex
			.getAttachment();
		if (groupVertexVisualizationData.isCPUBottleneck()) {
			this.warningComposite = createWarningComposite(WARNINGTEXT, SWT.ICON_WARNING);
			height += ICONSIZE;
		} else {
			this.warningComposite = null;
		}

		// Available task actions
		final Composite taskActionComposite = new Composite(getShell(), SWT.NONE);
		taskActionComposite.setLayout(new RowLayout(SWT.HORIZONTAL));

		/*
		 * final Button cancelTaskButton = new Button(taskActionComposite, SWT.PUSH);
		 * final ManagementVertexID vertexID = this.managementVertex.getID();
		 * cancelTaskButton.setText("Cancel task");
		 * cancelTaskButton.setEnabled(this.managementVertex.getExecutionState() == ExecutionState.RUNNING);
		 * cancelTaskButton.addListener(SWT.Selection, new Listener() {
		 * @Override
		 * public void handleEvent(Event arg0) {
		 * commandReceiver.cancelTask(vertexID, taskName);
		 * }
		 * });
		 */

		getShell().setSize(WIDTH, height);

		finishInstantiation(x, y, WIDTH);
	}

	private ChartComposite createThreadChart(VertexVisualizationData visualizationData) {

		final JFreeChart chart = ChartFactory.createStackedXYAreaChart(null, "Time [sec.]", "Thread Utilization [%]",
			visualizationData.getThreadDataSet(), PlotOrientation.VERTICAL, true, true, false);

		// Set axis properly
		final XYPlot xyPlot = chart.getXYPlot();
		xyPlot.getDomainAxis().setAutoRange(true);
		xyPlot.getDomainAxis().setAutoRangeMinimumSize(60);

		// xyPlot.getRangeAxis().setAutoRange(true);
		xyPlot.getRangeAxis().setRange(0, 100);

		return new ChartComposite(getShell(), SWT.NONE, chart, true);
	}

	public void updateView() {

		// Redraw the chart
		if (this.threadChart != null) {
			this.threadChart.getChart().getXYPlot().configureDomainAxes();
			this.threadChart.getChart().fireChartChanged();
		}

		// Update the labels
		this.executionStateLabel.setText(this.managementVertex.getExecutionState().toString());
		this.checkpointStateLabel.setText(this.managementVertex.getCheckpointState().toString());
		this.instanceIDLabel.setText(this.managementVertex.getInstanceName());
		this.instanceTypeLabel.setText(this.managementVertex.getInstanceType());

		final ManagementGroupVertex groupVertex = this.managementVertex.getGroupVertex();
		final GroupVertexVisualizationData groupVertexVisualizationData = (GroupVertexVisualizationData) groupVertex
			.getAttachment();
		if (groupVertexVisualizationData.isCPUBottleneck()) {
			if (this.warningComposite == null) {
				this.warningComposite = createWarningComposite(WARNINGTEXT, SWT.ICON_WARNING);
				Rectangle clientRect = getShell().getClientArea();
				clientRect.height += ICONSIZE;
				getShell().setSize(clientRect.width, clientRect.height);
			}
		} else {
			if (this.warningComposite != null) {
				this.warningComposite.dispose();
				this.warningComposite = null;
				Rectangle clientRect = getShell().getClientArea();
				clientRect.height -= ICONSIZE;
				getShell().setSize(clientRect.width, clientRect.height);
			}
		}
	}
}
