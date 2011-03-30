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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.experimental.chart.swt.ChartComposite;

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.managementgraph.ManagementGate;
import eu.stratosphere.nephele.managementgraph.ManagementGroupEdge;

public class SWTGateToolTip extends SWTToolTip {

	private static final String WARNINGTEXT = "Nephele has detected an I/O bottleneck at this gate";

	private final ChartComposite chart;

	private final Label channelTypeLabel;

	private final Label recordTypeLabel;

	private Composite warningComposite;

	private final static int WIDTH = 400;

	private final ManagementGate managementGate;

	public SWTGateToolTip(Shell parent, ManagementGate managementGate, int x, int y) {
		super(parent, x, y);

		int height;

		this.managementGate = managementGate;

		if (managementGate.isInputGate()) {
			setTitle("Input Gate " + managementGate.getIndex());
		} else {
			setTitle("Output Gate " + managementGate.getIndex());
		}

		GateVisualizationData gateVisualizationData = (GateVisualizationData) managementGate.getAttachment();

		if (gateVisualizationData.isProfilingEnabled()) {
			this.chart = createChart(gateVisualizationData);
			this.chart.setLayoutData(new GridData(GridData.FILL_BOTH));
			height = 200;
		} else {
			this.chart = null;
			height = 100;
		}

		final Composite tableComposite = new Composite(getShell(), SWT.NONE);
		final GridLayout tableGridLayout = new GridLayout(2, false);
		tableGridLayout.marginHeight = 0;
		tableGridLayout.marginLeft = 0;
		tableComposite.setLayout(tableGridLayout);

		// Channel type
		ChannelType channelType;
		if (managementGate.isInputGate()) {
			channelType = managementGate.getVertex().getGroupVertex().getBackwardEdge(managementGate.getIndex())
				.getChannelType();
		} else {
			channelType = managementGate.getVertex().getGroupVertex().getForwardEdge(managementGate.getIndex())
				.getChannelType();
		}
		new Label(tableComposite, SWT.NONE).setText("Channel type:");
		this.channelTypeLabel = new Label(tableComposite, SWT.NONE);
		this.channelTypeLabel.setText(channelType.toString());

		new Label(tableComposite, SWT.NONE).setText("Transmitted record type:");
		this.recordTypeLabel = new Label(tableComposite, SWT.NONE);
		this.recordTypeLabel.setText(managementGate.getRecordType());

		if (!this.managementGate.isInputGate()) {
			final ManagementGroupEdge groupEdge = this.managementGate.getVertex().getGroupVertex().getForwardEdge(
				this.managementGate.getIndex());
			final GroupEdgeVisualizationData groupEdgeVisualizationData = (GroupEdgeVisualizationData) groupEdge
				.getAttachment();

			if (groupEdgeVisualizationData.isIOBottleneck()) {
				this.warningComposite = createWarningComposite(WARNINGTEXT, SWT.ICON_WARNING);
				height += ICONSIZE;
			} else {
				this.warningComposite = null;
			}
		} else {
			this.warningComposite = null;
		}

		getShell().setSize(WIDTH, height);

		finishInstantiation(x, y, WIDTH);
	}

	private ChartComposite createChart(GateVisualizationData visualizationData) {

		final String yAxisLabel = this.managementGate.isInputGate() ? "No data available/sec."
			: "Capacity limit reached/sec.";

		final JFreeChart chart = ChartFactory.createXYLineChart(null, "Time [sec.]", yAxisLabel, visualizationData
			.getChartCollection(), PlotOrientation.VERTICAL, false, false, false);

		// Set axis properly
		final XYPlot xyPlot = chart.getXYPlot();
		xyPlot.getDomainAxis().setAutoRange(true);
		xyPlot.getDomainAxis().setAutoRangeMinimumSize(60);

		xyPlot.getRangeAxis().setAutoRange(true);
		// xyPlot.getRangeAxis().setRange(0, 100);

		return new ChartComposite(getShell(), SWT.NONE, chart, true);
	}

	public void updateView() {

		// Redraw the chart
		if (this.chart != null) {
			this.chart.getChart().getXYPlot().configureDomainAxes();
			this.chart.getChart().fireChartChanged();
		}

		if (!this.managementGate.isInputGate()) {

			final ManagementGroupEdge groupEdge = this.managementGate.getVertex().getGroupVertex().getForwardEdge(
				this.managementGate.getIndex());
			final GroupEdgeVisualizationData groupEdgeVisualizationData = (GroupEdgeVisualizationData) groupEdge
				.getAttachment();
			if (groupEdgeVisualizationData.isIOBottleneck()) {
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
