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
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.experimental.chart.swt.ChartComposite;

import eu.stratosphere.nephele.topology.NetworkNode;

public class SWTInstanceToolTip extends SWTToolTip {

	private final NetworkNode networkNode;

	private final ChartComposite cpuChart;

	private final ChartComposite memoryChart;

	private final ChartComposite networkChart;

	private final static int WIDTH = 400;

	public SWTInstanceToolTip(Shell parent, final SWTToolTipCommandReceiver commandReceiver, NetworkNode networkNode,
			int x, int y) {
		super(parent, x, y);

		this.networkNode = networkNode;

		boolean isProfilingEnabled = false;
		final InstanceVisualizationData instanceVisualizationData = (InstanceVisualizationData) networkNode
			.getAttachment();
		if (instanceVisualizationData != null) {
			isProfilingEnabled = instanceVisualizationData.isProfilingEnabledForJob();
		}

		int height;

		// Set the title
		setTitle(networkNode.getName());

		// Only create chart if profiling is enabled
		if (isProfilingEnabled) {
			this.cpuChart = createCPUChart(instanceVisualizationData);
			this.cpuChart.setLayoutData(new GridData(GridData.FILL_BOTH));
			this.memoryChart = createMemoryChart(instanceVisualizationData);
			this.memoryChart.setLayoutData(new GridData(GridData.FILL_BOTH));
			this.networkChart = createNetworkChart(instanceVisualizationData);
			this.networkChart.setLayoutData(new GridData(GridData.FILL_BOTH));
			height = 460;
		} else {
			this.cpuChart = null;
			this.memoryChart = null;
			this.networkChart = null;
			height = 75;
		}

		// Available instance actions
		final Composite instanceActionComposite = new Composite(getShell(), SWT.NONE);
		instanceActionComposite.setLayout(new RowLayout(SWT.HORIZONTAL));

		final Button killInstanceButton = new Button(instanceActionComposite, SWT.PUSH);
		final String instanceName = this.networkNode.getName();
		killInstanceButton.setText("Kill instance...");
		killInstanceButton.setEnabled(this.networkNode.isLeafNode());
		killInstanceButton.addListener(SWT.Selection, new Listener() {

			@Override
			public void handleEvent(Event arg0) {
				commandReceiver.killInstance(instanceName);
			}

		});

		getShell().setSize(WIDTH, height);

		finishInstantiation(x, y, WIDTH);
	}

	private ChartComposite createCPUChart(InstanceVisualizationData instanceVisualizationData) {

		final JFreeChart chart = ChartFactory.createStackedXYAreaChart(null, "Time [sec.]", "CPU",
			instanceVisualizationData.getCpuDataSet(), PlotOrientation.VERTICAL, true, true, false);

		// Set axis properly
		final XYPlot xyPlot = chart.getXYPlot();
		xyPlot.getDomainAxis().setAutoRange(true);
		xyPlot.getDomainAxis().setAutoRangeMinimumSize(60);

		xyPlot.getRangeAxis().setAutoRange(false);
		xyPlot.getRangeAxis().setRange(0, 100);

		return new ChartComposite(getShell(), SWT.NONE, chart, true);
	}

	private ChartComposite createMemoryChart(InstanceVisualizationData instanceVisualizationData) {

		final JFreeChart chart = ChartFactory.createStackedXYAreaChart(null, "Time [sec.]", "Memory",
			instanceVisualizationData.getMemoryDataSet(), PlotOrientation.VERTICAL, true, true, false);

		// Set axis properly
		final XYPlot xyPlot = chart.getXYPlot();
		xyPlot.getDomainAxis().setAutoRange(true);
		xyPlot.getDomainAxis().setAutoRangeMinimumSize(60);

		xyPlot.getRangeAxis().setAutoRange(false);
		xyPlot.getRangeAxis().setRange(0, instanceVisualizationData.getUpperBoundForMemoryChart());

		return new ChartComposite(getShell(), SWT.NONE, chart, true);
	}

	private ChartComposite createNetworkChart(InstanceVisualizationData instanceVisualizationData) {

		final JFreeChart chart = ChartFactory.createStackedXYAreaChart(null, "Time [sec.]", "Network",
			instanceVisualizationData.getNetworkDataSet(), PlotOrientation.VERTICAL, true, true, false);

		// Set axis properly
		final XYPlot xyPlot = chart.getXYPlot();
		xyPlot.getDomainAxis().setAutoRange(true);
		xyPlot.getDomainAxis().setAutoRangeMinimumSize(60);

		// TODO: Repair auto range for range axis
		xyPlot.getRangeAxis().setAutoRange(false);
		xyPlot.getRangeAxis().setRange(0, 4096);

		return new ChartComposite(getShell(), SWT.NONE, chart, true);
	}

	@Override
	public void updateView() {

		// Redraw the CPU chart
		if (this.cpuChart != null) {
			this.cpuChart.getChart().getXYPlot().configureDomainAxes();
			this.cpuChart.getChart().fireChartChanged();
		}
		if (this.memoryChart != null) {

			// Workaround because auto range function appears to be broken
			final InstanceVisualizationData instanceVisualizationData = (InstanceVisualizationData) this.networkNode
				.getAttachment();
			final double newUpperBound = instanceVisualizationData.getUpperBoundForMemoryChart();
			if (newUpperBound > this.memoryChart.getChart().getXYPlot().getRangeAxis().getUpperBound()) {
				this.memoryChart.getChart().getXYPlot().getRangeAxis().setRange(0, newUpperBound);
			}

			this.memoryChart.getChart().getXYPlot().configureDomainAxes();
			this.memoryChart.getChart().fireChartChanged();
		}
		if (this.networkChart != null) {
			this.networkChart.getChart().getXYPlot().configureDomainAxes();
			this.networkChart.getChart().fireChartChanged();
		}
	}

}
