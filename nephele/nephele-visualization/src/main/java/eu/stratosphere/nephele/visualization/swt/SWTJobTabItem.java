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
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.TableXYDataset;
import org.jfree.experimental.chart.swt.ChartComposite;

import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.topology.NetworkTopology;

public class SWTJobTabItem extends Composite implements Listener {

	private final ChartComposite cpuChart;

	private final ChartComposite memoryChart;

	private final ChartComposite networkChart;

	private final TabFolder tabFolder;

	private final SWTGraphCanvas graphCanvas;

	private final SWTTopologyCanvas topologyCanvas;

	private final GraphVisualizationData visualizationData;

	private final SWTVisualizationGUI visualizationGUI;

	public SWTJobTabItem(SWTVisualizationGUI visualizationGUI, GraphVisualizationData visualizationData,
			Composite parent, int style, boolean detectBottlenecks) {
		super(parent, style);

		this.setLayout(new FillLayout());

		this.visualizationGUI = visualizationGUI;
		this.visualizationData = visualizationData;

		// Layout of GUI depends on availability of profiling data
		if (visualizationData.isProfilingAvailableForJob()) {

			final SashForm verticalSash = new SashForm(this, SWT.VERTICAL);

			// Create the tabs
			this.tabFolder = new TabFolder(verticalSash, SWT.BOTTOM);
			this.tabFolder.addListener(SWT.Selection, this);

			final TabItem graphItem = new TabItem(this.tabFolder, SWT.NONE);
			this.graphCanvas = new SWTGraphCanvas(visualizationData, this, tabFolder, SWT.NONE, detectBottlenecks);
			graphItem.setControl(this.graphCanvas);
			graphItem.setText("Execution Graph");

			final TabItem topologyItem = new TabItem(tabFolder, SWT.NONE);
			this.topologyCanvas = new SWTTopologyCanvas(visualizationData, this, tabFolder, SWT.NONE);
			topologyItem.setControl(this.topologyCanvas);
			topologyItem.setText("Allocated Instances");

			final Composite statisticsComposite = new Composite(verticalSash, SWT.NONE);
			statisticsComposite.setLayout(new FillLayout(SWT.HORIZONTAL));

			final NetworkTopology networkTopology = visualizationData.getNetworkTopology();
			final InstanceVisualizationData summaryDataset = (InstanceVisualizationData) networkTopology
				.getAttachment();

			this.cpuChart = initializeCPUChart(statisticsComposite, summaryDataset.getCpuDataSet());
			this.memoryChart = initializeMemoryChart(statisticsComposite, summaryDataset.getMemoryDataSet());
			this.networkChart = initializeNetworkChart(statisticsComposite, summaryDataset.getNetworkDataSet());

			verticalSash.setWeights(new int[] { 7, 3 });

		} else {

			// Create the tabs
			this.tabFolder = new TabFolder(this, SWT.BOTTOM);
			final TabItem graphItem = new TabItem(this.tabFolder, SWT.NONE);
			this.graphCanvas = new SWTGraphCanvas(visualizationData, this, tabFolder, SWT.NONE, detectBottlenecks);
			graphItem.setControl(graphCanvas);
			graphItem.setText("Execution Graph");

			final TabItem topologyItem = new TabItem(tabFolder, SWT.NONE);
			this.topologyCanvas = new SWTTopologyCanvas(visualizationData, this, tabFolder, SWT.NONE);
			topologyItem.setControl(this.topologyCanvas);
			topologyItem.setText("Allocated Instances");

			this.cpuChart = null;
			this.memoryChart = null;
			this.networkChart = null;
		}

	}

	private ChartComposite initializeCPUChart(Composite parentComposite, TableXYDataset dataset) {

		final JFreeChart chart = ChartFactory.createStackedXYAreaChart("CPU", "Time [sec.]",
			"Average CPU utilization [%]", dataset, PlotOrientation.VERTICAL, true, true, false);

		// Set axis properly
		final XYPlot xyPlot = chart.getXYPlot();
		xyPlot.getDomainAxis().setAutoRange(true);
		xyPlot.getDomainAxis().setAutoRangeMinimumSize(60);

		xyPlot.getRangeAxis().setAutoRange(false);
		xyPlot.getRangeAxis().setRange(0, 100);

		return new ChartComposite(parentComposite, SWT.NONE, chart, true);
	}

	private ChartComposite initializeMemoryChart(Composite parentComposite, TableXYDataset dataset) {

		final JFreeChart chart = ChartFactory.createStackedXYAreaChart("Memory", "Time [sec.]",
			"Average amount of memory allocated [MB]", dataset, PlotOrientation.VERTICAL, true, true, false);

		// Set axis properly
		final XYPlot xyPlot = chart.getXYPlot();
		xyPlot.getDomainAxis().setAutoRange(true);
		xyPlot.getDomainAxis().setAutoRangeMinimumSize(60);

		// Workaround because auto range does not seem to work properly on the range axis
		final InstanceVisualizationData instanceVisualizationData = (InstanceVisualizationData) this.visualizationData
			.getNetworkTopology().getAttachment();
		xyPlot.getRangeAxis().setAutoRange(false);
		xyPlot.getRangeAxis().setRange(0, instanceVisualizationData.getUpperBoundForMemoryChart());

		return new ChartComposite(parentComposite, SWT.NONE, chart, true);
	}

	private ChartComposite initializeNetworkChart(Composite parentComposite, TableXYDataset dataset) {

		final JFreeChart chart = ChartFactory.createStackedXYAreaChart("Network", "Time [sec.]",
			"Average throughput [MBit/s]", dataset, PlotOrientation.VERTICAL, true, true, false);

		// Set axis properly
		final XYPlot xyPlot = chart.getXYPlot();
		xyPlot.getDomainAxis().setAutoRange(true);
		xyPlot.getDomainAxis().setAutoRangeMinimumSize(60);

		// TODO: Repair auto range for range axis
		xyPlot.getRangeAxis().setAutoRange(false);
		xyPlot.getRangeAxis().setRange(0, 4096);

		return new ChartComposite(parentComposite, SWT.NONE, chart, true);
	}

	public void updateView() {

		if (this.cpuChart != null) {
			this.cpuChart.getChart().getXYPlot().configureDomainAxes();
			this.cpuChart.getChart().fireChartChanged();
		}
		if (this.memoryChart != null) {

			// Workaround because auto range function appears to be broken
			final InstanceVisualizationData instanceVisualizationData = (InstanceVisualizationData) this.visualizationData
				.getNetworkTopology().getAttachment();
			final double newUpperBound = instanceVisualizationData.getUpperBoundForMemoryChart();
			if (newUpperBound > this.memoryChart.getChart().getXYPlot().getRangeAxis().getUpperBound()) {
				this.memoryChart.getChart().getXYPlot().getRangeAxis().setRange(0, newUpperBound);
			}

			this.memoryChart.getChart().getXYPlot().configureDomainAxes();
			this.memoryChart.getChart().fireChartChanged();
		}
		if (this.networkChart != null) {
			this.networkChart.getChart().getXYPlot().configureDomainAxes();
			this.networkChart.getChart().getXYPlot().configureRangeAxes();
			this.networkChart.getChart().fireChartChanged();
		}

		if (this.tabFolder.getSelectionIndex() == 0) {

			this.graphCanvas.updateView();

			// Graph canvas
			/*
			 * if(this.visualizationData.findBottlenecks()) {
			 * this.graphCanvas.startAnimation();
			 * } else {
			 * this.graphCanvas.stopAnimation();
			 * this.graphCanvas.updateView();
			 * }
			 */
		} else {
			this.topologyCanvas.updateView();
		}
	}

	public void switchToInstance(String instanceName) {

		MessageBox messageBox = new MessageBox(getShell(), SWT.ICON_ERROR);
		messageBox.setText("Unsupported feature");
		messageBox.setMessage("Displaying profiling data of individual instances is currently not supported.");
		messageBox.open();
	}

	@Override
	public void handleEvent(Event arg0) {

		/*
		 * if(arg0.widget == this.tabFolder) {
		 * if(this.tabFolder.getSelectionIndex() != 0) {
		 * this.graphCanvas.stopAnimation();
		 * } else {
		 * if(this.visualizationData.findBottlenecks()) {
		 * this.graphCanvas.startAnimation();
		 * }
		 * }
		 * }
		 */

	}

	public void tabSelected() {
		// TODO: Implement me
	}

	public void tabDeselected() {
		// TODO: Implement me
	}

	public void killTask(ManagementVertexID id, String taskName) {

		// Delegate call to the main GUI
		this.visualizationGUI.killTask(this.visualizationData.getJobID(), id, taskName);
	}

	public void killInstance(String instanceName) {

		// Delegate call to the main GUI
		this.visualizationGUI.killInstance(instanceName);
	}
}
