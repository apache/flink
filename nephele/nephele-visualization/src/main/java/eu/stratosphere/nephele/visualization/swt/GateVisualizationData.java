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

import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import eu.stratosphere.nephele.profiling.types.InputGateProfilingEvent;
import eu.stratosphere.nephele.profiling.types.OutputGateProfilingEvent;

public class GateVisualizationData {

	private static final double ALPHA = 0.25f;

	private final XYSeriesCollection chartCollection;

	private final XYSeries chartSeries;

	private final boolean profilingEnabled;

	private boolean isIOBottleneck = false;

	private double averageChartData = 0.0f;

	public GateVisualizationData(boolean profilingEnabled) {
		this.profilingEnabled = profilingEnabled;

		if (this.profilingEnabled) {
			this.chartSeries = new XYSeries("Chart data", false, false);
			this.chartSeries.setNotify(false);
			this.chartCollection = new XYSeriesCollection(this.chartSeries);
		} else {
			this.chartSeries = null;
			this.chartCollection = null;
		}
	}

	public boolean isProfilingEnabled() {
		return this.profilingEnabled;
	}

	public void processInputGateProfilingEvent(InputGateProfilingEvent inputGateProfilingEvent) {

		final double timestamp = VertexVisualizationData.getTimestamp(inputGateProfilingEvent);
		addChartData(timestamp, inputGateProfilingEvent.getNoRecordsAvailableCounter());
	}

	public void processOutputGateProfilingEvent(OutputGateProfilingEvent outputGateProfilingEvent) {

		final double timestamp = VertexVisualizationData.getTimestamp(outputGateProfilingEvent);
		addChartData(timestamp, outputGateProfilingEvent.getChannelCapacityExhausted());
	}

	private void addChartData(double timestamp, double chartData) {

		this.averageChartData = (this.averageChartData * (1 - ALPHA)) + (chartData * ALPHA);

		if (this.chartSeries != null) {
			this.chartSeries.addOrUpdate(timestamp, chartData);
		}
	}

	public XYDataset getChartCollection() {
		return this.chartCollection;
	}

	public void setIOBottleneck(boolean isIOBottleneck) {
		this.isIOBottleneck = isIOBottleneck;
	}

	public boolean isIOBottleneck() {
		return this.isIOBottleneck;
	}

	public double getAverageChartData() {
		return this.averageChartData;
	}
}
