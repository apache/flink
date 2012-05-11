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

import org.jfree.data.xy.DefaultTableXYDataset;
import org.jfree.data.xy.TableXYDataset;
import org.jfree.data.xy.XYSeries;

import eu.stratosphere.nephele.profiling.types.ProfilingEvent;
import eu.stratosphere.nephele.profiling.types.ThreadProfilingEvent;

public class VertexVisualizationData {

	private static final double ALPHA = 0.25f;

	private final DefaultTableXYDataset threadDataSet;

	private final boolean isProfilingAvailable;

	// Series for thread data
	private final XYSeries blockSeries;

	private final XYSeries waitSeries;

	private final XYSeries sysSeries;

	private final XYSeries usrSeries;

	private double averageUserRate = 0.0f;

	private double averageWaitRate = 0.0f;

	public VertexVisualizationData(boolean isProfilingAvailable) {

		this.isProfilingAvailable = isProfilingAvailable;

		if (this.isProfilingAvailable) {

			this.threadDataSet = new DefaultTableXYDataset();

			this.blockSeries = new XYSeries("BLK", false, false);
			this.blockSeries.setNotify(false);
			this.waitSeries = new XYSeries("WAIT", false, false);
			this.waitSeries.setNotify(false);
			this.sysSeries = new XYSeries("SYS", false, false);
			this.sysSeries.setNotify(false);
			this.usrSeries = new XYSeries("USR", false, false);
			this.usrSeries.setNotify(false);

			this.threadDataSet.addSeries(waitSeries);
			this.threadDataSet.addSeries(sysSeries);
			this.threadDataSet.addSeries(usrSeries);
			this.threadDataSet.addSeries(blockSeries);
		} else {
			this.threadDataSet = null;

			this.blockSeries = null;
			this.waitSeries = null;
			this.sysSeries = null;
			this.usrSeries = null;
		}
	}

	public TableXYDataset getThreadDataSet() {
		return this.threadDataSet;
	}

	public boolean isProfilingEnabledForJob() {
		return this.isProfilingAvailable;
	}

	public void processThreadProfilingEvent(ThreadProfilingEvent threadProfilingEvent) {

		final double timestamp = getTimestamp(threadProfilingEvent);
		this.averageUserRate = (this.averageUserRate * (1 - ALPHA)) + (threadProfilingEvent.getUserTime() * ALPHA);
		this.usrSeries.addOrUpdate(timestamp, threadProfilingEvent.getUserTime());
		this.sysSeries.addOrUpdate(timestamp, threadProfilingEvent.getSystemTime());
		this.averageWaitRate = (this.averageWaitRate * (1 - ALPHA)) + (threadProfilingEvent.getWaitedTime() * ALPHA);
		this.waitSeries.addOrUpdate(timestamp, threadProfilingEvent.getWaitedTime());
		this.blockSeries.addOrUpdate(timestamp, threadProfilingEvent.getBlockedTime());
	}

	public double getAverageWaitTime() {

		return this.averageWaitRate;
	}

	public double getAverageUserTime() {

		return this.averageUserRate;
	}

	public static final double getTimestamp(ProfilingEvent profilingEvent) {

		return Math.rint(profilingEvent.getProfilingTimestamp() / 1000);
	}
}
