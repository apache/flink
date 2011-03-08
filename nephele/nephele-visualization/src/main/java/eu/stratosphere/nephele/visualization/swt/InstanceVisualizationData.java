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

import java.util.Iterator;
import java.util.List;

import org.jfree.data.xy.DefaultTableXYDataset;
import org.jfree.data.xy.TableXYDataset;
import org.jfree.data.xy.XYDataItem;
import org.jfree.data.xy.XYSeries;

import eu.stratosphere.nephele.profiling.types.InstanceProfilingEvent;

public class InstanceVisualizationData {

	private final static long BYTE_TO_MEGABIT = 1024 * 1024 / 8;

	private final static long KILOBYTE_TO_MEGABYTE = 1024;

	private final DefaultTableXYDataset cpuDataSet;

	private final DefaultTableXYDataset memoryDataSet;

	private final DefaultTableXYDataset networkDataSet;

	// Series for CPU data
	private final XYSeries cpuUsrSeries;

	private final XYSeries cpuSysSeries;

	private final XYSeries cpuWaitSeries;

	private final XYSeries cpuHardIrqSeries;

	private final XYSeries cpuSoftIrqSeries;

	// Series for network data
	private final XYSeries networkReceivedSeries;

	private final XYSeries networkTransmittedSeries;

	// Series for memory data
	private final XYSeries totalMemorySeries;

	private final XYSeries usedMemorySeries;

	private final XYSeries cachedMemorySeries;

	private final boolean isProfilingAvailable;

	private long totalMemoryinMB = 1024;

	public InstanceVisualizationData(boolean isProfilingAvailable) {

		this.isProfilingAvailable = isProfilingAvailable;

		this.cpuDataSet = new DefaultTableXYDataset();
		this.memoryDataSet = new DefaultTableXYDataset();
		this.networkDataSet = new DefaultTableXYDataset();

		this.cpuUsrSeries = new XYSeries("USR", false, false);
		this.cpuUsrSeries.setNotify(false);
		this.cpuSysSeries = new XYSeries("SYS", false, false);
		this.cpuSysSeries.setNotify(false);
		this.cpuWaitSeries = new XYSeries("WAIT", false, false);
		this.cpuWaitSeries.setNotify(false);
		this.cpuHardIrqSeries = new XYSeries("HI", false, false);
		this.cpuHardIrqSeries.setNotify(false);
		this.cpuSoftIrqSeries = new XYSeries("SI", false, false);
		this.cpuSoftIrqSeries.setNotify(false);

		// Initialize data sets
		this.cpuDataSet.addSeries(this.cpuWaitSeries);
		this.cpuDataSet.addSeries(this.cpuSysSeries);
		this.cpuDataSet.addSeries(this.cpuUsrSeries);
		this.cpuDataSet.addSeries(this.cpuHardIrqSeries);
		this.cpuDataSet.addSeries(this.cpuSoftIrqSeries);

		this.networkReceivedSeries = new XYSeries("Received", false, false);
		this.networkReceivedSeries.setNotify(false);
		this.networkTransmittedSeries = new XYSeries("Transmitted", false, false);
		this.networkTransmittedSeries.setNotify(false);

		this.networkDataSet.addSeries(this.networkReceivedSeries);
		this.networkDataSet.addSeries(this.networkTransmittedSeries);

		this.totalMemorySeries = new XYSeries("Total", false, false);
		this.totalMemorySeries.setNotify(false);
		this.usedMemorySeries = new XYSeries("Used", false, false);
		this.usedMemorySeries.setNotify(false);
		this.cachedMemorySeries = new XYSeries("Cached", false, false);
		this.cachedMemorySeries.setNotify(false);

		// We do not add the total memory to the collection
		this.memoryDataSet.addSeries(this.cachedMemorySeries);
		this.memoryDataSet.addSeries(this.usedMemorySeries);
	}

	public TableXYDataset getCpuDataSet() {
		return this.cpuDataSet;
	}

	public TableXYDataset getMemoryDataSet() {
		return this.memoryDataSet;
	}

	public TableXYDataset getNetworkDataSet() {
		return this.networkDataSet;
	}

	public double getUpperBoundForMemoryChart() {
		return ((double) this.totalMemoryinMB) * 1.05;
	}

	public void processInstanceProfilingEvent(InstanceProfilingEvent instanceProfilingEvent) {

		double timestamp = VertexVisualizationData.getTimestamp(instanceProfilingEvent);

		final long instanceMemoryInMB = instanceProfilingEvent.getTotalMemory() / KILOBYTE_TO_MEGABYTE;
		if (instanceMemoryInMB > this.totalMemoryinMB) {
			this.totalMemoryinMB = instanceMemoryInMB;
		}

		final long cachedMemory = instanceProfilingEvent.getBufferedMemory() + instanceProfilingEvent.getCachedMemory()
			+ instanceProfilingEvent.getCachedSwapMemory();

		final long usedMemory = instanceProfilingEvent.getTotalMemory() - instanceProfilingEvent.getFreeMemory()
			- cachedMemory;

		this.cpuUsrSeries.addOrUpdate(timestamp, instanceProfilingEvent.getUserCPU());
		this.cpuSysSeries.addOrUpdate(timestamp, instanceProfilingEvent.getSystemCPU());
		this.cpuWaitSeries.addOrUpdate(timestamp, instanceProfilingEvent.getIOWaitCPU());
		this.cpuHardIrqSeries.addOrUpdate(timestamp, instanceProfilingEvent.getHardIrqCPU());
		this.cpuSoftIrqSeries.addOrUpdate(timestamp, instanceProfilingEvent.getSoftIrqCPU());
		this.totalMemorySeries.addOrUpdate(timestamp, instanceProfilingEvent.getTotalMemory() / KILOBYTE_TO_MEGABYTE);
		this.usedMemorySeries.addOrUpdate(timestamp, usedMemory / KILOBYTE_TO_MEGABYTE);
		this.cachedMemorySeries.addOrUpdate(timestamp, cachedMemory / KILOBYTE_TO_MEGABYTE);
		this.networkReceivedSeries.addOrUpdate(timestamp, toMBitPerSec(instanceProfilingEvent.getReceivedBytes(),
			instanceProfilingEvent.getProfilingInterval()));
		this.networkTransmittedSeries.addOrUpdate(timestamp, toMBitPerSec(instanceProfilingEvent.getTransmittedBytes(),
			instanceProfilingEvent.getProfilingInterval()));
	}

	@SuppressWarnings("unchecked")
	public double getAverageUserTime() {

		double av = 0.0f;

		if (this.cpuUsrSeries != null) {
			av = calculateAverage(this.cpuUsrSeries.getItems());
		}

		return av;
	}

	private double calculateAverage(List<XYDataItem> list) {

		double av = 0.0f;

		Iterator<XYDataItem> it = list.iterator();
		while (it.hasNext()) {
			av += it.next().getYValue();
		}

		return (av / (double) list.size());
	}

	private final double toMBitPerSec(long numberOfBytes, long profilingPeriod) {

		return (((double) numberOfBytes) / ((double) (BYTE_TO_MEGABIT * profilingPeriod / 1000L)));
	}

	public boolean isProfilingEnabledForJob() {
		return this.isProfilingAvailable;
	}
}
