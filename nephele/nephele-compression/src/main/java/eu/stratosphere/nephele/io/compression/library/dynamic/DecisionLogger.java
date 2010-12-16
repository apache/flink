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

package eu.stratosphere.nephele.io.compression.library.dynamic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;

public class DecisionLogger {

	private static final Log LOG = LogFactory.getLog(DecisionLogger.class);

	private static final Pattern CPU_PATTERN = Pattern
		.compile("^cpu\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+).+$");

	private static final Pattern NETWORK_PATTERN = Pattern
		.compile("^\\s*\\w+:(\\d+)\\s+\\d+\\s+\\d+\\s+\\d+\\s+\\d+\\s+\\d+\\s+\\d+\\s+\\d+\\s+(\\d+).+$");

	private static final int PERCENT = 100;

	private static final int FLUSHINTERVAL = 10;

	private final BufferedWriter bufferedWriter;

	private int flushCounter = 0;

	// CPU related variables
	private long lastCpuUser = 0;

	private long lastCpuNice = 0;

	private long lastCpuSys = 0;

	private long lastCpuIdle = 0;

	private long lastCpuIOWait = 0;

	private long lastCpuIrq = 0;

	private long lastCpuSoftirq = 0;

	private long lastTimestamp = -1;

	private long lastReceivedBytes = 0;

	private long lastTramsmittedBytes = 0;

	DecisionLogger(String filePrefix) {

		BufferedWriter tmpBufferedWriter = null;
		try {
			tmpBufferedWriter = new BufferedWriter(new FileWriter(GlobalConfiguration.getString(
				ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH)
				+ File.separator + filePrefix));
		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
		}
		this.bufferedWriter = tmpBufferedWriter;
	}

	public void log(long timestamp, double currentDataRate, int currentCompressionLevel) {

		if (this.bufferedWriter == null) {
			return;
		}

		try {
			bufferedWriter.write(timestamp + "\t" + currentDataRate + "\t" + currentCompressionLevel + "\t");

			if (lastTimestamp < 0) {
				updateCPUUtilization(timestamp, null);
				updateNetworkUtilization(timestamp, null);
				bufferedWriter.write("0\t0\t0\t0\t0\t0\t0\t0\n");
			} else {
				updateCPUUtilization(timestamp, bufferedWriter);
				updateNetworkUtilization(timestamp, bufferedWriter);
			}

			if (++flushCounter % FLUSHINTERVAL == 0) {

				this.bufferedWriter.flush();
				this.flushCounter = 0;
			}

		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
		}

		this.lastTimestamp = timestamp;
	}

	private void updateNetworkUtilization(long timestamp, BufferedWriter writer) throws IOException {

		try {
			final BufferedReader in = new BufferedReader(new FileReader("/proc/net/dev"));

			long receivedSum = 0;
			long transmittedSum = 0;

			String output;
			while ((output = in.readLine()) != null) {
				final Matcher networkMatcher = NETWORK_PATTERN.matcher(output);
				if (!networkMatcher.matches()) {
					continue;
				}
				/*
				 * Extract information according to
				 * http://linuxdevcenter.com/pub/a/linux/2000/11/16/LinuxAdmin.html
				 */

				receivedSum += Long.parseLong(networkMatcher.group(1));
				transmittedSum += Long.parseLong(networkMatcher.group(2));
			}

			in.close();

			long received = receivedSum - this.lastReceivedBytes;
			long transmitted = transmittedSum - this.lastTramsmittedBytes;

			// Convert to bytes per millisecond
			received /= (timestamp - this.lastTimestamp);
			transmitted /= (timestamp - this.lastTimestamp);

			// Convert to bytes per second
			received *= 1000;
			transmitted *= 1000;

			// Convert to MBit/s
			received /= (1024 * 1024 / 8);
			transmitted /= (1024 * 1024 / 8);

			if (writer != null) {
				writer.write(transmitted + "\t" + received + "\n");
			}

			// Store values for next call
			this.lastReceivedBytes = receivedSum;
			this.lastTramsmittedBytes = transmittedSum;

		} catch (NumberFormatException nfe) {
			throw new IOException("Error while reading network utilization: " + StringUtils.stringifyException(nfe));
		}
	}

	private void updateCPUUtilization(long timestamp, BufferedWriter writer) throws IOException {

		try {

			final BufferedReader in = new BufferedReader(new FileReader("/proc/stat"));
			final String output = in.readLine();
			if (output == null) {
				throw new IOException("Cannot read CPU utilization, return value is null");
			}

			in.close();

			final Matcher cpuMatcher = CPU_PATTERN.matcher(output);
			if (!cpuMatcher.matches()) {
				throw new IOException("Cannot extract CPU utilization from output \"" + output + "\"");
			}

			/*
			 * Extract the information from the read line according to
			 * http://www.linuxhowtos.org/System/procstat.htm
			 */

			final long cpuUser = Long.parseLong(cpuMatcher.group(1));
			final long cpuNice = Long.parseLong(cpuMatcher.group(2));
			final long cpuSys = Long.parseLong(cpuMatcher.group(3));
			final long cpuIdle = Long.parseLong(cpuMatcher.group(4));
			final long cpuIOWait = Long.parseLong(cpuMatcher.group(5));
			final long cpuIrq = Long.parseLong(cpuMatcher.group(6));
			final long cpuSoftirq = Long.parseLong(cpuMatcher.group(7));

			// Calculate deltas
			final long deltaCpuUser = cpuUser - this.lastCpuUser;
			final long deltaCpuNice = cpuNice - this.lastCpuNice;
			final long deltaCpuSys = cpuSys - this.lastCpuSys;
			final long deltaCpuIdle = cpuIdle - this.lastCpuIdle;
			final long deltaCpuIOWait = cpuIOWait - this.lastCpuIOWait;
			final long deltaCpuIrq = cpuIrq - this.lastCpuIrq;
			final long deltaCpuSoftirq = cpuSoftirq - this.lastCpuSoftirq;
			final long deltaSum = deltaCpuUser + deltaCpuNice + deltaCpuSys + deltaCpuIdle + deltaCpuIOWait
				+ deltaCpuIrq + deltaCpuSoftirq;

			// TODO: Fix deltaSum = 0 situation

			// Set the percentage values for the profiling data object
			/*
			 * profilingData.setIdleCPU((int)((deltaCpuIdle*PERCENT)/deltaSum));
			 * profilingData.setUserCPU((int)((deltaCpuUser*PERCENT)/deltaSum));
			 * profilingData.setSystemCPU((int)((deltaCpuSys*PERCENT)/deltaSum));
			 * profilingData.setIoWaitCPU((int)((deltaCpuIOWait*PERCENT)/deltaSum));
			 * profilingData.setHardIrqCPU((int)((deltaCpuIrq*PERCENT)/deltaSum));
			 * profilingData.setSoftIrqCPU((int)((deltaCpuSoftirq*PERCENT)/deltaSum));
			 */
			if (writer != null) {
				writer.write((deltaCpuUser * PERCENT / deltaSum) + "\t");
				writer.write((deltaCpuNice * PERCENT / deltaSum) + "\t");
				writer.write((deltaCpuSys * PERCENT / deltaSum) + "\t");
				writer.write((deltaCpuIOWait * PERCENT / deltaSum) + "\t");
				writer.write((deltaCpuIrq * PERCENT / deltaSum) + "\t");
				writer.write((deltaCpuSoftirq * PERCENT / deltaSum) + "\t");
			}

			// Store values for next call
			this.lastCpuUser = cpuUser;
			this.lastCpuNice = cpuNice;
			this.lastCpuSys = cpuSys;
			this.lastCpuIdle = cpuIdle;
			this.lastCpuIOWait = cpuIOWait;
			this.lastCpuIrq = cpuIrq;
			this.lastCpuSoftirq = cpuSoftirq;

		} catch (NumberFormatException nfe) {
			throw new IOException("Error while reading CPU utilization: " + StringUtils.stringifyException(nfe));
		}

	}

}
