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

package eu.stratosphere.nephele.profiling.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.profiling.impl.types.InternalInstanceProfilingData;
import eu.stratosphere.nephele.util.StringUtils;

public class InstanceProfiler /* implements CompressionInstanceProfiler */{ // TODO: Fix me

	private static final Pattern CPU_PATTERN = Pattern
		.compile("^cpu\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+).+$");

	private static final Pattern NETWORK_PATTERN = Pattern
		.compile("^\\s*\\w+:(\\d+)\\s+\\d+\\s+\\d+\\s+\\d+\\s+\\d+\\s+\\d+\\s+\\d+\\s+\\d+\\s+(\\d+).+$");

	private static final Pattern MEMORY_PATTERN = Pattern.compile("^\\w+:\\s*(\\d+)\\s+kB$");

	private static final int PERCENT = 100;

	private final InstanceConnectionInfo instanceConnectionInfo;

	private long lastTimestamp = 0;

	// CPU related variables
	private long lastCpuUser = 0;

	private long lastCpuNice = 0;

	private long lastCpuSys = 0;

	private long lastCpuIdle = 0;

	private long lastCpuIOWait = 0;

	private long lastCpuIrq = 0;

	private long lastCpuSoftirq = 0;

	// Network related variables
	private long lastReceivedBytes = 0;

	private long lastTramsmittedBytes = 0;

	private String interfaceName = ""; // eth0 or eth1 ...

	private int networkSpeedPerSecondInBytes = 0;

	public InstanceProfiler(InstanceConnectionInfo instanceConnectionInfo)
																			throws ProfilingException {

		this.instanceConnectionInfo = instanceConnectionInfo;

		// Initialize counters by calling generateProfilingData once and ignore the return value
		generateProfilingData(System.currentTimeMillis());
	}

	public InstanceProfiler(InstanceConnectionInfo instanceConnectionInfo, boolean forCompression)
																									throws ProfilingException {

		this.instanceConnectionInfo = instanceConnectionInfo;

		gatherNetworkInformation();
		System.out.println("Networkinterface is " + interfaceName + " Speed: " + networkSpeedPerSecondInBytes
			+ "B/s = " + networkSpeedPerSecondInBytes / 125000 + "Mbps");
		// Initialize counters by calling generateProfilingData once and ignore the return value
		// generateProfilingDataCompression(System.currentTimeMillis()); //TODO: Fix me
	}

	InternalInstanceProfilingData generateProfilingData(long timestamp) throws ProfilingException {

		final long profilingInterval = timestamp - lastTimestamp;

		final InternalInstanceProfilingData profilingData = new InternalInstanceProfilingData(
			this.instanceConnectionInfo, (int) profilingInterval);

		updateCPUUtilization(profilingData);
		updateMemoryUtilization(profilingData);
		updateNetworkUtilization(profilingData);

		// Update timestamp
		this.lastTimestamp = timestamp;

		return profilingData;
	}

	private void updateMemoryUtilization(InternalInstanceProfilingData profilingData) throws ProfilingException {

		try {

			final BufferedReader in = new BufferedReader(new FileReader("/proc/meminfo"));

			long freeMemory = 0;
			long totalMemory = 0;
			long bufferedMemory = 0;
			long cachedMemory = 0;
			long cachedSwapMemory = 0;

			int count = 0;
			String output;
			while ((output = in.readLine()) != null) {

				switch (count) {
				case 0: // Total memory
					totalMemory = extractMemoryValue(output);
					break;
				case 1: // Free memory
					freeMemory = extractMemoryValue(output);
					break;
				case 2: // Buffers
					bufferedMemory = extractMemoryValue(output);
					break;
				case 3: // Cache
					cachedMemory = extractMemoryValue(output);
					break;
				case 4:
					cachedSwapMemory = extractMemoryValue(output);
					break;
				default:
					break;
				}

				++count;
			}

			profilingData.setTotalMemory(totalMemory);
			profilingData.setFreeMemory(freeMemory);
			profilingData.setBufferedMemory(bufferedMemory);
			profilingData.setCachedMemory(cachedMemory);
			profilingData.setCachedSwapMemory(cachedSwapMemory);

		} catch (IOException ioe) {
			throw new ProfilingException("Error while reading network utilization: "
				+ StringUtils.stringifyException(ioe));
		}

	}

	private long extractMemoryValue(String line) throws ProfilingException {

		final Matcher matcher = MEMORY_PATTERN.matcher(line);
		if (!matcher.matches()) {
			throw new ProfilingException("Cannot extract memory data for profiling from line " + line);
		}

		return Long.parseLong(matcher.group(1));
	}

	private void updateNetworkUtilization(InternalInstanceProfilingData profilingData) throws ProfilingException {

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

			profilingData.setReceivedBytes(receivedSum - this.lastReceivedBytes);
			profilingData.setTransmittedBytes(transmittedSum - this.lastTramsmittedBytes);

			// Store values for next call
			this.lastReceivedBytes = receivedSum;
			this.lastTramsmittedBytes = transmittedSum;

		} catch (IOException ioe) {
			throw new ProfilingException("Error while reading network utilization: "
				+ StringUtils.stringifyException(ioe));
		} catch (NumberFormatException nfe) {
			throw new ProfilingException("Error while reading network utilization: "
				+ StringUtils.stringifyException(nfe));
		}
	}

	private void updateCPUUtilization(InternalInstanceProfilingData profilingData) throws ProfilingException {

		try {

			final BufferedReader in = new BufferedReader(new FileReader("/proc/stat"));
			final String output = in.readLine();
			if (output == null) {
				throw new ProfilingException("Cannot read CPU utilization, return value is null");
			}

			in.close();

			final Matcher cpuMatcher = CPU_PATTERN.matcher(output);
			if (!cpuMatcher.matches()) {
				throw new ProfilingException("Cannot extract CPU utilization from output \"" + output + "\"");
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
			// TODO: bad quick fix
			if (deltaSum > 0) {
				profilingData.setIdleCPU((int) ((deltaCpuIdle * PERCENT) / deltaSum));
				profilingData.setUserCPU((int) ((deltaCpuUser * PERCENT) / deltaSum));
				profilingData.setSystemCPU((int) ((deltaCpuSys * PERCENT) / deltaSum));
				profilingData.setIoWaitCPU((int) ((deltaCpuIOWait * PERCENT) / deltaSum));
				profilingData.setHardIrqCPU((int) ((deltaCpuIrq * PERCENT) / deltaSum));
				profilingData.setSoftIrqCPU((int) ((deltaCpuSoftirq * PERCENT) / deltaSum));
			} else {
				profilingData.setIdleCPU((int) (deltaCpuIdle));
				profilingData.setUserCPU((int) (deltaCpuUser));
				profilingData.setSystemCPU((int) (deltaCpuSys));
				profilingData.setIoWaitCPU((int) (deltaCpuIOWait));
				profilingData.setHardIrqCPU((int) (deltaCpuIrq));
				profilingData.setSoftIrqCPU((int) (deltaCpuSoftirq));
			}
			// Store values for next call
			this.lastCpuUser = cpuUser;
			this.lastCpuNice = cpuNice;
			this.lastCpuSys = cpuSys;
			this.lastCpuIdle = cpuIdle;
			this.lastCpuIOWait = cpuIOWait;
			this.lastCpuIrq = cpuIrq;
			this.lastCpuSoftirq = cpuSoftirq;

		} catch (IOException ioe) {
			throw new ProfilingException("Error while reading CPU utilization: " + StringUtils.stringifyException(ioe));
		} catch (NumberFormatException nfe) {
			throw new ProfilingException("Error while reading CPU utilization: " + StringUtils.stringifyException(nfe));
		}

	}

	// @Override //TODO: Fix me
	/*
	 * public InternalInstanceProfilingDataCompression generateProfilingDataCompression(
	 * long timestamp) {
	 * final long profilingInterval = timestamp - lastTimestamp;
	 * final InternalInstanceProfilingDataCompression profilingData = new
	 * InternalInstanceProfilingDataCompression(this.instanceConnectionInfo,
	 * (int) profilingInterval);
	 * try {
	 * updateCPUUtilization(profilingData);
	 * } catch (ProfilingException e) {
	 * // TODO Auto-generated catch block
	 * e.printStackTrace();
	 * }
	 * //updateMemoryUtilization(profilingData);
	 * try {
	 * updateNetworkUtilization(profilingData);
	 * profilingData.setGoodput((long)(networkSpeedPerSecondInBytes * 0.8));
	 * } catch (ProfilingException e) {
	 * // TODO Auto-generated catch block
	 * e.printStackTrace();
	 * }
	 * //Update timestamp
	 * this.lastTimestamp = timestamp;
	 * return profilingData;
	 * }
	 */

	/*
	 * private void updateNetworkUtilization(InternalInstanceProfilingDataCompression profilingData) throws
	 * ProfilingException {
	 * try {
	 * final BufferedReader in = new BufferedReader(new FileReader("/proc/net/dev"));
	 * long receivedSum = 0;
	 * long transmittedSum = 0;
	 * String output;
	 * while((output = in.readLine()) != null) {
	 * final Matcher networkMatcher = NETWORK_PATTERN.matcher(output);
	 * if(!networkMatcher.matches()) {
	 * continue;
	 * }
	 * if (output.indexOf(this.interfaceName) != -1){
	 */
	/*
	 * Extract information according to
	 * http://linuxdevcenter.com/pub/a/linux/2000/11/16/LinuxAdmin.html
	 */
	/*
	 * receivedSum += Long.parseLong(networkMatcher.group(1));
	 * transmittedSum += Long.parseLong(networkMatcher.group(2));
	 * break;
	 * }
	 * }
	 * in.close();
	 * profilingData.setReceivedBytes(receivedSum - this.lastReceivedBytes);
	 * profilingData.setTransmittedBytes(transmittedSum - this.lastTramsmittedBytes);
	 * //Store values for next call
	 * this.lastReceivedBytes = receivedSum;
	 * this.lastTramsmittedBytes = transmittedSum;
	 * } catch(IOException ioe) {
	 * throw new ProfilingException("Error while reading network utilization: " + StringUtils.stringifyException(ioe));
	 * } catch(NumberFormatException nfe) {
	 * throw new ProfilingException("Error while reading network utilization: " + StringUtils.stringifyException(nfe));
	 * }
	 * }
	 */

	/*
	 * private void updateCPUUtilization(InternalInstanceProfilingDataCompression profilingData) throws
	 * ProfilingException {
	 * try {
	 * final BufferedReader in = new BufferedReader(new FileReader("/proc/stat"));
	 * final String output = in.readLine();
	 * if(output == null) {
	 * throw new ProfilingException("Cannot read CPU utilization, return value is null");
	 * }
	 * in.close();
	 * final Matcher cpuMatcher = CPU_PATTERN.matcher(output);
	 * if(!cpuMatcher.matches()) {
	 * throw new ProfilingException("Cannot extract CPU utilization from output \"" + output + "\"");
	 * }
	 */

	/*
	 * Extract the information from the read line according to
	 * http://www.linuxhowtos.org/System/procstat.htm
	 */

	/*
	 * final long cpuUser = Long.parseLong(cpuMatcher.group(1));
	 * final long cpuNice = Long.parseLong(cpuMatcher.group(2));
	 * final long cpuSys = Long.parseLong(cpuMatcher.group(3));
	 * final long cpuIdle = Long.parseLong(cpuMatcher.group(4));
	 * final long cpuIOWait = Long.parseLong(cpuMatcher.group(5));
	 * final long cpuIrq = Long.parseLong(cpuMatcher.group(6));
	 * final long cpuSoftirq = Long.parseLong(cpuMatcher.group(7));
	 * //Calculate deltas
	 * final long deltaCpuUser = cpuUser - this.lastCpuUser;
	 * final long deltaCpuNice = cpuNice - this.lastCpuNice;
	 * final long deltaCpuSys = cpuSys - this.lastCpuSys;
	 * final long deltaCpuIdle = cpuIdle - this.lastCpuIdle;
	 * final long deltaCpuIOWait = cpuIOWait - this.lastCpuIOWait;
	 * final long deltaCpuIrq = cpuIrq - this.lastCpuIrq;
	 * final long deltaCpuSoftirq = cpuSoftirq - this.lastCpuSoftirq;
	 * final long deltaSum = deltaCpuUser + deltaCpuNice + deltaCpuSys + deltaCpuIdle + deltaCpuIOWait + deltaCpuIrq +
	 * deltaCpuSoftirq;
	 * //Set the percentage values for the profiling data object
	 * if (deltaSum > 0){
	 * profilingData.setIdleCPU((int)((deltaCpuIdle*PERCENT)/deltaSum));
	 * profilingData.setUserCPU((int)((deltaCpuUser*PERCENT)/deltaSum));
	 * profilingData.setSystemCPU((int)((deltaCpuSys*PERCENT)/deltaSum));
	 * profilingData.setIoWaitCPU((int)((deltaCpuIOWait*PERCENT)/deltaSum));
	 * }else{
	 * profilingData.setIdleCPU((int)(deltaCpuIdle));
	 * profilingData.setUserCPU((int)(deltaCpuUser));
	 * profilingData.setSystemCPU((int)(deltaCpuSys));
	 * profilingData.setIoWaitCPU((int)(deltaCpuIOWait));
	 * }
	 * //Store values for next call
	 * this.lastCpuUser = cpuUser;
	 * this.lastCpuNice = cpuNice;
	 * this.lastCpuSys = cpuSys;
	 * this.lastCpuIdle = cpuIdle;
	 * this.lastCpuIOWait = cpuIOWait;
	 * this.lastCpuIrq = cpuIrq;
	 * this.lastCpuSoftirq = cpuSoftirq;
	 * } catch(IOException ioe) {
	 * throw new ProfilingException("Error while reading CPU utilization: " + StringUtils.stringifyException(ioe));
	 * } catch(NumberFormatException nfe) {
	 * throw new ProfilingException("Error while reading CPU utilization: " + StringUtils.stringifyException(nfe));
	 * }
	 * }
	 */

	private void gatherNetworkInformation() throws ProfilingException {

		try {
			// first check /etc/network/interfaces to find out which eth fits our IP
			BufferedReader in = new BufferedReader(new FileReader("/etc/network/interfaces"));
			String instanceAdress = this.instanceConnectionInfo.getAddress().getHostAddress();

			String output;
			String eth = "";
			boolean found = false;
			int foundEths = 0;
			while ((output = in.readLine()) != null) {
				int ethIndex = output.indexOf("eth");
				if (ethIndex != -1) {
					eth = output.substring(ethIndex, ethIndex + 4);
					foundEths++;
					continue;
				}

				int ipIndex = output.indexOf(instanceAdress);
				if (ipIndex != -1) {
					found = true;
					this.interfaceName = eth;
					break;
				}

			}

			if (!found && foundEths == 1)
				this.interfaceName = eth;

			in.close();

			if (found) {
				// now try to figure out the interface speed
				in = new BufferedReader(new FileReader("/var/log/dmesg"));

				while ((output = in.readLine()) != null) {
					int ethIndex = output.indexOf(eth);
					if (ethIndex != -1) {
						int speedIndex = output.indexOf("Mbps");
						if (speedIndex != -1) {
							int speedIndex2 = output.indexOf('1', speedIndex - 6);
							if (speedIndex2 != -1) {
								String speed = output.substring(speedIndex2, speedIndex);
								int ethspeed = Integer.parseInt(speed);
								this.networkSpeedPerSecondInBytes = ethspeed * 125000;
							}

							break;
						}

						continue;
					}

				}

				in.close();
			}

		} catch (IOException ioe) {
			throw new ProfilingException("Error while reading network utilization: "
				+ StringUtils.stringifyException(ioe));
		} catch (NumberFormatException nfe) {
			throw new ProfilingException("Error while reading network utilization: "
				+ StringUtils.stringifyException(nfe));
		}
	}
}
