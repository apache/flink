/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.util.OperatingSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class to extract hardware specifics of the computer executing the running JVM.
 */
public class Hardware {

	private static final Logger LOG = LoggerFactory.getLogger(Hardware.class);

	private static final String LINUX_MEMORY_INFO_PATH = "/proc/meminfo";

	private static final Pattern LINUX_MEMORY_REGEX = Pattern.compile("^MemTotal:\\s*(\\d+)\\s+kB$");

	private static final String LINUX_PROCESS_STATUS_PATH = "/proc/self/status";

	private static final Pattern LINUX_PROCESS_VIRTUAL_MEMORY_REGEX = Pattern.compile("^VmSize:\\s*(\\d+)\\s+kB$");

	private static final Pattern LINUX_PROCESS_PHYSICAL_MEMORY_REGEX = Pattern.compile("^VmRSS:\\s*(\\d+)\\s+kB$");

	private static CpuStatServiceForLinux cpuStatServiceForLinux = null;

	// ------------------------------------------------------------------------

	/**
	 * start hardware specifics service
	 */
	static {
		// try the OS specific access paths
		switch (OperatingSystem.getCurrentOperatingSystem()) {
			case LINUX:
				cpuStatServiceForLinux = new CpuStatServiceForLinux();
				break;

			case WINDOWS:
			case MAC_OS:
			case FREE_BSD:
				break;

			case UNKNOWN:
				LOG.error("Unknown operating system.");
				break;

			default:
				LOG.error("Unrecognized OS: " + OperatingSystem.getCurrentOperatingSystem());
		}

	}

	// ------------------------------------------------------------------------

	/**
	 * Shutdown hardware specifics service
	 */
	public static void shutdown() {
		if (cpuStatServiceForLinux != null) {
			cpuStatServiceForLinux.shutdown();
			cpuStatServiceForLinux = null;
		}
	}
	
	/**
	 * Gets the number of CPU cores (hardware contexts) that the JVM has access to.
	 * 
	 * @return The number of CPU cores.
	 */
	public static int getNumberCPUCores() {
		return Runtime.getRuntime().availableProcessors();
	}

	/**
	 * Returns the size of the physical memory in bytes.
	 * 
	 * @return the size of the physical memory in bytes or {@code -1}, if
	 *         the size could not be determined.
	 */
	public static long getSizeOfPhysicalMemory() {
		// first try if the JVM can directly tell us what the system memory is
		// this works only on Oracle JVMs
		try {
			Class<?> clazz = Class.forName("com.sun.management.OperatingSystemMXBean");
			Method method = clazz.getMethod("getTotalPhysicalMemorySize");
			OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

			// someone may install different beans, so we need to check whether the bean
			// is in fact the sun management bean
			if (clazz.isInstance(operatingSystemMXBean)) {
				return (Long) method.invoke(operatingSystemMXBean);
			}
		}
		catch (ClassNotFoundException e) {
			// this happens on non-Oracle JVMs, do nothing and use the alternative code paths
		}
		catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			LOG.warn("Access to physical memory size: " +
					"com.sun.management.OperatingSystemMXBean incompatibly changed.", e);
		}

		// we now try the OS specific access paths
		switch (OperatingSystem.getCurrentOperatingSystem()) {
			case LINUX:
				return getSizeOfPhysicalMemoryForLinux();

			case WINDOWS:
				return getSizeOfPhysicalMemoryForWindows();

			case MAC_OS:
				return getSizeOfPhysicalMemoryForMac();

			case FREE_BSD:
				return getSizeOfPhysicalMemoryForFreeBSD();

			case UNKNOWN:
				LOG.error("Cannot determine size of physical memory for unknown operating system");
				return -1;

			default:
				LOG.error("Unrecognized OS: " + OperatingSystem.getCurrentOperatingSystem());
				return -1;
		}
	}

	/**
	 * Returns the size of the physical memory in bytes on a Linux-based
	 * operating system.
	 * 
	 * @return the size of the physical memory in bytes or {@code -1}, if
	 *         the size could not be determined
	 */
	private static long getSizeOfPhysicalMemoryForLinux() {
		try (BufferedReader lineReader = new BufferedReader(new FileReader(LINUX_MEMORY_INFO_PATH))) {
			String line;
			while ((line = lineReader.readLine()) != null) {
				Matcher matcher = LINUX_MEMORY_REGEX.matcher(line);
				if (matcher.matches()) {
					String totalMemory = matcher.group(1);
					return Long.parseLong(totalMemory) * 1024L; // Convert from kilobyte to byte
				}
			}
			// expected line did not come
			LOG.error("Cannot determine the size of the physical memory for Linux host (using '/proc/meminfo'). " +
					"Unexpected format.");
			return -1;
		}
		catch (NumberFormatException e) {
			LOG.error("Cannot determine the size of the physical memory for Linux host (using '/proc/meminfo'). " +
					"Unexpected format.");
			return -1;
		}
		catch (Throwable t) {
			LOG.error("Cannot determine the size of the physical memory for Linux host (using '/proc/meminfo') ", t);
			return -1;
		}
	}

	/**
	 * Returns the size of the physical memory in bytes on a Mac OS-based
	 * operating system
	 * 
	 * @return the size of the physical memory in bytes or {@code -1}, if
	 *         the size could not be determined
	 */
	private static long getSizeOfPhysicalMemoryForMac() {
		BufferedReader bi = null;
		try {
			Process proc = Runtime.getRuntime().exec("sysctl hw.memsize");

			bi = new BufferedReader(new InputStreamReader(proc.getInputStream()));

			String line;
			while ((line = bi.readLine()) != null) {
				if (line.startsWith("hw.memsize")) {
					long memsize = Long.parseLong(line.split(":")[1].trim());
					bi.close();
					proc.destroy();
					return memsize;
				}
			}

		} catch (Throwable t) {
			LOG.error("Cannot determine physical memory of machine for MacOS host", t);
			return -1;
		} finally {
			if (bi != null) {
				try {
					bi.close();
				} catch (IOException ignored) {}
			}
		}
		return -1;
	}

	/**
	 * Returns the size of the physical memory in bytes on FreeBSD.
	 * 
	 * @return the size of the physical memory in bytes or {@code -1}, if
	 *         the size could not be determined
	 */
	private static long getSizeOfPhysicalMemoryForFreeBSD() {
		BufferedReader bi = null;
		try {
			Process proc = Runtime.getRuntime().exec("sysctl hw.physmem");

			bi = new BufferedReader(new InputStreamReader(proc.getInputStream()));

			String line;
			while ((line = bi.readLine()) != null) {
				if (line.startsWith("hw.physmem")) {
					long memsize = Long.parseLong(line.split(":")[1].trim());
					bi.close();
					proc.destroy();
					return memsize;
				}
			}
			
			LOG.error("Cannot determine the size of the physical memory for FreeBSD host " +
					"(using 'sysctl hw.physmem').");
			return -1;
		}
		catch (Throwable t) {
			LOG.error("Cannot determine the size of the physical memory for FreeBSD host " +
					"(using 'sysctl hw.physmem')", t);
			return -1;
		}
		finally {
			if (bi != null) {
				try {
					bi.close();
				} catch (IOException ignored) {}
			}
		}
	}

	/**
	 * Returns the size of the physical memory in bytes on Windows.
	 * 
	 * @return the size of the physical memory in bytes or {@code -1}, if
	 *         the size could not be determined
	 */
	private static long getSizeOfPhysicalMemoryForWindows() {
		BufferedReader bi = null;
		try {
			Process proc = Runtime.getRuntime().exec("wmic memorychip get capacity");

			bi = new BufferedReader(new InputStreamReader(proc.getInputStream()));

			String line = bi.readLine();
			if (line == null) {
				return -1L;
			}

			if (!line.startsWith("Capacity")) {
				return -1L;
			}

			long sizeOfPhyiscalMemory = 0L;
			while ((line = bi.readLine()) != null) {
				if (line.isEmpty()) {
					continue;
				}

				line = line.replaceAll(" ", "");
				sizeOfPhyiscalMemory += Long.parseLong(line);
			}
			return sizeOfPhyiscalMemory;
		}
		catch (Throwable t) {
			LOG.error("Cannot determine the size of the physical memory for Windows host " +
					"(using 'wmic memorychip')", t);
			return -1L;
		}
		finally {
			if (bi != null) {
				try {
					bi.close();
				} catch (Throwable ignored) {}
			}
		}
	}

	/**
	 * Returns the size of the process virtual memory in bytes.
	 *
	 * @return the size of the process virtual memory in bytes or {@code -1}, if
	 *         the size could not be determined.
	 */
	public static long getSizeOfProcessVirtualMemory() {
		// try the OS specific access paths
		switch (OperatingSystem.getCurrentOperatingSystem()) {
			case LINUX:
				return getSizeOfProcessVirtualMemoryForLinux();

			case WINDOWS:
			case MAC_OS:
			case FREE_BSD:
				LOG.warn("Unsupported access process virtual memory for OS:" + OperatingSystem.getCurrentOperatingSystem());
				return -1;

			case UNKNOWN:
				LOG.error("Cannot determine size of process virtual memory for unknown operating system");
				return -1;

			default:
				LOG.error("Unrecognized OS: " + OperatingSystem.getCurrentOperatingSystem());
				return -1;
		}
	}

	/**
	 * Returns the size of the process physical memory in bytes.
	 *
	 * @return the size of the process physical memory in bytes or {@code -1}, if
	 *         the size could not be determined.
	 */
	public static long getSizeOfProcessPhysicalMemory() {
		// try the OS specific access paths
		switch (OperatingSystem.getCurrentOperatingSystem()) {
			case LINUX:
				return getSizeOfProcessPhysicalMemoryForLinux();

			case WINDOWS:
			case MAC_OS:
			case FREE_BSD:
				LOG.warn("Unsupported access process physical memory for OS:" + OperatingSystem.getCurrentOperatingSystem());
				return -1;

			case UNKNOWN:
				LOG.error("Cannot determine size of process physical memory for unknown operating system");
				return -1;

			default:
				LOG.error("Unrecognized OS: " + OperatingSystem.getCurrentOperatingSystem());
				return -1;
		}
	}

	/**
	 * Returns the size of the process virtual memory in bytes on a Linux-based
	 * operating system.
	 *
	 * @return the size of the process virtual memory in bytes or {@code -1}, if
	 *         the size could not be determined
	 */
	private static long getSizeOfProcessVirtualMemoryForLinux() {
		try {
			String virtualMemory = getProcessStatusForLinux(LINUX_PROCESS_VIRTUAL_MEMORY_REGEX);
			return Long.parseLong(virtualMemory) * 1024L; // Convert from kilobyte to byte
		}
		catch (NumberFormatException e) {
			LOG.error("Cannot determine the size of the process virtual memory(VmSize) for Linux host. " +
					"Not a number format.");
			return -1;
		}
	}

	/**
	 * Returns the size of the process physical memory in bytes on a Linux-based
	 * operating system.
	 *
	 * @return the size of the process physical memory in bytes or {@code -1}, if
	 *         the size could not be determined
	 */
	private static long getSizeOfProcessPhysicalMemoryForLinux() {
		try {
			String physicalMemory = getProcessStatusForLinux(LINUX_PROCESS_PHYSICAL_MEMORY_REGEX);
			return Long.parseLong(physicalMemory) * 1024L; // Convert from kilobyte to byte
		}
		catch (NumberFormatException e) {
			LOG.error("Cannot determine the size of the process physical memory(VmRSS) for Linux host. " +
					"Not a number format.");
			return -1;
		}
	}

	/**
	 * Returns the process status value which pattern matched on a Linux-based
	 * operating system.
	 *
	 * @param pattern The pattern to match the process status.
	 * @return the process status value which pattern matched or {@code ""}, if
	 *         the pattern unmatched
	 */
	private static String getProcessStatusForLinux(Pattern pattern) {
		try (BufferedReader lineReader = new BufferedReader(new FileReader(LINUX_PROCESS_STATUS_PATH))) {
			String line;
			while ((line = lineReader.readLine()) != null) {
				Matcher matcher = pattern.matcher(line);
				if (matcher.matches()) {
					return matcher.group(1);
				}
			}
			// expected line did not come
			LOG.error("Cannot determine process '" + pattern + "' for Linux host (using '" + LINUX_PROCESS_STATUS_PATH
					+ "'). " + "Unexpected format.");
			return "";
		}
		catch (Throwable t) {
			LOG.error("Cannot determine process '" + pattern + "' for Linux host (using '" + LINUX_PROCESS_STATUS_PATH
					+ "').", t);
			return "";
		}
	}

	/**
	 * Returns the process cpu usage.
	 *
	 * @return the process cpu usage or {@code 0.0}, if
	 *         the cpu usage could not be determined.
	 */
	public static double getProcessCpuUsage() {
		// try the OS specific access paths
		switch (OperatingSystem.getCurrentOperatingSystem()) {
			case LINUX:
				return cpuStatServiceForLinux.getProcessCpuUsage();

			case WINDOWS:
			case MAC_OS:
			case FREE_BSD:
				LOG.warn("Unsupported access process cpu usage for OS:" + OperatingSystem.getCurrentOperatingSystem());
				return 0.0;

			case UNKNOWN:
				LOG.error("Cannot determine process cpu usage for unknown operating system");
				return 0.0;

			default:
				LOG.error("Unrecognized OS: " + OperatingSystem.getCurrentOperatingSystem());
				return 0.0;
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Service to extract CPU specifics on Linux host.
	 */
	private static class CpuStatServiceForLinux {

		private static final String LINUX_TOTAL_CPU_STAT_PATH = "/proc/stat";

		private static final String LINUX_PROCESS_CPU_STAT_PATH = "/proc/self/stat";

		/** The Number of CPU cores */
		private static final int CPU_CORES_NUMBER = Runtime.getRuntime().availableProcessors();

		/** Executor Service to collect CPU specifics periodically */
		private final ScheduledExecutorService scheduleExecutor;

		/** total cpu clock of last collect */
		private double lastTotalCpuClock = 0.0;

		/** self process cpu clock of last collect */
		private double lastProcessCpuClock = 0.0;

		/** self process cpu usage of last collect */
		private double processCpuUsage = 0.0;

		// --------------------------------------------------------------------------------------------

		public CpuStatServiceForLinux() {
			// start Executor Service to collect CPU specifics periodically if on Linux host
			if (OperatingSystem.getCurrentOperatingSystem() == OperatingSystem.LINUX) {
				scheduleExecutor = Executors.newSingleThreadScheduledExecutor();
				scheduleExecutor.scheduleWithFixedDelay(new TimerTask() {
					@Override
					public void run() {
						// collect cup usage periodically
						collectLastCpuUsage();
					}
				}, 10, 10, TimeUnit.SECONDS);
			} else {
				scheduleExecutor = null;
			}
		}

		/**
		 * shutdown Executor Service to collect CPU specifics
		 */
		public void shutdown() {
			if (scheduleExecutor != null) {
				scheduleExecutor.shutdown();
			}
		}

		/**
		 * Returns the process cpu usage.
		 *
		 * @return the process cpu usage.
		 */
		public double getProcessCpuUsage() {
			return processCpuUsage;
		}

		/**
		 * Collect last CPU usage on Linux host
		 */
		private void collectLastCpuUsage() {
			try {
				double currentTotalCpuClock = getTotalCpuClock();
				double currentProcessCpuClock = getProcessCpuClock();

				if ((currentTotalCpuClock - lastTotalCpuClock) == 0.0) {
					processCpuUsage = 0.0;
				} else {
					processCpuUsage = ((currentProcessCpuClock - lastProcessCpuClock) / (currentTotalCpuClock - lastTotalCpuClock))
							* CPU_CORES_NUMBER;
				}

				lastTotalCpuClock = currentTotalCpuClock;
				lastProcessCpuClock = currentProcessCpuClock;
			}
			catch (Throwable t) {
				LOG.error("Collect last cpu usage for Linux host failure.", t);
			}
		}

		/**
		 * Returns the self process cpu clock on Linux host.
		 *
		 * @return the self process cpu clock.
		 */
		private double getProcessCpuClock() throws IOException, NumberFormatException {
			String processCpuStat = getFirstLineFromFile(LINUX_PROCESS_CPU_STAT_PATH);
			if (processCpuStat == null) {
				throw new IOException("Cannot read file '" + LINUX_PROCESS_CPU_STAT_PATH + "' for Linux host.");
			}

			String[] cpuStats = processCpuStat.split(" ", -1);
			if (cpuStats.length < 17) {
				LOG.error("Cannot determine process cpu stat '" + processCpuStat + "' for Linux host (using '" + LINUX_PROCESS_CPU_STAT_PATH
						+ "'). " + "Unexpected format.");
				throw new IOException("Parse process cpu stat '" + processCpuStat + "' for Linux host failed. Unexpected format.");
			}

			int rightBracketPos = -1;
			for (int i = cpuStats.length - 1; i > 0; i--) {
				if (cpuStats[i].contains(")")) {
					rightBracketPos = i;
					break;
				}
			}
			if (rightBracketPos == -1) {
				throw new IOException("Process cpu stat '" + processCpuStat + "' has no right bracket for Linux host. Unexpected format");
			}

			double processCpuClock = 0.0;
			for (int i = rightBracketPos + 12; i < rightBracketPos + 16; i++) {
				processCpuClock += Double.parseDouble(cpuStats[i]);
			}

			return processCpuClock;
		}

		/**
		 * Returns the total process cpu clock on Linux host.
		 *
		 * @return the total process cpu clock.
		 */
		private double getTotalCpuClock() throws IOException, NumberFormatException {
			String totalCpuStat = getFirstLineFromFile(LINUX_TOTAL_CPU_STAT_PATH);
			if (totalCpuStat == null) {
				throw new IOException("Cannot read file '" + LINUX_TOTAL_CPU_STAT_PATH + "' for Linux host.");
			}

			String[] cpuStats = totalCpuStat.split(" ", -1);
			if (cpuStats.length < 11) {
				LOG.error("Cannot determine total cpu stat '" + totalCpuStat + "' for Linux host (using '" + LINUX_TOTAL_CPU_STAT_PATH
						+ "'). " + "Unexpected format.");
				throw new IOException("Parse total cpu stat '" + totalCpuStat + "' for Linux host failed. Unexpected format.");
			}

			double totalCpuClock = 0.0;
			for (int i = 2; i < cpuStats.length; i++) {
				totalCpuClock += Double.parseDouble(cpuStats[i]);
			}

			return totalCpuClock;
		}

		/**
		 * Returns the first line of cpu stat file on a Linux-based
		 * operating system.
		 *
		 * @param fileName The cpu stat file.
		 * @return the first line of cpu stat file or {@code null}, if
		 *         the file cannot read
		 */
		private String getFirstLineFromFile(String fileName) {
			try (BufferedReader lineReader = new BufferedReader(new FileReader(fileName))) {
				String line = lineReader.readLine();
				return line;
			}
			catch (Throwable t) {
				LOG.error("Cannot read file '" + fileName + "' for Linux host.", t);
				return null;
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private Hardware() {}
}
