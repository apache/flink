/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.instance;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.util.OperatingSystem;

/**
 * A factory to construct {@link HardwareDescription} objects. In particular,
 * the factory can automatically generate a {@link HardwareDescription} object
 * from the system it is executed on.
 * <p>
 * This class is thread-safe.
 */
public class HardwareDescriptionFactory {

	/**
	 * The log object used to report errors.
	 */
	private static final Log LOG = LogFactory.getLog(HardwareDescriptionFactory.class);

	/**
	 * The path to the interface to extract memory information under Linux.
	 */
	private static final String LINUX_MEMORY_INFO_PATH = "/proc/meminfo";

	/**
	 * The regular expression used to extract the size of the physical memory
	 * under Linux.
	 */
	private static final Pattern LINUX_MEMORY_REGEX = Pattern
			.compile("^MemTotal:\\s*(\\d+)\\s+kB$");

	/**
	 * The fraction of free memory that goes into the memory manager by default.
	 */
	private static float RUNTIME_MEMORY_THRESHOLD = GlobalConfiguration.getFloat(
		ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY, ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION);

	
	/**
	 * Private constructor, so class cannot be instantiated.
	 */
	private HardwareDescriptionFactory() {}

	
	/**
	 * Extracts a hardware description object from the system.
	 * 
	 * @return the hardware description object or <code>null</code> if at least
	 *         one value for the hardware description cannot be determined
	 */
	public static HardwareDescription extractFromSystem() {
		return extractFromSystem(1);
	}
	
	public static HardwareDescription extractFromSystem(final int taskManagersPerJVM) {

		final int numberOfCPUCores = Runtime.getRuntime().availableProcessors();

		final long sizeOfPhysicalMemory = getSizeOfPhysicalMemory();
		if (sizeOfPhysicalMemory < 0) {
			return null;
		}

		final long sizeOfFreeMemory = getSizeOfFreeMemory() / taskManagersPerJVM;
		if (sizeOfFreeMemory < 0) {
			return null;
		}

		return new HardwareDescription(numberOfCPUCores, sizeOfPhysicalMemory,
				sizeOfFreeMemory);
	}

	/**
	 * Constructs a new hardware description object.
	 * 
	 * @param numberOfCPUCores
	 *        the number of CPU cores available to the JVM on the compute
	 *        node
	 * @param sizeOfPhysicalMemory
	 *        the size of physical memory in bytes available on the compute
	 *        node
	 * @param sizeOfFreeMemory
	 *        the size of free memory in bytes available to the JVM on the
	 *        compute node
	 * @return the hardware description object
	 */
	public static HardwareDescription construct(int numberOfCPUCores,long sizeOfPhysicalMemory, long sizeOfFreeMemory) {
		return new HardwareDescription(numberOfCPUCores, sizeOfPhysicalMemory, sizeOfFreeMemory);
	}

	/**
	 * Returns the size of free memory in bytes available to the JVM.
	 * 
	 * @return the size of the free memory in bytes available to the JVM or <code>-1</code> if the size cannot be
	 *         determined
	 */
	private static long getSizeOfFreeMemory() {
		Runtime r = Runtime.getRuntime();
		return (long) (RUNTIME_MEMORY_THRESHOLD * (r.maxMemory() - r.totalMemory() + r.freeMemory()));
	}

	/**
	 * Returns the size of the physical memory in bytes.
	 * 
	 * @return the size of the physical memory in bytes or <code>-1</code> if
	 *         the size could not be determined
	 */
	private static long getSizeOfPhysicalMemory() {
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
				LOG.error("Unrecognized OS");
				return -1;
		}
	}

	/**
	 * Returns the size of the physical memory in bytes on a Linux-based
	 * operating system.
	 * 
	 * @return the size of the physical memory in bytes or <code>-1</code> if
	 *         the size could not be determined
	 */
	@SuppressWarnings("resource")
	private static long getSizeOfPhysicalMemoryForLinux() {
		BufferedReader lineReader = null;
		try {
			lineReader = new BufferedReader(new FileReader(LINUX_MEMORY_INFO_PATH));

			String line = null;
			while ((line = lineReader.readLine()) != null) {
				Matcher matcher = LINUX_MEMORY_REGEX.matcher(line);
				if (matcher.matches()) {
					String totalMemory = matcher.group(1);
					return Long.parseLong(totalMemory) * 1024L; // Convert from kilobyte to byte
				}
			}
			
			// expected line did not come
			LOG.error("Cannot determine the size of the physical memory using '/proc/meminfo'. Unexpected format.");
			return -1;
		}
		catch (NumberFormatException e) {
			LOG.error("Cannot determine the size of the physical memory using '/proc/meminfo'. Unexpected format.");
			return -1;
		}
		catch (IOException e) {
			LOG.error("Cannot determine the size of the physical memory using '/proc/meminfo': " + e.getMessage(), e);
			return -1;
		}
		finally {
			// Make sure we always close the file handle
			try {
				if (lineReader != null) {
					lineReader.close();
				}
			} catch (Throwable t) {}
		}
	}

	/**
	 * Returns the size of the physical memory in bytes on a Mac OS-based
	 * operating system
	 * 
	 * @return the size of the physical memory in bytes or <code>-1</code> if
	 *         the size could not be determined
	 */
	private static long getSizeOfPhysicalMemoryForMac() {

		BufferedReader bi = null;

		try {
			Process proc = Runtime.getRuntime().exec("sysctl hw.memsize");

			bi = new BufferedReader(
					new InputStreamReader(proc.getInputStream()));

			String line;

			while ((line = bi.readLine()) != null) {
				if (line.startsWith("hw.memsize")) {
					long memsize = Long.parseLong(line.split(":")[1].trim());
					bi.close();
					proc.destroy();
					return memsize;
				}
			}

		} catch (Exception e) {
			LOG.error(e);
			return -1;
		} finally {
			if (bi != null) {
				try {
					bi.close();
				} catch (IOException ioe) {
				}
			}
		}
		return -1;
	}

	/**
	 * Returns the size of the physical memory in bytes on FreeBSD.
	 * 
	 * @return the size of the physical memory in bytes or <code>-1</code> if
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
			
			LOG.error("Cannot determine the size of the physical memory using 'sysctl hw.physmem'.");
			return -1;
		}
		catch (Exception e) {
			LOG.error("Cannot determine the size of the physical memory using 'sysctl hw.physmem': " + e.getMessage(), e);
			return -1;
		}
		finally {
			if (bi != null) {
				try {
					bi.close();
				} catch (IOException ioe) {
				}
			}
		}
	}

	/**
	 * Returns the size of the physical memory in bytes on Windows.
	 * 
	 * @return the size of the physical memory in bytes or <code>-1</code> if
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
		catch (Exception e) {
			LOG.error("Cannot determine the size of the physical memory using 'wmic memorychip': " + e.getMessage(), e);
			return -1L;
		}
		finally {
			if (bi != null) {
				try {
					bi.close();
				} catch (Throwable t) {}
			}
		}
	}
}
