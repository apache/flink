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

package eu.stratosphere.nephele.instance;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A factory to construct {@link HardwareDescription} objects. In particular, the factory can automatically generate a
 * {@link HardwareDescription} object from the system it is executed on.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class HardwareDescriptionFactory {

	/**
	 * The log object used to report errors.
	 */
	private static final Log LOG = LogFactory.getLog(HardwareDescriptionFactory.class);

	/**
	 * The key to extract the operating system name from the system properties.
	 */
	private static final String OS_KEY = "os.name";

	/**
	 * The expected prefix for Linux operating systems.
	 */
	private static final String LINUX_OS_PREFIX = "Linux";

	/**
	 * The expected prefix for Windows operating systems.
	 */
	private static final String WINDOWS_OS_PREFIX = "Windows";

	/**
	 * The expected prefix for Mac OS operating systems.
	 */
	private static final String MAC_OS_PREFIX = "Mac";
	
	/**
	 * The path to the interface to extract memory information under Linux.
	 */
	private static final String LINUX_MEMORY_INFO_PATH = "/proc/meminfo";

	/**
	 * The regular expression used to extract the size of the physical memory under Linux.
	 */
	private static final Pattern LINUX_MEMORY_REGEX = Pattern.compile("^MemTotal:\\s*(\\d+)\\s+kB$");

	/**
	 * The names of the tenured memory pool
	 */
	private static final String[] TENURED_POOL_NAMES = {"Tenured Gen", "PS Old Gen", "CMS Old Gen"};

	/**
	 * The operating system name.
	 */
	private static String os = null;
	
	/**
	 * The memory threshold to be used when tenured pool can be determined
	 */
	private static float TENURED_POOL_THRESHOLD = 0.8f;
	
	/**
	 * The memory threshold to be used when tenured pool can not be determined
	 */
	private static float RUNTIME_MEMORY_THRESHOLD = 0.7f;

	/**
	 * Private constructor, so class cannot be instantiated.
	 */
	private HardwareDescriptionFactory() {
	}

	/**
	 * Extracts a hardware description object from the system.
	 * 
	 * @return the hardware description object or <code>null</code> if at least one value for the hardware description
	 *         cannot be determined
	 */
	public static HardwareDescription extractFromSystem() {

		final int numberOfCPUCores = Runtime.getRuntime().availableProcessors();

		final long sizeOfPhysicalMemory = getSizeOfPhysicalMemory();
		if (sizeOfPhysicalMemory < 0) {
			return null;
		}

		final long sizeOfFreeMemory = getSizeOfFreeMemory();
		if (sizeOfFreeMemory < 0) {
			return null;
		}

		return new HardwareDescription(numberOfCPUCores, sizeOfPhysicalMemory, sizeOfFreeMemory);
	}

	/**
	 * Constructs a new hardware description object.
	 * 
	 * @param numberOfCPUCores
	 *        the number of CPU cores available to the JVM on the compute node
	 * @param sizeOfPhysicalMemory
	 *        the size of physical memory in bytes available on the compute node
	 * @param sizeOfFreeMemory
	 *        the size of free memory in bytes available to the JVM on the compute node
	 * @return the hardware description object
	 */
	public static HardwareDescription construct(int numberOfCPUCores, long sizeOfPhysicalMemory, long sizeOfFreeMemory) {

		return new HardwareDescription(numberOfCPUCores, sizeOfPhysicalMemory, sizeOfFreeMemory);
	}

	/**
	 * Returns the size of free memory in bytes available to the JVM.
	 * 
	 * @return the size of the free memory in bytes available to the JVM or <code>-1</code> if the size cannot be
	 *         determined
	 */
	private static long getSizeOfFreeMemory() {

		// in order to prevent allocations of arrays that are too big for the JVM's different memory pools,
		// make sure that the maximum segment size is 70% of the currently free tenure heap
		final MemoryPoolMXBean tenuredpool = findTenuredGenPool();
		
		if(tenuredpool != null){
			final MemoryUsage usage = tenuredpool.getUsage();
			long tenuredSize =  usage.getMax() - usage.getUsed();
			LOG.info("found tenured gen pool. Max: " + tenuredSize + " used: " + usage.getUsed() + ".");
			//TODO: make the constant configurable
			return (long) (tenuredSize * TENURED_POOL_THRESHOLD);
		}
		
		LOG.info("could not determine tenured gen pool. Using JVM Runtime information instead.");
		Runtime r = Runtime.getRuntime();
		final long maximum = r.maxMemory();
		
		//TODO: Make 0.7f configurable
		return (long) ( RUNTIME_MEMORY_THRESHOLD * (maximum - r.totalMemory() + r.freeMemory()));

	}

	/**
	 * Returns the tenured gen pool
	 * @return tenured gen pool, or <code>null</code>, if not detectable 
	 */
	private static MemoryPoolMXBean findTenuredGenPool() {
		for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
			
			for(String s: TENURED_POOL_NAMES){
				if(pool.getName().equals(s)){
					//seems that we found the tenured pool
					//double check, if it MemoryType is HEAP and usageThreshold supported..
					if (pool.getType() == MemoryType.HEAP && pool.isUsageThresholdSupported()) {
						return pool;
					}
				}
			}
		}
		return null;
	}
	
	/**
	 * Returns the operating system this JVM runs on.
	 * 
	 * @return the operating system this JVM runs on
	 */
	private static String getOperatingSystemName() {

		if (os == null) {
			os = System.getProperty(OS_KEY);
		}

		return os;
	}

	/**
	 * Checks whether the operating system this JVM runs on is Windows.
	 * 
	 * @return <code>true</code> if the operating system this JVM runs on is Windows, <code>false</code> otherwise
	 */
	private static boolean isWindows() {

		if (getOperatingSystemName().startsWith(WINDOWS_OS_PREFIX)) {
			return true;
		}

		return false;
	}

	/**
	 * Checks whether the operating system this JVM runs on is Linux.
	 * 
	 * @return <code>true</code> if the operating system this JVM runs on is Linux, <code>false</code> otherwise
	 */
	private static boolean isLinux() {

		if (getOperatingSystemName().startsWith(LINUX_OS_PREFIX)) {
			return true;
		}

		return false;
	}

	/**
	 * Checks whether the operating system this JVM runs on is Windows.
	 * 
	 * @return <code>true</code> if the operating system this JVM runs on is Windows, <code>false</code> otherwise
	 */
	private static boolean isMac() {

		if (getOperatingSystemName().startsWith(MAC_OS_PREFIX)) {
			return true;
		}

		return false;
	}	
	
	/**
	 * Returns the size of the physical memory in bytes.
	 * 
	 * @return the size of the physical memory in bytes or <code>-1</code> if the size could not be determined
	 */
	private static long getSizeOfPhysicalMemory() {

		if (isLinux()) {
			return getSizeOfPhysicalMemoryForLinux();
		} else if (isWindows()) {
			LOG.error("Cannot determine size of physical memory: Support for Windows is not yet implemented");
		} else if (isMac()){
			return getSizeOfPhysicalMemoryForMac();
		}
		else {
			LOG.error("Cannot determine size of physical memory: Unknown operating system");
		}

		return -1;
	}

	/**
	 * Returns the size of the physical memory in bytes on a Linux-based operating system.
	 * 
	 * @return the size of the physical memory in bytes or <code>-1</code> if the size could not be determined
	 */
	private static long getSizeOfPhysicalMemoryForLinux() {

		BufferedReader lineReader = null;

		try {

			lineReader = new BufferedReader(new FileReader(LINUX_MEMORY_INFO_PATH));

			String line = lineReader.readLine();
			while (line != null) {

				final Matcher matcher = LINUX_MEMORY_REGEX.matcher(line);
				if (matcher.matches()) {
					final String totalMemory = matcher.group(1);
					try {
						return Long.parseLong(totalMemory) * 1024L; // Convert from kilobyte to byte
					} catch (NumberFormatException nfe) {
						LOG.error(nfe);
						return -1;
					}
				}

				line = lineReader.readLine();
			}

		} catch (IOException e) {
			LOG.error(e);
		} finally {

			// Make sure we always close the file handle
			try {
				if (lineReader != null) {
					lineReader.close();
				}
			} catch (IOException ioe) {
				LOG.error(ioe);
			}
		}

		return -1;
	}
	
	/**
	 * Returns the size of the physical memory in bytes on a Mac OS-based operating system
	 * 
	 * @return the size of the physical memory in bytes or <code>-1</code> if the size could not be determined
	 */
	private static long getSizeOfPhysicalMemoryForMac() {

		try {
			Process proc = Runtime.getRuntime().exec("sysctl hw");

			BufferedReader bi = new BufferedReader(new InputStreamReader(proc.getInputStream()));

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
		}
		return -1;
	}
	
}
