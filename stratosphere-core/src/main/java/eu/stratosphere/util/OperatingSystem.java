/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.util;


/**
 * An enumeration indicating the operating system that the engine runs on.
 */
public enum OperatingSystem {

	// --------------------------------------------------------------------------------------------
	//  Constants to extract the OS type from the java environment 
	// --------------------------------------------------------------------------------------------
	
	LINUX,
	WINDOWS,
	MAC_OS,
	FREE_BSD,
	UNKNOWN;
	
	// --------------------------------------------------------------------------------------------
	//  Constants to extract the OS type from the java environment 
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the operating system that the JVM runs on from the java system properties.
	 * this method returns {@link UNKNOWN}, if the operating system was not successfully determined.
	 * 
	 * @return The enum constant for the operating system, or {@link UNKNOWN}, if it was not possible to determine.
	 */
	public static OperatingSystem getCurrentOperatingSystem() {
		return os;
	}
	
	/**
	 * Checks whether the operating system this JVM runs on is Windows.
	 * 
	 * @return <code>true</code> if the operating system this JVM runs on is
	 *         Windows, <code>false</code> otherwise
	 */
	public static boolean isWindows() {
		return getCurrentOperatingSystem() == WINDOWS;
	}

	/**
	 * Checks whether the operating system this JVM runs on is Linux.
	 * 
	 * @return <code>true</code> if the operating system this JVM runs on is
	 *         Linux, <code>false</code> otherwise
	 */
	public static boolean isLinux() {
		return getCurrentOperatingSystem() == LINUX;
	}

	/**
	 * Checks whether the operating system this JVM runs on is Windows.
	 * 
	 * @return <code>true</code> if the operating system this JVM runs on is
	 *         Windows, <code>false</code> otherwise
	 */
	public static boolean isMac() {
		return getCurrentOperatingSystem() == MAC_OS;
	}

	/**
	 * Checks whether the operating system this JVM runs on is FreeBSD.
	 * 
	 * @return <code>true</code> if the operating system this JVM runs on is
	 *         FreeBSD, <code>false</code> otherwise
	 */
	public static boolean isFreeBSD() {
		return getCurrentOperatingSystem() == FREE_BSD;
	}
	
	/**
	 * The enum constant for the operating system.
	 */
	private static final OperatingSystem os = readOSFromSystemProperties();
	
	/**
	 * Parses the operating system that the JVM runs on from the java system properties.
	 * If the operating system was not successfully determined, this method returns {@link UNKNOWN}.
	 * 
	 * @return The enum constant for the operating system, or {@link UNKNOWN}, if it was not possible to determine.
	 */
	private static OperatingSystem readOSFromSystemProperties() {
		String osName = System.getProperty(OS_KEY);
		
		if (osName.startsWith(LINUX_OS_PREFIX))
			return LINUX;
		if (osName.startsWith(WINDOWS_OS_PREFIX))
			return WINDOWS;
		if (osName.startsWith(MAC_OS_PREFIX))
			return MAC_OS;
		if (osName.startsWith(FREEBSD_OS_PREFIX))
			return FREE_BSD;
		
		return UNKNOWN;
	}
	
	
	
	// --------------------------------------------------------------------------------------------
	//  Constants to extract the OS type from the java environment 
	// --------------------------------------------------------------------------------------------
	
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
	 * The expected prefix for FreeBSD.
	 */
	private static final String FREEBSD_OS_PREFIX = "FreeBSD";
}
