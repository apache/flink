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

package org.apache.flink.util;

import java.util.Arrays;
import java.util.List;

/**
 * The memory architecture (32 bit / 64 bit) of the current process.
 * Note that this might be different than the actual operating system's architecture, for example
 * when installing a 32 bit JRE in a 64 bit OS.
 */
public enum ProcessorArchitecture {
	/**
	 * X86 platform.
	 */
	X86,

	/**
	 * arm platform.
	 */
	ARM,

	/**
	 * 32 bit memory address size.
	 */
	_32_BIT,

	/**
	 * 64 bit memory address size.
	 */
	_64_BIT,

	/**
	 * Unknown architecture, could not be determined.
	 */
	UNKNOWN;


	private static final ProcessorArchitecture arch = readArchFromSystemProperties();
	private static final ProcessorArchitecture size = readSizeFromSystemProperties();

	private static ProcessorArchitecture readArchFromSystemProperties() {
		final List<String> namesX86 = Arrays.asList("amd64", "x86_64", "x86", "i386", "i486", "i586", "i686");
		final List<String> namesArm = Arrays.asList("arm", "aarch64");
		final String arch = System.getProperty("os.arch");

		if (namesX86.contains(arch)) {
			return X86;
		} else if (namesArm.contains(arch)) {
			return ARM;
		} else {
			return UNKNOWN;
		}
	}

	private static ProcessorArchitecture readSizeFromSystemProperties() {
		// putting these into the method to avoid having objects on the heap that are not needed
		// any more after initialization
		final List<String> names64bit = Arrays.asList("amd64", "x86_64", "aarch64");
		final List<String> names32bit = Arrays.asList("x86", "i386", "i486", "i586", "i686");
		final String arch = System.getProperty("os.arch");

		if (names64bit.contains(arch)) {
			return _64_BIT;
		} else if (names32bit.contains(arch)) {
			return _32_BIT;
		} else {
			return UNKNOWN;
		}
	}

	public static ProcessorArchitecture getCurrentOperatingSystemArch() {
		return arch;
	}

	public static ProcessorArchitecture getCurrentOperatingSystemSize() {
		return size;
	}

}
