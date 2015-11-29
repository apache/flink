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

import org.apache.flink.configuration.Configuration;

/**
 * Version numbers to check compatibility between JobManager, TaskManager and JobClient.
 */
public class VersionUtils {

	public static final String FLINK_VERSION = Configuration.FLINK_VERSION;
	/**
	 * Lower limit on client versions this job manager can work with.
	 */
	public static final String JOB_CLIENT_VERSION_LOWER = "0.9.0";

	/**
	 * Gets the minimum supported Client version by this Job Manager.
	 *
	 * @return Minimum supported client version number.
	 */
	public static String getJobClientVersionLower() {
		return JOB_CLIENT_VERSION_LOWER;
	}

	/**
	 * Checks whether the given client version is compatible with this Job Manager
	 *
	 * @param clientVersion Version of the client
	 * @return Whether the given client is compatible with the Job Manager.
	 */
	public static boolean isClientCompatible(String clientVersion) {
		return versionComparator(JOB_CLIENT_VERSION_LOWER, clientVersion) <= 0 && versionComparator(clientVersion, FLINK_VERSION) <= 0;
	}

	/**
	 * Checks which of the two given version strings is higher.
	 *
	 * @param version1 Version 1
	 * @param version2 Version 2
	 * @return 1 if version1 > version2, -1 if version1 < version2 and 0 if version1 = version2
	 * <p>
	 * Code taken from <a href = "http://stackoverflow.com/questions/6701948/efficient-way-to-compare-version-strings-in-java">Stack Overflow</a>
	 */
	private static int versionComparator(String version1, String version2) {
		String[] vals1 = version1.split("\\.");
		String[] vals2 = version2.split("\\.");
		int i = 0;
		// set index to first non-equal ordinal or length of shortest version string
		while (i < vals1.length && i < vals2.length && vals1[i].equals(vals2[i])) {
			i++;
		}
		// compare first non-equal ordinal number
		if (i < vals1.length && i < vals2.length) {
			int diff = Integer.valueOf(vals1[i]).compareTo(Integer.valueOf(vals2[i]));
			return Integer.signum(diff);
		}
		// the strings are equal or one string is a substring of the other
		// e.g. "1.2.3" = "1.2.3" or "1.2.3" < "1.2.3.4"
		else {
			return Integer.signum(vals1.length - vals2.length);
		}
	}
}
