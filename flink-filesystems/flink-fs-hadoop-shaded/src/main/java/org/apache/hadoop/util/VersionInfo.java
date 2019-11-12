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

package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Properties;

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
//
//  This class is copied from the Hadoop Source Code (Apache License 2.0)
//  in order to override default behavior i the presence of shading
//
// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

@SuppressWarnings("all")
@InterfaceAudience.Public
@InterfaceStability.Stable
public class VersionInfo {

	private final Properties info;

	protected VersionInfo(String component) {
		info = new Properties();

		if ("common".equals(component)) {
			info.setProperty("version", "3.1.0");
			info.setProperty("revision", "16b70619a24cdcf5d3b0fcf4b58ca77238ccbe6d");
			info.setProperty("branch", "branch-3.1.0");
			info.setProperty("user", "wtan");
			info.setProperty("date", "2018-04-03T04:00Z");
			info.setProperty("url", "git@github.com:hortonworks/hadoop-common-trunk.git");
			info.setProperty("srcChecksum", "14182d20c972b3e2105580a1ad6990");
			info.setProperty("protocVersion", "2.5.0");
		}
	}

	protected String _getVersion() {
		return info.getProperty("version", "Unknown");
	}

	protected String _getRevision() {
		return info.getProperty("revision", "Unknown");
	}

	protected String _getBranch() {
		return info.getProperty("branch", "Unknown");
	}

	protected String _getDate() {
		return info.getProperty("date", "Unknown");
	}

	protected String _getUser() {
		return info.getProperty("user", "Unknown");
	}

	protected String _getUrl() {
		return info.getProperty("url", "Unknown");
	}

	protected String _getSrcChecksum() {
		return info.getProperty("srcChecksum", "Unknown");
	}

	protected String _getBuildVersion(){
		return _getVersion() +
				" from " + _getRevision() +
				" by " + _getUser() +
				" source checksum " + _getSrcChecksum();
	}

	protected String _getProtocVersion() {
		return info.getProperty("protocVersion", "Unknown");
	}

	private static final VersionInfo COMMON_VERSION_INFO = new VersionInfo("common");

	public static String getVersion() {
		return COMMON_VERSION_INFO._getVersion();
	}

	public static String getRevision() {
		return COMMON_VERSION_INFO._getRevision();
	}

	public static String getBranch() {
		return COMMON_VERSION_INFO._getBranch();
	}

	public static String getDate() {
		return COMMON_VERSION_INFO._getDate();
	}

	public static String getUser() {
		return COMMON_VERSION_INFO._getUser();
	}

	public static String getUrl() {
		return COMMON_VERSION_INFO._getUrl();
	}

	public static String getSrcChecksum() {
		return COMMON_VERSION_INFO._getSrcChecksum();
	}

	public static String getBuildVersion(){
		return COMMON_VERSION_INFO._getBuildVersion();
	}

	public static String getProtocVersion(){
		return COMMON_VERSION_INFO._getProtocVersion();
	}
}
