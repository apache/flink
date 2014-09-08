/**
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

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.UserGroupInformation;

public class EnvironmentInformation {

	private static final Logger LOG = LoggerFactory.getLogger(EnvironmentInformation.class);

	private static final String UNKNOWN = "<unknown>";

	/**
	 * Returns the version of the code as String. If version == null, then the JobManager does not run from a
	 * maven build. An example is a source code checkout, compile, and run from inside an IDE.
	 * 
	 * @return The version string.
	 */
	public static String getVersion() {
		return EnvironmentInformation.class.getPackage().getImplementationVersion();
	}

	/**
	 * Returns the code revision (commit and commit date) of Flink.
	 * 
	 * @return The code revision.
	 */
	public static RevisionInformation getRevisionInformation() {
		RevisionInformation info = new RevisionInformation();
		String revision = UNKNOWN;
		String commitDate = UNKNOWN;
		try {
			Properties properties = new Properties();
			InputStream propFile = EnvironmentInformation.class.getClassLoader().getResourceAsStream(".version.properties");
			if (propFile != null) {
				properties.load(propFile);
				revision = properties.getProperty("git.commit.id.abbrev");
				commitDate = properties.getProperty("git.commit.time");
			}
		} catch (Throwable t) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Cannot determine code revision: Unable ro read version property file.", t);
			} else {
				LOG.info("Cannot determine code revision: Unable ro read version property file.");
			}
		}
		info.commitId = revision;
		info.commitDate = commitDate;
		return info;
	}

	public static class RevisionInformation {
		public String commitId;
		public String commitDate;
	}

	public static String getUserRunning() {
		try {
			return UserGroupInformation.getCurrentUser().getShortUserName();
		}
		catch (Throwable t) {
			if (LOG.isDebugEnabled() && !(t instanceof ClassNotFoundException)) {
				LOG.debug("Cannot determine user/group information using Hadoop utils.", t);
			}
		}
		
		String user = System.getProperty("user.name");
		if (user == null) {
			user = UNKNOWN;
			if (LOG.isDebugEnabled()) {
				LOG.debug("Cannot determine user/group information for the current user.");
			}
		}
		return user;
	}

	public static long getMaxJvmMemory() {
		return Runtime.getRuntime().maxMemory() >>> 20;
	}

	public static String getJvmVersion() {
		try {
			final RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
			return bean.getVmName() + " - " + bean.getVmVendor() + " - " + bean.getSpecVersion() + '/' + bean.getVmVersion();
		}
		catch (Throwable t) {
			return UNKNOWN;
		}
	}

	public static String getJvmStartupOptions() {
		try {
			final RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
			final StringBuilder bld = new StringBuilder();
			return bld.toString();
		}
		catch (Throwable t) {
			return UNKNOWN;
		}
	}

	public static void logEnvironmentInfo(Logger log, String componentName) {
		if (log.isInfoEnabled()) {
			RevisionInformation rev = getRevisionInformation();
			String version = getVersion();
			
			String user = getUserRunning();
			
			String jvmVersion = getJvmVersion();
			String options = getJvmStartupOptions();
			
			String javaHome = System.getenv("JAVA_HOME");
			
			long memory = getMaxJvmMemory();
			
			log.info("-------------------------------------------------------");
			log.info(" Starting " + componentName + " (Version: " + version + ", "
					+ "Rev:" + rev.commitId + ", " + "Date:" + rev.commitDate + ")");
			log.info(" Current user: " + user);
			log.info(" JVM: " + jvmVersion);
			log.info(" Startup Options: " + options);
			log.info(" Maximum heap size: " + memory + " MiBytes");
			log.info(" JAVA_HOME: " + (javaHome == null ? "not set" : javaHome));
			log.info("-------------------------------------------------------");
		}
	}

	// --------------------------------------------------------------------------------------------
	
	private EnvironmentInformation() {}
	
	public static void main(String[] args) {
		logEnvironmentInfo(LOG, "Test");
	}
}
