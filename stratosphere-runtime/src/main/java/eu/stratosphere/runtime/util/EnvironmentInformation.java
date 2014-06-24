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

package eu.stratosphere.runtime.util;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

public class EnvironmentInformation {

	private static final Log LOG = LogFactory.getLog(EnvironmentInformation.class);
	
	private static final String UNKNOWN = "<unknown>";
	
	private static final String LOG_FILE_OPTION = "-Dlog.file";
	
	private static final String LOG_CONFIGURAION_OPTION = "-Dlog4j.configuration";
	
	
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
	 * Returns the code revision (commit and commit date) of Stratosphere.
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
		} catch (Throwable t) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Cannot determine user/group information for the current user.", t);
			} else {
				LOG.info("Cannot determine user/group information for the current user.");
			}
			return UNKNOWN;
		}
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
			for (String s : bean.getInputArguments()) {
				if (!s.startsWith(LOG_FILE_OPTION) && !s.startsWith(LOG_CONFIGURAION_OPTION)) {
					bld.append(s).append(' ');
				}
			}
			return bld.toString();
		}
		catch (Throwable t) {
			return UNKNOWN;
		}
	}
	
	
	public static void logEnvironmentInfo(Log log, String componentName) {
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
