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

package org.apache.flink.runtime.webmonitor;

import akka.actor.ActorSystem;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for the web runtime monitor. This class contains for example methods to build
 * messages with aggregate information about the state of an execution graph, to be send
 * to the web server.
 */
public final class WebMonitorUtils {

	private static final Logger LOG = LoggerFactory.getLogger(WebMonitorUtils.class);

	/**
	 * Singleton to hold the log and stdout file
	 */
	public static class LogFileLocation {

		public final File logFile;
		public final File stdOutFile;

		private LogFileLocation(File logFile, File stdOutFile) {
			this.logFile = logFile;
			this.stdOutFile = stdOutFile;
		}
		

		/**
		 * Finds the Flink log directory using log.file Java property that is set during startup.
		 */
		public static LogFileLocation find(Configuration config) {
			final String logEnv = "log.file";
			String logFilePath = System.getProperty(logEnv);
			
			if (logFilePath == null) {
				LOG.warn("Log file environment variable '{}' is not set.", logEnv);
				logFilePath = config.getString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY, null);
			}
			
			// not configured, cannot serve log files
			if (logFilePath == null || logFilePath.length() < 4) {
				LOG.warn("JobManager log files are unavailable in the web dashboard. " +
					"Log file location not found in environment variable '{}' or configuration key '{}'.",
					logEnv, ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY);
				return new LogFileLocation(null, null);
			}
			
			String outFilePath = logFilePath.substring(0, logFilePath.length() - 3).concat("out");

			LOG.info("Determined location of JobManager log file: {}", logFilePath);
			LOG.info("Determined location of JobManager stdout file: {}", outFilePath);
			
			return new LogFileLocation(resolveFileLocation(logFilePath), resolveFileLocation(outFilePath));
		}

		/**
		 * Verify log file location
		 * @param logFilePath Path to log file
		 * @return File or null if not a valid log file
		 */
		private static File resolveFileLocation(String logFilePath) {
			File logFile = new File(logFilePath);
			return (logFile.exists() && logFile.canRead()) ? logFile : null;
		}
	}

	/**
	 * Starts the web runtime monitor. Because the actual implementation of the runtime monitor is
	 * in another project, we load the runtime monitor dynamically.
	 * <p>
	 * Because failure to start the web runtime monitor is not considered fatal, this method does
	 * not throw any exceptions, but only logs them.
	 *
	 * @param config                 The configuration for the runtime monitor.
	 * @param leaderRetrievalService Leader retrieval service to get the leading JobManager
	 */
	public static WebMonitor startWebRuntimeMonitor(
			Configuration config,
			LeaderRetrievalService leaderRetrievalService,
			ActorSystem actorSystem) {
		// try to load and instantiate the class
		try {
			String classname = "org.apache.flink.runtime.webmonitor.WebRuntimeMonitor";
			Class<? extends WebMonitor> clazz = Class.forName(classname).asSubclass(WebMonitor.class);
			
			Constructor<? extends WebMonitor> constructor = clazz.getConstructor(Configuration.class,
					LeaderRetrievalService.class,
					ActorSystem.class);
			return constructor.newInstance(config, leaderRetrievalService, actorSystem);
		} catch (ClassNotFoundException e) {
			LOG.error("Could not load web runtime monitor. " +
					"Probably reason: flink-runtime-web is not in the classpath");
			LOG.debug("Caught exception", e);
			return null;
		} catch (InvocationTargetException e) {
			LOG.error("WebServer could not be created", e.getTargetException());
			return null;
		} catch (Throwable t) {
			LOG.error("Failed to instantiate web runtime monitor.", t);
			return null;
		}
	}

	public static Map<String, String> fromKeyValueJsonArray(JSONArray parsed) throws JSONException {
		Map<String, String> hashMap = new HashMap<>();

		for (int i = 0; i < parsed.length(); i++) {
			JSONObject jsonObject = parsed.getJSONObject(i);
			String key = jsonObject.getString("key");
			String value = jsonObject.getString("value");
			hashMap.put(key, value);
		}

		return hashMap;
	}

	public static JobDetails createDetailsForJob(ExecutionGraph job) {
		JobStatus status = job.getState();

		long started = job.getStatusTimestamp(JobStatus.CREATED);
		long finished = status.isTerminalState() ? job.getStatusTimestamp(status) : -1L;

		int[] countsPerStatus = new int[ExecutionState.values().length];
		long lastChanged = 0;
		int numTotalTasks = 0;

		for (ExecutionJobVertex ejv : job.getVerticesTopologically()) {
			ExecutionVertex[] vertices = ejv.getTaskVertices();
			numTotalTasks += vertices.length;

			for (ExecutionVertex vertex : vertices) {
				ExecutionState state = vertex.getExecutionState();
				countsPerStatus[state.ordinal()]++;
				lastChanged = Math.max(lastChanged, vertex.getStateTimestamp(state));
			}
		}

		lastChanged = Math.max(lastChanged, finished);

		return new JobDetails(job.getJobID(), job.getJobName(),
				started, finished, status, lastChanged,
				countsPerStatus, numTotalTasks);
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private WebMonitorUtils() {
		throw new RuntimeException();
	}
}
