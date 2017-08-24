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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobView;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.JobManagerRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Utilities for the web runtime monitor. This class contains for example methods to build
 * messages with aggregate information about the state of an execution graph, to be send
 * to the web server.
 */
public final class WebMonitorUtils {

	private static final Logger LOG = LoggerFactory.getLogger(WebMonitorUtils.class);

	/**
	 * Singleton to hold the log and stdout file.
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
				logFilePath = config.getString(WebOptions.LOG_PATH);
			}

			// not configured, cannot serve log files
			if (logFilePath == null || logFilePath.length() < 4) {
				LOG.warn("JobManager log files are unavailable in the web dashboard. " +
					"Log file location not found in environment variable '{}' or configuration key '{}'.",
					logEnv, WebOptions.LOG_PATH);
				return new LogFileLocation(null, null);
			}

			String outFilePath = logFilePath.substring(0, logFilePath.length() - 3).concat("out");

			LOG.info("Determined location of JobManager log file: {}", logFilePath);
			LOG.info("Determined location of JobManager stdout file: {}", outFilePath);

			return new LogFileLocation(resolveFileLocation(logFilePath), resolveFileLocation(outFilePath));
		}

		/**
		 * Verify log file location.
		 *
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
	 *
	 * <p>Because failure to start the web runtime monitor is not considered fatal, this method does
	 * not throw any exceptions, but only logs them.
	 *
	 * @param config The configuration for the runtime monitor.
	 * @param highAvailabilityServices HighAvailabilityServices used to start the WebRuntimeMonitor
	 * @param jobManagerRetriever which retrieves the currently leading JobManager
	 * @param queryServiceRetriever which retrieves the query service
	 * @param timeout for asynchronous operations
	 * @param executor to run asynchronous operations
	 */
	public static WebMonitor startWebRuntimeMonitor(
			Configuration config,
			HighAvailabilityServices highAvailabilityServices,
			JobManagerRetriever jobManagerRetriever,
			MetricQueryServiceRetriever queryServiceRetriever,
			Time timeout,
			Executor executor) {
		// try to load and instantiate the class
		try {
			String classname = "org.apache.flink.runtime.webmonitor.WebRuntimeMonitor";
			Class<? extends WebMonitor> clazz = Class.forName(classname).asSubclass(WebMonitor.class);

			Constructor<? extends WebMonitor> constructor = clazz.getConstructor(
				Configuration.class,
				LeaderRetrievalService.class,
				BlobView.class,
				JobManagerRetriever.class,
				MetricQueryServiceRetriever.class,
				Time.class,
				Executor.class);
			return constructor.newInstance(
				config,
				highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
				highAvailabilityServices.createBlobStore(),
				jobManagerRetriever,
				queryServiceRetriever,
				timeout,
				executor);
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

	public static JsonArchivist[] getJsonArchivists() {
		try {
			String classname = "org.apache.flink.runtime.webmonitor.WebRuntimeMonitor";
			Class<? extends WebMonitor> clazz = Class.forName(classname).asSubclass(WebMonitor.class);
			Method method = clazz.getMethod("getJsonArchivists");
			JsonArchivist[] result = (JsonArchivist[]) method.invoke(null);
			return result;
		} catch (ClassNotFoundException e) {
			LOG.error("Could not load web runtime monitor. " +
				"Probably reason: flink-runtime-web is not in the classpath");
			LOG.debug("Caught exception", e);
			return new JsonArchivist[0];
		} catch (Throwable t) {
			LOG.error("Failed to retrieve archivers from web runtime monitor.", t);
			return new JsonArchivist[0];
		}
	}

	public static Map<String, String> fromKeyValueJsonArray(String jsonString) {
		try {
			Map<String, String> map = new HashMap<>();
			ObjectMapper m = new ObjectMapper();
			ArrayNode array = (ArrayNode) m.readTree(jsonString);

			Iterator<JsonNode> elements = array.elements();
			while (elements.hasNext()) {
				JsonNode node = elements.next();
				String key = node.get("key").asText();
				String value = node.get("value").asText();
				map.put(key, value);
			}

			return map;
		}
		catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	public static JobDetails createDetailsForJob(AccessExecutionGraph job) {
		JobStatus status = job.getState();

		long started = job.getStatusTimestamp(JobStatus.CREATED);
		long finished = status.isGloballyTerminalState() ? job.getStatusTimestamp(status) : -1L;

		int[] countsPerStatus = new int[ExecutionState.values().length];
		long lastChanged = 0;
		int numTotalTasks = 0;

		for (AccessExecutionJobVertex ejv : job.getVerticesTopologically()) {
			AccessExecutionVertex[] vertices = ejv.getTaskVertices();
			numTotalTasks += vertices.length;

			for (AccessExecutionVertex vertex : vertices) {
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
	 * Checks and normalizes the given URI. This method first checks the validity of the
	 * URI (scheme and path are not null) and then normalizes the URI to a path.
	 *
	 * @param archiveDirUri The URI to check and normalize.
	 * @return A normalized URI as a Path.
	 *
	 * @throws IllegalArgumentException Thrown, if the URI misses scheme or path.
	 */
	public static Path validateAndNormalizeUri(URI archiveDirUri) {
		final String scheme = archiveDirUri.getScheme();
		final String path = archiveDirUri.getPath();

		// some validity checks
		if (scheme == null) {
			throw new IllegalArgumentException("The scheme (hdfs://, file://, etc) is null. " +
				"Please specify the file system scheme explicitly in the URI.");
		}
		if (path == null) {
			throw new IllegalArgumentException("The path to store the job archive data in is null. " +
				"Please specify a directory path for the archiving the job data.");
		}

		return new Path(archiveDirUri);
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private WebMonitorUtils() {
		throw new RuntimeException();
	}
}
