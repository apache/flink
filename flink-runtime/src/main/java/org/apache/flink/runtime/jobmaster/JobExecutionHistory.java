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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.PropertyAccessor.FIELD;

/**
 * JobExecutionHistory is used to capture [flink job id --> application id & container id] mapping
 * when we deploy jobs through Yarn. The following is an example on what JobExecutionHistory may capture.
 * <p>
 * {
 * "clusterId" : "application_1560449379756_1548",
 * "jobId" : "7fd4d6fe0baa53bfec4c278f18188ee6",
 * "archivePath" : "hdfs:///flink/completed-jobs",
 * "containers" : {
 * "container_1560449379756_1548_01_000002" : "hadoo-data-slave-dev-0a025632.sample.com:8042"
 * },
 * "attempts" : {
 * "8c39493bc486784f62c582c646b1c02e" : "Reduce (SUM(1), at main(WordCount.java:79) (1/1)",
 * "95899b08e08e6e9d051654ffb67f8f64" : "DataSink (collect()) (1/1)",
 * "39138b3e6df1c083b170525faf2bf80e" : "CHAIN DataSource (at getDefaultTextLineDataSet(WordCountData.java:70)))\
 * -> FlatMap (FlatMap at main(WordCount.java:76)) -> Combine (SUM(1), at main(WordCount.java:79) (1/1)"
 * }
 */
public class JobExecutionHistory {

	protected static final Logger log = LoggerFactory.getLogger(JobExecutionHistory.class);
	protected static JobExecutionHistory INSTANCE;

	private static final String LOG_FILE_ENV = "log.file";

	public static String hadoopUser = EnvironmentInformation.getHadoopUser();

	// clusterId will be yarn application id, if the job is deployed through yarn
	private String clusterId;
	private String resourceManagerEpoch;
	private String jobId;
	private String archivePath;
	private String historyServerUrl;

	private Map<String, String> containers;

	// [host:port --> container id] mapping
	private Map<String, String> taskManagerLocationsToContainersMap;

	private Map<String, Set<String>> vertexToAttemptIdsMap;

	private Map<String, String> attemptsInfo;

	public static JobExecutionHistory getInstance() {
		if (INSTANCE == null) {
			synchronized (JobExecutionHistory.class) {
				if (INSTANCE == null) {
					INSTANCE = new JobExecutionHistory();
				}
			}
		}
		return INSTANCE;
	}

	/**
	 * In this method, we try to infer flink config directory based on 'log.file' jvm option parameters.
	 * After finding the log file configuration jvm option, we replace `userlogs` substring with
	 * 'nm-local-dir/usercache/$hadoopUser/appcache' to get the flink configuration file that is generated
	 * on the fly.
	 * <p>
	 * Sample JVM Options:
	 * -Xmx424m
	 * -Dlog.file=/data/nvme1n1/userlogs/application_15756_1549/container_e02_15756_1549_01_000001/jobmanager.log
	 * -Dlogback.configurationFile=file:logback.xml
	 * -Dlog4j.configuration=file:log4j.properties
	 */

	public static String getFlinkConfDirectory(String[] jvmOptions) {
		for (String jvmOption : jvmOptions) {
			if (jvmOption.startsWith("-D" + LOG_FILE_ENV)) {
				String logFilePath = jvmOption.split("=")[1];
				String logFileDir = logFilePath.substring(0, logFilePath.lastIndexOf("/"));
				String result = logFileDir.replace("userlogs", "nm-local-dir/usercache/" + hadoopUser + "/appcache");
				return result;
			}
		}
		// return null if jvmOptions does not have the right settings
		return null;
	}

	/**
	 *  This method infers yarn resource manager epoch number based on flink config directory.
	 *  The reason that we need this method is that in yarn the container id is like
	 *   `container_e02_1560449379756_1860_01_000002`. However, flink container id does not have `e02` substring.
	 */
	public static String getResourceManagerEpoch(String flinkConfDir) {
		String[] dirStrs = flinkConfDir.split("/");
		for (String dirStr : dirStrs) {
			if (dirStr.startsWith("container_")) {
				String[] strs= dirStr.split("_");
				if (strs.length > 2) {
					return strs[1];
				}
				break;
			}
		}
		log.error("Failed to find resource manager epoch from {}", flinkConfDir);
		return "";
	}

	JobExecutionHistory() {
		String[] jvmOptions = EnvironmentInformation.getJvmStartupOptionsArray();
		String flinkConfDir = getFlinkConfDirectory(jvmOptions);
		log.info("Flink configuration dir: {}", flinkConfDir);

		Configuration configuration = GlobalConfiguration.loadConfiguration(flinkConfDir);
		this.clusterId = configuration.getString(HighAvailabilityOptions.HA_CLUSTER_ID);
		this.resourceManagerEpoch = getResourceManagerEpoch(flinkConfDir);
		this.archivePath = configuration.getString(JobManagerOptions.ARCHIVE_DIR);
		this.historyServerUrl = configuration.getString(JobManagerOptions.JOB_MANAGER_HADOOP_HISTORYSERVER_URL);
		this.containers = new HashMap<>();
		this.attemptsInfo = new HashMap<>();
		this.vertexToAttemptIdsMap = new HashMap<>();
		this.taskManagerLocationsToContainersMap = new HashMap<>();
	}

	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	public String getClusterId() {
		return clusterId;
	}

	public String getResourceManagerEpoch() {
		return resourceManagerEpoch;
	}

	public String getHistoryServerUrl() {
		return historyServerUrl;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public synchronized void addContainerInfo(String containerId, String hostInfo) {
		containers.put(containerId, hostInfo);
	}

	public synchronized void addTaskManagerLocationToContainerInfo(String taskManagerLocation, String containerId) {
		taskManagerLocationsToContainersMap.put(taskManagerLocation, containerId);
	}

	public String getContainerInfo(String taskManagerHostPort) {
		return taskManagerLocationsToContainersMap.getOrDefault(taskManagerHostPort, "(unknown)");
	}

	public synchronized void addVertextToAttemptIdInfo(String vertexId, String attemptId) {
		if (!vertexToAttemptIdsMap.containsKey(vertexId)) {
			vertexToAttemptIdsMap.put(vertexId, new HashSet<>());
		}
		vertexToAttemptIdsMap.get(vertexId).add(attemptId);
	}

	public synchronized void addAttemptInfo(String attemptId, String vertexInfo) {
		attemptsInfo.put(attemptId, vertexInfo);
	}

	public void saveToArchiveDir() {
		try {
			if (!Strings.isNullOrEmpty(archivePath)) {
				org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
				FileSystem fileSystem = FileSystem.get(new URI(archivePath), configuration);
				Path file = new Path(archivePath + "/" + jobId + ".execution_history");
				if (fileSystem.exists(file)) {
					fileSystem.delete(file, true);
				}
				OutputStream os = fileSystem.create(file, true);
				BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
				br.write(this.toString());
				br.close();
			}
		} catch (Exception e) {
			log.error("Failed to write to archive directory : {}", jobId, e);
		}
	}

	public String toString() {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.setVisibility(FIELD, JsonAutoDetect.Visibility.ANY);
		String result = "";
		try {
			result = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
		} catch (Exception e) {
			log.error("Failed to generate json string", e);
		}
		return result;
	}
}
