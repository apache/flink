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
package org.apache.flink.runtime.yarn;

import org.apache.hadoop.fs.Path;
import java.io.File;
import java.util.List;

/**
 * Abstract interface for an implementation of a Flink on YARN client to deploy.
 *
 * The Client describes the properties of the YARN application to create.
 */
public abstract class AbstractFlinkYarnClient {

	// ---- Setter for YARN Cluster properties ----- //

	/**
	 * @param memoryMB The amount of memory for the JobManager (in MB)
	 */
	public abstract void setJobManagerMemory(int memoryMB);

	/**
	 * @param memoryMB The memory per TaskManager (in MB)
	 */
	public abstract void setTaskManagerMemory(int memoryMB);

	/**
	 * Flink configuration
	 */
	public abstract void setFlinkConfigurationObject(org.apache.flink.configuration.Configuration conf);

	/**
	 *
	 * @param slots The number of TaskManager slots per TaskManager.
	 */
	public abstract void setTaskManagerSlots(int slots);

	/**
	 * @return the number of TaskManager processing slots per TaskManager.
	 */
	public abstract int getTaskManagerSlots();

	/**
	 * @param queue Name of the YARN queue
	 */
	public abstract void setQueue(String queue);

	/**
	 *
	 * @param localJarPath Local Path to the Flink uberjar
	 */
	public abstract void setLocalJarPath(Path localJarPath);

	/**
	 *
	 * @param confPath local path to the Flink configuration file
	 */
	public abstract void setConfigurationFilePath(Path confPath);

	/**
	 *
	 * @param logConfPath local path to the flink logging configuration
	 */
	public abstract void setFlinkLoggingConfigurationPath(Path logConfPath);
	public abstract Path getFlinkLoggingConfigurationPath();

	/**
	 *
	 * @param tmCount number of TaskManagers to start
	 */
	public abstract void setTaskManagerCount(int tmCount);
	public abstract int getTaskManagerCount();

	/**
	 * @param confDirPath Path to config directory.
	 */
	public abstract void setConfigurationDirectory(String confDirPath);

	/**
	 * List of files to transfer to the YARN containers.
	 */
	public abstract void setShipFiles(List<File> shipFiles);

	/**
	 *
	 * @param dynamicPropertiesEncoded Encoded String of the dynamic properties (-D configuration values of the Flink configuration)
	 */
	public abstract void setDynamicPropertiesEncoded(String dynamicPropertiesEncoded);
	public abstract String getDynamicPropertiesEncoded();

	// --------------------------------------- Operations on the YARN cluster ----- //

	/**
	 * Returns a String containing details about the cluster (NodeManagers, available memory, ...)
	 *
	 */
	public abstract String getClusterDescription() throws Exception;

	/**
	 * Trigger the deployment to YARN.
	 *
	 */
	public abstract AbstractFlinkYarnCluster deploy() throws Exception;

	/**
	 * @param detachedMode If true, the Flink YARN client is non-blocking. That means it returns
	 *                        once Flink has been started successfully on YARN.
	 */
	public abstract void setDetachedMode(boolean detachedMode);

	public abstract boolean isDetached();

	/**
	 * @return The string representation of the Path to the YARN session files. This is a temporary
	 * directory in HDFS that contains the jar files and configuration which is shipped to all the containers.
	 */
	public abstract String getSessionFilesDir();

	/**
	 * Set a name for the YARN application
	 * @param name
	 */
	public abstract void setName(String name);
}
