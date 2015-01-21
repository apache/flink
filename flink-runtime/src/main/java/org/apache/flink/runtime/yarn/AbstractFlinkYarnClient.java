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

public abstract class AbstractFlinkYarnClient {

	// ---- Setter for YARN Cluster properties ----- //
	public abstract void setJobManagerMemory(int memoryMB);
	public abstract void setTaskManagerMemory(int memoryMB);

	public abstract void setFlinkConfigurationObject(org.apache.flink.configuration.Configuration conf);

	public abstract void setTaskManagerSlots(int slots);
	public abstract int getTaskManagerSlots();
	public abstract void setQueue(String queue);
	public abstract void setLocalJarPath(Path localJarPath);
	public abstract void setConfigurationFilePath(Path confPath);
	public abstract void setFlinkLoggingConfigurationPath(Path logConfPath);
	public abstract Path getFlinkLoggingConfigurationPath();
	public abstract void setTaskManagerCount(int tmCount);
	public abstract int getTaskManagerCount();
	public abstract void setConfigurationDirectory(String confDirPath);
	// List of files to transfer to the YARN containers.
	public abstract void setShipFiles(List<File> shipFiles);
	public abstract void setDynamicPropertiesEncoded(String dynamicPropertiesEncoded);
	public abstract String getDynamicPropertiesEncoded();

	// ---- Operations on the YARN cluster ----- //
	public abstract String getClusterDescription() throws Exception;

	public abstract AbstractFlinkYarnCluster deploy(String clusterName) throws Exception;


}
