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

package org.apache.flink.connectors.test.common.environment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.test.common.utils.FlinkContainers;

/**
 * Test environment running job on {@link FlinkContainers}.
 */
public class FlinkContainersTestEnvironment extends RemoteClusterTestEnvironment {

	private final FlinkContainers flink;

	/**
	 * Construct a test environment for Flink containers.
	 * @param flink Flink cluster on Testcontainers
	 * @param jarPath Path of JARs to be shipped to Flink cluster
	 */
	public FlinkContainersTestEnvironment(FlinkContainers flink, String... jarPath) {
		this(flink, new Configuration(), jarPath);
	}

	/**
	 * Construct a test environment related to Flink containers with configurations.
	 * @param flink Flink cluster on Testcontainers
	 * @param config Configurations of the test environment
	 * @param jarPath Path of JARs to be shipped to Flink cluster
	 */
	public FlinkContainersTestEnvironment(FlinkContainers flink, Configuration config, String... jarPath) {
		super(flink.getJobManagerHost(), flink.getJobManagerRESTPort(), jarPath);
		this.flink = flink;
	}

	/**
	 * Get instance of Flink containers for cluster controlling.
	 * @return Flink cluster on Testcontainers
	 */
	public FlinkContainers getFlinkContainers() {
		return flink;
	}
}
