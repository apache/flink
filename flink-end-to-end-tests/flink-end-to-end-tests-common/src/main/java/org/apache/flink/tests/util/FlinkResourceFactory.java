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

package org.apache.flink.tests.util;

/**
 * Flink resource factory to create {@link FlinkResource}. it will be a {@link LocalStandaloneFlinkResource} or
 * {@link DistributionFlinkResource}, depends on the JVM property -De2e.flink.mode setting.
 */
public class FlinkResourceFactory {

	public static final String E2E_FLINK_MODE = "e2e.flink.mode";
	public static final String E2E_FLINK_MODE_LOCAL_STANDALONE = "localStandalone";
	public static final String E2E_FLINK_MODE_DISTRIBUTED = "distributed";

	public static final String E2E_FLINK_HOSTS = "e2e.flink.hosts";
	public static final String E2E_FLINK_HOSTS_DEFAULT = "localhost:6123";

	public static final String E2E_FLINK_DIST_DIR = "distDir";

	public static FlinkResource create() {
		String flinkMode = System.getProperty(E2E_FLINK_MODE, E2E_FLINK_MODE_LOCAL_STANDALONE);

		// Validate the -DdistDir property.
		final String distDirProperty = System.getProperty(E2E_FLINK_DIST_DIR);
		if (distDirProperty == null) {
			throw new IllegalArgumentException("The distDir property was not set. You can set it by setting JVM property -DdistDir=<path>.");
		}

		// Initialize the flink resource.
		if (E2E_FLINK_MODE_LOCAL_STANDALONE.equalsIgnoreCase(flinkMode)) {
			return new LocalStandaloneFlinkResource(distDirProperty);
		} else if (E2E_FLINK_MODE_DISTRIBUTED.equalsIgnoreCase(flinkMode)) {
			String flinkHosts = System.getProperty(E2E_FLINK_HOSTS, E2E_FLINK_HOSTS_DEFAULT);
			return new DistributionFlinkResource(distDirProperty, flinkHosts);
		} else {
			throw new IllegalArgumentException("Invalid JVM property -D" + E2E_FLINK_MODE + "=" + flinkMode
				+ ", use -D" + E2E_FLINK_MODE + "=" + E2E_FLINK_MODE_LOCAL_STANDALONE
				+ " or use -D" + E2E_FLINK_MODE + "=" + E2E_FLINK_MODE_DISTRIBUTED);
		}
	}
}
