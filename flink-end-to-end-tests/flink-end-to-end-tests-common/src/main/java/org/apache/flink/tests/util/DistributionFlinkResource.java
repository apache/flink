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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The distribution flink resource which the end-to-end tests will connect to. We left the cluster starting and stopping
 * job as dummy because usually we will connect to a external flink cluster and have no permission to start or stop the
 * jobManager and taskManager.
 */
public class DistributionFlinkResource implements FlinkResource {

	private static final Logger LOG = LoggerFactory.getLogger(DistributionFlinkResource.class);
	private final String flinkHosts;
	private final Path distDir;
	private final Path binDir;

	public DistributionFlinkResource(String distDirProperty, String flinkHosts) {
		this.distDir = Paths.get(distDirProperty);
		this.binDir = distDir.resolve("bin");
		this.flinkHosts = flinkHosts;
	}

	@Override
	public void startCluster(int taskManagerNum) throws IOException {
		logDoNothingInfo("startCluster");
	}

	@Override
	public void stopJobManager() throws IOException {
		logDoNothingInfo("stopJobManager");
	}

	@Override
	public void stopTaskMangers() throws IOException {
		logDoNothingInfo("stopTaskManagers");
	}

	@Override
	public void stopCluster() throws IOException {
		logDoNothingInfo("stopCluster");
	}

	@Override
	public FlinkClient createFlinkClient() throws IOException {
		return new FlinkClient(this.binDir).jobManagerAddress(flinkHosts);
	}

	@Override
	public FlinkSQLClient createFlinkSQLClient() throws IOException {
		return new FlinkSQLClient(this.binDir);
	}

	private void logDoNothingInfo(String action) {
		LOG.info("The {}() will do nothing because we are in distribute mode by setting JVM property -D{}={}",
			action,
			FlinkResourceFactory.E2E_FLINK_MODE,
			FlinkResourceFactory.E2E_FLINK_MODE_DISTRIBUTED);
	}
}
