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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.category.Flip6;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

/**
 * Integration test cases for the {@link MiniCluster}.
 */
@Category(Flip6.class)
public class MiniClusterITCase extends TestLogger {

	// ------------------------------------------------------------------------
	//  Simple Job Running Tests
	// ------------------------------------------------------------------------

	private static final MiniClusterConfiguration defaultConfiguration = null;

	@Test
	public void runJobWithSingleRpcService() throws Exception {
		MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setRpcServiceSharing(MiniClusterConfiguration.RpcServiceSharing.SHARED)
			.build();

		MiniCluster miniCluster = new MiniCluster(cfg);
		try {
			miniCluster.start();
			executeJob(miniCluster);
		}
		finally {
			miniCluster.shutdown();
		}
	}

	@Test
	public void runJobWithMultipleRpcServices() throws Exception {
		MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setRpcServiceSharing(MiniClusterConfiguration.RpcServiceSharing.DEDICATED)
			.build();

		MiniCluster miniCluster = new MiniCluster(cfg);
		try {
			miniCluster.start();
			executeJob(miniCluster);
		}
		finally {
			miniCluster.shutdown();
		}
	}

	@Test
	public void runJobWithMultipleJobManagers() throws Exception {
		MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
		.setNumJobManagers(3)
		.build();

		MiniCluster miniCluster = new MiniCluster(cfg);
		try {
			miniCluster.start();
			executeJob(miniCluster);
		}
		finally {
			miniCluster.shutdown();
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static void executeJob(MiniCluster miniCluster) throws Exception {
		JobGraph job = getSimpleJob();
		miniCluster.executeJobBlocking(job);
	}

	private static JobGraph getSimpleJob() throws IOException {
		JobVertex task = new JobVertex("Test task");
		task.setParallelism(1);
		task.setMaxParallelism(1);
		task.setInvokableClass(NoOpInvokable.class);

		JobGraph jg = new JobGraph(new JobID(), "Test Job", task);
		jg.setAllowQueuedScheduling(true);
		jg.setScheduleMode(ScheduleMode.EAGER);

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));
		jg.setExecutionConfig(executionConfig);

		return jg;
	}
}
