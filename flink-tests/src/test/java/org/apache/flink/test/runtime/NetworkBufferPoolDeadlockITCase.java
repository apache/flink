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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Network buffer pool deadlock ITCase.
 */
public class NetworkBufferPoolDeadlockITCase extends TestLogger {

	@Test(timeout = 60000L)
	public void testNetworkBufferPoolDeadlock() throws Exception {
		int parallelism = 2;
		int numBuffers = 4;
		int numRecords = (int) ((numBuffers - 0.5) * 32 * 1024 / 8);
		Configuration configuration = new Configuration();
		configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, numBuffers * 32 * 1024);
		configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, numBuffers * 32 * 1024);
		configuration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL, 2);
		configuration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_SUBPARTITION, 2);
		configuration.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, 0);

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(2)
			.setNumSlotsPerTaskManager(1)
			.build();

		try (MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			MiniClusterClient miniClusterClient = new MiniClusterClient(configuration, miniCluster);
			JobGraph jobGraph = createJobGraph(ExecutionMode.BATCH, parallelism, numRecords);
			jobGraph.addCustomConfiguration(configuration);
			jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

			CompletableFuture<JobSubmissionResult> submissionFuture = miniClusterClient.submitJob(jobGraph);

			// wait for the submission to succeed
			JobSubmissionResult jobSubmissionResult = submissionFuture.get();

			CompletableFuture<JobResult> resultFuture = miniClusterClient.requestJobResult(jobSubmissionResult.getJobID());

			JobResult jobResult = resultFuture.get();

			assertThat(jobResult.getSerializedThrowable().isPresent(), is(false));
		}
	}

	private JobGraph createJobGraph(ExecutionMode executionMode, int parallelism, int numRecords) {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(parallelism);
		env.getConfig().setExecutionMode(executionMode);

		DataStream<Integer> stream = env.addSource(new RichParallelSourceFunction<Integer>() {
			private boolean isRunning = true;
			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
				int counter = 0;
				while (isRunning && ++counter < numRecords) {
					ctx.collect(666);
				}
			}

			@Override
			public void cancel() {
				isRunning = false;
			}
		}).setParallelism(parallelism);

		stream.rebalance().addSink(new RichSinkFunction<Integer>() {
				@Override
				public void invoke(Integer value, Context context) throws Exception {
					// just drop all the input
				}
			});

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		return jobGraph;
	}
}
