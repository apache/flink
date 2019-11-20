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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.types.LongValue;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests pipeline/blocking shuffle when data compression is enabled.
 */
public class ShuffleCompressionITCase {

	private static final int NUM_RECORDS_TO_SEND = 10 * 1024 * 1024;

	private static final int PARALLELISM = 2;

	@Test
	public void testDataCompressionForPipelineShuffle() throws Exception {
		executeTest(createJobGraph(ScheduleMode.EAGER, ResultPartitionType.PIPELINED, ExecutionMode.PIPELINED));
	}

	@Test
	public void testDataCompressionForBlockingShuffle() throws Exception {
		executeTest(createJobGraph(ScheduleMode.LAZY_FROM_SOURCES, ResultPartitionType.BLOCKING, ExecutionMode.BATCH));
	}

	private void executeTest(JobGraph jobGraph) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(TaskManagerOptions.TOTAL_FLINK_MEMORY, "1g");
		configuration.setBoolean(NettyShuffleEnvironmentOptions.BLOCKING_SHUFFLE_COMPRESSION_ENABLED, true);
		configuration.setBoolean(NettyShuffleEnvironmentOptions.PIPELINED_SHUFFLE_COMPRESSION_ENABLED, true);

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(PARALLELISM)
			.setNumSlotsPerTaskManager(1)
			.build();

		try (MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			MiniClusterClient miniClusterClient = new MiniClusterClient(configuration, miniCluster);
			// wait for the submission to succeed
			JobID jobID = miniClusterClient.submitJob(jobGraph).get();

			CompletableFuture<JobResult> resultFuture = miniClusterClient.requestJobResult(jobID);
			assertFalse(resultFuture.get().getSerializedThrowable().isPresent());
		}
	}

	private static JobGraph createJobGraph(
			ScheduleMode scheduleMode,
			ResultPartitionType resultPartitionType,
			ExecutionMode executionMode) throws IOException {
		SlotSharingGroup slotSharingGroup = new SlotSharingGroup();

		JobVertex source = new JobVertex("source");
		source.setInvokableClass(LongValueSource.class);
		source.setParallelism(PARALLELISM);
		source.setSlotSharingGroup(slotSharingGroup);

		JobVertex sink = new JobVertex("sink");
		sink.setInvokableClass(ResultVerifyingSink.class);
		sink.setParallelism(PARALLELISM);
		sink.setSlotSharingGroup(slotSharingGroup);

		sink.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, resultPartitionType);
		JobGraph jobGraph = new JobGraph(source, sink);
		jobGraph.setScheduleMode(scheduleMode);

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setExecutionMode(executionMode);
		jobGraph.setExecutionConfig(executionConfig);

		return jobGraph;
	}

	/**
	 * Test source that emits {@link LongValue} to downstream.
	 */
	public static final class LongValueSource extends AbstractInvokable {

		public LongValueSource(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			ResultPartitionWriter resultPartitionWriter = getEnvironment().getWriter(0);
			RecordWriterBuilder<LongValue> recordWriterBuilder = new RecordWriterBuilder<>();
			if (getEnvironment().getExecutionConfig().getExecutionMode() == ExecutionMode.PIPELINED) {
				// enable output flush for pipeline mode
				recordWriterBuilder.setTimeout(100);
			}
			RecordWriter<LongValue> writer = recordWriterBuilder.build(resultPartitionWriter);

			for (int i = 0; i < NUM_RECORDS_TO_SEND; ++i) {
				LongValue value = new LongValue(i);
				writer.broadcastEmit(value);
			}
			writer.flushAll();
		}
	}

	/**
	 * Test sink that receives {@link LongValue} and verifies the received records.
	 */
	public static final class ResultVerifyingSink extends AbstractInvokable {

		public ResultVerifyingSink(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			MutableRecordReader<LongValue> reader = new MutableRecordReader<>(
				getEnvironment().getInputGate(0),
				new String[]{EnvironmentInformation.getTemporaryFileDirectory()});

			LongValue value = new LongValue();
			for (int i = 0; i < NUM_RECORDS_TO_SEND; ++i) {
				reader.next(value);
				assertEquals(i, value.getValue());
			}
		}
	}
}
