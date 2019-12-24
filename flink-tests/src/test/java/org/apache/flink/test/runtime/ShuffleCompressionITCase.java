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
import org.apache.flink.configuration.MemorySize;
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
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.types.LongValue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests network shuffle when data compression is enabled.
 */
@RunWith(Parameterized.class)
public class ShuffleCompressionITCase {

	private static final int NUM_BUFFERS_TO_SEND = 1000;

	private static final int BUFFER_SIZE = 32 * 1024;

	private static final int BYTES_PER_RECORD = 12;

	/** We plus 1 to guarantee that the last buffer contains no more than one record and can not be compressed. */
	private static final int NUM_RECORDS_TO_SEND = NUM_BUFFERS_TO_SEND * BUFFER_SIZE / BYTES_PER_RECORD + 1;

	private static final int NUM_TASKMANAGERS = 2;

	private static final int NUM_SLOTS = 4;

	private static final int PARALLELISM = NUM_TASKMANAGERS * NUM_SLOTS;

	private static final LongValue RECORD_TO_SEND = new LongValue(4387942071694473832L);

	@Parameterized.Parameter
	public static boolean useBroadcastPartitioner = false;

	@Parameterized.Parameters(name = "useBroadcastPartitioner = {0}")
	public static Boolean[] params() {
		return new Boolean[] { true, false };
	}

	@Test
	public void testDataCompressionForBlockingShuffle() throws Exception {
		executeTest(createJobGraph(ScheduleMode.LAZY_FROM_SOURCES, ResultPartitionType.BLOCKING, ExecutionMode.BATCH));
	}

	private void executeTest(JobGraph jobGraph) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("1g"));
		configuration.setBoolean(NettyShuffleEnvironmentOptions.BLOCKING_SHUFFLE_COMPRESSION_ENABLED, true);

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(NUM_TASKMANAGERS)
			.setNumSlotsPerTaskManager(NUM_SLOTS)
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

		sink.connectNewDataSetAsInput(source, DistributionPattern.ALL_TO_ALL, resultPartitionType);
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
			if (useBroadcastPartitioner) {
				recordWriterBuilder.setChannelSelector(new BroadcastPartitioner());
			}
			RecordWriter<LongValue> writer = recordWriterBuilder.build(resultPartitionWriter);

			for (int i = 0; i < NUM_RECORDS_TO_SEND; ++i) {
				writer.broadcastEmit(RECORD_TO_SEND);
			}
			writer.flushAll();
			writer.clearBuffers();
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
			for (int i = 0; i < PARALLELISM * NUM_RECORDS_TO_SEND; ++i) {
				reader.next(value);
				assertEquals(RECORD_TO_SEND.getValue(), value.getValue());
			}
		}
	}
}
