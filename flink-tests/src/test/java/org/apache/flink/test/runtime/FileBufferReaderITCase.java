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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.partition.BoundedBlockingSubpartitionType;
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
import org.apache.flink.testutils.serialization.types.ByteArrayType;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests the bug reported in FLINK-131O0.
 *
 * <p>The implementation of {@link org.apache.flink.runtime.io.network.partition.BoundedData.Reader#nextBuffer()}
 * for {@link BoundedBlockingSubpartitionType#FILE} assumes that there is always an available buffer, otherwise
 * an IOException is thrown and it always assumes that pool of two buffers is enough (before using the 3rd buffer,
 * first one was expected to be recycled already). But in the case of pending flush operation (when the socket channel
 * is not writable while netty thread is calling {@link ChannelHandlerContext#writeAndFlush(Object, ChannelPromise)}),
 * the first fetched buffer from {@link org.apache.flink.runtime.io.network.partition.FileChannelBoundedData} has not
 * been recycled while fetching the second buffer to trigger next read ahead, which breaks the above assumption.
 */
public class FileBufferReaderITCase extends TestLogger {

	private static final int parallelism = 8;

	private static final int numRecords = 100_000;

	private static final int bufferSize = 4096;

	private static final int headerSize = 8;

	private static final int recordSize = bufferSize - headerSize;

	private static final byte[] dataSource = new byte[recordSize];

	@BeforeClass
	public static void setup() {
		for (int i = 0; i < dataSource.length; i++) {
			dataSource[i] = 0;
		}
	}

	@Test
	public void testSequentialReading() throws Exception {
		// setup
		final Configuration configuration = new Configuration();
		configuration.setString(RestOptions.BIND_PORT, "0");
		configuration.setString(NettyShuffleEnvironmentOptions.NETWORK_BLOCKING_SHUFFLE_TYPE, "file");
		configuration.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("1g"));
		configuration.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse(bufferSize + "b"));

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(parallelism)
			.setNumSlotsPerTaskManager(1)
			.build();

		try (final MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			final MiniClusterClient client = new MiniClusterClient(configuration, miniCluster);
			final JobGraph jobGraph = createJobGraph();
			// wait for the submission to succeed
			final JobID jobID = client.submitJob(jobGraph).get();

			final CompletableFuture<JobResult> resultFuture = client.requestJobResult(jobID);
			final JobResult jobResult = resultFuture.get();

			assertThat(jobResult.getSerializedThrowable().isPresent(), is(false));
		}
	}

	private static JobGraph createJobGraph() {
		final SlotSharingGroup group1 = new SlotSharingGroup();
		final SlotSharingGroup group2 = new SlotSharingGroup();

		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(TestSourceInvokable.class);
		source.setParallelism(parallelism);
		source.setSlotSharingGroup(group1);

		final JobVertex sink = new JobVertex("sink");
		sink.setInvokableClass(TestSinkInvokable.class);
		sink.setParallelism(parallelism);
		sink.setSlotSharingGroup(group2);

		sink.connectNewDataSetAsInput(source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(source, sink);
		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

		return jobGraph;
	}

	/**
	 * Basic source {@link AbstractInvokable} which sends the elements to the
	 * {@link TestSinkInvokable}.
	 */
	public static final class TestSourceInvokable extends AbstractInvokable {

		/**
		 * Create an Invokable task and set its environment.
		 *
		 * @param environment The environment assigned to this invokable.
		 */
		public TestSourceInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			final RecordWriter<ByteArrayType> writer = new RecordWriterBuilder<ByteArrayType>().build(getEnvironment().getWriter(0));

			final ByteArrayType bytes = new ByteArrayType(dataSource);
			int counter = 0;
			while (counter++ < numRecords) {
				writer.emit(bytes);
				writer.flushAll();
			}
		}
	}

	/**
	 * Basic sink {@link AbstractInvokable} which verifies the sent elements
	 * from the {@link TestSourceInvokable}.
	 */
	public static final class TestSinkInvokable extends AbstractInvokable {

		private int numReceived = 0;

		/**
		 * Create an Invokable task and set its environment.
		 *
		 * @param environment The environment assigned to this invokable.
		 */
		public TestSinkInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			final RecordReader<ByteArrayType> reader = new RecordReader<>(
				getEnvironment().getInputGate(0),
				ByteArrayType.class,
				getEnvironment().getTaskManagerInfo().getTmpDirectories());

			while (reader.hasNext()) {
				reader.next();
				numReceived++;
			}

			assertThat(numReceived, is(numRecords));
		}
	}
}
