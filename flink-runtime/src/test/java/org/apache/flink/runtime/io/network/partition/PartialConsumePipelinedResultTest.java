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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.testutils.junit.category.AlsoRunWithLegacyScheduler;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for consuming a pipelined result only partially.
 */
@Category(AlsoRunWithLegacyScheduler.class)
public class PartialConsumePipelinedResultTest extends TestLogger {

	// Test configuration
	private static final int NUMBER_OF_TMS = 1;
	private static final int NUMBER_OF_SLOTS_PER_TM = 1;
	private static final int PARALLELISM = NUMBER_OF_TMS * NUMBER_OF_SLOTS_PER_TM;

	private static final int NUMBER_OF_NETWORK_BUFFERS = 128;

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE = new MiniClusterResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getFlinkConfiguration())
			.setNumberTaskManagers(NUMBER_OF_TMS)
			.setNumberSlotsPerTaskManager(NUMBER_OF_SLOTS_PER_TM)
			.build());

	private static Configuration getFlinkConfiguration() {
		final Configuration config = new Configuration();
		config.setString(AkkaOptions.ASK_TIMEOUT, TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT());
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, NUMBER_OF_NETWORK_BUFFERS);

		return config;
	}

	/**
	 * Tests a fix for FLINK-1930.
	 *
	 * <p>When consuming a pipelined result only partially, is is possible that local channels
	 * release the buffer pool, which is associated with the result partition, too early. If the
	 * producer is still producing data when this happens, it runs into an IllegalStateException,
	 * because of the destroyed buffer pool.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-1930">FLINK-1930</a>
	 */
	@Test
	public void testPartialConsumePipelinedResultReceiver() throws Exception {
		final JobVertex sender = new JobVertex("Sender");
		sender.setInvokableClass(SlowBufferSender.class);
		sender.setParallelism(PARALLELISM);

		final JobVertex receiver = new JobVertex("Receiver");
		receiver.setInvokableClass(SingleBufferReceiver.class);
		receiver.setParallelism(PARALLELISM);

		// The partition needs to be pipelined, otherwise the original issue does not occur, because
		// the sender and receiver are not online at the same time.
		receiver.connectNewDataSetAsInput(
			sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph("Partial Consume of Pipelined Result", sender, receiver);

		final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();

		sender.setSlotSharingGroup(slotSharingGroup);
		receiver.setSlotSharingGroup(slotSharingGroup);

		MINI_CLUSTER_RESOURCE.getMiniCluster().executeJobBlocking(jobGraph);
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Sends a fixed number of buffers and sleeps in-between sends.
	 */
	public static class SlowBufferSender extends AbstractInvokable {

		public SlowBufferSender(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			final ResultPartitionWriter writer = getEnvironment().getWriter(0);

			for (int i = 0; i < 8; i++) {
				final BufferBuilder bufferBuilder = writer.getBufferBuilder(0);
				writer.addBufferConsumer(bufferBuilder.createBufferConsumer(), 0);
				Thread.sleep(50);
				bufferBuilder.finish();
			}
		}
	}

	/**
	 * Reads a single buffer and recycles it.
	 */
	public static class SingleBufferReceiver extends AbstractInvokable {

		public SingleBufferReceiver(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			InputGate gate = getEnvironment().getInputGate(0);
			gate.requestPartitions();
			Buffer buffer = gate.getNext().orElseThrow(IllegalStateException::new).getBuffer();
			if (buffer != null) {
				buffer.recycleBuffer();
			}
		}
	}
}
