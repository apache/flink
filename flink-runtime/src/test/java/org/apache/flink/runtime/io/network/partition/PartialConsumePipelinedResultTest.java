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

import akka.actor.ActorSystem;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class PartialConsumePipelinedResultTest {

	// Test configuration
	private final static int NUMBER_OF_TMS = 1;
	private final static int NUMBER_OF_SLOTS_PER_TM = 1;
	private final static int PARALLELISM = NUMBER_OF_TMS * NUMBER_OF_SLOTS_PER_TM;

	private final static int NUMBER_OF_NETWORK_BUFFERS = 128;

	private static TestingCluster flink;
	private static ActorSystem jobClient;

	@BeforeClass
	public static void setUp() throws Exception {
		final Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUMBER_OF_TMS);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUMBER_OF_SLOTS_PER_TM);
		config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT());
		config.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, NUMBER_OF_NETWORK_BUFFERS);

		flink = new TestingCluster(config, true);

		flink.start();

		jobClient = JobClient.startJobClientActorSystem(flink.configuration());
	}

	@AfterClass
	public static void tearDown() throws Exception {
		flink.stop();
	}

	/**
	 * Tests a fix for FLINK-1930.
	 *
	 * <p> When consuming a pipelined result only partially, is is possible that local channels
	 * release the buffer pool, which is associated with the result partition, too early.  If the
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

		final JobGraph jobGraph = new JobGraph(
				"Partial Consume of Pipelined Result", sender, receiver);

		final SlotSharingGroup slotSharingGroup = new SlotSharingGroup(
				sender.getID(), receiver.getID());

		sender.setSlotSharingGroup(slotSharingGroup);
		receiver.setSlotSharingGroup(slotSharingGroup);

		JobClient.submitJobAndWait(
				jobClient,
				flink.getLeaderGateway(TestingUtils.TESTING_DURATION()),
				jobGraph,
				TestingUtils.TESTING_DURATION(),
				false, this.getClass().getClassLoader());
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Sends a fixed number of buffers and sleeps in-between sends.
	 */
	public static class SlowBufferSender extends AbstractInvokable {

		@Override
		public void registerInputOutput() {
			// Nothing to do
		}

		@Override
		public void invoke() throws Exception {
			final ResultPartitionWriter writer = getEnvironment().getWriter(0);

			for (int i = 0; i < 8; i++) {
				final Buffer buffer = writer.getBufferProvider().requestBufferBlocking();
				writer.writeBuffer(buffer, 0);

				Thread.sleep(50);
			}
		}
	}

	/**
	 * Reads a single buffer and recycles it.
	 */
	public static class SingleBufferReceiver extends AbstractInvokable {

		@Override
		public void registerInputOutput() {
			// Nothing to do
		}

		@Override
		public void invoke() throws Exception {
			final BufferReader reader = new BufferReader(getEnvironment().getInputGate(0));

			final Buffer buffer = reader.getNextBuffer();

			buffer.recycle();
		}
	}
}
