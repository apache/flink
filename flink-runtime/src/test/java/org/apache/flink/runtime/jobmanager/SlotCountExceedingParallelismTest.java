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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.BitSet;

public class SlotCountExceedingParallelismTest extends TestLogger {

	// Test configuration
	private static final int NUMBER_OF_TMS = 2;
	private static final int NUMBER_OF_SLOTS_PER_TM = 2;
	private static final int PARALLELISM = NUMBER_OF_TMS * NUMBER_OF_SLOTS_PER_TM;

	public static final String JOB_NAME = "SlotCountExceedingParallelismTest (no slot sharing, blocking results)";

	private static MiniCluster flink;

	@BeforeClass
	public static void setUp() throws Exception {
		final Configuration config = new Configuration();
		config.setString(AkkaOptions.ASK_TIMEOUT, TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT());

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(config)
			.setNumTaskManagers(NUMBER_OF_TMS)
			.setNumSlotsPerTaskManager(NUMBER_OF_SLOTS_PER_TM)
			.build();

		flink = new MiniCluster(miniClusterConfiguration);

		flink.start();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (flink != null) {
			flink.close();
		}
	}

	@Test
	public void testNoSlotSharingAndBlockingResultSender() throws Exception {
		// Sender with higher parallelism than available slots
		JobGraph jobGraph = createTestJobGraph(JOB_NAME, PARALLELISM * 2, PARALLELISM);
		submitJobGraphAndWait(jobGraph);
	}

	@Test
	public void testNoSlotSharingAndBlockingResultReceiver() throws Exception {
		// Receiver with higher parallelism than available slots
		JobGraph jobGraph = createTestJobGraph(JOB_NAME, PARALLELISM, PARALLELISM * 2);
		submitJobGraphAndWait(jobGraph);
	}

	@Test
	public void testNoSlotSharingAndBlockingResultBoth() throws Exception {
		// Both sender and receiver with higher parallelism than available slots
		JobGraph jobGraph = createTestJobGraph(JOB_NAME, PARALLELISM * 2, PARALLELISM * 2);
		submitJobGraphAndWait(jobGraph);
	}

	// ---------------------------------------------------------------------------------------------

	private void submitJobGraphAndWait(final JobGraph jobGraph) throws JobExecutionException, InterruptedException {
		flink.executeJobBlocking(jobGraph);
	}

	private JobGraph createTestJobGraph(
			String jobName,
			int senderParallelism,
			int receiverParallelism) {

		// The sender and receiver invokable logic ensure that each subtask gets the expected data
		final JobVertex sender = new JobVertex("Sender");
		sender.setInvokableClass(RoundRobinSubtaskIndexSender.class);
		sender.getConfiguration().setInteger(RoundRobinSubtaskIndexSender.CONFIG_KEY, receiverParallelism);
		sender.setParallelism(senderParallelism);

		final JobVertex receiver = new JobVertex("Receiver");
		receiver.setInvokableClass(SubtaskIndexReceiver.class);
		receiver.getConfiguration().setInteger(SubtaskIndexReceiver.CONFIG_KEY, senderParallelism);
		receiver.setParallelism(receiverParallelism);

		receiver.connectNewDataSetAsInput(
				sender,
				DistributionPattern.ALL_TO_ALL,
				ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(jobName, sender, receiver);

		// We need to allow queued scheduling, because there are not enough slots available
		// to run all tasks at once. We queue tasks and then let them finish/consume the blocking
		// result one after the other.
		jobGraph.setAllowQueuedScheduling(true);

		return jobGraph;
	}

	/**
	 * Sends the subtask index a configurable number of times in a round-robin fashion.
	 */
	public static class RoundRobinSubtaskIndexSender extends AbstractInvokable {

		public static final String CONFIG_KEY = "number-of-times-to-send";

		public RoundRobinSubtaskIndexSender(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			RecordWriter<IntValue> writer = new RecordWriter<>(getEnvironment().getWriter(0));
			final int numberOfTimesToSend = getTaskConfiguration().getInteger(CONFIG_KEY, 0);

			final IntValue subtaskIndex = new IntValue(
					getEnvironment().getTaskInfo().getIndexOfThisSubtask());

			try {
				for (int i = 0; i < numberOfTimesToSend; i++) {
					writer.emit(subtaskIndex);
				}
				writer.flushAll();
			}
			finally {
				writer.clearBuffers();
			}
		}
	}

	/**
	 * Expects to receive the subtask index from a configurable number of sender tasks.
	 */
	public static class SubtaskIndexReceiver extends AbstractInvokable {

		public static final String CONFIG_KEY = "number-of-indexes-to-receive";

		public SubtaskIndexReceiver(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			RecordReader<IntValue> reader = new RecordReader<>(
					getEnvironment().getInputGate(0),
					IntValue.class,
					getEnvironment().getTaskManagerInfo().getTmpDirectories());

			try {
				final int numberOfSubtaskIndexesToReceive = getTaskConfiguration().getInteger(CONFIG_KEY, 0);
				final BitSet receivedSubtaskIndexes = new BitSet(numberOfSubtaskIndexesToReceive);

				IntValue record;

				int numberOfReceivedSubtaskIndexes = 0;

				while ((record = reader.next()) != null) {
					// Check that we don't receive more than expected
					numberOfReceivedSubtaskIndexes++;

					if (numberOfReceivedSubtaskIndexes > numberOfSubtaskIndexesToReceive) {
						throw new IllegalStateException("Received more records than expected.");
					}

					int subtaskIndex = record.getValue();

					// Check that we only receive each subtask index once
					if (receivedSubtaskIndexes.get(subtaskIndex)) {
						throw new IllegalStateException("Received expected subtask index twice.");
					}
					else {
						receivedSubtaskIndexes.set(subtaskIndex, true);
					}
				}

				// Check that we have received all expected subtask indexes
				if (receivedSubtaskIndexes.cardinality() != numberOfSubtaskIndexesToReceive) {
					throw new IllegalStateException("Finished receive, but did not receive "
							+ "all expected subtask indexes.");
				}
			}
			finally {
				reader.clearBuffers();
			}
		}
	}
}
