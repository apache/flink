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

package org.apache.flink.runtime.jobmanager.scheduler;

import akka.actor.ActorRef;
import com.google.common.collect.Lists;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.SlotCountExceedingParallelismTest;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.types.IntegerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.apache.flink.runtime.jobmanager.SlotCountExceedingParallelismTest.SubtaskIndexReceiver.CONFIG_KEY;

public class ScheduleOrUpdateConsumersTest {

	private final static int NUMBER_OF_TMS = 2;
	private final static int NUMBER_OF_SLOTS_PER_TM = 2;
	private final static int PARALLELISM = NUMBER_OF_TMS * NUMBER_OF_SLOTS_PER_TM;

	private static TestingCluster flink;
	private static ActorRef jobClient;

	@BeforeClass
	public static void setUp() throws Exception {
		flink = TestingUtils.startTestingCluster(
				NUMBER_OF_SLOTS_PER_TM,
				NUMBER_OF_TMS,
				TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT());

		jobClient = JobClient.createJobClientFromConfig(
				flink.configuration(),
				true,
				flink.jobManagerActorSystem());
	}

	@AfterClass
	public static void tearDown() throws Exception {
		flink.stop();
	}

	@Test
	public void testMixedPipelinedAndBlockingResults() throws Exception {
		final AbstractJobVertex sender = new AbstractJobVertex("Sender");
		sender.setInvokableClass(BinaryRoundRobinSubtaskIndexSender.class);
		sender.getConfiguration().setInteger(BinaryRoundRobinSubtaskIndexSender.CONFIG_KEY, PARALLELISM);
		sender.setParallelism(PARALLELISM);

		final AbstractJobVertex pipelinedReceiver = new AbstractJobVertex("Pipelined Receiver");
		pipelinedReceiver.setInvokableClass(SlotCountExceedingParallelismTest.SubtaskIndexReceiver.class);
		pipelinedReceiver.getConfiguration().setInteger(CONFIG_KEY, PARALLELISM);
		pipelinedReceiver.setParallelism(PARALLELISM);

		pipelinedReceiver.connectNewDataSetAsInput(
				sender,
				DistributionPattern.ALL_TO_ALL,
				ResultPartitionType.PIPELINED);

		final AbstractJobVertex blockingReceiver = new AbstractJobVertex("Blocking Receiver");
		blockingReceiver.setInvokableClass(SlotCountExceedingParallelismTest.SubtaskIndexReceiver.class);
		blockingReceiver.getConfiguration().setInteger(CONFIG_KEY, PARALLELISM);
		blockingReceiver.setParallelism(PARALLELISM);

		blockingReceiver.connectNewDataSetAsInput(sender,
				DistributionPattern.ALL_TO_ALL,
				ResultPartitionType.BLOCKING);

		SlotSharingGroup slotSharingGroup = new SlotSharingGroup(sender.getID(), pipelinedReceiver.getID(), blockingReceiver.getID());

		sender.setSlotSharingGroup(slotSharingGroup);
		pipelinedReceiver.setSlotSharingGroup(slotSharingGroup);
		blockingReceiver.setSlotSharingGroup(slotSharingGroup);

		final JobGraph jobGraph = new JobGraph("", sender, pipelinedReceiver, blockingReceiver);

		JobClient.submitJobAndWait(jobGraph, false, jobClient, TestingUtils.TESTING_DURATION());
	}

	// ---------------------------------------------------------------------------------------------

	public static class BinaryRoundRobinSubtaskIndexSender extends AbstractInvokable {

		public final static String CONFIG_KEY = "number-of-times-to-send";

		private List<RecordWriter<IntegerRecord>> writers = Lists.newArrayListWithCapacity(2);

		private int numberOfTimesToSend;

		@Override
		public void registerInputOutput() {
			// The order of intermediate result creation in the job graph specifies which produced
			// result partition is pipelined/blocking.
			final RecordWriter<IntegerRecord> pipelinedWriter =
					new RecordWriter<IntegerRecord>(getEnvironment().getWriter(0));

			final RecordWriter<IntegerRecord> blockingWriter =
					new RecordWriter<IntegerRecord>(getEnvironment().getWriter(1));

			writers.add(pipelinedWriter);
			writers.add(blockingWriter);

			numberOfTimesToSend = getTaskConfiguration().getInteger(CONFIG_KEY, 0);
		}

		@Override
		public void invoke() throws Exception {
			final IntegerRecord subtaskIndex = new IntegerRecord(
					getEnvironment().getIndexInSubtaskGroup());

			// Produce the first intermediate result and then the second in a serial fashion.
			for (RecordWriter<IntegerRecord> writer : writers) {
				try {
					for (int i = 0; i < numberOfTimesToSend; i++) {
						writer.emit(subtaskIndex);
					}
					writer.flush();
				}
				finally {
					writer.clearBuffers();
				}
			}
		}
	}
}
