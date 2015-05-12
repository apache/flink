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

package org.apache.flink.test.resume;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.testkit.JavaTestKit;
import akka.util.Timeout;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.messages.JobManagerMessages.*;
import org.apache.flink.runtime.messages.TaskMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.*;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.types.IntValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import akka.actor.Status.Success;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;


import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ResumeITCase {

	private static int PARALLELISM = 10;
	ActorSystem system;

	@Before
	public void setup(){
		system = ActorSystem.create("TestingActorSystem", TestingUtils.testConfig());
		TestingUtils.setCallingThreadDispatcher(system);
	}

	@After
	public void teardown(){
		TestingUtils.setGlobalExecutionContext();
		JavaTestKit.shutdownActorSystem(system);
	}

	@Test
	public void testResumeFromIntermediateResults() {

		final JobID jobId = new JobID();
		final String jobName = "Resume from intermediate result";
		final Configuration cfg = new Configuration();

		/* Simple test to resume from an intermediate result cached as a ResultPartition at a task manager

					O receiver          receiver O  O sink
					^                            | /
					|                            |/
					■ intermediate result        ■ intermediate result
					^                            |
					|                            |
					O source                     O source

         */

		AbstractJobVertex source = new AbstractJobVertex("source");
		source.setInvokableClass(Sender.class);

		final IntermediateDataSet intermediateResult = source.createAndAddResultDataSet(ResultPartitionType.BLOCKING);

		final AbstractJobVertex receiver = new AbstractJobVertex("receiver");
		receiver.setInvokableClass(Receiver.class);
		receiver.connectDataSetAsInput(intermediateResult, DistributionPattern.ALL_TO_ALL);

		// first job graph
		final JobGraph jobGraph1 = new JobGraph(jobId, jobName, source, receiver);
		jobGraph1.setScheduleMode(ScheduleMode.BACKTRACKING);
		jobGraph1.setParallelism(PARALLELISM);

		final AbstractJobVertex sink = new AbstractJobVertex("sink");
		sink.setInvokableClass(Receiver.class);
		sink.setResumeFromIntermediateResult();

		sink.connectDataSetAsInput(intermediateResult, DistributionPattern.ALL_TO_ALL);

		// second job graph
		final JobGraph jobGraph2 = new JobGraph(jobId, jobName, sink);
		jobGraph2.setScheduleMode(ScheduleMode.BACKTRACKING);
		jobGraph2.setParallelism(PARALLELISM);

		ForkableFlinkMiniCluster cluster = ForkableFlinkMiniCluster.startCluster(2 * PARALLELISM, 1, TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT());

		final ActorRef jobManager = cluster.getJobManager();
		final ActorRef taskManager = cluster.getTaskManagersAsJava().get(0);


		new JavaTestKit(system) {{
			new Within(TestingUtils.TESTING_DURATION()) {
				@Override
				protected void run() {
					taskManager.tell(new TestingTaskManagerMessages.NotifyWhenTaskSubmitted(jobId), getRef());

					jobManager.tell(new SubmitJob(jobGraph1, false), getRef());
					expectMsgClass(Success.class);

					// TODO check caching of intermediate result
					//expectMsgEquals(new TestingTaskManagerMessages.ResultPartitionCached(intermediateResult.getId()));

					for (int i = 0; i < PARALLELISM * 2; i++) {
						expectMsgClass(TaskMessages.SubmitTask.class);
					}

					expectMsgClass(JobResultSuccess.class);

					// request execution graph
					FiniteDuration t = new FiniteDuration(5, TimeUnit.SECONDS);
					Future<Object> future = Patterns.ask(jobManager, new RequestExecutionGraph(jobId), new Timeout(t));
					ExecutionGraph executionGraph = null;
					try {
						executionGraph = ((ExecutionGraphFound)Await.result(future, t)).executionGraph();
					} catch (Exception e) {
						e.printStackTrace();
						fail("Failed to get ExecutionGraph of first job.");
					}

					// list of intermediate result partitions that should be reused
					Set<IntermediateResultPartitionID> resultsToBeAcknowledged = new HashSet<IntermediateResultPartitionID>();
					for (IntermediateResult result : executionGraph.getAllIntermediateResults().values()) {
						if(result.getId().equals(intermediateResult.getId())) {
							for (IntermediateResultPartition partition : result.getPartitions()) {
								IntermediateResultPartitionID partitionId = partition.getPartitionId();
								resultsToBeAcknowledged.add(partitionId);
								// call back for partition changes
								taskManager.tell(new TestingTaskManagerMessages.NotifyWhenResultPartitionChanges(partitionId), getRef());
							}
						}
					}

					// submit second job and register for execution graph changes
					jobManager.tell(new SubmitJob(jobGraph2, false), getRef());
					expectMsgClass(Success.class);


					for (Object msg : receiveN(PARALLELISM * 2)) {

						if (msg instanceof TestingTaskManagerMessages.ResultPartitionLocked) {
							assertTrue("Locked ResultPartitions should be correct.",
									resultsToBeAcknowledged.contains(((TestingTaskManagerMessages.ResultPartitionLocked) msg).partitionID()));
						} else if (msg instanceof TaskMessages.SubmitTask) {
							assertTrue("Scheduled vertices should be correct.",
									((TaskMessages.SubmitTask) msg).tasks().getVertexID().equals(sink.getID()));
						} else {
							fail("Unknown message " + msg);
						}

					}

					expectMsgClass(JobResultSuccess.class);

				}
			};
		}};

	}

	public static class Sender extends AbstractInvokable {

		private RecordWriter<IntValue> writer;

		@Override
		public void registerInputOutput() {
			writer = new RecordWriter<IntValue>(getEnvironment().getWriter(0));
		}

		@Override
		public void invoke() throws Exception {

			try {
				for (int i=0; i < PARALLELISM; i++) {
					writer.emit(new IntValue(23));
				}
				writer.flush();
			}
			finally {
				writer.clearBuffers();
			}
		}
	}

	public static class Receiver extends AbstractInvokable {

		private RecordReader<IntValue> reader;

		@Override
		public void registerInputOutput() {
			reader = new RecordReader<IntValue>(
					getEnvironment().getInputGate(0),
					IntValue.class);
		}

		@Override
		public void invoke() throws Exception {
			try {
				IntValue record;
				int numValues = 0;

				while ((record = reader.next()) != null) {
					record.getValue();
					numValues++;
				}

				assertTrue("There should be no more values available.", numValues == PARALLELISM);
			}
			finally {
				reader.clearBuffers();
			}
		}
	}

}
