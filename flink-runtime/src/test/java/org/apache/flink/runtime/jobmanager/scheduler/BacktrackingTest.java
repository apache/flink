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
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.testkit.JavaTestKit;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.InstanceDiedException;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputFormatVertex;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OutputFormatVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.messages.TaskMessages;
import org.apache.flink.runtime.messages.TaskMessages.*;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BacktrackingTest {

	public static ActorSystem system;

	/**
	 * TaskManager stub which acknowledges/denies the availability of IntermediateResults
	 */
	public static class TestTaskManager extends UntypedActor {

		ActorRef testEnvironment;
		private Set<IntermediateResultPartitionID> availableResults = new HashSet<IntermediateResultPartitionID>();

		public TestTaskManager(ActorRef testEnvironment) {
			this.testEnvironment = testEnvironment;
		}

		@Override
		public void onReceive(Object msg) throws Exception {

			if (msg instanceof TaskMessages.SubmitTask) {
				//System.out.println("task submitted: " + msg);
				testEnvironment.forward(msg, getContext());

			} else if (msg instanceof IntermediateResultPartitionID) {
				// collect all intermediate results ids sent by the testing system
				availableResults.add((IntermediateResultPartitionID) msg);

			} else if (msg instanceof LockResultPartition) {
				LockResultPartition unpacked = ((LockResultPartition) msg);
				IntermediateResultPartitionID partitionID = unpacked.partitionID();
				System.out.println("intermediate partition requested: " + partitionID);
				if (availableResults.contains(partitionID)) {
					System.out.println("acknowledging result: " + partitionID);
					getSender().tell(new LockResultPartitionReply(true), getSelf());
				} else {
					System.out.println("denying result: " + partitionID);
					getSender().tell(new LockResultPartitionReply(false), getSelf());
				}
				testEnvironment.forward(msg, getContext());

			} else {
				System.out.println("Unknown msg " + msg);
			}
		}

	}

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

	private AbstractJobVertex createNode(String name) {
		AbstractJobVertex abstractJobVertex = new AbstractJobVertex(name);
		abstractJobVertex.setInvokableClass(Tasks.NoOpInvokable.class);
		return abstractJobVertex;
	}

	private AbstractJobVertex createOutputNode(String name) {
		AbstractJobVertex abstractJobVertex = new OutputFormatVertex(name);
		abstractJobVertex.setInvokableClass(Tasks.NoOpInvokable.class);
		return abstractJobVertex;
	}

	private AbstractJobVertex createInputNode(String name) {
		AbstractJobVertex abstractJobVertex = new InputFormatVertex(name);
		abstractJobVertex.setInvokableClass(Tasks.NoOpInvokable.class);
		return abstractJobVertex;
	}

	@Test
	public void testBacktrackingIntermediateResults() throws InstanceDiedException, NoResourceAvailableException {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";
		final Configuration cfg = new Configuration();

		// JobVertex which has intermediate results available
		final AbstractJobVertex resumePoint;

        /*
               sink1         sink2
                 O             O
                 ^             ^
                ´ `           ´ `
               ´   `         ´   `
             _´     `_     _´     `_
             O       O     O       O    <---- resume starts here
             ^       ^     ^       ^
              `     ´       `     ´
               `   ´         `   ´
                `_´           `_´
                 O             O    <----- result available
                 ^             ^
                  `           ´
                   `         ´
                    `       ´
                     `     ´
                      `   ´
                       `_´
                        O
                     source
        */

		// topologically sorted list
		final List<AbstractJobVertex> list = new ArrayList<AbstractJobVertex>();

		final AbstractJobVertex source = createInputNode("source1");
		list.add(source);

		AbstractJobVertex node1 = createOutputNode("sink1");
		{
			AbstractJobVertex child1 = createNode("sink1-child1");
			AbstractJobVertex child2 = createNode("sink1-child2");
			node1.connectNewDataSetAsInput(child1, DistributionPattern.ALL_TO_ALL);
			node1.connectNewDataSetAsInput(child2, DistributionPattern.ALL_TO_ALL);

			AbstractJobVertex child1child2child = createNode("sink1-child1-child2-child");
			child1.connectNewDataSetAsInput(child1child2child, DistributionPattern.ALL_TO_ALL);
			child2.connectNewDataSetAsInput(child1child2child, DistributionPattern.ALL_TO_ALL);

			child1child2child.connectNewDataSetAsInput(source, DistributionPattern.ALL_TO_ALL);

			list.add(child1child2child);
			list.add(child1);
			list.add(child2);
		}

		AbstractJobVertex node2 = createOutputNode("sink2");
		final AbstractJobVertex child1 = createNode("sink2-child1");
		final AbstractJobVertex child2 = createNode("sink2-child2");
		node2.connectNewDataSetAsInput(child1, DistributionPattern.ALL_TO_ALL);
		node2.connectNewDataSetAsInput(child2, DistributionPattern.ALL_TO_ALL);

		// resume from this node
		AbstractJobVertex child1child2child = createNode("sink1-child1-child2-child");
		resumePoint = child1child2child;

		child1.connectNewDataSetAsInput(child1child2child, DistributionPattern.ALL_TO_ALL);
		child2.connectNewDataSetAsInput(child1child2child, DistributionPattern.ALL_TO_ALL);

		child1child2child.connectNewDataSetAsInput(source, DistributionPattern.ALL_TO_ALL);

		list.add(child1child2child);
		list.add(child1);
		list.add(child2);

		list.add(node1);
		list.add(node2);

		final ExecutionGraph eg = new ExecutionGraph(jobId, jobName, cfg, AkkaUtils.getDefaultTimeout());

		new JavaTestKit(system) {
			{

				final Props props = Props.create(TestTaskManager.class, getRef());
				final ActorRef taskManagerActor = system.actorOf(props);

				eg.setScheduleMode(ScheduleMode.BACKTRACKING);

				try {
					eg.attachJobGraph(list);
				} catch (JobException e) {
					e.printStackTrace();
					fail("Job failed with exception: " + e.getMessage());
				}

				// list if of partition that should be requested
				final Set<IntermediateResultPartitionID> requestedPartitions = new HashSet<IntermediateResultPartitionID>();

				for (ExecutionJobVertex ejv : eg.getAllVertices().values()) {
					for (IntermediateResult result : ejv.getInputs()) {
						for (IntermediateResultPartition partition : result.getPartitions()) {
							if (result.getProducer().getJobVertex() == resumePoint) {
								// mock an instance
								Instance mockInstance = Mockito.mock(Instance.class);
								Mockito.when(mockInstance.isAlive()).thenReturn(true);
								Mockito.when(mockInstance.getTaskManager()).thenReturn(taskManagerActor);
								InstanceConnectionInfo instanceConnectionInfo = new InstanceConnectionInfo();
								Mockito.when(mockInstance.getInstanceConnectionInfo()).thenReturn(instanceConnectionInfo);
								// set the mock as a location
								partition.getProducer().getCurrentExecutionAttempt().setResultPartitionLocation(mockInstance);
								// send the tests actor ref to the TestTaskManager to receive messages
								taskManagerActor.tell(partition.getPartitionId(), getRef());
								requestedPartitions.add(partition.getPartitionId());
							}
						}
					}
				}

				final Set<JobVertexID> schedulePoints = new HashSet<JobVertexID>();
				// list of execution vertices to be scheduled
				schedulePoints.add(child1.getID());
				schedulePoints.add(child2.getID());
				schedulePoints.add(source.getID());

				final Scheduler scheduler = new Scheduler();//Mockito.mock(Scheduler.class);

				Instance i1 = null;
				try {
					i1 = ExecutionGraphTestUtils.getInstance(taskManagerActor, 10);
				} catch (Exception e) {
					e.printStackTrace();
					fail("Couldn't get instance: " + e.getMessage());
				}

				scheduler.newInstanceAvailable(i1);

				try {
					eg.scheduleForExecution(scheduler);
				} catch (JobException e) {
					e.printStackTrace();
					fail();
				}

				new Within(duration("5 seconds")) {
					@Override
					protected void run() {
						Object[] messages = receiveN(schedulePoints.size() + requestedPartitions.size());
						for (Object msg : messages) {
							if (msg instanceof LockResultPartition) {
								LockResultPartition msg1 = (LockResultPartition) msg;
								assertTrue("Available partition should have been requested.",
										requestedPartitions.contains(msg1.partitionID()));
							} else if (msg instanceof SubmitTask) {
								SubmitTask msg1 = (SubmitTask) msg;
								assertTrue("These nodes should have been scheduled",
										schedulePoints.contains(msg1.tasks().getVertexID()));
							} else {
								fail("Unexpected message");
							}
						}
					}
				};

			}
		};
	}

	@Test
	public void testMassiveExecutionGraphExecutionVertexScheduling() {

		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";
		final Configuration cfg = new Configuration();

		final int numSinks = 10;
		final int depth = 50;
		final int parallelism = 100;

		//Random rand = new Random(System.currentTimeMillis());

		LinkedList<AbstractJobVertex> allNodes = new LinkedList<AbstractJobVertex>();

		for (int s = 0; s < numSinks; s++) {
			AbstractJobVertex node = new OutputFormatVertex("sink" + s);
			node.setInvokableClass(Tasks.NoOpInvokable.class);

			//node.setParallelism(rand.nextInt(maxParallelism) + 1);
			node.setParallelism(parallelism);
			allNodes.addLast(node);

			for (int i = 0; i < depth; i++) {
				AbstractJobVertex other = new AbstractJobVertex("vertex" + i + " sink" + s);
				other.setParallelism(parallelism);
				other.setInvokableClass(Tasks.NoOpInvokable.class);
				node.connectNewDataSetAsInput(other, DistributionPattern.ALL_TO_ALL);
				allNodes.addFirst(other);
				node = other;
			}

		}

		final ExecutionGraph eg = new ExecutionGraph(jobId, jobName, cfg, AkkaUtils.getDefaultTimeout());

		eg.setScheduleMode(ScheduleMode.BACKTRACKING);

		try {
			eg.attachJobGraph(allNodes);
		} catch (JobException e) {
			e.printStackTrace();
			fail("Job failed with exception: " + e.getMessage());
		}

		new JavaTestKit(system) {
			{

				final Props props = Props.create(TestTaskManager.class, getRef());
				final ActorRef taskManagerActor = system.actorOf(props);


				Scheduler scheduler = new Scheduler();

				Instance i1 = null;
				try {
					i1 = ExecutionGraphTestUtils.getInstance(taskManagerActor, numSinks * parallelism);
				} catch (Exception e) {
					e.printStackTrace();
				}

				scheduler.newInstanceAvailable(i1);

				try {
					eg.scheduleForExecution(scheduler);
				} catch (JobException e) {
					e.printStackTrace();
					fail("Failed to schedule ExecutionGraph");
				}

				for (int i=0; i < numSinks * parallelism; i++) {
					// all sources should be scheduled
					expectMsgClass(duration("1 second"), TaskMessages.SubmitTask.class);
				}

			}
		};
	}
}
