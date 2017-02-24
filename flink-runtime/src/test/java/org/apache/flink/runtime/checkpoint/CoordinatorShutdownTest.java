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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CoordinatorShutdownTest {
	
	@Test
	public void testCoordinatorShutsDownOnFailure() {
		LocalFlinkMiniCluster cluster = null;
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
			cluster = new LocalFlinkMiniCluster(config, true);
			cluster.start();
			
			// build a test graph with snapshotting enabled
			JobVertex vertex = new JobVertex("Test Vertex");
			vertex.setInvokableClass(FailingBlockingInvokable.class);
			List<JobVertexID> vertexIdList = Collections.singletonList(vertex.getID());
			
			JobGraph testGraph = new JobGraph("test job", vertex);
			testGraph.setSnapshotSettings(new JobSnapshottingSettings(vertexIdList, vertexIdList, vertexIdList, 
					5000, 60000, 0L, Integer.MAX_VALUE, ExternalizedCheckpointSettings.none(), null, true));
			
			ActorGateway jmGateway = cluster.getLeaderGateway(TestingUtils.TESTING_DURATION());

			FiniteDuration timeout = new FiniteDuration(60, TimeUnit.SECONDS);
			JobManagerMessages.SubmitJob submitMessage = new JobManagerMessages.SubmitJob(
					testGraph,
					ListeningBehaviour.EXECUTION_RESULT);
			
			// submit is successful, but then the job blocks due to the invokable
			Future<Object> submitFuture = jmGateway.ask(submitMessage, timeout);
			Await.result(submitFuture, timeout);

			// get the execution graph and store the ExecutionGraph reference
			Future<Object> jobRequestFuture = jmGateway.ask(
					new JobManagerMessages.RequestJob(testGraph.getJobID()),
					timeout);
			
			ExecutionGraph graph = (ExecutionGraph)((JobManagerMessages.JobFound) Await.result(jobRequestFuture, timeout)).executionGraph();
			
			assertNotNull(graph);

			FailingBlockingInvokable.unblock();

			graph.waitUntilFinished();
			
			// verify that the coordinator was shut down
			CheckpointCoordinator coord = graph.getCheckpointCoordinator();
			assertTrue(coord == null || coord.isShutdown());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (cluster != null) {
				cluster.shutdown();
				cluster.awaitTermination();
			}
		}
	}

	@Test
	public void testCoordinatorShutsDownOnSuccess() {
		LocalFlinkMiniCluster cluster = null;
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
			cluster = new LocalFlinkMiniCluster(config, true);
			cluster.start();
			
			// build a test graph with snapshotting enabled
			JobVertex vertex = new JobVertex("Test Vertex");
			vertex.setInvokableClass(BlockingInvokable.class);
			List<JobVertexID> vertexIdList = Collections.singletonList(vertex.getID());

			JobGraph testGraph = new JobGraph("test job", vertex);
			testGraph.setSnapshotSettings(new JobSnapshottingSettings(vertexIdList, vertexIdList, vertexIdList,
					5000, 60000, 0L, Integer.MAX_VALUE, ExternalizedCheckpointSettings.none(), null, true));
			
			ActorGateway jmGateway = cluster.getLeaderGateway(TestingUtils.TESTING_DURATION());

			FiniteDuration timeout = new FiniteDuration(60, TimeUnit.SECONDS);
			JobManagerMessages.SubmitJob submitMessage = new JobManagerMessages.SubmitJob(
					testGraph,
					ListeningBehaviour.EXECUTION_RESULT);

			// submit is successful, but then the job blocks due to the invokable
			Future<Object> submitFuture = jmGateway.ask(submitMessage, timeout);
			Await.result(submitFuture, timeout);

			// get the execution graph and store the ExecutionGraph reference
			Future<Object> jobRequestFuture = jmGateway.ask(
					new JobManagerMessages.RequestJob(testGraph.getJobID()),
					timeout);

			ExecutionGraph graph = (ExecutionGraph)((JobManagerMessages.JobFound) Await.result(jobRequestFuture, timeout)).executionGraph();

			assertNotNull(graph);

			BlockingInvokable.unblock();
			
			graph.waitUntilFinished();

			// verify that the coordinator was shut down
			CheckpointCoordinator coord = graph.getCheckpointCoordinator();
			assertTrue(coord == null || coord.isShutdown());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (cluster != null) {
				cluster.shutdown();
				cluster.awaitTermination();
			}
		}
	}

	public static class BlockingInvokable extends AbstractInvokable {
		private static boolean blocking = true;
		private static final Object lock = new Object();

		@Override
		public void invoke() throws Exception {
			while (blocking) {
				synchronized (lock) {
					lock.wait();
				}
			}
		}

		public static void unblock() {
			blocking = false;

			synchronized (lock) {
				lock.notifyAll();
			}
		}
	}

	public static class FailingBlockingInvokable extends AbstractInvokable {
		private static boolean blocking = true;
		private static final Object lock = new Object();

		@Override
		public void invoke() throws Exception {
			while (blocking) {
				synchronized (lock) {
					lock.wait();
				}
			}
			throw new RuntimeException("This exception is expected.");
		}

		public static void unblock() {
			blocking = false;

			synchronized (lock) {
				lock.notifyAll();
			}
		}
	}
}
