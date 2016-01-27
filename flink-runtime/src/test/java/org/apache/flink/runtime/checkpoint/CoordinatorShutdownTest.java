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
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.jobmanager.Tasks;

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

import static org.junit.Assert.*;

public class CoordinatorShutdownTest {
	
	@Test
	public void testCoordinatorShutsDownOnFailure() {
		LocalFlinkMiniCluster cluster = null;
		try {
			Configuration noTaskManagerConfig = new Configuration();
			noTaskManagerConfig.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 0);
			cluster = new LocalFlinkMiniCluster(noTaskManagerConfig, true);
			cluster.start();
			
			// build a test graph with snapshotting enabled
			JobVertex vertex = new JobVertex("Test Vertex");
			vertex.setInvokableClass(Tasks.NoOpInvokable.class);
			List<JobVertexID> vertexIdList = Collections.singletonList(vertex.getID());
			
			JobGraph testGraph = new JobGraph("test job", vertex);
			testGraph.setSnapshotSettings(new JobSnapshottingSettings(vertexIdList, vertexIdList, vertexIdList, 
					5000, 60000, 0L, Integer.MAX_VALUE));
			
			ActorGateway jmGateway = cluster.getLeaderGateway(TestingUtils.TESTING_DURATION());

			FiniteDuration timeout = new FiniteDuration(60, TimeUnit.SECONDS);
			JobManagerMessages.SubmitJob submitMessage = new JobManagerMessages.SubmitJob(
					testGraph,
					ListeningBehaviour.EXECUTION_RESULT);
			
			// submit is successful, but then the job dies because no TaskManager / slot is available
			Future<Object> submitFuture = jmGateway.ask(submitMessage, timeout);
			Await.result(submitFuture, timeout);

			// get the execution graph and make sure the coordinator is properly shut down
			Future<Object> jobRequestFuture = jmGateway.ask(
					new JobManagerMessages.RequestJob(testGraph.getJobID()),
					timeout);
			
			ExecutionGraph graph = ((JobManagerMessages.JobFound) Await.result(jobRequestFuture, timeout)).executionGraph();
			
			assertNotNull(graph);
			graph.waitUntilFinished();
			
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
			cluster = new LocalFlinkMiniCluster(new Configuration(), true);
			cluster.start();
			
			// build a test graph with snapshotting enabled
			JobVertex vertex = new JobVertex("Test Vertex");
			vertex.setInvokableClass(Tasks.NoOpInvokable.class);
			List<JobVertexID> vertexIdList = Collections.singletonList(vertex.getID());

			JobGraph testGraph = new JobGraph("test job", vertex);
			testGraph.setSnapshotSettings(new JobSnapshottingSettings(vertexIdList, vertexIdList, vertexIdList,
					5000, 60000, 0L, Integer.MAX_VALUE));
			
			ActorGateway jmGateway = cluster.getLeaderGateway(TestingUtils.TESTING_DURATION());

			FiniteDuration timeout = new FiniteDuration(60, TimeUnit.SECONDS);
			JobManagerMessages.SubmitJob submitMessage = new JobManagerMessages.SubmitJob(
					testGraph,
					ListeningBehaviour.EXECUTION_RESULT);

			// submit is successful, but then the job dies because no TaskManager / slot is available
			Future<Object> submitFuture = jmGateway.ask(submitMessage, timeout);
			Await.result(submitFuture, timeout);

			// get the execution graph and make sure the coordinator is properly shut down
			Future<Object> jobRequestFuture = jmGateway.ask(
					new JobManagerMessages.RequestJob(testGraph.getJobID()),
					timeout);

			ExecutionGraph graph = ((JobManagerMessages.JobFound) Await.result(jobRequestFuture, timeout)).executionGraph();

			assertNotNull(graph);
			graph.waitUntilFinished();

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
}
