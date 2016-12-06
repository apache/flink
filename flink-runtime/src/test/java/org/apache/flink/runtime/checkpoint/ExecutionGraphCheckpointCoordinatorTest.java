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

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.checkpoint.stats.DisabledCheckpointStatsTracker;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.SerializedValue;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.Matchers;
import scala.concurrent.duration.FiniteDuration;

import java.net.URL;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ExecutionGraphCheckpointCoordinatorTest {

	private static ActorSystem system = AkkaUtils.createLocalActorSystem(new Configuration());

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	/**
	 * Tests that a shut down checkpoint coordinator calls shutdown on
	 * the store and counter.
	 */
	@Test
	public void testShutdownCheckpointCoordinator() throws Exception {
		CheckpointIDCounter counter = mock(CheckpointIDCounter.class);
		CompletedCheckpointStore store = mock(CompletedCheckpointStore.class);

		ExecutionGraph graph = createExecutionGraphAndEnableCheckpointing(counter, store);
		graph.fail(new Exception("Test Exception"));

		verify(counter, times(1)).shutdown(JobStatus.FAILED);
		verify(store, times(1)).shutdown(JobStatus.FAILED);
	}

	/**
	 * Tests that a suspended checkpoint coordinator calls suspend on
	 * the store and counter.
	 */
	@Test
	public void testSuspendCheckpointCoordinator() throws Exception {
		CheckpointIDCounter counter = mock(CheckpointIDCounter.class);
		CompletedCheckpointStore store = mock(CompletedCheckpointStore.class);

		ExecutionGraph graph = createExecutionGraphAndEnableCheckpointing(counter, store);
		graph.suspend(new Exception("Test Exception"));

		// No shutdown
		verify(counter, times(1)).shutdown(Matchers.eq(JobStatus.SUSPENDED));
		verify(store, times(1)).shutdown(Matchers.eq(JobStatus.SUSPENDED));
	}

	private ExecutionGraph createExecutionGraphAndEnableCheckpointing(
			CheckpointIDCounter counter,
			CompletedCheckpointStore store) throws Exception {
		ExecutionGraph executionGraph = new ExecutionGraph(
			TestingUtils.defaultExecutionContext(),
			TestingUtils.defaultExecutionContext(),
			new JobID(),
			"test",
			new Configuration(),
			new SerializedValue<>(new ExecutionConfig()),
			Time.days(1L),
			new NoRestartStrategy(),
			Collections.<BlobKey>emptyList(),
			Collections.<URL>emptyList(),
			ClassLoader.getSystemClassLoader(),
			new UnregisteredMetricsGroup());

		executionGraph.enableSnapshotCheckpointing(
				100,
				100,
				100,
				1,
				ExternalizedCheckpointSettings.none(),
				Collections.<ExecutionJobVertex>emptyList(),
				Collections.<ExecutionJobVertex>emptyList(),
				Collections.<ExecutionJobVertex>emptyList(),
				counter,
				store,
				null,
				new DisabledCheckpointStatsTracker());

		JobVertex jobVertex = new JobVertex("MockVertex");
		jobVertex.setInvokableClass(AbstractInvokable.class);
		executionGraph.attachJobGraph(Collections.singletonList(jobVertex));

		return executionGraph;
	}
}
