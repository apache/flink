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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CoordinatorShutdownTest;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StateUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ExecutionGraphBuilderTest {

	private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraphBuilderTest.class);

	/**
	 * Tests that the state backend checkpoint directory is forwarded to the
	 * checkpoint coordinator.
	 */
	@Test
	public void testExternalizedCheckpointsCheckpointDirectory() throws Exception {
		JobVertex jobVertex = new JobVertex("TestVertex");
		jobVertex.setParallelism(1);
		jobVertex.setInvokableClass(CoordinatorShutdownTest.BlockingInvokable.class);

		Path checkpointDirectory = new Path("expectedCheckpointDirectory");
		AbstractStateBackend backend = new DummyCheckpointDirectoryTestStateBackend(checkpointDirectory);
		StateUtil.serializeStateBackend(jobVertex.getConfiguration(), backend);

		JobGraph jobGraph = new JobGraph(jobVertex);

		List<JobVertexID> trigger = new ArrayList<>();
		trigger.add(jobVertex.getID());

		JobSnapshottingSettings snapshottingSettings = new JobSnapshottingSettings(
				trigger,
				Collections.<JobVertexID>emptyList(),
				Collections.<JobVertexID>emptyList(),
				Long.MAX_VALUE,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.externalizeCheckpoints(true));

		jobGraph.setSnapshotSettings(snapshottingSettings);

		Configuration jobManagerConfig = new Configuration();

		ExecutionGraph eg = ExecutionGraphBuilder.buildGraph(
				null,
				jobGraph,
				jobManagerConfig,
				mock(Executor.class),
				Thread.currentThread().getContextClassLoader(),
				new StandaloneCheckpointRecoveryFactory(),
				Time.seconds(30),
				new NoRestartStrategy(),
				mock(MetricGroup.class),
				1,
				LOG);

		Path actual = eg.getCheckpointCoordinator().getCheckpointDirectory();
		assertEquals(checkpointDirectory, actual);
	}

	/**
	 * Tests that if no checkpoint directory is configured, but externalized
	 * checkpoints are enabled, the execution graph building fails.
	 */
	@Test
	public void testExternalizedCheckpointsNoCheckpointDirectoryConfigured() throws Exception {
		JobVertex jobVertex = new JobVertex("TestVertex");
		jobVertex.setParallelism(1);
		jobVertex.setInvokableClass(CoordinatorShutdownTest.BlockingInvokable.class);

		Path checkpointDirectory = null;
		AbstractStateBackend backend = new DummyCheckpointDirectoryTestStateBackend(checkpointDirectory);
		StateUtil.serializeStateBackend(jobVertex.getConfiguration(), backend);

		JobGraph jobGraph = new JobGraph(jobVertex);

		List<JobVertexID> trigger = new ArrayList<>();
		trigger.add(jobVertex.getID());

		JobSnapshottingSettings snapshottingSettings = new JobSnapshottingSettings(
				trigger,
				Collections.<JobVertexID>emptyList(),
				Collections.<JobVertexID>emptyList(),
				Long.MAX_VALUE,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.externalizeCheckpoints(true));

		jobGraph.setSnapshotSettings(snapshottingSettings);

		try {
			ExecutionGraphBuilder.buildGraph(
					null,
					jobGraph,
					new Configuration(),
					mock(Executor.class),
					Thread.currentThread().getContextClassLoader(),
					new StandaloneCheckpointRecoveryFactory(),
					Time.seconds(30),
					new NoRestartStrategy(),
					mock(MetricGroup.class),
					1,
					LOG);

			fail("Did not throw expected IllegalStateException because of missing checkpoint directory");
		} catch (IllegalStateException ignored) {
		}
	}

	private static class DummyCheckpointDirectoryTestStateBackend extends AbstractStateBackend {

		private final Path checkpointDirectory;

		public DummyCheckpointDirectoryTestStateBackend(Path checkpointDirectory) {
			this.checkpointDirectory = checkpointDirectory;
		}

		@Override
		public CheckpointStreamFactory createStreamFactory(JobID jobId, String operatorIdentifier) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(Environment env, JobID jobID, String operatorIdentifier, TypeSerializer<K> keySerializer, int numberOfKeyGroups, KeyGroupRange keyGroupRange, TaskKvStateRegistry kvStateRegistry) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public <K> AbstractKeyedStateBackend<K> restoreKeyedStateBackend(Environment env, JobID jobID, String operatorIdentifier, TypeSerializer<K> keySerializer, int numberOfKeyGroups, KeyGroupRange keyGroupRange, Collection<KeyGroupsStateHandle> restoredState, TaskKvStateRegistry kvStateRegistry) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public Path getCheckpointDirectory() {
			return checkpointDirectory;
		}
	}
}
