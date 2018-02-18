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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test validates that the checkpoint settings serialize correctly
 * in the presence of user-defined objects.
 */
public class CheckpointSettingsSerializableTest extends TestLogger {

	@Test
	public void testDeserializationOfUserCodeWithUserClassLoader() throws Exception {
		final ClassLoader classLoader = new URLClassLoader(new URL[0], getClass().getClassLoader());
		final Serializable outOfClassPath = CommonTestUtils.createObjectForClassNotInClassPath(classLoader);

		final MasterTriggerRestoreHook.Factory[] hooks = {
				new TestFactory(outOfClassPath) };
		final SerializedValue<MasterTriggerRestoreHook.Factory[]> serHooks = new SerializedValue<>(hooks);

		final JobCheckpointingSettings checkpointingSettings = new JobCheckpointingSettings(
				Collections.<JobVertexID>emptyList(),
				Collections.<JobVertexID>emptyList(),
				Collections.<JobVertexID>emptyList(),
				new CheckpointCoordinatorConfiguration(
					1000L,
					10000L,
					0L,
					1,
					CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
					true),
				new SerializedValue<StateBackend>(new CustomStateBackend(outOfClassPath)),
				serHooks);

		final JobGraph jobGraph = new JobGraph(new JobID(), "test job");
		jobGraph.setSnapshotSettings(checkpointingSettings);

		// to serialize/deserialize the job graph to see if the behavior is correct under
		// distributed execution
		final JobGraph copy = CommonTestUtils.createCopySerializable(jobGraph);

		final Time timeout = Time.seconds(10L);
		final ExecutionGraph eg = ExecutionGraphBuilder.buildGraph(
			null,
			copy,
			new Configuration(),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			mock(SlotProvider.class),
			classLoader,
			new StandaloneCheckpointRecoveryFactory(),
			timeout,
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			10,
			VoidBlobWriter.getInstance(),
			timeout,
			log);

		assertEquals(1, eg.getCheckpointCoordinator().getNumberOfRegisteredMasterHooks());
		assertTrue(jobGraph.getCheckpointingSettings().getDefaultStateBackend().deserializeValue(classLoader) instanceof CustomStateBackend);
	}

	// ------------------------------------------------------------------------

	private static final class TestFactory implements MasterTriggerRestoreHook.Factory {

		private static final long serialVersionUID = -612969579110202607L;
		
		private final Serializable payload;

		TestFactory(Serializable payload) {
			this.payload = payload;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <V> MasterTriggerRestoreHook<V> create() {
			MasterTriggerRestoreHook<V> hook = mock(MasterTriggerRestoreHook.class);
			when(hook.getIdentifier()).thenReturn("id");
			return hook;
		}
	}

	private static final class CustomStateBackend implements StateBackend {

		private static final long serialVersionUID = -6107964383429395816L;
		/**
		 * Simulate a custom option that is not in the normal classpath.
		 */
		@SuppressWarnings("unused")
		private Serializable customOption;

		public CustomStateBackend(Serializable customOption) {
			this.customOption = customOption;
		}

		@Override
		public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
			return mock(CheckpointStorage.class);
		}

		@Override
		public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public OperatorStateBackend createOperatorStateBackend(
			Environment env, String operatorIdentifier) throws Exception {
			throw new UnsupportedOperationException();
		}
	}
}
