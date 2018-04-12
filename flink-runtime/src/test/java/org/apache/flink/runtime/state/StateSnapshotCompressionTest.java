/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.RunnableFuture;

import static org.mockito.Mockito.mock;

public class StateSnapshotCompressionTest extends TestLogger {

	@Test
	public void testCompressionConfiguration() {

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setUseSnapshotCompression(true);

		AbstractKeyedStateBackend<String> stateBackend = new HeapKeyedStateBackend<>(
			mock(TaskKvStateRegistry.class),
			StringSerializer.INSTANCE,
			StateSnapshotCompressionTest.class.getClassLoader(),
			16,
			new KeyGroupRange(0, 15),
			true,
			executionConfig,
			TestLocalRecoveryConfig.disabled());

		try {
			Assert.assertTrue(
				SnappyStreamCompressionDecorator.INSTANCE.equals(stateBackend.getKeyGroupCompressionDecorator()));

		} finally {
			IOUtils.closeQuietly(stateBackend);
			stateBackend.dispose();
		}

		executionConfig = new ExecutionConfig();
		executionConfig.setUseSnapshotCompression(false);

		stateBackend = new HeapKeyedStateBackend<>(
			mock(TaskKvStateRegistry.class),
			StringSerializer.INSTANCE,
			StateSnapshotCompressionTest.class.getClassLoader(),
			16,
			new KeyGroupRange(0, 15),
			true,
			executionConfig,
			TestLocalRecoveryConfig.disabled());

		try {
			Assert.assertTrue(
				UncompressedStreamCompressionDecorator.INSTANCE.equals(stateBackend.getKeyGroupCompressionDecorator()));

		} finally {
			IOUtils.closeQuietly(stateBackend);
			stateBackend.dispose();
		}
	}

	@Test
	public void snapshotRestoreRoundtripWithCompression() throws Exception {
		snapshotRestoreRoundtrip(true);
	}

	@Test
	public void snapshotRestoreRoundtripUncompressed() throws Exception {
		snapshotRestoreRoundtrip(false);
	}

	private void snapshotRestoreRoundtrip(boolean useCompression) throws Exception {

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setUseSnapshotCompression(useCompression);

		KeyedStateHandle stateHandle = null;

		ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("test", String.class);
		stateDescriptor.initializeSerializerUnlessSet(executionConfig);

		AbstractKeyedStateBackend<String> stateBackend = new HeapKeyedStateBackend<>(
			mock(TaskKvStateRegistry.class),
			StringSerializer.INSTANCE,
			StateSnapshotCompressionTest.class.getClassLoader(),
			16,
			new KeyGroupRange(0, 15),
			true,
			executionConfig,
			TestLocalRecoveryConfig.disabled());

		try {

			InternalValueState<String, VoidNamespace, String> state =
				stateBackend.createValueState(
					new VoidNamespaceSerializer(),
					stateDescriptor);

			stateBackend.setCurrentKey("A");
			state.setCurrentNamespace(VoidNamespace.INSTANCE);
			state.update("42");
			stateBackend.setCurrentKey("B");
			state.setCurrentNamespace(VoidNamespace.INSTANCE);
			state.update("43");
			stateBackend.setCurrentKey("C");
			state.setCurrentNamespace(VoidNamespace.INSTANCE);
			state.update("44");
			stateBackend.setCurrentKey("D");
			state.setCurrentNamespace(VoidNamespace.INSTANCE);
			state.update("45");
			CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(4 * 1024 * 1024);
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				stateBackend.snapshot(0L, 0L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			snapshot.run();
			SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
			stateHandle = snapshotResult.getJobManagerOwnedSnapshot();

		} finally {
			IOUtils.closeQuietly(stateBackend);
			stateBackend.dispose();
		}

		executionConfig = new ExecutionConfig();

		stateBackend = new HeapKeyedStateBackend<>(
			mock(TaskKvStateRegistry.class),
			StringSerializer.INSTANCE,
			StateSnapshotCompressionTest.class.getClassLoader(),
			16,
			new KeyGroupRange(0, 15),
			true,
			executionConfig,
			TestLocalRecoveryConfig.disabled());
		try {

			stateBackend.restore(StateObjectCollection.singleton(stateHandle));

			InternalValueState<String, VoidNamespace, String> state = stateBackend.createValueState(
				new VoidNamespaceSerializer(),
				stateDescriptor);

			stateBackend.setCurrentKey("A");
			state.setCurrentNamespace(VoidNamespace.INSTANCE);
			Assert.assertEquals("42", state.value());
			stateBackend.setCurrentKey("B");
			state.setCurrentNamespace(VoidNamespace.INSTANCE);
			Assert.assertEquals("43", state.value());
			stateBackend.setCurrentKey("C");
			state.setCurrentNamespace(VoidNamespace.INSTANCE);
			Assert.assertEquals("44", state.value());
			stateBackend.setCurrentKey("D");
			state.setCurrentNamespace(VoidNamespace.INSTANCE);
			Assert.assertEquals("45", state.value());

		} finally {
			IOUtils.closeQuietly(stateBackend);
			stateBackend.dispose();
		}
	}
}
