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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.compression.LZ4StreamCompressionDecorator;
import org.apache.flink.runtime.state.compression.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.compression.StreamCompressionDecorator;
import org.apache.flink.runtime.state.compression.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class StateSnapshotCompressionTest extends TestLogger {

	@Test
	public void testCompressionConfiguration() throws BackendBuildingException {

		// deprecated configure way, setUseSnapshotCompression as true
		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setUseSnapshotCompression(true);

		AbstractKeyedStateBackend<String> stateBackend = getStringHeapKeyedStateBackend(executionConfig, null);

		try {
			assertEquals(SnappyStreamCompressionDecorator.INSTANCE, stateBackend.getKeyGroupCompressionDecorator());

		} finally {
			IOUtils.closeQuietly(stateBackend);
			stateBackend.dispose();
		}

		// deprecated configure way, setUseSnapshotCompression as false
		executionConfig = new ExecutionConfig();
		executionConfig.setUseSnapshotCompression(false);

		stateBackend = getStringHeapKeyedStateBackend(executionConfig, null);

		try {
			assertEquals(UncompressedStreamCompressionDecorator.INSTANCE, stateBackend.getKeyGroupCompressionDecorator());

		} finally {
			IOUtils.closeQuietly(stateBackend);
			stateBackend.dispose();
		}

		// new configure way, provide explicit stream compression decorator and ignore the executionConfig.
		for (StreamCompressionDecorator streamCompressionDecorator : Arrays.asList(
				LZ4StreamCompressionDecorator.INSTANCE,
				SnappyStreamCompressionDecorator.INSTANCE,
				UncompressedStreamCompressionDecorator.INSTANCE)) {

			stateBackend = getStringHeapKeyedStateBackend(executionConfig, streamCompressionDecorator);

			try {
				assertEquals(streamCompressionDecorator, stateBackend.getKeyGroupCompressionDecorator());

			} finally {
				IOUtils.closeQuietly(stateBackend);
				stateBackend.dispose();
			}
		}
	}

	@Test
	public void testSnapshotRestoreRoundTrip() throws Exception {
		List<StreamCompressionDecorator> compressionDecorators = Arrays.asList(
			LZ4StreamCompressionDecorator.INSTANCE,
			SnappyStreamCompressionDecorator.INSTANCE,
			UncompressedStreamCompressionDecorator.INSTANCE);

		for (StreamCompressionDecorator oldCompressionDecorator : compressionDecorators) {

			ExecutionConfig executionConfig = new ExecutionConfig();

			KeyedStateHandle stateHandle = null;

			ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("test", String.class);
			stateDescriptor.initializeSerializerUnlessSet(executionConfig);

			AbstractKeyedStateBackend<String> stateBackend = getStringHeapKeyedStateBackend(executionConfig, oldCompressionDecorator);

			try {

				InternalValueState<String, VoidNamespace, String> state =
					stateBackend.createInternalState(new VoidNamespaceSerializer(), stateDescriptor);

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

			for (StreamCompressionDecorator newCompressionDecorator : compressionDecorators) {

				stateBackend = getStringHeapKeyedStateBackend(
					executionConfig, newCompressionDecorator, StateObjectCollection.singleton(stateHandle));
				try {
					InternalValueState<String, VoidNamespace, String> state = stateBackend.createInternalState(
						new VoidNamespaceSerializer(),
						stateDescriptor);

					stateBackend.setCurrentKey("A");
					state.setCurrentNamespace(VoidNamespace.INSTANCE);
					assertEquals("42", state.value());
					stateBackend.setCurrentKey("B");
					state.setCurrentNamespace(VoidNamespace.INSTANCE);
					assertEquals("43", state.value());
					stateBackend.setCurrentKey("C");
					state.setCurrentNamespace(VoidNamespace.INSTANCE);
					assertEquals("44", state.value());
					stateBackend.setCurrentKey("D");
					state.setCurrentNamespace(VoidNamespace.INSTANCE);
					assertEquals("45", state.value());

				} finally {
					IOUtils.closeQuietly(stateBackend);
					stateBackend.dispose();
				}
			}
		}
	}

	private AbstractKeyedStateBackend<String> getStringHeapKeyedStateBackend(
		ExecutionConfig executionConfig,
		StreamCompressionDecorator streamCompressionDecorator) throws BackendBuildingException {

		return getStringHeapKeyedStateBackend(executionConfig, streamCompressionDecorator, Collections.emptyList());
	}

	private AbstractKeyedStateBackend<String> getStringHeapKeyedStateBackend(
		ExecutionConfig executionConfig,
		StreamCompressionDecorator streamCompressionDecorator,
		Collection<KeyedStateHandle> stateHandles) throws BackendBuildingException {
		MemoryStateBackend memoryStateBackend = new MemoryStateBackend();
		if (streamCompressionDecorator != null) {
			memoryStateBackend.setStreamCompressionDecorator(streamCompressionDecorator);
		}
		return memoryStateBackend.createKeyedStateBackend(
			new DummyEnvironment("Test", 1, 0, 16, executionConfig),
			new JobID(),
			"testOperator",
			StringSerializer.INSTANCE,
			16,
			new KeyGroupRange(0, 15),
			mock(TaskKvStateRegistry.class),
			TtlTimeProvider.DEFAULT,
			new UnregisteredMetricsGroup(),
			stateHandles,
			new CloseableRegistry());
	}

}
