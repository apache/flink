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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.util.BlockerCheckpointStreamFactory;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;

class StateSnapshotTransformerTest {
	private final AbstractKeyedStateBackend<Integer> backend;
	private final BlockerCheckpointStreamFactory streamFactory;
	private final StateSnapshotTransformFactory<?> snapshotTransformFactory;

	StateSnapshotTransformerTest(
		AbstractKeyedStateBackend<Integer> backend,
		BlockerCheckpointStreamFactory streamFactory) {

		this.backend = backend;
		this.streamFactory = streamFactory;
		this.snapshotTransformFactory = SingleThreadAccessCheckingSnapshotTransformFactory.create();
	}

	void testNonConcurrentSnapshotTransformerAccess() throws Exception {
		List<TestState> testStates = Arrays.asList(
			new TestValueState(),
			new TestListState(),
			new TestMapState()
		);

		for (TestState state : testStates) {
			for (int i = 0; i < 100; i++) {
				backend.setCurrentKey(i);
				state.setToRandomValue();
			}

			CheckpointOptions checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation();

			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot1 =
				backend.snapshot(1L, 0L, streamFactory, checkpointOptions);

			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot2 =
				backend.snapshot(2L, 0L, streamFactory, checkpointOptions);

			Thread runner1 = new Thread(snapshot1, "snapshot1");
			runner1.start();
			Thread runner2 = new Thread(snapshot2, "snapshot2");
			runner2.start();

			runner1.join();
			runner2.join();

			snapshot1.get();
			snapshot2.get();
		}
	}

	private abstract class TestState {
		final Random rnd;

		private TestState() {
			this.rnd = new Random();
		}

		abstract void setToRandomValue() throws Exception;

		String getRandomString() {
			return StringUtils.getRandomString(rnd, 5, 10);
		}
	}

	private class TestValueState extends TestState {
		private final InternalValueState<Integer, VoidNamespace, String> state;

		private TestValueState() throws Exception {
			this.state = backend.createInternalState(
				VoidNamespaceSerializer.INSTANCE,
				new ValueStateDescriptor<>("TestValueState", StringSerializer.INSTANCE),
				snapshotTransformFactory);
			state.setCurrentNamespace(VoidNamespace.INSTANCE);
		}

		@Override
		void setToRandomValue() throws Exception {
			state.update(getRandomString());
		}
	}

	private class TestListState extends TestState {
		private final InternalListState<Integer, VoidNamespace, String> state;

		private TestListState() throws Exception {
			this.state = backend.createInternalState(
				VoidNamespaceSerializer.INSTANCE,
				new ListStateDescriptor<>("TestListState", new SingleThreadAccessCheckingTypeSerializer()),
				snapshotTransformFactory);
			state.setCurrentNamespace(VoidNamespace.INSTANCE);
		}

		@Override
		void setToRandomValue() throws Exception {
			int length = rnd.nextInt(10);
			for (int i = 0; i < length; i++) {
				state.add(getRandomString());
			}
		}
	}

	private class TestMapState extends TestState {
		private final InternalMapState<Integer, VoidNamespace, String, String> state;

		private TestMapState() throws Exception {
			this.state = backend.createInternalState(
				VoidNamespaceSerializer.INSTANCE,
				new MapStateDescriptor<>("TestMapState", StringSerializer.INSTANCE, StringSerializer.INSTANCE),
				snapshotTransformFactory);
			state.setCurrentNamespace(VoidNamespace.INSTANCE);
		}

		@Override
		void setToRandomValue() throws Exception {
			int length = rnd.nextInt(10);
			for (int i = 0; i < length; i++) {
				state.put(getRandomString(), getRandomString());
			}
		}
	}

	private static class SingleThreadAccessCheckingSnapshotTransformFactory<T>
		implements StateSnapshotTransformFactory<T> {

		private final SingleThreadAccessChecker singleThreadAccessChecker = new SingleThreadAccessChecker();

		static <T> StateSnapshotTransformFactory<T> create() {
			return new SingleThreadAccessCheckingSnapshotTransformFactory<>();
		}

		@Override
		public Optional<StateSnapshotTransformer<T>> createForDeserializedState() {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return createStateSnapshotTransformer();
		}

		@Override
		public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return createStateSnapshotTransformer();
		}

		private <T1> Optional<StateSnapshotTransformer<T1>> createStateSnapshotTransformer() {
			return Optional.of(new StateSnapshotTransformer<T1>() {
				private final SingleThreadAccessChecker singleThreadAccessChecker = new SingleThreadAccessChecker();

				@Nullable
				@Override
				public T1 filterOrTransform(@Nullable T1 value) {
					singleThreadAccessChecker.checkSingleThreadAccess();
					return value;
				}
			});
		}
	}

	private static class SingleThreadAccessCheckingTypeSerializer extends TypeSerializer<String> {
		private final SingleThreadAccessChecker singleThreadAccessChecker = new SingleThreadAccessChecker();

		@Override
		public boolean isImmutableType() {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return StringSerializer.INSTANCE.isImmutableType();
		}

		@Override
		public TypeSerializer<String> duplicate() {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return new SingleThreadAccessCheckingTypeSerializer();
		}

		@Override
		public String createInstance() {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return StringSerializer.INSTANCE.createInstance();
		}

		@Override
		public String copy(String from) {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return StringSerializer.INSTANCE.copy(from);
		}

		@Override
		public String copy(String from, String reuse) {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return StringSerializer.INSTANCE.copy(from, reuse);
		}

		@Override
		public int getLength() {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return StringSerializer.INSTANCE.getLength();
		}

		@Override
		public void serialize(String record, DataOutputView target) throws IOException {
			singleThreadAccessChecker.checkSingleThreadAccess();
			StringSerializer.INSTANCE.serialize(record, target);
		}

		@Override
		public String deserialize(DataInputView source) throws IOException {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return StringSerializer.INSTANCE.deserialize(source);
		}

		@Override
		public String deserialize(String reuse, DataInputView source) throws IOException {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return StringSerializer.INSTANCE.deserialize(reuse, source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			singleThreadAccessChecker.checkSingleThreadAccess();
			StringSerializer.INSTANCE.copy(source, target);
		}

		@Override
		public boolean equals(Object obj) {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return obj == this ||
				(obj != null && obj.getClass() == getClass() &&
					StringSerializer.INSTANCE.equals(obj));
		}

		@Override
		public boolean canEqual(Object obj) {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return (obj != null && obj.getClass() == getClass() &&
				StringSerializer.INSTANCE.canEqual(obj));
		}

		@Override
		public int hashCode() {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return StringSerializer.INSTANCE.hashCode();
		}

		@Override
		public TypeSerializerSnapshot<String> snapshotConfiguration() {
			singleThreadAccessChecker.checkSingleThreadAccess();
			return StringSerializer.INSTANCE.snapshotConfiguration();
		}
	}

	private static class SingleThreadAccessChecker {
		private Thread currentThread = null;

		void checkSingleThreadAccess() {
			if (currentThread == null) {
				currentThread = Thread.currentThread();
			} else {
				assertEquals("Concurrent access from another thread",
					currentThread, Thread.currentThread());
			}
		}
	}
}
