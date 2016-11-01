/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.migration.contrib.streaming.state;

import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.migration.runtime.state.AbstractStateBackend;
import org.apache.flink.migration.runtime.state.KvState;
import org.apache.flink.migration.runtime.state.KvStateSnapshot;
import org.apache.flink.migration.runtime.state.StateHandle;

import java.io.IOException;
import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class RocksDBStateBackend extends AbstractStateBackend {
	private static final long serialVersionUID = 1L;

	@Override
	public void disposeAllStateForCurrentJob() throws Exception {

	}

	@Override
	public void close() throws Exception {

	}

	@Override
	protected <N, T> ValueState<T> createValueState(TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<T> stateDesc) throws Exception {
		return null;
	}

	@Override
	protected <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer, ListStateDescriptor<T> stateDesc) throws Exception {
		return null;
	}

	@Override
	protected <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer, ReducingStateDescriptor<T> stateDesc) throws Exception {
		return null;
	}

	@Override
	protected <N, T, ACC> FoldingState<T, ACC> createFoldingState(TypeSerializer<N> namespaceSerializer, FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
		return null;
	}

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp) throws Exception {
		return null;
	}

	@Override
	public <S extends Serializable> StateHandle<S> checkpointStateSerializable(S state, long checkpointID, long timestamp) throws Exception {
		return null;
	}

	/**
	 * Dummy {@link KvStateSnapshot} that holds the state of our one RocksDB data base.
	 */
	public static class FinalFullyAsyncSnapshot implements KvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, RocksDBStateBackend> {
		private static final long serialVersionUID = 1L;

		public final StateHandle<DataInputView> stateHandle;
		final long checkpointId;

		/**
		 * Creates a new snapshot from the given state parameters.
		 */
		private FinalFullyAsyncSnapshot(StateHandle<DataInputView> stateHandle, long checkpointId) {
			this.stateHandle = requireNonNull(stateHandle);
			this.checkpointId = checkpointId;
		}

		@Override
		public final KvState<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, RocksDBStateBackend> restoreState(
				RocksDBStateBackend stateBackend,
				TypeSerializer<Object> keySerializer,
				ClassLoader classLoader) throws Exception {
			throw new RuntimeException("Should never happen.");
		}

		@Override
		public final void discardState() throws Exception {
			stateHandle.discardState();
		}

		@Override
		public final long getStateSize() throws Exception {
			return stateHandle.getStateSize();
		}

		@Override
		public void close() throws IOException {
			stateHandle.close();
		}
	}
}
