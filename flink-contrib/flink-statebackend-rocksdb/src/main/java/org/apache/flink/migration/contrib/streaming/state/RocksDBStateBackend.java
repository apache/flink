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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.migration.runtime.state.AbstractStateBackend;
import org.apache.flink.migration.runtime.state.KvStateSnapshot;
import org.apache.flink.migration.runtime.state.StateHandle;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * @deprecated Internal class used for backwards compatibility.
 */
@Deprecated
public class RocksDBStateBackend extends AbstractStateBackend {
	private static final long serialVersionUID = 1L;

	/**
	 * Dummy {@link KvStateSnapshot} that holds the state of our one RocksDB data base.
	 */
	public static class FinalFullyAsyncSnapshot implements KvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>> {
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

	public static class FinalSemiAsyncSnapshot {

		static {
			throwExceptionOnLoadingThisClass();
		}

		private static void throwExceptionOnLoadingThisClass() {
			throw new RuntimeException("Attempt to migrate RocksDB state created with semi async snapshot mode failed. "
					+ "Unfortunately, this is not supported. Please create a new savepoint for the job using fully "
					+ "async mode in Flink 1.1 and run migration again with the new savepoint.");
		}
	}
}
