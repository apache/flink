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

package org.apache.flink.migration.v0.runtime.rocksdb;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.migration.v0.runtime.AbstractStateBackendV0;
import org.apache.flink.migration.v0.runtime.KvStateSnapshotV0;
import org.apache.flink.migration.v0.api.StateDescriptorV0;
import org.apache.flink.migration.v0.runtime.StateHandleV0;

import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Deprecated
@SuppressWarnings("deprecation")
public class RocksDBStateBackendV0 extends AbstractStateBackendV0 {
	private static final long serialVersionUID = 1L;
	
	public static class FinalFullyAsyncSnapshot implements KvStateSnapshotV0 {
		private static final long serialVersionUID = 1L;

		final StateHandleV0<DataInputView> stateHandle;
		final long checkpointId;

		/**
		 * Creates a new snapshot from the given state parameters.
		 */
		private FinalFullyAsyncSnapshot(StateHandleV0 stateHandle, long checkpointId) {
			this.stateHandle = requireNonNull(stateHandle);
			this.checkpointId = checkpointId;
		}

		public StateHandleV0<DataInputView> getStateHandle() {
			return stateHandle;
		}

		public long getCheckpointId() {
			return checkpointId;
		}
	}

	public static class FinalSemiAsyncSnapshot implements KvStateSnapshotV0 {

		private static final long serialVersionUID = 1L;

		final URI backupUri;
		final long checkpointId;
		private final List<StateDescriptorV0> stateDescriptors;

		/**
		 * Creates a new snapshot from the given state parameters.
		 */
		private FinalSemiAsyncSnapshot(URI backupUri, long checkpointId, List<StateDescriptorV0> stateDescriptors) {
			this.backupUri = backupUri;
			this.checkpointId = checkpointId;
			this.stateDescriptors = stateDescriptors;
		}

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
