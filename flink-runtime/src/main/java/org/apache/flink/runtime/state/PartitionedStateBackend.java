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

import org.apache.flink.api.common.state.PartitionedState;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Collection;

public interface PartitionedStateBackend<KEY> {
	/**
	 * Closes the state backend, releasing all internal resources, but does not delete any persistent
	 * checkpoint data.
	 *
	 * @throws Exception Exceptions can be forwarded and will be logged by the system
	 */
	void close() throws Exception;

	void dispose();

	<S extends PartitionedState> void dispose(StateDescriptor<S, ?> stateDescriptor);

	void setCurrentKey(KEY key);

	KEY getCurrentKey();

	<N, S extends PartitionedState> S getPartitionedState(
		final N namespace,
		final TypeSerializer<N> namespaceSerializer,
		final StateDescriptor<S, ?> stateDescriptor) throws Exception;

	<N, S extends MergingState<?, ?>> void mergePartitionedStates(
		final N target,
		Collection<N> sources,
		final TypeSerializer<N> namespaceSerializer,
		final StateDescriptor<S, ?> stateDescriptor) throws Exception;

	PartitionedStateSnapshot snapshotPartitionedState(long checkpointId, long timestamp) throws Exception;

	void restorePartitionedState(PartitionedStateSnapshot partitionedStateSnapshot, long recoveryTimestamp) throws Exception;

	void notifyCompletedCheckpoint(long checkpointId) throws Exception;
}
