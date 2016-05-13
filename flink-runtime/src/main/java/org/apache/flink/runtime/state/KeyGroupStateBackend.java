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
import java.util.Map;

/**
 * Interface for a state backend which supports key groups. A key group is a subset of the key
 * space on which the data is partitioned. Each key group state backend can have multiple key groups
 * assigned.
 *
 * A KeyGroupStateBackend is similar to the {@link PartitionedStateBackend}. The difference is that
 * the KeyGroupStateBackend is aware of the key groups and thus returns for each key group a
 * partitioned state snapshot.
 *
 * @param <KEY> Type of the key on which the data is partitioned.
 */
public interface KeyGroupStateBackend<KEY> extends AutoCloseable {

	/**
	 * Dispose the state associated with the given state descriptor. Disposing means that all
	 * allocated resources are released.
	 *
	 * @param stateDescriptor State descriptor identifying the partitioned state to be disposed
	 * @param <S> Type of the state
	 */
	<S extends PartitionedState> void dispose(StateDescriptor<S, ?> stateDescriptor);

	/**
	 * Sets the current key for all state objects requested from this state backend. This means that
	 * update and read operations on the state objects will access the state values for this key.
	 *
	 * @param key Current key
	 */
	void setCurrentKey(KEY key) throws Exception;

	/**
	 * Gets the current key value which is set for all state objects requested from this state
	 * backend.
	 *
	 * @return Current key value
	 */
	KEY getCurrentKey();

	/**
	 * Requests a partitioned state object. The state object is identified by its namespace, the
	 * corresponding namespace serializer and the state descriptor.
	 *
	 * @param namespace Namespace of the state object
	 * @param namespaceSerializer Namespace serializer
	 * @param stateDescriptor State descriptor identifying the state type and its name
	 * @param <N> Namespace type
	 * @param <S> Partitioned state type
	 * @return Partitioned state object
	 * @throws Exception
	 */
	<N, S extends PartitionedState> S getPartitionedState(
		final N namespace,
		final TypeSerializer<N> namespaceSerializer,
		final StateDescriptor<S, ?> stateDescriptor) throws Exception;

	/**
	 * Merges partitioned state specified by the state descriptor and the list of namespaces.
	 *
	 * The state descriptor specifies the state object whose namespaced states shall be merged.
	 * States for the given source namespaces are merged together and stored under the given
	 * target namespace.
	 *
	 * @param target Target namespace of the merged result
	 * @param sources List of namespaces to be merged
	 * @param namespaceSerializer Namespace serializer
	 * @param stateDescriptor State descriptor specifying the state object
	 * @param <N> Namespace type
	 * @param <S> State type
	 * @throws Exception
	 */
	<N, S extends MergingState<?, ?>> void mergePartitionedStates(
		final N target,
		Collection<N> sources,
		final TypeSerializer<N> namespaceSerializer,
		final StateDescriptor<S, ?> stateDescriptor) throws Exception;

	/**
	 * Snapshots the state of the assigned key groups.
	 *
	 * @param checkpointId Id of the checkpoint to be taken
	 * @param timestamp Timestamp of the checkpoint
	 * @return Map of partitioned state snapshots. For each key group there is one partitioned state
	 * snapshot. The partitioned state snapshot are indexed by their key group index.
	 * @throws Exception
	 */
	Map<Integer, PartitionedStateSnapshot> snapshotPartitionedState(long checkpointId, long timestamp) throws Exception;

	/**
	 * Restores the state of the given key groups
	 *
	 * @param partitionedStateSnapshots Map of partitioned state snapshots. Every map entry is the
	 *                                  state for a distinct key group.
	 * @param recoveryTimestamp Timestamp of the recovery
	 * @throws Exception
	 */
	void restorePartitionedState(Map<Integer, PartitionedStateSnapshot> partitionedStateSnapshots, long recoveryTimestamp) throws Exception;

	/**
	 * Callback which is called when a checkpoint has completed. The id of the completed checkpoint
	 * is given to the method.
	 *
	 * @param checkpointId Id of the completed checkpoint.
	 * @throws Exception
	 */
	void notifyCompletedCheckpoint(long checkpointId) throws Exception;
}
