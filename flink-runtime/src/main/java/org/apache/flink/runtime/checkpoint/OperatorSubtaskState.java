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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class encapsulates the state for one parallel instance of an operator. The complete state of a (logical)
 * operator (e.g. a flatmap operator) consists of the union of all {@link OperatorSubtaskState}s from all
 * parallel tasks that physically execute parallelized, physical instances of the operator.
 *
 * <p>The full state of the logical operator is represented by {@link OperatorState} which consists of
 * {@link OperatorSubtaskState}s.
 *
 * <p>Typically, we expect all collections in this class to be of size 0 or 1, because there is up to one state handle
 * produced per state type (e.g. managed-keyed, raw-operator, ...). In particular, this holds when taking a snapshot.
 * The purpose of having the state handles in collections is that this class is also reused in restoring state.
 * Under normal circumstances, the expected size of each collection is still 0 or 1, except for scale-down. In
 * scale-down, one operator subtask can become responsible for the state of multiple previous subtasks. The collections
 * can then store all the state handles that are relevant to build up the new subtask state.
 *
 * <p>There is no collection for legacy state because it is not rescalable.
 */
public class OperatorSubtaskState implements CompositeStateHandle {

	private static final Logger LOG = LoggerFactory.getLogger(OperatorSubtaskState.class);

	private static final long serialVersionUID = -2394696997971923995L;

	/**
	 * Legacy (non-repartitionable) operator state.
	 *
	 * @deprecated Non-repartitionable operator state that has been deprecated.
	 * Can be removed when we remove the APIs for non-repartitionable operator state.
	 */
	@Deprecated
	@Nullable
	private final StreamStateHandle legacyOperatorState;

	/**
	 * Snapshot from the {@link org.apache.flink.runtime.state.OperatorStateBackend}.
	 */
	@Nonnull
	private final Collection<OperatorStateHandle> managedOperatorState;

	/**
	 * Snapshot written using {@link org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream}.
	 */
	@Nonnull
	private final Collection<OperatorStateHandle> rawOperatorState;

	/**
	 * Snapshot from {@link org.apache.flink.runtime.state.KeyedStateBackend}.
	 */
	@Nonnull
	private final Collection<KeyedStateHandle> managedKeyedState;

	/**
	 * Snapshot written using {@link org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream}.
	 */
	@Nonnull
	private final Collection<KeyedStateHandle> rawKeyedState;

	/**
	 * The state size. This is also part of the deserialized state handle.
	 * We store it here in order to not deserialize the state handle when
	 * gathering stats.
	 */
	private final long stateSize;

	@VisibleForTesting
	public OperatorSubtaskState(StreamStateHandle legacyOperatorState) {

		this(legacyOperatorState,
			Collections.<OperatorStateHandle>emptyList(),
			Collections.<OperatorStateHandle>emptyList(),
			Collections.<KeyedStateHandle>emptyList(),
			Collections.<KeyedStateHandle>emptyList());
	}

	/**
	 * Empty state.
	 */
	public OperatorSubtaskState() {
		this(null);
	}

	public OperatorSubtaskState(
		StreamStateHandle legacyOperatorState,
		Collection<OperatorStateHandle> managedOperatorState,
		Collection<OperatorStateHandle> rawOperatorState,
		Collection<KeyedStateHandle> managedKeyedState,
		Collection<KeyedStateHandle> rawKeyedState) {

		this.legacyOperatorState = legacyOperatorState;
		this.managedOperatorState = Preconditions.checkNotNull(managedOperatorState);
		this.rawOperatorState = Preconditions.checkNotNull(rawOperatorState);
		this.managedKeyedState = Preconditions.checkNotNull(managedKeyedState);
		this.rawKeyedState = Preconditions.checkNotNull(rawKeyedState);

		try {
			long calculateStateSize = getSizeNullSafe(legacyOperatorState);
			calculateStateSize += sumAllSizes(managedOperatorState);
			calculateStateSize += sumAllSizes(rawOperatorState);
			calculateStateSize += sumAllSizes(managedKeyedState);
			calculateStateSize += sumAllSizes(rawKeyedState);
			stateSize = calculateStateSize;
		} catch (Exception e) {
			throw new RuntimeException("Failed to get state size.", e);
		}
	}

	/**
	 * For convenience because the size of the collections is typically 0 or 1. Null values are translated into empty
	 * Collections (except for legacy state).
	 */
	public OperatorSubtaskState(
		StreamStateHandle legacyOperatorState,
		OperatorStateHandle managedOperatorState,
		OperatorStateHandle rawOperatorState,
		KeyedStateHandle managedKeyedState,
		KeyedStateHandle rawKeyedState) {

		this(legacyOperatorState,
			singletonOrEmptyOnNull(managedOperatorState),
			singletonOrEmptyOnNull(rawOperatorState),
			singletonOrEmptyOnNull(managedKeyedState),
			singletonOrEmptyOnNull(rawKeyedState));
	}

	private static <T> Collection<T> singletonOrEmptyOnNull(T element) {
		return element != null ? Collections.singletonList(element) : Collections.<T>emptyList();
	}

	private static long sumAllSizes(Collection<? extends StateObject> stateObject) throws Exception {
		long size = 0L;
		for (StateObject object : stateObject) {
			size += getSizeNullSafe(object);
		}

		return size;
	}

	private static long getSizeNullSafe(StateObject stateObject) throws Exception {
		return stateObject != null ? stateObject.getStateSize() : 0L;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * @deprecated Non-repartitionable operator state that has been deprecated.
	 * Can be removed when we remove the APIs for non-repartitionable operator state.
	 */
	@Deprecated
	@Nullable
	public StreamStateHandle getLegacyOperatorState() {
		return legacyOperatorState;
	}

	/**
	 * Returns a handle to the managed operator state.
	 */
	@Nonnull
	public Collection<OperatorStateHandle> getManagedOperatorState() {
		return managedOperatorState;
	}

	/**
	 * Returns a handle to the raw operator state.
	 */
	@Nonnull
	public Collection<OperatorStateHandle> getRawOperatorState() {
		return rawOperatorState;
	}

	/**
	 * Returns a handle to the managed keyed state.
	 */
	@Nonnull
	public Collection<KeyedStateHandle> getManagedKeyedState() {
		return managedKeyedState;
	}

	/**
	 * Returns a handle to the raw keyed state.
	 */
	@Nonnull
	public Collection<KeyedStateHandle> getRawKeyedState() {
		return rawKeyedState;
	}

	@Override
	public void discardState() {
		try {
			List<StateObject> toDispose =
				new ArrayList<>(1 +
					managedOperatorState.size() +
					rawOperatorState.size() +
					managedKeyedState.size() +
					rawKeyedState.size());
			toDispose.add(legacyOperatorState);
			toDispose.addAll(managedOperatorState);
			toDispose.addAll(rawOperatorState);
			toDispose.addAll(managedKeyedState);
			toDispose.addAll(rawKeyedState);
			StateUtil.bestEffortDiscardAllStateObjects(toDispose);
		} catch (Exception e) {
			LOG.warn("Error while discarding operator states.", e);
		}
	}

	@Override
	public void registerSharedStates(SharedStateRegistry sharedStateRegistry) {
		registerSharedState(sharedStateRegistry, managedKeyedState);
		registerSharedState(sharedStateRegistry, rawKeyedState);
	}

	private static void registerSharedState(
		SharedStateRegistry sharedStateRegistry,
		Iterable<KeyedStateHandle> stateHandles) {
		for (KeyedStateHandle stateHandle : stateHandles) {
			if (stateHandle != null) {
				stateHandle.registerSharedStates(sharedStateRegistry);
			}
		}
	}

	@Override
	public long getStateSize() {
		return stateSize;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		OperatorSubtaskState that = (OperatorSubtaskState) o;

		if (getStateSize() != that.getStateSize()) {
			return false;
		}
		if (getLegacyOperatorState() != null ? !getLegacyOperatorState().equals(that.getLegacyOperatorState()) : that.getLegacyOperatorState() != null) {
			return false;
		}
		if (!getManagedOperatorState().equals(that.getManagedOperatorState())) {
			return false;
		}
		if (!getRawOperatorState().equals(that.getRawOperatorState())) {
			return false;
		}
		if (!getManagedKeyedState().equals(that.getManagedKeyedState())) {
			return false;
		}
		return getRawKeyedState().equals(that.getRawKeyedState());
	}

	@Override
	public int hashCode() {
		int result = getLegacyOperatorState() != null ? getLegacyOperatorState().hashCode() : 0;
		result = 31 * result + getManagedOperatorState().hashCode();
		result = 31 * result + getRawOperatorState().hashCode();
		result = 31 * result + getManagedKeyedState().hashCode();
		result = 31 * result + getRawKeyedState().hashCode();
		result = 31 * result + (int) (getStateSize() ^ (getStateSize() >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "SubtaskState{" +
			"legacyState=" + legacyOperatorState +
			", operatorStateFromBackend=" + managedOperatorState +
			", operatorStateFromStream=" + rawOperatorState +
			", keyedStateFromBackend=" + managedKeyedState +
			", keyedStateFromStream=" + rawKeyedState +
			", stateSize=" + stateSize +
			'}';
	}

	public boolean hasState() {
		return legacyOperatorState != null
			|| hasState(managedOperatorState)
			|| hasState(rawOperatorState)
			|| hasState(managedKeyedState)
			|| hasState(rawKeyedState);
	}

	private boolean hasState(Iterable<? extends StateObject> states) {
		for (StateObject state : states) {
			if (state != null) {
				return true;
			}
		}
		return false;
	}
}
