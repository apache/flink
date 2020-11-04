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
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.state.AbstractChannelStateHandle.collectUniqueDelegates;
import static org.apache.flink.util.Preconditions.checkNotNull;

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
 */
public class OperatorSubtaskState implements CompositeStateHandle {

	private static final Logger LOG = LoggerFactory.getLogger(OperatorSubtaskState.class);

	private static final long serialVersionUID = -2394696997971923995L;

	/**
	 * Snapshot from the {@link org.apache.flink.runtime.state.OperatorStateBackend}.
	 */
	@Nonnull
	private final StateObjectCollection<OperatorStateHandle> managedOperatorState;

	/**
	 * Snapshot written using {@link org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream}.
	 */
	@Nonnull
	private final StateObjectCollection<OperatorStateHandle> rawOperatorState;

	/**
	 * Snapshot from {@link org.apache.flink.runtime.state.KeyedStateBackend}.
	 */
	@Nonnull
	private final StateObjectCollection<KeyedStateHandle> managedKeyedState;

	/**
	 * Snapshot written using {@link org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream}.
	 */
	@Nonnull
	private final StateObjectCollection<KeyedStateHandle> rawKeyedState;

	@Nonnull
	private final StateObjectCollection<InputChannelStateHandle> inputChannelState;

	@Nonnull
	private final StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState;

	/**
	 * The state size. This is also part of the deserialized state handle.
	 * We store it here in order to not deserialize the state handle when
	 * gathering stats.
	 */
	private final long stateSize;

	private OperatorSubtaskState(
			@Nonnull StateObjectCollection<OperatorStateHandle> managedOperatorState,
			@Nonnull StateObjectCollection<OperatorStateHandle> rawOperatorState,
			@Nonnull StateObjectCollection<KeyedStateHandle> managedKeyedState,
			@Nonnull StateObjectCollection<KeyedStateHandle> rawKeyedState,
			@Nonnull StateObjectCollection<InputChannelStateHandle> inputChannelState,
			@Nonnull StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState) {

		this.managedOperatorState = checkNotNull(managedOperatorState);
		this.rawOperatorState = checkNotNull(rawOperatorState);
		this.managedKeyedState = checkNotNull(managedKeyedState);
		this.rawKeyedState = checkNotNull(rawKeyedState);
		this.inputChannelState = checkNotNull(inputChannelState);
		this.resultSubpartitionState = checkNotNull(resultSubpartitionState);

		long calculateStateSize = managedOperatorState.getStateSize();
		calculateStateSize += rawOperatorState.getStateSize();
		calculateStateSize += managedKeyedState.getStateSize();
		calculateStateSize += rawKeyedState.getStateSize();
		calculateStateSize += inputChannelState.getStateSize();
		calculateStateSize += resultSubpartitionState.getStateSize();
		stateSize = calculateStateSize;
	}

	@VisibleForTesting
	OperatorSubtaskState() {
		this(StateObjectCollection.empty(),
			StateObjectCollection.empty(),
			StateObjectCollection.empty(),
			StateObjectCollection.empty(),
			StateObjectCollection.empty(),
			StateObjectCollection.empty());
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns a handle to the managed operator state.
	 */
	@Nonnull
	public StateObjectCollection<OperatorStateHandle> getManagedOperatorState() {
		return managedOperatorState;
	}

	/**
	 * Returns a handle to the raw operator state.
	 */
	@Nonnull
	public StateObjectCollection<OperatorStateHandle> getRawOperatorState() {
		return rawOperatorState;
	}

	/**
	 * Returns a handle to the managed keyed state.
	 */
	@Nonnull
	public StateObjectCollection<KeyedStateHandle> getManagedKeyedState() {
		return managedKeyedState;
	}

	/**
	 * Returns a handle to the raw keyed state.
	 */
	@Nonnull
	public StateObjectCollection<KeyedStateHandle> getRawKeyedState() {
		return rawKeyedState;
	}

	@Nonnull
	public StateObjectCollection<InputChannelStateHandle> getInputChannelState() {
		return inputChannelState;
	}

	@Nonnull
	public StateObjectCollection<ResultSubpartitionStateHandle> getResultSubpartitionState() {
		return resultSubpartitionState;
	}

	@Override
	public void discardState() {
		try {
			List<StateObject> toDispose =
				new ArrayList<>(
					managedOperatorState.size() +
						rawOperatorState.size() +
						managedKeyedState.size() +
						rawKeyedState.size() +
						inputChannelState.size() +
						resultSubpartitionState.size());
			toDispose.addAll(managedOperatorState);
			toDispose.addAll(rawOperatorState);
			toDispose.addAll(managedKeyedState);
			toDispose.addAll(rawKeyedState);
			toDispose.addAll(collectUniqueDelegates(inputChannelState, resultSubpartitionState));
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
		if (!getManagedOperatorState().equals(that.getManagedOperatorState())) {
			return false;
		}
		if (!getRawOperatorState().equals(that.getRawOperatorState())) {
			return false;
		}
		if (!getManagedKeyedState().equals(that.getManagedKeyedState())) {
			return false;
		}
		if (!getInputChannelState().equals(that.getInputChannelState())) {
			return false;
		}
		if (!getResultSubpartitionState().equals(that.getResultSubpartitionState())) {
			return false;
		}
		return getRawKeyedState().equals(that.getRawKeyedState());
	}

	@Override
	public int hashCode() {
		int result = getManagedOperatorState().hashCode();
		result = 31 * result + getRawOperatorState().hashCode();
		result = 31 * result + getManagedKeyedState().hashCode();
		result = 31 * result + getRawKeyedState().hashCode();
		result = 31 * result + getInputChannelState().hashCode();
		result = 31 * result + getResultSubpartitionState().hashCode();
		result = 31 * result + (int) (getStateSize() ^ (getStateSize() >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "SubtaskState{" +
			"operatorStateFromBackend=" + managedOperatorState +
			", operatorStateFromStream=" + rawOperatorState +
			", keyedStateFromBackend=" + managedKeyedState +
			", keyedStateFromStream=" + rawKeyedState +
			", inputChannelState=" + inputChannelState +
			", resultSubpartitionState=" + resultSubpartitionState +
			", stateSize=" + stateSize +
			'}';
	}

	public boolean hasState() {
		return managedOperatorState.hasState()
			|| rawOperatorState.hasState()
			|| managedKeyedState.hasState()
			|| rawKeyedState.hasState()
			|| inputChannelState.hasState()
			|| resultSubpartitionState.hasState();
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * The builder for a new {@link OperatorSubtaskState} which can be obtained by {@link #builder()}.
	 */
	public static class Builder {
		private StateObjectCollection<OperatorStateHandle> managedOperatorState = StateObjectCollection.empty();
		private StateObjectCollection<OperatorStateHandle> rawOperatorState = StateObjectCollection.empty();
		private StateObjectCollection<KeyedStateHandle> managedKeyedState = StateObjectCollection.empty();
		private StateObjectCollection<KeyedStateHandle> rawKeyedState = StateObjectCollection.empty();
		private StateObjectCollection<InputChannelStateHandle> inputChannelState = StateObjectCollection.empty();
		private StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState = StateObjectCollection.empty();

		private Builder() {
		}

		public Builder setManagedOperatorState(StateObjectCollection<OperatorStateHandle> managedOperatorState) {
			this.managedOperatorState = checkNotNull(managedOperatorState);
			return this;
		}

		public Builder setManagedOperatorState(OperatorStateHandle managedOperatorState) {
			return setManagedOperatorState(StateObjectCollection.singleton(checkNotNull(managedOperatorState)));
		}

		public Builder setRawOperatorState(StateObjectCollection<OperatorStateHandle> rawOperatorState) {
			this.rawOperatorState = checkNotNull(rawOperatorState);
			return this;
		}

		public Builder setRawOperatorState(OperatorStateHandle rawOperatorState) {
			return setRawOperatorState(StateObjectCollection.singleton(checkNotNull(rawOperatorState)));
		}

		public Builder setManagedKeyedState(StateObjectCollection<KeyedStateHandle> managedKeyedState) {
			this.managedKeyedState = checkNotNull(managedKeyedState);
			return this;
		}

		public Builder setManagedKeyedState(KeyedStateHandle managedKeyedState) {
			return setManagedKeyedState(StateObjectCollection.singleton(checkNotNull(managedKeyedState)));
		}

		public Builder setRawKeyedState(StateObjectCollection<KeyedStateHandle> rawKeyedState) {
			this.rawKeyedState = checkNotNull(rawKeyedState);
			return this;
		}

		public Builder setRawKeyedState(KeyedStateHandle rawKeyedState) {
			return setRawKeyedState(StateObjectCollection.singleton(checkNotNull(rawKeyedState)));
		}

		public Builder setInputChannelState(StateObjectCollection<InputChannelStateHandle> inputChannelState) {
			this.inputChannelState = checkNotNull(inputChannelState);
			return this;
		}

		public Builder setResultSubpartitionState(StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState) {
			this.resultSubpartitionState = checkNotNull(resultSubpartitionState);
			return this;
		}

		public OperatorSubtaskState build() {
			return new OperatorSubtaskState(
				managedOperatorState,
				rawOperatorState,
				managedKeyedState,
				rawKeyedState,
				inputChannelState,
				resultSubpartitionState);
		}
	}
}
