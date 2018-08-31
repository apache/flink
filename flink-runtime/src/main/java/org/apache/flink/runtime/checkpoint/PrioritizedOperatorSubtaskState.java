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

package org.apache.flink.runtime.checkpoint;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateObject;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

/**
 * This class is a wrapper over multiple alternative {@link OperatorSubtaskState} that are (partial) substitutes for
 * each other and imposes a priority ordering over all alternatives for the different states which define an order in
 * which the operator should attempt to restore the state from them. One OperatorSubtaskState is considered as the
 * "ground truth" about which state should be represented. Alternatives may be complete or partial substitutes for
 * the "ground truth" with a higher priority (if they had a lower alternative, they would not really be alternatives).
 * Substitution is determined on a per-sub-state basis.
 */
@Internal
public class PrioritizedOperatorSubtaskState {

	/** Singleton instance for an empty, non-restored operator state. */
	private static final PrioritizedOperatorSubtaskState EMPTY_NON_RESTORED_INSTANCE =
		new PrioritizedOperatorSubtaskState.Builder(new OperatorSubtaskState(), Collections.emptyList(), false)
			.build();

	/** List of prioritized snapshot alternatives for managed operator state. */
	private final List<StateObjectCollection<OperatorStateHandle>> prioritizedManagedOperatorState;

	/** List of prioritized snapshot alternatives for raw operator state. */
	private final List<StateObjectCollection<OperatorStateHandle>> prioritizedRawOperatorState;

	/** List of prioritized snapshot alternatives for managed keyed state. */
	private final List<StateObjectCollection<KeyedStateHandle>> prioritizedManagedKeyedState;

	/** List of prioritized snapshot alternatives for raw keyed state. */
	private final List<StateObjectCollection<KeyedStateHandle>> prioritizedRawKeyedState;

	/** Signal flag if this represents state for a restored operator. */
	private final boolean restored;

	PrioritizedOperatorSubtaskState(
		@Nonnull List<StateObjectCollection<KeyedStateHandle>> prioritizedManagedKeyedState,
		@Nonnull List<StateObjectCollection<KeyedStateHandle>> prioritizedRawKeyedState,
		@Nonnull List<StateObjectCollection<OperatorStateHandle>> prioritizedManagedOperatorState,
		@Nonnull List<StateObjectCollection<OperatorStateHandle>> prioritizedRawOperatorState,
		boolean restored) {

		this.prioritizedManagedOperatorState = prioritizedManagedOperatorState;
		this.prioritizedRawOperatorState = prioritizedRawOperatorState;
		this.prioritizedManagedKeyedState = prioritizedManagedKeyedState;
		this.prioritizedRawKeyedState = prioritizedRawKeyedState;
		this.restored = restored;
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Returns an immutable list with all alternative snapshots to restore the managed operator state, in the order in
	 * which we should attempt to restore.
	 */
	@Nonnull
	public List<StateObjectCollection<OperatorStateHandle>> getPrioritizedManagedOperatorState() {
		return prioritizedManagedOperatorState;
	}

	/**
	 * Returns an immutable list with all alternative snapshots to restore the raw operator state, in the order in
	 * which we should attempt to restore.
	 */
	@Nonnull
	public List<StateObjectCollection<OperatorStateHandle>> getPrioritizedRawOperatorState() {
		return prioritizedRawOperatorState;
	}

	/**
	 * Returns an immutable list with all alternative snapshots to restore the managed keyed state, in the order in
	 * which we should attempt to restore.
	 */
	@Nonnull
	public List<StateObjectCollection<KeyedStateHandle>> getPrioritizedManagedKeyedState() {
		return prioritizedManagedKeyedState;
	}

	/**
	 * Returns an immutable list with all alternative snapshots to restore the raw keyed state, in the order in which we
	 * should attempt to restore.
	 */
	@Nonnull
	public List<StateObjectCollection<KeyedStateHandle>> getPrioritizedRawKeyedState() {
		return prioritizedRawKeyedState;
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Returns the managed operator state from the job manager, which represents the ground truth about what this state
	 * should represent. This is the alternative with lowest priority.
	 */
	@Nonnull
	public StateObjectCollection<OperatorStateHandle> getJobManagerManagedOperatorState() {
		return lastElement(prioritizedManagedOperatorState);
	}

	/**
	 * Returns the raw operator state from the job manager, which represents the ground truth about what this state
	 * should represent. This is the alternative with lowest priority.
	 */
	@Nonnull
	public StateObjectCollection<OperatorStateHandle> getJobManagerRawOperatorState() {
		return lastElement(prioritizedRawOperatorState);
	}

	/**
	 * Returns the managed keyed state from the job manager, which represents the ground truth about what this state
	 * should represent. This is the alternative with lowest priority.
	 */
	@Nonnull
	public StateObjectCollection<KeyedStateHandle> getJobManagerManagedKeyedState() {
		return lastElement(prioritizedManagedKeyedState);
	}

	/**
	 * Returns the raw keyed state from the job manager, which represents the ground truth about what this state
	 * should represent. This is the alternative with lowest priority.
	 */
	@Nonnull
	public StateObjectCollection<KeyedStateHandle> getJobManagerRawKeyedState() {
		return lastElement(prioritizedRawKeyedState);
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Returns true if this was created for a restored operator, false otherwise. Restored operators are operators that
	 * participated in a previous checkpoint, even if they did not emit any state snapshots.
	 */
	public boolean isRestored() {
		return restored;
	}


	private static <T extends StateObject> StateObjectCollection<T> lastElement(List<StateObjectCollection<T>> list) {
		return list.get(list.size() - 1);
	}

	/**
	 * Returns an empty {@link PrioritizedOperatorSubtaskState} singleton for an empty, not-restored operator state.
	 */
	public static PrioritizedOperatorSubtaskState emptyNotRestored() {
		return EMPTY_NON_RESTORED_INSTANCE;
	}

	@Internal
	public static class Builder {

		/** Ground truth of state, provided by job manager. */
		@Nonnull
		private final OperatorSubtaskState jobManagerState;

		/** (Local) alternatives to the job manager state. */
		@Nonnull
		private final List<OperatorSubtaskState> alternativesByPriority;

		/** Flag if the states have been restored. */
		private final boolean restored;

		public Builder(
			@Nonnull OperatorSubtaskState jobManagerState,
			@Nonnull List<OperatorSubtaskState> alternativesByPriority) {
			this(jobManagerState, alternativesByPriority, true);
		}

		public Builder(
			@Nonnull OperatorSubtaskState jobManagerState,
			@Nonnull List<OperatorSubtaskState> alternativesByPriority,
			boolean restored) {

			this.jobManagerState = jobManagerState;
			this.alternativesByPriority = alternativesByPriority;
			this.restored = restored;
		}

		public PrioritizedOperatorSubtaskState build() {
			int size = alternativesByPriority.size();
			List<StateObjectCollection<OperatorStateHandle>> managedOperatorAlternatives = new ArrayList<>(size);
			List<StateObjectCollection<KeyedStateHandle>> managedKeyedAlternatives = new ArrayList<>(size);
			List<StateObjectCollection<OperatorStateHandle>> rawOperatorAlternatives = new ArrayList<>(size);
			List<StateObjectCollection<KeyedStateHandle>> rawKeyedAlternatives = new ArrayList<>(size);

			for (OperatorSubtaskState subtaskState : alternativesByPriority) {

				if (subtaskState != null) {
					managedKeyedAlternatives.add(subtaskState.getManagedKeyedState());
					rawKeyedAlternatives.add(subtaskState.getRawKeyedState());
					managedOperatorAlternatives.add(subtaskState.getManagedOperatorState());
					rawOperatorAlternatives.add(subtaskState.getRawOperatorState());
				}
			}

			// Key-groups should match.
			BiFunction<KeyedStateHandle, KeyedStateHandle, Boolean> keyedStateApprover =
				(ref, alt) -> ref.getKeyGroupRange().equals(alt.getKeyGroupRange());

			// State meta data should match.
			BiFunction<OperatorStateHandle, OperatorStateHandle, Boolean> operatorStateApprover =
				(ref, alt) -> ref.getStateNameToPartitionOffsets().equals(alt.getStateNameToPartitionOffsets());

			return new PrioritizedOperatorSubtaskState(
				resolvePrioritizedAlternatives(
					jobManagerState.getManagedKeyedState(),
					managedKeyedAlternatives,
					keyedStateApprover),
				resolvePrioritizedAlternatives(
					jobManagerState.getRawKeyedState(),
					rawKeyedAlternatives,
					keyedStateApprover),
				resolvePrioritizedAlternatives(
					jobManagerState.getManagedOperatorState(),
					managedOperatorAlternatives,
					operatorStateApprover),
				resolvePrioritizedAlternatives(
					jobManagerState.getRawOperatorState(),
					rawOperatorAlternatives,
					operatorStateApprover),
				restored);
		}

		/**
		 * This helper method resolves the dependencies between the ground truth of the operator state obtained from the
		 * job manager and potential alternatives for recovery, e.g. from a task-local source.
		 */
		protected <T extends StateObject> List<StateObjectCollection<T>> resolvePrioritizedAlternatives(
			StateObjectCollection<T> jobManagerState,
			List<StateObjectCollection<T>> alternativesByPriority,
			BiFunction<T, T, Boolean> approveFun) {

			// Nothing to resolve if there are no alternatives, or the ground truth has already no state, or if we can
			// assume that a rescaling happened because we find more than one handle in the JM state (this is more a sanity
			// check).
			if (alternativesByPriority == null
				|| alternativesByPriority.isEmpty()
				|| !jobManagerState.hasState()
				|| jobManagerState.size() != 1) {

				return Collections.singletonList(jobManagerState);
			}

			// As we know size is == 1
			T reference = jobManagerState.iterator().next();

			// This will contain the end result, we initialize it with the potential max. size.
			List<StateObjectCollection<T>> approved =
				new ArrayList<>(1 + alternativesByPriority.size());

			for (StateObjectCollection<T> alternative : alternativesByPriority) {

				// We found an alternative to the JM state if it has state, we have a 1:1 relationship, and the
				// approve-function signaled true.
				if (alternative != null
					&& alternative.hasState()
					&& alternative.size() == 1
					&& BooleanUtils.isTrue(approveFun.apply(reference, alternative.iterator().next()))) {

					approved.add(alternative);
				}
			}

			// Of course we include the ground truth as last alternative.
			approved.add(jobManagerState);
			return Collections.unmodifiableList(approved);
		}
	}
}
