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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;

import org.apache.commons.lang3.BooleanUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class is a wrapper over multiple alternative {@link OperatorSubtaskState} that are (partial)
 * substitutes for each other and imposes a priority ordering over all alternatives for the
 * different states which define an order in which the operator should attempt to restore the state
 * from them. One OperatorSubtaskState is considered as the "ground truth" about which state should
 * be represented. Alternatives may be complete or partial substitutes for the "ground truth" with a
 * higher priority (if they had a lower alternative, they would not really be alternatives).
 * Substitution is determined on a per-sub-state basis.
 */
@Internal
public class PrioritizedOperatorSubtaskState {

    private static final OperatorSubtaskState EMPTY_JM_STATE_STATE =
            OperatorSubtaskState.builder().build();

    /** Singleton instance for an empty, non-restored operator state. */
    private static final PrioritizedOperatorSubtaskState EMPTY_NON_RESTORED_INSTANCE =
            new PrioritizedOperatorSubtaskState.Builder(
                            EMPTY_JM_STATE_STATE, Collections.emptyList(), null)
                    .build();

    /** List of prioritized snapshot alternatives for managed operator state. */
    private final List<StateObjectCollection<OperatorStateHandle>> prioritizedManagedOperatorState;

    /** List of prioritized snapshot alternatives for raw operator state. */
    private final List<StateObjectCollection<OperatorStateHandle>> prioritizedRawOperatorState;

    /** List of prioritized snapshot alternatives for managed keyed state. */
    private final List<StateObjectCollection<KeyedStateHandle>> prioritizedManagedKeyedState;

    /** List of prioritized snapshot alternatives for raw keyed state. */
    private final List<StateObjectCollection<KeyedStateHandle>> prioritizedRawKeyedState;

    private final List<StateObjectCollection<InputChannelStateHandle>> prioritizedInputChannelState;

    private final List<StateObjectCollection<ResultSubpartitionStateHandle>>
            prioritizedResultSubpartitionState;

    /** Checkpoint id for a restored operator or null if not restored. */
    private final @Nullable Long restoredCheckpointId;

    PrioritizedOperatorSubtaskState(
            @Nonnull List<StateObjectCollection<KeyedStateHandle>> prioritizedManagedKeyedState,
            @Nonnull List<StateObjectCollection<KeyedStateHandle>> prioritizedRawKeyedState,
            @Nonnull
                    List<StateObjectCollection<OperatorStateHandle>>
                            prioritizedManagedOperatorState,
            @Nonnull List<StateObjectCollection<OperatorStateHandle>> prioritizedRawOperatorState,
            @Nonnull
                    List<StateObjectCollection<InputChannelStateHandle>>
                            prioritizedInputChannelState,
            @Nonnull
                    List<StateObjectCollection<ResultSubpartitionStateHandle>>
                            prioritizedResultSubpartitionState,
            @Nullable Long restoredCheckpointId) {

        this.prioritizedManagedOperatorState = prioritizedManagedOperatorState;
        this.prioritizedRawOperatorState = prioritizedRawOperatorState;
        this.prioritizedManagedKeyedState = prioritizedManagedKeyedState;
        this.prioritizedRawKeyedState = prioritizedRawKeyedState;
        this.prioritizedInputChannelState = prioritizedInputChannelState;
        this.prioritizedResultSubpartitionState = prioritizedResultSubpartitionState;
        this.restoredCheckpointId = restoredCheckpointId;
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Returns an immutable list with all alternative snapshots to restore the managed operator
     * state, in the order in which we should attempt to restore.
     */
    @Nonnull
    public List<StateObjectCollection<OperatorStateHandle>> getPrioritizedManagedOperatorState() {
        return prioritizedManagedOperatorState;
    }

    /**
     * Returns an immutable list with all alternative snapshots to restore the raw operator state,
     * in the order in which we should attempt to restore.
     */
    @Nonnull
    public List<StateObjectCollection<OperatorStateHandle>> getPrioritizedRawOperatorState() {
        return prioritizedRawOperatorState;
    }

    /**
     * Returns an immutable list with all alternative snapshots to restore the managed keyed state,
     * in the order in which we should attempt to restore.
     */
    @Nonnull
    public List<StateObjectCollection<KeyedStateHandle>> getPrioritizedManagedKeyedState() {
        return prioritizedManagedKeyedState;
    }

    /**
     * Returns an immutable list with all alternative snapshots to restore the raw keyed state, in
     * the order in which we should attempt to restore.
     */
    @Nonnull
    public List<StateObjectCollection<KeyedStateHandle>> getPrioritizedRawKeyedState() {
        return prioritizedRawKeyedState;
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Returns the managed operator state from the job manager, which represents the ground truth
     * about what this state should represent. This is the alternative with lowest priority.
     */
    @Nonnull
    public StateObjectCollection<OperatorStateHandle> getJobManagerManagedOperatorState() {
        return lastElement(prioritizedManagedOperatorState);
    }

    /**
     * Returns the raw operator state from the job manager, which represents the ground truth about
     * what this state should represent. This is the alternative with lowest priority.
     */
    @Nonnull
    public StateObjectCollection<OperatorStateHandle> getJobManagerRawOperatorState() {
        return lastElement(prioritizedRawOperatorState);
    }

    /**
     * Returns the managed keyed state from the job manager, which represents the ground truth about
     * what this state should represent. This is the alternative with lowest priority.
     */
    @Nonnull
    public StateObjectCollection<KeyedStateHandle> getJobManagerManagedKeyedState() {
        return lastElement(prioritizedManagedKeyedState);
    }

    /**
     * Returns the raw keyed state from the job manager, which represents the ground truth about
     * what this state should represent. This is the alternative with lowest priority.
     */
    @Nonnull
    public StateObjectCollection<KeyedStateHandle> getJobManagerRawKeyedState() {
        return lastElement(prioritizedRawKeyedState);
    }

    @Nonnull
    public StateObjectCollection<InputChannelStateHandle> getPrioritizedInputChannelState() {
        return lastElement(prioritizedInputChannelState);
    }

    @Nonnull
    public StateObjectCollection<ResultSubpartitionStateHandle>
            getPrioritizedResultSubpartitionState() {
        return lastElement(prioritizedResultSubpartitionState);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Returns true if this was created for a restored operator, false otherwise. Restored operators
     * are operators that participated in a previous checkpoint, even if they did not emit any state
     * snapshots.
     */
    public boolean isRestored() {
        return restoredCheckpointId != null;
    }

    /**
     * Returns the checkpoint id if this was created for a restored operator, null otherwise.
     * Restored operators are operators that participated in a previous checkpoint, even if they did
     * not emit any state snapshots.
     */
    @Nullable
    public Long getRestoredCheckpointId() {
        return restoredCheckpointId;
    }

    private static <T extends StateObject> StateObjectCollection<T> lastElement(
            List<StateObjectCollection<T>> list) {
        return list.get(list.size() - 1);
    }

    /**
     * Returns an empty {@link PrioritizedOperatorSubtaskState} singleton for an empty, not-restored
     * operator state.
     */
    public static PrioritizedOperatorSubtaskState emptyNotRestored() {
        return EMPTY_NON_RESTORED_INSTANCE;
    }

    public static PrioritizedOperatorSubtaskState empty(long restoredCheckpointId) {
        return new PrioritizedOperatorSubtaskState.Builder(
                        EMPTY_JM_STATE_STATE, Collections.emptyList(), restoredCheckpointId)
                .build();
    }

    /** A builder for PrioritizedOperatorSubtaskState. */
    @Internal
    public static class Builder {

        /** Ground truth of state, provided by job manager. */
        @Nonnull private final OperatorSubtaskState jobManagerState;

        /** (Local) alternatives to the job manager state. */
        @Nonnull private final List<OperatorSubtaskState> alternativesByPriority;

        /** Checkpoint id of the restored checkpoint or null if not restored. */
        private final @Nullable Long restoredCheckpointId;

        public Builder(
                @Nonnull OperatorSubtaskState jobManagerState,
                @Nonnull List<OperatorSubtaskState> alternativesByPriority) {
            this(jobManagerState, alternativesByPriority, null);
        }

        public Builder(
                @Nonnull OperatorSubtaskState jobManagerState,
                @Nonnull List<OperatorSubtaskState> alternativesByPriority,
                @Nullable Long restoredCheckpointId) {

            this.jobManagerState = jobManagerState;
            this.alternativesByPriority = alternativesByPriority;
            this.restoredCheckpointId = restoredCheckpointId;
        }

        public PrioritizedOperatorSubtaskState build() {
            int size = alternativesByPriority.size();
            List<StateObjectCollection<OperatorStateHandle>> managedOperatorAlternatives =
                    new ArrayList<>(size);
            List<StateObjectCollection<KeyedStateHandle>> managedKeyedAlternatives =
                    new ArrayList<>(size);
            List<StateObjectCollection<OperatorStateHandle>> rawOperatorAlternatives =
                    new ArrayList<>(size);
            List<StateObjectCollection<KeyedStateHandle>> rawKeyedAlternatives =
                    new ArrayList<>(size);
            List<StateObjectCollection<InputChannelStateHandle>> inputChannelStateAlternatives =
                    new ArrayList<>(size);
            List<StateObjectCollection<ResultSubpartitionStateHandle>>
                    resultSubpartitionStateAlternatives = new ArrayList<>(size);

            for (OperatorSubtaskState subtaskState : alternativesByPriority) {

                if (subtaskState != null) {
                    managedKeyedAlternatives.add(subtaskState.getManagedKeyedState());
                    rawKeyedAlternatives.add(subtaskState.getRawKeyedState());
                    managedOperatorAlternatives.add(subtaskState.getManagedOperatorState());
                    rawOperatorAlternatives.add(subtaskState.getRawOperatorState());
                    inputChannelStateAlternatives.add(subtaskState.getInputChannelState());
                    resultSubpartitionStateAlternatives.add(
                            subtaskState.getResultSubpartitionState());
                }
            }

            return new PrioritizedOperatorSubtaskState(
                    computePrioritizedAlternatives(
                            jobManagerState.getManagedKeyedState(),
                            managedKeyedAlternatives,
                            KeyedStateHandle::getKeyGroupRange),
                    computePrioritizedAlternatives(
                            jobManagerState.getRawKeyedState(),
                            rawKeyedAlternatives,
                            KeyedStateHandle::getKeyGroupRange),
                    resolvePrioritizedAlternatives(
                            jobManagerState.getManagedOperatorState(),
                            managedOperatorAlternatives,
                            eqStateApprover(OperatorStateHandle::getStateNameToPartitionOffsets)),
                    resolvePrioritizedAlternatives(
                            jobManagerState.getRawOperatorState(),
                            rawOperatorAlternatives,
                            eqStateApprover(OperatorStateHandle::getStateNameToPartitionOffsets)),
                    resolvePrioritizedAlternatives(
                            jobManagerState.getInputChannelState(),
                            inputChannelStateAlternatives,
                            eqStateApprover(InputChannelStateHandle::getInfo)),
                    resolvePrioritizedAlternatives(
                            jobManagerState.getResultSubpartitionState(),
                            resultSubpartitionStateAlternatives,
                            eqStateApprover(ResultSubpartitionStateHandle::getInfo)),
                    restoredCheckpointId);
        }

        /**
         * This method creates an alternative recovery option by replacing as much job manager state
         * with higher prioritized (=local) alternatives as possible.
         *
         * @param jobManagerState the state that the task got assigned from the job manager (this
         *     state lives in remote storage).
         * @param alternativesByPriority local alternatives to the job manager state, ordered by
         *     priority.
         * @param identityExtractor function to extract an identifier from a state object.
         * @return prioritized state alternatives.
         * @param <STATE_OBJ_TYPE> the type of the state objects we process.
         * @param <ID_TYPE> the type of object that represents the id the state object type.
         */
        <STATE_OBJ_TYPE extends StateObject, ID_TYPE>
                List<StateObjectCollection<STATE_OBJ_TYPE>> computePrioritizedAlternatives(
                        StateObjectCollection<STATE_OBJ_TYPE> jobManagerState,
                        List<StateObjectCollection<STATE_OBJ_TYPE>> alternativesByPriority,
                        Function<STATE_OBJ_TYPE, ID_TYPE> identityExtractor) {

            if (alternativesByPriority != null
                    && !alternativesByPriority.isEmpty()
                    && jobManagerState.hasState()) {

                Optional<StateObjectCollection<STATE_OBJ_TYPE>> mergedAlternative =
                        tryComputeMixedLocalAndRemoteAlternative(
                                jobManagerState, alternativesByPriority, identityExtractor);

                // Return the mix of local/remote state as first and pure remote state as second
                // alternative (in case that we fail to recover from the local state, e.g. because
                // of corruption).
                if (mergedAlternative.isPresent()) {
                    return Arrays.asList(mergedAlternative.get(), jobManagerState);
                }
            }

            return Collections.singletonList(jobManagerState);
        }

        /**
         * This method creates an alternative recovery option by replacing as much job manager state
         * with higher prioritized (=local) alternatives as possible. Returns empty Optional if the
         * JM state is empty or nothing could be replaced.
         *
         * @param jobManagerState the state that the task got assigned from the job manager (this
         *     state lives in remote storage).
         * @param alternativesByPriority local alternatives to the job manager state, ordered by
         *     priority.
         * @param identityExtractor function to extract an identifier from a state object.
         * @return A state collection where all JM state handles for which we could find local *
         *     alternatives are replaced by the alternative with the highest priority. Empty
         *     optional if no state could be replaced.
         * @param <STATE_OBJ_TYPE> the type of the state objects we process.
         * @param <ID_TYPE> the type of object that represents the id the state object type.
         */
        static <STATE_OBJ_TYPE extends StateObject, ID_TYPE>
                Optional<StateObjectCollection<STATE_OBJ_TYPE>>
                        tryComputeMixedLocalAndRemoteAlternative(
                                StateObjectCollection<STATE_OBJ_TYPE> jobManagerState,
                                List<StateObjectCollection<STATE_OBJ_TYPE>> alternativesByPriority,
                                Function<STATE_OBJ_TYPE, ID_TYPE> identityExtractor) {

            List<STATE_OBJ_TYPE> result = Collections.emptyList();

            // Build hash index over ids of the JM state
            Map<ID_TYPE, STATE_OBJ_TYPE> indexById =
                    jobManagerState.stream()
                            .collect(Collectors.toMap(identityExtractor, Function.identity()));

            // Move through all alternative in order from high to low priority
            for (StateObjectCollection<STATE_OBJ_TYPE> alternative : alternativesByPriority) {
                // Check all the state objects in the alternative if they can replace JM state
                for (STATE_OBJ_TYPE stateHandle : alternative) {
                    // Remove the current state object's id from the index to check for a match
                    if (indexById.remove(identityExtractor.apply(stateHandle)) != null) {
                        if (result.isEmpty()) {
                            // Lazy init result collection
                            result = new ArrayList<>(jobManagerState.size());
                        }
                        // If the id was still in the index, replace with higher prio alternative
                        result.add(stateHandle);

                        // If the index is empty we are already done, all JM state was replaces with
                        // the best alternative.
                        if (indexById.isEmpty()) {
                            return Optional.of(new StateObjectCollection<>(result));
                        }
                    }
                }
            }

            // Nothing useful to return
            if (result.isEmpty()) {
                return Optional.empty();
            }

            // Add all remaining JM state objects that we could not replace from the index to the
            // final result
            result.addAll(indexById.values());
            return Optional.of(new StateObjectCollection<>(result));
        }

        /**
         * This helper method resolves the dependencies between the ground truth of the operator
         * state obtained from the job manager and potential alternatives for recovery, e.g. from a
         * task-local source.
         */
        <T extends StateObject> List<StateObjectCollection<T>> resolvePrioritizedAlternatives(
                StateObjectCollection<T> jobManagerState,
                List<StateObjectCollection<T>> alternativesByPriority,
                BiFunction<T, T, Boolean> approveFun) {

            // Nothing to resolve if there are no alternatives, or the ground truth has already no
            // state, or if we can assume that a rescaling happened because we find more than one
            // handle in the JM state
            // (this is more a sanity check).
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

                // We found an alternative to the JM state if it has state, we have a 1:1
                // relationship, and the approve-function signaled true.
                if (alternative != null
                        && alternative.hasState()
                        && alternative.size() == 1
                        && BooleanUtils.isTrue(
                                approveFun.apply(reference, alternative.iterator().next()))) {

                    approved.add(alternative);
                }
            }

            // Of course we include the ground truth as last alternative.
            approved.add(jobManagerState);
            return Collections.unmodifiableList(approved);
        }
    }

    private static <T, E> BiFunction<T, T, Boolean> eqStateApprover(
            Function<T, E> identityExtractor) {
        return (ref, alt) -> identityExtractor.apply(ref).equals(identityExtractor.apply(alt));
    }
}
