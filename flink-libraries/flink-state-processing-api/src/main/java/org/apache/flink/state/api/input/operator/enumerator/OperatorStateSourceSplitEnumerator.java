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

package org.apache.flink.state.api.input.operator.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.RoundRobinOperatorStateRepartitioner;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.state.api.input.source.common.NoOpEnumState;
import org.apache.flink.state.api.input.splits.OperatorStateSourceSplit;
import org.apache.flink.util.CollectionUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.reDistributePartitionableStates;

/** A {@link SplitEnumerator} for operator state based sources. */
@Internal
public class OperatorStateSourceSplitEnumerator
        implements SplitEnumerator<OperatorStateSourceSplit, NoOpEnumState> {

    private final SplitEnumeratorContext<OperatorStateSourceSplit> context;

    private final Queue<OperatorStateSourceSplit> remainingSplits;

    public OperatorStateSourceSplitEnumerator(
            SplitEnumeratorContext<OperatorStateSourceSplit> context, OperatorState operatorState) {
        this.context = context;
        this.remainingSplits = getOperatorStateSourceSplitsQueue(operatorState, 1);
    }

    @Nonnull
    protected Queue<OperatorStateSourceSplit> getOperatorStateSourceSplitsQueue(
            OperatorState operatorState, int minNumSplits) {
        OperatorStateSourceSplit[] splits =
                getOperatorStateSourceSplits(operatorState, minNumSplits);
        return Arrays.stream(splits).collect(Collectors.toCollection(ArrayDeque::new));
    }

    @Nonnull
    protected OperatorStateSourceSplit[] getOperatorStateSourceSplits(
            OperatorState operatorState, int minNumSplits) {
        Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates =
                new HashMap<>();
        reDistributePartitionableStates(
                singletonMap(operatorState.getOperatorID(), operatorState),
                minNumSplits,
                OperatorSubtaskState::getManagedOperatorState,
                RoundRobinOperatorStateRepartitioner.INSTANCE,
                newManagedOperatorStates);

        return CollectionUtil.mapWithIndex(
                        newManagedOperatorStates.values(),
                        (handles, index) ->
                                new OperatorStateSourceSplit(
                                        new StateObjectCollection<>(handles), index))
                .toArray(OperatorStateSourceSplit[]::new);
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        final OperatorStateSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<OperatorStateSourceSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public NoOpEnumState snapshotState(long checkpointId) {
        return new NoOpEnumState();
    }

    @Override
    public void close() throws IOException {}
}
