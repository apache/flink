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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.state.api.input.source.common.NoOpEnumState;
import org.apache.flink.state.api.input.source.keyed.KeyedStateSource;
import org.apache.flink.state.api.input.splits.KeyGroupRangeSourceSplit;
import org.apache.flink.util.CollectionUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

/** A {@link SplitEnumerator} for {@link KeyedStateSource}. */
@Internal
public final class KeyGroupRangeSourceSplitEnumerator
        implements SplitEnumerator<KeyGroupRangeSourceSplit, NoOpEnumState> {

    private final SplitEnumeratorContext<KeyGroupRangeSourceSplit> context;

    private final Queue<KeyGroupRangeSourceSplit> remainingSplits;

    public KeyGroupRangeSourceSplitEnumerator(
            SplitEnumeratorContext<KeyGroupRangeSourceSplit> context, OperatorState operatorState) {
        this.context = context;
        final int maxParallelism = operatorState.getMaxParallelism();
        final List<KeyGroupRange> keyGroups =
                sortedKeyGroupRanges(context.currentParallelism(), maxParallelism);
        this.remainingSplits =
                CollectionUtil.mapWithIndex(
                                keyGroups,
                                (keyGroupRange, split) ->
                                        createKeyGroupRangeInputSplit(
                                                operatorState,
                                                maxParallelism,
                                                keyGroupRange,
                                                split))
                        .collect(Collectors.toCollection(ArrayDeque::new));
    }

    @VisibleForTesting
    public KeyGroupRangeSourceSplit[] getRemainingSplits() {
        return remainingSplits.toArray(new KeyGroupRangeSourceSplit[0]);
    }

    @Nonnull
    private List<KeyGroupRange> sortedKeyGroupRanges(int parallelism, int maxParallelism) {
        List<KeyGroupRange> keyGroups =
                StateAssignmentOperation.createKeyGroupPartitions(
                        maxParallelism, Math.min(parallelism, maxParallelism));

        keyGroups.sort(Comparator.comparing(KeyGroupRange::getStartKeyGroup));
        return keyGroups;
    }

    @Nonnull
    private KeyGroupRangeSourceSplit createKeyGroupRangeInputSplit(
            OperatorState operatorState,
            int maxParallelism,
            KeyGroupRange keyGroupRange,
            int split) {

        final List<KeyedStateHandle> managedKeyedState =
                StateAssignmentOperation.getManagedKeyedStateHandles(operatorState, keyGroupRange);
        final List<KeyedStateHandle> rawKeyedState =
                StateAssignmentOperation.getRawKeyedStateHandles(operatorState, keyGroupRange);

        return new KeyGroupRangeSourceSplit(
                managedKeyedState, rawKeyedState, maxParallelism, split);
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        final KeyGroupRangeSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<KeyGroupRangeSourceSplit> splits, int subtaskId) {
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
