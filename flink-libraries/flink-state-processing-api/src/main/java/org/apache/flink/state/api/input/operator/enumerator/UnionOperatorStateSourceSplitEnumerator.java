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
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.state.api.input.source.union.UnionStateSource;
import org.apache.flink.state.api.input.splits.OperatorStateSourceSplit;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;
import java.util.stream.Collectors;

/** A {@link SplitEnumerator} for {@link UnionStateSource}. */
@Internal
public final class UnionOperatorStateSourceSplitEnumerator
        extends OperatorStateSourceSplitEnumerator {

    public UnionOperatorStateSourceSplitEnumerator(
            SplitEnumeratorContext<OperatorStateSourceSplit> context, OperatorState operatorState) {
        super(context, operatorState);
    }

    @Nonnull
    protected Queue<OperatorStateSourceSplit> getOperatorStateSourceSplitsQueue(
            OperatorState operatorState, int minNumSplits) {
        OperatorStateSourceSplit[] splits =
                getOperatorStateSourceSplits(operatorState, minNumSplits);

        // We only want to output a single instance of the union state so we only need
        // to transform a single input split. An arbitrary split is chosen and
        // sub-partitioned for better data distribution across the cluster.
        Preconditions.checkArgument(splits.length >= 1, "Expected at least one split");
        return CollectionUtil.mapWithIndex(
                        CollectionUtil.partition(
                                splits[0].getPrioritizedManagedOperatorState().asList(),
                                minNumSplits),
                        (state, index) ->
                                new OperatorStateSourceSplit(
                                        new StateObjectCollection<>(new ArrayList<>(state)), index))
                .collect(Collectors.toCollection(ArrayDeque::new));
    }
}
