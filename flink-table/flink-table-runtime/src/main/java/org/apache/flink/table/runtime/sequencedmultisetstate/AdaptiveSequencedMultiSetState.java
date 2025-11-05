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

package org.apache.flink.table.runtime.sequencedmultisetstate;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.sequencedmultisetstate.linked.LinkedMultiSetState;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An {@link SequencedMultiSetState} that switches dynamically between {@link
 * ValueStateMultiSetState} and {@link LinkedMultiSetState} based on the number of elements.
 */
class AdaptiveSequencedMultiSetState implements SequencedMultiSetState<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveSequencedMultiSetState.class);

    private final ValueStateMultiSetState smallState;
    private final LinkedMultiSetState largeState;
    private final long switchToLargeThreshold;
    private final long switchToSmallThreshold;

    AdaptiveSequencedMultiSetState(
            ValueStateMultiSetState smallState,
            LinkedMultiSetState largeState,
            long switchToLargeThreshold,
            long switchToSmallThreshold) {
        checkArgument(switchToLargeThreshold > switchToSmallThreshold);
        this.smallState = smallState;
        this.largeState = largeState;
        this.switchToLargeThreshold = switchToLargeThreshold;
        this.switchToSmallThreshold = switchToSmallThreshold;
        LOG.info(
                "Created {} with thresholds: {}=>large, {}=>small",
                this.getClass().getSimpleName(),
                switchToLargeThreshold,
                switchToSmallThreshold);
    }

    @Override
    public StateChangeInfo<RowData> add(RowData element, long timestamp) throws Exception {
        return execute(
                state -> state.add(element, timestamp), StateChangeInfo::getSizeAfter, "add");
    }

    @Override
    public StateChangeInfo<RowData> append(RowData element, long timestamp) throws Exception {
        return execute(
                state -> state.append(element, timestamp), StateChangeInfo::getSizeAfter, "append");
    }

    @Override
    public Iterator<Tuple2<RowData, Long>> iterator() throws Exception {
        if (smallState.isEmpty()) {
            return largeState.iterator();
        } else {
            return smallState.iterator();
        }
    }

    @Override
    public boolean isEmpty() throws IOException {
        // large state check is faster
        return largeState.isEmpty() && smallState.isEmpty();
    }

    public StateChangeInfo<RowData> remove(RowData element) throws Exception {
        return execute(state -> state.remove(element), StateChangeInfo::getSizeAfter, "remove");
    }

    @Override
    public void clear() {
        clearCache();
        smallState.clear();
        largeState.clear();
    }

    @Override
    public void loadCache() throws IOException {
        smallState.loadCache();
        largeState.loadCache();
    }

    @Override
    public void clearCache() {
        smallState.clearCache();
        largeState.clearCache();
    }

    private <T> T execute(
            FunctionWithException<SequencedMultiSetState<RowData>, T, Exception> stateOp,
            Function<T, Long> getNewSize,
            String action)
            throws Exception {

        final boolean isUsingLarge = isIsUsingLargeState();

        // start with small state, i.e. choose smallState when both are empty
        SequencedMultiSetState<RowData> currentState = isUsingLarge ? largeState : smallState;
        SequencedMultiSetState<RowData> otherState = isUsingLarge ? smallState : largeState;

        T result = stateOp.apply(currentState);
        final long sizeAfter = getNewSize.apply(result);

        final boolean thresholdReached =
                isUsingLarge
                        ? sizeAfter <= switchToSmallThreshold
                        : sizeAfter >= switchToLargeThreshold;

        if (thresholdReached) {
            LOG.debug(
                    "Switch {} -> {} because '{}' resulted in state size reaching {} elements",
                    currentState.getClass().getSimpleName(),
                    otherState.getClass().getSimpleName(),
                    action,
                    sizeAfter);
            switchState(currentState, otherState);
        }

        clearCache();
        return result;
    }

    @VisibleForTesting
    boolean isIsUsingLargeState() throws IOException {
        smallState.loadCache();
        if (!smallState.isEmpty()) {
            return false;
        }
        largeState.loadCache();
        return !largeState.isEmpty();
    }

    private void switchState(
            SequencedMultiSetState<RowData> src, SequencedMultiSetState<RowData> dst)
            throws Exception {
        Iterator<Tuple2<RowData, Long>> it = src.iterator();
        while (it.hasNext()) {
            Tuple2<RowData, Long> next = it.next();
            dst.append(next.f0, next.f1);
        }
        src.clear();
    }

    public static AdaptiveSequencedMultiSetState create(
            SequencedMultiSetStateConfig sequencedMultiSetStateConfig,
            String backendTypeIdentifier,
            ValueStateMultiSetState smallState,
            LinkedMultiSetState largeState) {
        return new AdaptiveSequencedMultiSetState(
                smallState,
                largeState,
                sequencedMultiSetStateConfig
                        .getAdaptiveHighThresholdOverride()
                        .orElse(
                                isHeap(backendTypeIdentifier)
                                        ? ADAPTIVE_HEAP_HIGH_THRESHOLD
                                        : ADAPTIVE_ROCKSDB_HIGH_THRESHOLD),
                sequencedMultiSetStateConfig
                        .getAdaptiveLowThresholdOverride()
                        .orElse(
                                isHeap(backendTypeIdentifier)
                                        ? ADAPTIVE_HEAP_LOW_THRESHOLD
                                        : ADAPTIVE_ROCKSDB_LOW_THRESHOLD));
    }

    private static final long ADAPTIVE_HEAP_HIGH_THRESHOLD = 400;
    private static final long ADAPTIVE_HEAP_LOW_THRESHOLD = 300;
    private static final long ADAPTIVE_ROCKSDB_HIGH_THRESHOLD = 50;
    private static final long ADAPTIVE_ROCKSDB_LOW_THRESHOLD = 40;

    private static boolean isHeap(String stateBackend) {
        String trim = stateBackend.trim();
        return trim.equalsIgnoreCase("hashmap") || trim.equalsIgnoreCase("heap");
    }
}
