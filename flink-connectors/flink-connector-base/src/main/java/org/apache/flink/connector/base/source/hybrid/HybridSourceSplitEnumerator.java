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

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * Wraps the actual split enumerators and facilitates source switching. Enumerators are created
 * lazily when source switch occurs to support runtime position conversion.
 *
 * <p>This enumerator delegates to the current underlying split enumerator and transitions to the
 * next source once all readers have indicated via {@link SourceReaderFinishedEvent} that all input
 * was consumed.
 *
 * <p>Switching between enumerators occurs by creating the new enumerator via {@link
 * Source#createEnumerator(SplitEnumeratorContext)}. The start position can be fixed at pipeline
 * construction time through the source or supplied at switch time through a converter function by
 * using the end state of the previous enumerator.
 *
 * <p>During subtask recovery, splits that have been assigned since the last checkpoint will be
 * added back by the source coordinator. These splits may originate from a previous enumerator that
 * is no longer active. In that case {@link HybridSourceSplitEnumerator} will suspend forwarding to
 * the current enumerator and replay the returned splits by activating the previous readers. After
 * returned splits were processed, delegation to the current underlying enumerator resumes.
 */
public class HybridSourceSplitEnumerator
        implements SplitEnumerator<HybridSourceSplit, HybridSourceEnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(HybridSourceSplitEnumerator.class);

    private final SplitEnumeratorContext<HybridSourceSplit> context;
    private final List<HybridSource.SourceListEntry> sources;
    private final SwitchedSources switchedSources = new SwitchedSources();
    // Splits that have been returned due to subtask reset
    private final Map<Integer, TreeMap<Integer, List<HybridSourceSplit>>> pendingSplits;
    private final Set<Integer> finishedReaders;
    private final Map<Integer, Integer> readerSourceIndex;
    private int currentSourceIndex;
    private HybridSourceEnumeratorState restoredEnumeratorState;
    private SplitEnumerator<SourceSplit, Object> currentEnumerator;
    private SimpleVersionedSerializer<Object> currentEnumeratorCheckpointSerializer;

    public HybridSourceSplitEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> context,
            List<HybridSource.SourceListEntry> sources,
            int initialSourceIndex,
            HybridSourceEnumeratorState restoredEnumeratorState) {
        Preconditions.checkArgument(initialSourceIndex < sources.size());
        this.context = context;
        this.sources = sources;
        this.currentSourceIndex = initialSourceIndex;
        this.pendingSplits = new HashMap<>();
        this.finishedReaders = new HashSet<>();
        this.readerSourceIndex = new HashMap<>();
        this.restoredEnumeratorState = restoredEnumeratorState;
    }

    @Override
    public void start() {
        switchEnumerator();
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        LOG.debug(
                "handleSplitRequest subtask={} sourceIndex={} pendingSplits={}",
                subtaskId,
                currentSourceIndex,
                pendingSplits);
        Preconditions.checkState(pendingSplits.isEmpty() || !pendingSplits.containsKey(subtaskId));
        currentEnumerator.handleSplitRequest(subtaskId, requesterHostname);
    }

    @Override
    public void addSplitsBack(List<HybridSourceSplit> splits, int subtaskId) {
        LOG.debug("Adding splits back for subtask={} splits={}", subtaskId, splits);

        // Splits returned can belong to multiple sources, after switching since last checkpoint
        TreeMap<Integer, List<HybridSourceSplit>> splitsBySourceIndex = new TreeMap<>();

        for (HybridSourceSplit split : splits) {
            splitsBySourceIndex
                    .computeIfAbsent(split.sourceIndex(), k -> new ArrayList<>())
                    .add(split);
        }

        splitsBySourceIndex.forEach(
                (k, splitsPerSource) -> {
                    if (k == currentSourceIndex) {
                        currentEnumerator.addSplitsBack(
                                HybridSourceSplit.unwrapSplits(splitsPerSource, switchedSources),
                                subtaskId);
                    } else {
                        pendingSplits
                                .computeIfAbsent(subtaskId, sourceIndex -> new TreeMap<>())
                                .put(k, splitsPerSource);
                    }
                });
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("addReader subtaskId={}", subtaskId);
        readerSourceIndex.remove(subtaskId);
    }

    private void sendSwitchSourceEvent(int subtaskId, int sourceIndex) {
        readerSourceIndex.put(subtaskId, sourceIndex);
        Source source = switchedSources.sourceOf(sourceIndex);
        context.sendEventToSourceReader(
                subtaskId,
                new SwitchSourceEvent(sourceIndex, source, sourceIndex >= (sources.size() - 1)));
        // send pending splits, if any
        TreeMap<Integer, List<HybridSourceSplit>> splitsBySource = pendingSplits.get(subtaskId);
        if (splitsBySource != null) {
            List<HybridSourceSplit> splits = splitsBySource.remove(sourceIndex);
            if (splits != null && !splits.isEmpty()) {
                LOG.debug("Restoring splits to subtask={} {}", subtaskId, splits);
                context.assignSplits(
                        new SplitsAssignment<>(Collections.singletonMap(subtaskId, splits)));
                context.signalNoMoreSplits(subtaskId);
            }
            if (splitsBySource.isEmpty()) {
                pendingSplits.remove(subtaskId);
            }
        }

        if (sourceIndex == currentSourceIndex) {
            LOG.debug("adding reader subtask={} sourceIndex={}", subtaskId, currentSourceIndex);
            currentEnumerator.addReader(subtaskId);
        }
    }

    @Override
    public HybridSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        Object enumState = currentEnumerator.snapshotState(checkpointId);
        byte[] enumStateBytes = currentEnumeratorCheckpointSerializer.serialize(enumState);
        return new HybridSourceEnumeratorState(
                currentSourceIndex,
                enumStateBytes,
                currentEnumeratorCheckpointSerializer.getVersion());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        currentEnumerator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        currentEnumerator.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.debug(
                "handleSourceEvent {} subtask={} pendingSplits={}",
                sourceEvent,
                subtaskId,
                pendingSplits);
        if (sourceEvent instanceof SourceReaderFinishedEvent) {
            SourceReaderFinishedEvent srfe = (SourceReaderFinishedEvent) sourceEvent;

            int subtaskSourceIndex =
                    readerSourceIndex.computeIfAbsent(
                            subtaskId,
                            k -> {
                                // first time we see reader after cold start or recovery
                                LOG.debug(
                                        "New reader subtask={} sourceIndex={}",
                                        subtaskId,
                                        srfe.sourceIndex());
                                return srfe.sourceIndex();
                            });

            if (srfe.sourceIndex() < subtaskSourceIndex) {
                // duplicate event
                return;
            }

            if (subtaskSourceIndex < currentSourceIndex) {
                subtaskSourceIndex++;
                sendSwitchSourceEvent(subtaskId, subtaskSourceIndex);
                return;
            }

            // track readers that have finished processing for current enumerator
            finishedReaders.add(subtaskId);
            if (finishedReaders.size() == context.currentParallelism()) {
                LOG.debug("All readers finished, ready to switch enumerator!");
                if (currentSourceIndex + 1 < sources.size()) {
                    switchEnumerator();
                    // switch all readers prior to sending split assignments
                    for (int i = 0; i < context.currentParallelism(); i++) {
                        sendSwitchSourceEvent(i, currentSourceIndex);
                    }
                }
            }
        } else {
            currentEnumerator.handleSourceEvent(subtaskId, sourceEvent);
        }
    }

    @Override
    public void close() throws IOException {
        currentEnumerator.close();
    }

    private void switchEnumerator() {

        SplitEnumerator<SourceSplit, Object> previousEnumerator = currentEnumerator;
        if (currentEnumerator != null) {
            try {
                currentEnumerator.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            currentEnumerator = null;
            currentSourceIndex++;
        }

        HybridSource.SourceSwitchContext<?> switchContext =
                new HybridSource.SourceSwitchContext<Object>() {
                    @Override
                    public Object getPreviousEnumerator() {
                        return previousEnumerator;
                    }
                };

        Source<?, ? extends SourceSplit, Object> source =
                sources.get(currentSourceIndex).factory.create(switchContext);
        switchedSources.put(currentSourceIndex, source);
        currentEnumeratorCheckpointSerializer = source.getEnumeratorCheckpointSerializer();
        SplitEnumeratorContextProxy delegatingContext =
                new SplitEnumeratorContextProxy(
                        currentSourceIndex, context, readerSourceIndex, switchedSources);
        try {
            if (restoredEnumeratorState == null) {
                currentEnumerator = source.createEnumerator(delegatingContext);
            } else {
                LOG.info("Restoring enumerator for sourceIndex={}", currentSourceIndex);
                Object nestedEnumState =
                        currentEnumeratorCheckpointSerializer.deserialize(
                                restoredEnumeratorState.getWrappedStateSerializerVersion(),
                                restoredEnumeratorState.getWrappedState());
                currentEnumerator = source.restoreEnumerator(delegatingContext, nestedEnumState);
                restoredEnumeratorState = null;
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create enumerator for sourceIndex=" + currentSourceIndex, e);
        }
        LOG.info("Starting enumerator for sourceIndex={}", currentSourceIndex);
        currentEnumerator.start();
    }

    /**
     * The {@link SplitEnumeratorContext} that is provided to the currently active enumerator.
     *
     * <p>This context is used to wrap the splits into {@link HybridSourceSplit} and track
     * assignment to readers.
     */
    private static class SplitEnumeratorContextProxy<SplitT extends SourceSplit>
            implements SplitEnumeratorContext<SplitT> {
        private static final Logger LOG =
                LoggerFactory.getLogger(SplitEnumeratorContextProxy.class);

        private final SplitEnumeratorContext<HybridSourceSplit> realContext;
        private final int sourceIndex;
        private final Map<Integer, Integer> readerSourceIndex;
        private final SwitchedSources switchedSources;

        private SplitEnumeratorContextProxy(
                int sourceIndex,
                SplitEnumeratorContext<HybridSourceSplit> realContext,
                Map<Integer, Integer> readerSourceIndex,
                SwitchedSources switchedSources) {
            this.realContext = realContext;
            this.sourceIndex = sourceIndex;
            this.readerSourceIndex = readerSourceIndex;
            this.switchedSources = switchedSources;
        }

        @Override
        public SplitEnumeratorMetricGroup metricGroup() {
            return realContext.metricGroup();
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
            realContext.sendEventToSourceReader(subtaskId, event);
        }

        @Override
        public int currentParallelism() {
            return realContext.currentParallelism();
        }

        @Override
        public Map<Integer, ReaderInfo> registeredReaders() {
            // TODO: not start enumerator until readers are ready?
            Map<Integer, ReaderInfo> readers = realContext.registeredReaders();
            if (readers.size() != readerSourceIndex.size()) {
                return filterRegisteredReaders(readers);
            }
            Integer lastIndex = null;
            for (Integer sourceIndex : readerSourceIndex.values()) {
                if (lastIndex != null && lastIndex != sourceIndex) {
                    return filterRegisteredReaders(readers);
                }
                lastIndex = sourceIndex;
            }
            return readers;
        }

        private Map<Integer, ReaderInfo> filterRegisteredReaders(Map<Integer, ReaderInfo> readers) {
            Map<Integer, ReaderInfo> readersForSource = new HashMap<>(readers.size());
            for (Map.Entry<Integer, ReaderInfo> e : readers.entrySet()) {
                if (readerSourceIndex.get(e.getKey()) == (Integer) sourceIndex) {
                    readersForSource.put(e.getKey(), e.getValue());
                }
            }
            return readersForSource;
        }

        @Override
        public void assignSplits(SplitsAssignment<SplitT> newSplitAssignments) {
            Map<Integer, List<HybridSourceSplit>> wrappedAssignmentMap = new HashMap<>();
            for (Map.Entry<Integer, List<SplitT>> e : newSplitAssignments.assignment().entrySet()) {
                List<HybridSourceSplit> splits =
                        HybridSourceSplit.wrapSplits(e.getValue(), sourceIndex, switchedSources);
                wrappedAssignmentMap.put(e.getKey(), splits);
            }
            SplitsAssignment<HybridSourceSplit> wrappedAssignments =
                    new SplitsAssignment<>(wrappedAssignmentMap);
            LOG.debug("Assigning splits sourceIndex={} {}", sourceIndex, wrappedAssignments);
            realContext.assignSplits(wrappedAssignments);
        }

        @Override
        public void assignSplit(SplitT split, int subtask) {
            HybridSourceSplit wrappedSplit =
                    HybridSourceSplit.wrapSplit(split, sourceIndex, switchedSources);
            realContext.assignSplit(wrappedSplit, subtask);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            realContext.signalNoMoreSplits(subtask);
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
            realContext.callAsync(callable, handler);
        }

        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
                long initialDelay,
                long period) {
            realContext.callAsync(callable, handler, initialDelay, period);
        }

        @Override
        public void runInCoordinatorThread(Runnable runnable) {
            realContext.runInCoordinatorThread(runnable);
        }
    }
}
