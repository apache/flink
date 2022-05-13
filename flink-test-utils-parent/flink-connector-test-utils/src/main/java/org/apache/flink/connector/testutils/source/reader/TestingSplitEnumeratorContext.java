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

package org.apache.flink.connector.testutils.source.reader;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A test implementation of the {@link SplitEnumeratorContext}, with manual, non-concurrent
 * interaction and intercepting of state.
 *
 * @param <SplitT> The generic type of the splits.
 */
public class TestingSplitEnumeratorContext<SplitT extends SourceSplit>
        implements SplitEnumeratorContext<SplitT> {

    private final ManuallyTriggeredScheduledExecutorService executor =
            new ManuallyTriggeredScheduledExecutorService();

    private final HashMap<Integer, SplitAssignmentState<SplitT>> splitAssignments = new HashMap<>();

    private final HashMap<Integer, List<SourceEvent>> events = new HashMap<>();

    private final HashMap<Integer, ReaderInfo> registeredReaders = new HashMap<>();

    private final int parallelism;

    public TestingSplitEnumeratorContext(int parallelism) {
        this.parallelism = parallelism;
    }

    // ------------------------------------------------------------------------
    //  access to events / properties / execution
    // ------------------------------------------------------------------------

    public void triggerAllActions() {
        executor.triggerPeriodicScheduledTasks();
        executor.triggerAll();
    }

    public ManuallyTriggeredScheduledExecutorService getExecutorService() {
        return executor;
    }

    public Map<Integer, SplitAssignmentState<SplitT>> getSplitAssignments() {
        return splitAssignments;
    }

    public Map<Integer, List<SourceEvent>> getSentEvents() {
        return events;
    }

    public void registerReader(int subtask, String hostname) {
        checkState(!registeredReaders.containsKey(subtask), "Reader already registered");
        registeredReaders.put(subtask, new ReaderInfo(subtask, hostname));
    }

    // ------------------------------------------------------------------------
    //  SplitEnumeratorContext methods
    // ------------------------------------------------------------------------

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
        return UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup();
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        final List<SourceEvent> eventsForSubTask =
                events.computeIfAbsent(subtaskId, (key) -> new ArrayList<>());
        eventsForSubTask.add(event);
    }

    @Override
    public int currentParallelism() {
        return parallelism;
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
        return registeredReaders;
    }

    @Override
    public void assignSplits(SplitsAssignment<SplitT> newSplitAssignments) {
        for (final Map.Entry<Integer, List<SplitT>> entry :
                newSplitAssignments.assignment().entrySet()) {
            final SplitAssignmentState<SplitT> assignment =
                    splitAssignments.computeIfAbsent(
                            entry.getKey(), (key) -> new SplitAssignmentState<>());

            assignment.splits.addAll(entry.getValue());
        }
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        final SplitAssignmentState<?> assignment =
                splitAssignments.computeIfAbsent(subtask, (key) -> new SplitAssignmentState<>());
        assignment.noMoreSplits = true;
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        executor.execute(callableWithResultHandler(callable, handler));
    }

    @Override
    public <T> void callAsync(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelay,
            long period) {
        executor.scheduleWithFixedDelay(
                callableWithResultHandler(callable, handler),
                initialDelay,
                period,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
        executor.execute(runnable);
    }

    private static <T> Runnable callableWithResultHandler(
            Callable<T> callable, BiConsumer<T, Throwable> handler) {
        return () -> {
            try {
                final T result = callable.call();
                handler.accept(result, null);
            } catch (Throwable t) {
                handler.accept(null, t);
            }
        };
    }

    // ------------------------------------------------------------------------

    /** The state of the split assignment for a subtask. */
    public static final class SplitAssignmentState<SplitT extends SourceSplit> {

        final List<SplitT> splits = new ArrayList<>();
        boolean noMoreSplits;

        public List<SplitT> getAssignedSplits() {
            return splits;
        }

        public boolean hasReceivedNoMoreSplitsSignal() {
            return noMoreSplits;
        }
    }
}
