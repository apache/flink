/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.api.connector.source.mocks;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.SupportsIntermediateNoMoreSplits;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.ThrowableCatchingRunnable;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/** A mock class for {@link SplitEnumeratorContext}. */
public class MockSplitEnumeratorContext<SplitT extends SourceSplit>
        implements SplitEnumeratorContext<SplitT>, SupportsIntermediateNoMoreSplits, AutoCloseable {
    private final Map<Integer, List<SourceEvent>> sentSourceEvent;
    private final ConcurrentMap<Integer, ReaderInfo> registeredReaders;
    private final List<SplitsAssignment<SplitT>> splitsAssignmentSequence;
    private final ExecutorService workerExecutor;
    private final ExecutorService mainExecutor;
    private final TestingExecutorThreadFactory mainThreadFactory;
    private final AtomicReference<Throwable> errorInWorkerThread;
    private final AtomicReference<Throwable> errorInMainThread;
    private final BlockingQueue<Callable<Future<?>>> oneTimeCallables;
    private final List<Callable<Future<?>>> periodicCallables;
    private final AtomicBoolean stoppedAcceptAsyncCalls;
    private final boolean[] subtaskHasNoMoreSplits;

    private final int parallelism;

    public MockSplitEnumeratorContext(int parallelism) {
        this.sentSourceEvent = new HashMap<>();
        this.registeredReaders = new ConcurrentHashMap<>();
        this.splitsAssignmentSequence = new ArrayList<>();
        this.parallelism = parallelism;
        this.errorInWorkerThread = new AtomicReference<>();
        this.errorInMainThread = new AtomicReference<>();
        this.oneTimeCallables = new ArrayBlockingQueue<>(100);
        this.periodicCallables = Collections.synchronizedList(new ArrayList<>());
        this.mainThreadFactory = getThreadFactory("SplitEnumerator-main", errorInMainThread);
        this.workerExecutor =
                getExecutor(getThreadFactory("SplitEnumerator-worker", errorInWorkerThread));
        this.mainExecutor = getExecutor(mainThreadFactory);
        this.stoppedAcceptAsyncCalls = new AtomicBoolean(false);
        this.subtaskHasNoMoreSplits = new boolean[parallelism];
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
        return UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup();
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        try {
            if (!mainThreadFactory.isCurrentThreadMainExecutorThread()) {
                mainExecutor
                        .submit(
                                () ->
                                        sentSourceEvent
                                                .computeIfAbsent(subtaskId, id -> new ArrayList<>())
                                                .add(event))
                        .get();
            } else {
                sentSourceEvent.computeIfAbsent(subtaskId, id -> new ArrayList<>()).add(event);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to assign splits", e);
        }
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
        splitsAssignmentSequence.add(newSplitAssignments);
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        subtaskHasNoMoreSplits[subtask] = true;
    }

    @Override
    public void signalIntermediateNoMoreSplits(int subtask) {}

    public void resetNoMoreSplits(int subtask) {
        subtaskHasNoMoreSplits[subtask] = false;
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        if (stoppedAcceptAsyncCalls.get()) {
            return;
        }
        oneTimeCallables.add(
                () ->
                        workerExecutor.submit(
                                wrap(
                                        errorInWorkerThread,
                                        () -> {
                                            try {
                                                T result = callable.call();
                                                mainExecutor
                                                        .submit(
                                                                wrap(
                                                                        errorInMainThread,
                                                                        () ->
                                                                                handler.accept(
                                                                                        result,
                                                                                        null)))
                                                        .get();
                                            } catch (Throwable t) {
                                                handler.accept(null, t);
                                            }
                                        })));
    }

    @Override
    public <T> void callAsync(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelay,
            long period) {
        if (stoppedAcceptAsyncCalls.get()) {
            return;
        }
        periodicCallables.add(
                () ->
                        workerExecutor.submit(
                                wrap(
                                        errorInWorkerThread,
                                        () -> {
                                            try {
                                                T result = callable.call();
                                                mainExecutor
                                                        .submit(
                                                                wrap(
                                                                        errorInMainThread,
                                                                        () ->
                                                                                handler.accept(
                                                                                        result,
                                                                                        null)))
                                                        .get();
                                            } catch (Throwable t) {
                                                handler.accept(null, t);
                                            }
                                        })));
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
        mainExecutor.execute(runnable);
    }

    @Override
    public void setIsProcessingBacklog(boolean isProcessingBacklog) {}

    public void close() throws Exception {
        stoppedAcceptAsyncCalls.set(true);
        workerExecutor.shutdownNow();
        mainExecutor.shutdownNow();
    }

    // ------------ helper method to manipulate the context -------------

    public void runNextOneTimeCallable() throws Throwable {
        oneTimeCallables.take().call().get();
        checkError();
    }

    public void runPeriodicCallable(int index) throws Throwable {
        periodicCallables.get(index).call().get();
        checkError();
    }

    public Map<Integer, List<SourceEvent>> getSentSourceEvent() throws Exception {
        return workerExecutor.submit(() -> new HashMap<>(sentSourceEvent)).get();
    }

    public void registerReader(ReaderInfo readerInfo) {
        registeredReaders.put(readerInfo.getSubtaskId(), readerInfo);
    }

    public void unregisterReader(int readerId) {
        registeredReaders.remove(readerId);
    }

    public List<Callable<Future<?>>> getPeriodicCallables() {
        return periodicCallables;
    }

    public BlockingQueue<Callable<Future<?>>> getOneTimeCallables() {
        return oneTimeCallables;
    }

    public List<SplitsAssignment<SplitT>> getSplitsAssignmentSequence() {
        return splitsAssignmentSequence;
    }

    public boolean hasNoMoreSplits(int subtaskIndex) {
        return subtaskHasNoMoreSplits[subtaskIndex];
    }

    // ------------- private helpers -------------

    private void checkError() throws Throwable {
        if (errorInMainThread.get() != null) {
            throw errorInMainThread.get();
        }
        if (errorInWorkerThread.get() != null) {
            throw errorInWorkerThread.get();
        }
    }

    private static TestingExecutorThreadFactory getThreadFactory(
            String threadName, AtomicReference<Throwable> error) {
        return new TestingExecutorThreadFactory(threadName, error);
    }

    private static ExecutorService getExecutor(TestingExecutorThreadFactory threadFactory) {
        return Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    private static ThrowableCatchingRunnable wrap(AtomicReference<Throwable> error, Runnable r) {
        return new ThrowableCatchingRunnable(
                t -> {
                    if (!error.compareAndSet(null, t)) {
                        error.get().addSuppressed(t);
                    }
                },
                r);
    }

    // -------- private class -----------

    /** A thread factory class that provides some helper methods. */
    public static class TestingExecutorThreadFactory implements ThreadFactory {
        private final String coordinatorThreadName;
        private final AtomicReference<Throwable> error;
        private Thread t;

        TestingExecutorThreadFactory(
                String coordinatorThreadName, AtomicReference<Throwable> error) {
            this.coordinatorThreadName = coordinatorThreadName;
            this.error = error;
            this.t = null;
        }

        @Override
        public Thread newThread(@Nonnull Runnable r) {
            if (t != null) {
                throw new IllegalStateException(
                        "Should never happen. This factory should only be used by a "
                                + "SingleThreadExecutor.");
            }
            t = new Thread(r, coordinatorThreadName);
            t.setUncaughtExceptionHandler(
                    (t1, e) -> {
                        if (!error.compareAndSet(null, e)) {
                            error.get().addSuppressed(e);
                        }
                    });
            return t;
        }

        boolean isCurrentThreadMainExecutorThread() {
            return Thread.currentThread() == t;
        }
    }
}
