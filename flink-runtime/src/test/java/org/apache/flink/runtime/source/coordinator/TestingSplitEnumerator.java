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

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * A mock {@link SplitEnumerator} for unit tests.
 *
 * <p>In contrast to the {@link org.apache.flink.api.connector.source.mocks.MockSplitEnumerator}
 * class which this largely copies, this class does not implement any source logic directly, like
 * split assignments, etc. This class simply captures which modifications happened to support test
 * assertions.
 */
public class TestingSplitEnumerator<SplitT extends SourceSplit>
        implements SplitEnumerator<SplitT, Set<SplitT>> {

    private final SplitEnumeratorContext<SplitT> context;

    private final Queue<SplitT> splits;
    private final List<SourceEvent> handledEvents;
    private final List<Long> successfulCheckpoints;
    private final Set<Integer> registeredReaders;

    private volatile boolean started;
    private volatile boolean closed;

    public TestingSplitEnumerator(SplitEnumeratorContext<SplitT> context) {
        this(context, Collections.emptySet());
    }

    public TestingSplitEnumerator(
            SplitEnumeratorContext<SplitT> context, Collection<SplitT> restoredSplits) {
        this.context = context;
        this.splits = new ArrayDeque<>(restoredSplits);
        this.handledEvents = new ArrayList<>();
        this.successfulCheckpoints = new ArrayList<>();
        this.registeredReaders = new HashSet<>();
    }

    // ------------------------------------------------------------------------

    @Override
    public void start() {
        this.started = true;
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        handledEvents.add(sourceEvent);
    }

    @Override
    public void addSplitsBack(List<SplitT> splitsToAddBack, int subtaskId) {
        splits.addAll(splitsToAddBack);
    }

    @Override
    public void addReader(int subtaskId) {
        registeredReaders.add(subtaskId);
    }

    @Override
    public Set<SplitT> snapshotState(long checkpointId) {
        return new HashSet<>(splits);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        successfulCheckpoints.add(checkpointId);
    }

    // ------------------------------------------------------------------------

    public SplitEnumeratorContext<SplitT> getContext() {
        return context;
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isClosed() {
        return closed;
    }

    public Set<SplitT> getUnassignedSplits() {
        return new HashSet<>(splits);
    }

    public List<SourceEvent> getHandledSourceEvent() {
        return Collections.unmodifiableList(handledEvents);
    }

    public List<Long> getSuccessfulCheckpoints() {
        return Collections.unmodifiableList(successfulCheckpoints);
    }

    public Set<Integer> getRegisteredReaders() {
        return Collections.unmodifiableSet(registeredReaders);
    }

    // ------------------------------------------------------------------------
    //  simple test actions to trigger
    // ------------------------------------------------------------------------

    @SafeVarargs
    public final void addNewSplits(SplitT... newSplits) {
        addNewSplits(Arrays.asList(newSplits));
    }

    public void addNewSplits(Collection<SplitT> newSplits) {
        runInEnumThreadAndSync(() -> splits.addAll(newSplits));
    }

    public void executeAssignOneSplit(int subtask) {
        runInEnumThreadAndSync(
                () -> {
                    Preconditions.checkState(!splits.isEmpty(), "no splits available");
                    final SplitT split = splits.poll();
                    context.assignSplit(split, subtask);
                });
    }

    public void runInEnumThreadAndSync(Runnable action) {
        final CompletableFuture<?> future = new CompletableFuture<>();
        context.runInCoordinatorThread(
                () -> {
                    try {
                        action.run();
                        future.complete(null);
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                });

        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            ExceptionUtils.rethrow(ExceptionUtils.stripExecutionException(e));
        }
    }

    // ------------------------------------------------------------------------
    //  Source that solely acts as a factory for this enumerator
    //
    //  This is needed for now because the SourceCoordinator does not accept
    //  slim dependencies (like only the serializer, or suppliers for the
    //  enumerator), but needs a reference to the source as a whole.
    // ------------------------------------------------------------------------

    public static <T, SplitT extends SourceSplit> Source<T, SplitT, Set<SplitT>> factorySource(
            SimpleVersionedSerializer<SplitT> splitSerializer,
            SimpleVersionedSerializer<Set<SplitT>> checkpointSerializer) {
        return new FactorySource<>(splitSerializer, checkpointSerializer);
    }

    @SuppressWarnings("serial")
    private static final class FactorySource<T, SplitT extends SourceSplit>
            implements Source<T, SplitT, Set<SplitT>> {

        private final SimpleVersionedSerializer<SplitT> splitSerializer;
        private final SimpleVersionedSerializer<Set<SplitT>> checkpointSerializer;

        public FactorySource(
                SimpleVersionedSerializer<SplitT> splitSerializer,
                SimpleVersionedSerializer<Set<SplitT>> checkpointSerializer) {
            this.splitSerializer = splitSerializer;
            this.checkpointSerializer = checkpointSerializer;
        }

        @Override
        public Boundedness getBoundedness() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceReader<T, SplitT> createReader(SourceReaderContext readerContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TestingSplitEnumerator<SplitT> createEnumerator(
                SplitEnumeratorContext<SplitT> enumContext) {
            return new TestingSplitEnumerator<>(enumContext);
        }

        @Override
        public SplitEnumerator<SplitT, Set<SplitT>> restoreEnumerator(
                SplitEnumeratorContext<SplitT> enumContext, Set<SplitT> checkpoint) {
            return new TestingSplitEnumerator<>(enumContext, checkpoint);
        }

        @Override
        public SimpleVersionedSerializer<SplitT> getSplitSerializer() {
            return splitSerializer;
        }

        @Override
        public SimpleVersionedSerializer<Set<SplitT>> getEnumeratorCheckpointSerializer() {
            return checkpointSerializer;
        }
    }
}
