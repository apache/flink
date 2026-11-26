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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.operators.coordination.ComponentClosingUtils.closeAsyncWithTimeout;

/**
 * A class that will recreate a new {@link OperatorCoordinator} instance when reset to checkpoint.
 */
public class RecreateOnResetOperatorCoordinator implements OperatorCoordinator {
    private static final Logger LOG =
            LoggerFactory.getLogger(RecreateOnResetOperatorCoordinator.class);
    private static final long CLOSING_TIMEOUT_MS = 60000L;
    private final Provider provider;
    private final long closingTimeoutMs;
    private final OperatorCoordinator.Context context;
    private final ThreadFactory stateReaderThreads;
    private DeferrableCoordinator coordinator;
    private boolean started;
    private volatile boolean closed;

    private RecreateOnResetOperatorCoordinator(
            OperatorCoordinator.Context context,
            Provider provider,
            long closingTimeoutMs,
            ThreadFactory stateReaderThreads)
            throws Exception {
        this.context = context;
        this.provider = provider;
        this.stateReaderThreads = stateReaderThreads;
        this.coordinator =
                new DeferrableCoordinator(context.getOperatorId(), context.getJobID(), null);
        this.coordinator.createNewInternalCoordinator(context, provider);
        this.coordinator.processPendingCalls();
        this.closingTimeoutMs = closingTimeoutMs;
        this.started = false;
        this.closed = false;
    }

    @Override
    public void start() throws Exception {
        Preconditions.checkState(!started, "coordinator already started");
        started = true;
        coordinator.applyCall("start", OperatorCoordinator::start);
    }

    @Override
    public void close() throws Exception {
        closed = true;
        coordinator.closeAsync(closingTimeoutMs);
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
            throws Exception {
        coordinator.applyCall(
                "handleEventFromOperator",
                c -> c.handleEventFromOperator(subtask, attemptNumber, event));
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        coordinator.applyCall(
                "executionAttemptFailed",
                c -> c.executionAttemptFailed(subtask, attemptNumber, reason));
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        coordinator.applyCall("subtaskReset", c -> c.subtaskReset(subtask, checkpointId));
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        coordinator.applyCall(
                "executionAttemptReady",
                c -> c.executionAttemptReady(subtask, attemptNumber, gateway));
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception {
        coordinator.applyCall(
                "checkpointCoordinator", c -> c.checkpointCoordinator(checkpointId, resultFuture));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        coordinator.applyCall("checkpointComplete", c -> c.notifyCheckpointComplete(checkpointId));
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        coordinator.applyCall("checkpointAborted", c -> c.notifyCheckpointAborted(checkpointId));
    }

    @Override
    public void resetToCheckpoint(final long checkpointId, @Nullable final byte[] checkpointData) {
        resetToCheckpoint(checkpointId, CompletableFuture.completedFuture(checkpointData), null);
    }

    @Override
    public boolean supportsCoordinatorStateInFile() {
        return true;
    }

    /**
     * Reads the state in a separate thread, so this method does not block on I/O. The new
     * coordinator is created and reset once the state has been read and the old coordinator has
     * closed; until then, calls are deferred as for any reset. If a later reset replaces this one,
     * or the coordinator is closed, before the state has been read, the read is stopped.
     */
    @Override
    public void resetToCheckpointFromHandle(
            final long checkpointId, final StreamStateHandle checkpointState) {
        final CompletableFuture<byte[]> checkpointData = new CompletableFuture<>();
        final StateReader stateReader =
                StateReader.start(
                        checkpointId,
                        context.getOperatorId(),
                        MdcUtils.asContextData(context.getJobID()),
                        checkpointState,
                        stateReaderThreads,
                        checkpointData);
        resetToCheckpoint(checkpointId, checkpointData, stateReader);
    }

    private void resetToCheckpoint(
            final long checkpointId,
            final CompletableFuture<byte[]> checkpointData,
            @Nullable final StateReader stateReader) {
        // First bump up the coordinator epoch to fence out the active coordinator.
        LOG.info("Resetting coordinator to checkpoint.");
        // Replace the coordinator variable with a new DeferrableCoordinator instance.
        // At this point the internal coordinator of the new coordinator has not been created.
        // After this point all the subsequent calls will be made to the new coordinator.
        final DeferrableCoordinator oldCoordinator = coordinator;
        final DeferrableCoordinator newCoordinator =
                new DeferrableCoordinator(context.getOperatorId(), context.getJobID(), stateReader);
        coordinator = newCoordinator;
        // Close the old coordinator asynchronously in a separate closing thread.
        // The future will be completed when the old coordinator closes.
        CompletableFuture<Void> closingFuture = oldCoordinator.closeAsync(closingTimeoutMs);

        // Create and possibly start the coordinator and apply all meanwhile deferred calls
        // capture the status whether the coordinator was started when this method was called
        final boolean wasStarted = this.started;

        closingFuture
                .handle(
                        (ignored, e) -> {
                            if (e != null) {
                                LOG.warn(
                                        "Received exception when closing operator coordinator for"
                                                + " {}.",
                                        oldCoordinator.operatorId,
                                        e);
                            }
                            return null;
                        })
                .thenCombine(checkpointData, (ignored, data) -> data)
                .handle(
                        (data, e) -> {
                            if (!closed) {
                                if (e != null) {
                                    newCoordinator.failToReadState(
                                            context,
                                            checkpointId,
                                            ExceptionUtils.stripCompletionException(e));
                                    return null;
                                }
                                // The previous coordinator has closed. Create a new one.
                                newCoordinator.createNewInternalCoordinatorOnReset(
                                        context, provider);
                                newCoordinator.resetAndStart(checkpointId, data, wasStarted);
                                newCoordinator.processPendingCalls();
                            }
                            return null;
                        })
                .exceptionally(
                        t -> {
                            LOG.error(
                                    "Failed to reset the coordinator to checkpoint {}.",
                                    checkpointId,
                                    t);
                            return null;
                        });
    }

    @Override
    public boolean supportsBatchSnapshot() {
        try {
            return getInternalCoordinator().supportsBatchSnapshot();
        } catch (Exception e) {
            String msg = "Could not get internal coordinator";
            LOG.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    // ---------------------

    public OperatorCoordinator getInternalCoordinator() throws Exception {
        waitForAllAsyncCallsFinish();
        return coordinator.internalCoordinator;
    }

    @VisibleForTesting
    QuiesceableContext getQuiesceableContext() throws Exception {
        waitForAllAsyncCallsFinish();
        return coordinator.internalQuiesceableContext;
    }

    @VisibleForTesting
    void waitForAllAsyncCallsFinish() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        coordinator.applyCall("waitForAllAsyncCallsFinish", c -> future.complete(null));
        future.get();
    }

    // ---------------------

    /** The provider for a private RecreateOnResetOperatorCoordinator. */
    public abstract static class Provider implements OperatorCoordinator.Provider {
        private static final long serialVersionUID = 3002837631612629071L;
        private final OperatorID operatorID;

        public Provider(OperatorID operatorID) {
            this.operatorID = operatorID;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(Context context) throws Exception {
            return create(context, CLOSING_TIMEOUT_MS);
        }

        @VisibleForTesting
        protected OperatorCoordinator create(Context context, long closingTimeoutMs)
                throws Exception {
            final Map<String, String> mdc = MdcUtils.asContextData(context.getJobID());
            return create(
                    context,
                    closingTimeoutMs,
                    // Like the source coordinator's own thread, log an uncaught error rather than
                    // end the JobManager process. A failed read fails the job through the reset
                    // that waits for it.
                    new ExecutorThreadFactory(
                            "Coordinator state reader for " + context.getOperatorId(),
                            (thread, error) -> {
                                try (MdcUtils.MdcCloseable ignored = MdcUtils.withContext(mdc)) {
                                    LOG.error(
                                            "Thread '{}' produced an uncaught exception.",
                                            thread.getName(),
                                            error);
                                }
                            }));
        }

        @VisibleForTesting
        OperatorCoordinator create(
                Context context, long closingTimeoutMs, ThreadFactory stateReaderThreads)
                throws Exception {
            return new RecreateOnResetOperatorCoordinator(
                    context, this, closingTimeoutMs, stateReaderThreads);
        }

        protected abstract OperatorCoordinator getCoordinator(OperatorCoordinator.Context context)
                throws Exception;
    }

    // ----------------------

    /**
     * A wrapper class around the operator coordinator context to allow quiescence. When a new
     * operator coordinator is created, we need to quiesce the old operator coordinator to prevent
     * it from making any further impact to the job master. This is done by quiesce the operator
     * coordinator context. After the quiescence, the "reading" methods will still work, but the
     * "writing" methods will become a no-op or fail immediately.
     */
    @VisibleForTesting
    static class QuiesceableContext implements OperatorCoordinator.Context {
        private final OperatorCoordinator.Context context;
        private volatile boolean quiesced;

        QuiesceableContext(OperatorCoordinator.Context context) {
            this.context = context;
            quiesced = false;
        }

        @Override
        public JobID getJobID() {
            return context.getJobID();
        }

        @Override
        public JobInfo getJobInfo() {
            return context.getJobInfo();
        }

        @Override
        public OperatorID getOperatorId() {
            return context.getOperatorId();
        }

        @Override
        public OperatorCoordinatorMetricGroup metricGroup() {
            return context.metricGroup();
        }

        @Override
        public synchronized void failJob(Throwable cause) {
            if (quiesced) {
                return;
            }
            context.failJob(cause);
        }

        @Override
        public int currentParallelism() {
            return context.currentParallelism();
        }

        @Override
        public ClassLoader getUserCodeClassloader() {
            return context.getUserCodeClassloader();
        }

        @Override
        public CoordinatorStore getCoordinatorStore() {
            return context.getCoordinatorStore();
        }

        @Override
        public boolean isConcurrentExecutionAttemptsSupported() {
            return context.isConcurrentExecutionAttemptsSupported();
        }

        @Override
        @Nullable
        public CheckpointCoordinator getCheckpointCoordinator() {
            return context.getCheckpointCoordinator();
        }

        @VisibleForTesting
        synchronized void quiesce() {
            quiesced = true;
        }

        @VisibleForTesting
        boolean isQuiesced() {
            return quiesced;
        }

        private OperatorCoordinator.Context getContext() {
            return context;
        }
    }

    /**
     * A class that helps realize the fully async {@link #resetToCheckpoint(long, byte[])} behavior.
     * The class wraps an {@link OperatorCoordinator} instance. It is accessed by the scheduler
     * thread, by the closing thread created in {@link #closeAsync(long)} and, when the state to
     * reset to is read from its own file, by the thread that reads it. A DeferrableCoordinator
     * could be in three states:
     *
     * <ul>
     *   <li><b>deferred:</b> The internal {@link OperatorCoordinator} has not been created and all
     *       the method calls to the RecreateOnResetOperatorCoordinator are added to a Queue.
     *   <li><b>catching up:</b> The internal {@link OperatorCoordinator} has been created and is
     *       processing the queued up method calls. In this state, all the method calls to the
     *       RecreateOnResetOperatorCoordinator are still going to be enqueued to ensure the correct
     *       execution order.
     *   <li><b>caught up:</b> The internal {@link OperatorCoordinator} has finished processing all
     *       the queued up method calls. From this point on, the method calls to this coordinator
     *       will be executed in the caller thread directly instead of being put into the queue.
     * </ul>
     */
    private static class DeferrableCoordinator {
        private final OperatorID operatorId;
        private final BlockingQueue<NamedCall> pendingCalls;
        private final Map<String, String> mdc;
        @Nullable private final StateReader stateReader;
        private QuiesceableContext internalQuiesceableContext;
        private OperatorCoordinator internalCoordinator;
        private boolean hasCaughtUp;
        private boolean closed;
        private volatile boolean failed;

        private DeferrableCoordinator(
                OperatorID operatorId, JobID jobID, @Nullable StateReader stateReader) {
            this.operatorId = operatorId;
            this.stateReader = stateReader;
            this.pendingCalls = new LinkedBlockingQueue<>();
            this.hasCaughtUp = false;
            this.closed = false;
            this.failed = false;
            this.mdc = MdcUtils.asContextData(jobID);
        }

        synchronized <T extends Exception> void applyCall(
                String name, ThrowingConsumer<OperatorCoordinator, T> call) throws T {
            synchronized (this) {
                if (hasCaughtUp) {
                    // The new coordinator has caught up.
                    try (MdcUtils.MdcCloseable ignored = MdcUtils.withContext(mdc)) {
                        call.accept(internalCoordinator);
                    }
                } else {
                    pendingCalls.add(new NamedCall(name, call));
                }
            }
        }

        synchronized void createNewInternalCoordinator(
                OperatorCoordinator.Context context, Provider provider) {
            if (closed) {
                return;
            }
            // Create a new internal coordinator and a new quiesceable context.
            // We assume that the coordinator creation is fast. Otherwise the creation
            // of the new internal coordinator may block the applyCall() method
            // which is invoked in the scheduler main thread.
            try {
                internalQuiesceableContext = new QuiesceableContext(context);
                internalCoordinator = provider.getCoordinator(internalQuiesceableContext);
            } catch (Exception e) {
                LOG.error("Failed to create new internal coordinator due to ", e);
                cleanAndFailJob(e);
            }
        }

        /**
         * Creates the internal coordinator of a reset, and fails the job on an error, as {@link
         * #createNewInternalCoordinator} does on an exception.
         */
        synchronized void createNewInternalCoordinatorOnReset(
                OperatorCoordinator.Context context, Provider provider) {
            try {
                createNewInternalCoordinator(context, provider);
            } catch (Error e) {
                LOG.error("Failed to create new internal coordinator due to ", e);
                cleanAndFailJob(e);
            }
        }

        synchronized CompletableFuture<Void> closeAsync(long timeoutMs) {
            closed = true;
            if (stateReader != null) {
                stateReader.stop();
            }
            if (internalCoordinator != null) {
                internalQuiesceableContext.quiesce();
                pendingCalls.clear();
                return closeAsyncWithTimeout(
                                "SourceCoordinator for " + operatorId,
                                (ThrowingRunnable<Exception>) internalCoordinator::close,
                                Duration.ofMillis(timeoutMs))
                        .exceptionally(
                                e -> {
                                    cleanAndFailJob(e);
                                    return null;
                                });
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }

        void processPendingCalls() {
            if (failed || closed || internalCoordinator == null) {
                return;
            }
            String name = "Unknown Call Name";
            try {
                while (!hasCaughtUp) {
                    while (!pendingCalls.isEmpty()) {
                        NamedCall namedCall = pendingCalls.poll();
                        if (namedCall != null) {
                            name = namedCall.name;
                            namedCall.getConsumer().accept(internalCoordinator);
                        }
                    }
                    synchronized (this) {
                        // We need to check the pending calls queue again in case a new
                        // pending call is added after we process the last one and before
                        // we grab the lock.
                        if (pendingCalls.isEmpty()) {
                            hasCaughtUp = true;
                        }
                    }
                }
            } catch (Throwable t) {
                LOG.error("Failed to process pending calls {} on coordinator.", name, t);
                cleanAndFailJob(t);
            }
        }

        void start() throws Exception {
            internalCoordinator.start();
        }

        void resetAndStart(
                final long checkpointId,
                @Nullable final byte[] checkpointData,
                final boolean started) {

            if (failed || closed || internalCoordinator == null) {
                return;
            }
            try {
                internalCoordinator.resetToCheckpoint(checkpointId, checkpointData);
                // Start the new coordinator if this coordinator has been started before reset to
                // the checkpoint.
                if (started) {
                    internalCoordinator.start();
                }
            } catch (Throwable t) {
                LOG.error("Failed to reset the coordinator to checkpoint and start.", t);
                cleanAndFailJob(t);
            }
        }

        /**
         * Fails the job because the state to reset to could not be read, unless this coordinator
         * has already been replaced by a later reset or closed. No internal coordinator is created,
         * so calls stay deferred until the next reset replaces this coordinator.
         */
        synchronized void failToReadState(
                OperatorCoordinator.Context context, long checkpointId, Throwable t) {
            if (closed) {
                return;
            }
            final String heapHint =
                    ExceptionUtils.findThrowable(t, ExceptionUtils::isHeapSpaceOutOfMemoryError)
                                    .isPresent()
                            ? String.format(
                                    " The JobManager ran out of heap memory, which can be"
                                            + " increased with '%s'.",
                                    JobManagerOptions.JVM_HEAP_MEMORY.key())
                            : "";
            context.failJob(
                    new FlinkException(
                            String.format(
                                    "Could not read the coordinator state of checkpoint %d from"
                                            + " %s.%s",
                                    checkpointId,
                                    Preconditions.checkNotNull(stateReader).stateDescription,
                                    heapHint),
                            t));
        }

        private void cleanAndFailJob(Throwable t) {
            // Don't repeatedly fail the job.
            if (!failed) {
                failed = true;
                internalQuiesceableContext.getContext().failJob(t);
                pendingCalls.clear();
            }
        }
    }

    /**
     * Reads the state to reset to in a separate thread, so the caller does not block on I/O. Each
     * read has a thread of its own, as each close in {@link ComponentClosingUtils} has, because it
     * may block for a long time, which a thread of a shared pool must not. The read can be stopped
     * if the reset that needs the state is replaced or the coordinator is closed. It keeps no
     * reference to the state; only the future it completes does.
     */
    private static class StateReader {
        private final CloseableRegistry openStreams = new CloseableRegistry();
        private final ThreadFactory threads;
        private final String stateDescription;

        private StateReader(ThreadFactory threads, String stateDescription) {
            this.threads = threads;
            this.stateDescription = stateDescription;
        }

        private static StateReader start(
                long checkpointId,
                OperatorID operatorId,
                Map<String, String> mdc,
                StreamStateHandle checkpointState,
                ThreadFactory threads,
                CompletableFuture<byte[]> checkpointData) {
            final StateReader stateReader = new StateReader(threads, checkpointState.toString());
            threads.newThread(
                            () -> {
                                try (MdcUtils.MdcCloseable ignored = MdcUtils.withContext(mdc)) {
                                    stateReader.read(
                                            checkpointId,
                                            operatorId,
                                            checkpointState,
                                            checkpointData);
                                }
                            })
                    .start();
            return stateReader;
        }

        private void read(
                long checkpointId,
                OperatorID operatorId,
                StreamStateHandle checkpointState,
                CompletableFuture<byte[]> checkpointData) {
            try {
                LOG.info(
                        "Reading the coordinator state of operator {} for checkpoint {} from {}.",
                        operatorId,
                        checkpointId,
                        checkpointState);
                final long startNanos = System.nanoTime();
                final byte[] data;
                try {
                    final FSDataInputStream in = checkpointState.openInputStream();
                    // if the read has already been stopped, this closes the stream and throws
                    openStreams.registerCloseable(in);
                    data = readState(in, checkpointState.getStateSize());
                } finally {
                    // Close the stream before completing the future, which may run the reset
                    // in this thread.
                    IOUtils.closeQuietly(openStreams);
                }
                LOG.info(
                        "Read {} bytes of the coordinator state of operator {} for checkpoint {}"
                                + " from {} in {} ms.",
                        data.length,
                        operatorId,
                        checkpointId,
                        checkpointState,
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                checkpointData.complete(data);
            } catch (Throwable t) {
                checkpointData.completeExceptionally(t);
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            }
        }

        /**
         * Reads exactly the recorded size of the state, if it is known, so that the state is held
         * in memory only once, and a file of another size fails the read.
         */
        private static byte[] readState(InputStream in, long recordedSize) throws IOException {
            if (recordedSize <= 0 || recordedSize > Integer.MAX_VALUE - 8) {
                return in.readAllBytes();
            }
            final byte[] data = new byte[(int) recordedSize];
            IOUtils.readFully(in, data, 0, data.length);
            if (in.read() != -1) {
                throw new IOException(
                        String.format(
                                "The state is larger than its recorded size of %d bytes.",
                                recordedSize));
            }
            return data;
        }

        /**
         * Stops the read, if it is still in progress, by closing its stream. A stream still being
         * opened is closed as soon as it is open.
         */
        private void stop() {
            if (openStreams.isClosed()) {
                return;
            }
            // Closing the stream may block, for example until a read in progress returns.
            threads.newThread(() -> IOUtils.closeQuietly(openStreams)).start();
        }
    }

    private static class NamedCall {
        private final String name;
        private final ThrowingConsumer<OperatorCoordinator, ?> consumer;

        private NamedCall(String name, ThrowingConsumer<OperatorCoordinator, ?> consumer) {
            this.name = name;
            this.consumer = consumer;
        }

        public String getName() {
            return name;
        }

        public ThrowingConsumer<OperatorCoordinator, ?> getConsumer() {
            return consumer;
        }
    }
}
