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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.event.Level;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RecreateOnResetOperatorCoordinator}. */
class RecreateOnResetOperatorCoordinatorTest {

    @RegisterExtension
    private final LoggerAuditingExtension log =
            new LoggerAuditingExtension(RecreateOnResetOperatorCoordinator.class, Level.WARN);

    private static final OperatorID OPERATOR_ID = new OperatorID(1234L, 5678L);
    private static final int NUM_SUBTASKS = 1;

    @Test
    void testQuiesceableContextForwardsProperties() {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator.QuiesceableContext quiesceableContext =
                new RecreateOnResetOperatorCoordinator.QuiesceableContext(context);

        assertThat(quiesceableContext.getOperatorId()).isEqualTo(OPERATOR_ID);
        assertThat(quiesceableContext.currentParallelism()).isEqualTo(NUM_SUBTASKS);
    }

    @Test
    void testQuiesceableContextNotQuiesced() {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator.QuiesceableContext quiesceableContext =
                new RecreateOnResetOperatorCoordinator.QuiesceableContext(context);

        final Exception exception = new Exception();
        quiesceableContext.failJob(exception);

        assertThat(context.isJobFailed()).isTrue();
        assertThat(context.getJobFailureReason()).isEqualTo(exception);
    }

    @Test
    void testQuiescedContext() {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator.QuiesceableContext quiesceableContext =
                new RecreateOnResetOperatorCoordinator.QuiesceableContext(context);

        quiesceableContext.quiesce();
        quiesceableContext.failJob(new Exception());

        assertThat(context.isJobFailed()).isFalse();
    }

    @Test
    void testResetToCheckpoint() throws Exception {
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider(null);
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator coordinator = createCoordinator(provider, context);

        RecreateOnResetOperatorCoordinator.QuiesceableContext contextBeforeReset =
                coordinator.getQuiesceableContext();
        TestingOperatorCoordinator internalCoordinatorBeforeReset =
                getInternalCoordinator(coordinator);

        byte[] stateToRestore = new byte[0];
        coordinator.resetToCheckpoint(1L, stateToRestore);

        // Use the checkpoint to ensure all the previous method invocation has succeeded.
        coordinator.waitForAllAsyncCallsFinish();

        assertThat(contextBeforeReset.isQuiesced()).isTrue();
        assertThat(internalCoordinatorBeforeReset.getLastRestoredCheckpointState()).isNull();

        TestingOperatorCoordinator internalCoordinatorAfterReset =
                getInternalCoordinator(coordinator);
        assertThat(internalCoordinatorAfterReset.getLastRestoredCheckpointState())
                .isEqualTo(stateToRestore);
        assertThat(internalCoordinatorBeforeReset).isNotEqualTo(internalCoordinatorAfterReset);
    }

    @Test
    void testResetToCheckpointReadsStateFromHandleInSeparateThread(@TempDir java.nio.file.Path tmp)
            throws Exception {
        final byte[] stateToRestore = new byte[4096];
        new Random(42).nextBytes(stateToRestore);
        final java.nio.file.Path file = tmp.resolve("coordinator-state");
        Files.write(file, stateToRestore);
        // the recorded size may be unknown
        final ThreadRecordingStateHandle handle =
                new ThreadRecordingStateHandle(new FileStateHandle(new Path(file.toUri()), -1));
        final RecordingThreadFactory threads = new RecordingThreadFactory();
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(
                        new TestingCoordinatorProvider(null),
                        new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS),
                        threads);

        coordinator.resetToCheckpointFromHandle(1L, handle);

        assertThat(getInternalCoordinator(coordinator).getLastRestoredCheckpointState())
                .isEqualTo(stateToRestore);
        assertThat(handle.readingThread).isSameAs(threads.get(0));
    }

    @Test
    void testResetToCheckpointReadsStateOfItsRecordedSize(@TempDir java.nio.file.Path tmp)
            throws Exception {
        final byte[] stateToRestore = new byte[4096];
        new Random(42).nextBytes(stateToRestore);
        final java.nio.file.Path file = tmp.resolve("coordinator-state");
        Files.write(file, stateToRestore);
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(
                        new TestingCoordinatorProvider(null),
                        new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS));

        coordinator.resetToCheckpointFromHandle(
                1L, new FileStateHandle(new Path(file.toUri()), stateToRestore.length));

        assertThat(getInternalCoordinator(coordinator).getLastRestoredCheckpointState())
                .isEqualTo(stateToRestore);
    }

    @ParameterizedTest(name = "recorded size differs from the file size by {0}")
    @ValueSource(ints = {1, -1})
    void testResetToCheckpointFailsJobIfStateIsNotOfItsRecordedSize(
            int sizeDifference, @TempDir java.nio.file.Path tmp) throws Exception {
        final java.nio.file.Path file = tmp.resolve("coordinator-state");
        Files.write(file, new byte[4096]);
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final TestingCoordinatorProvider provider = new TestingCoordinatorProvider(null);
        final RecreateOnResetOperatorCoordinator coordinator = createCoordinator(provider, context);

        coordinator.resetToCheckpointFromHandle(
                1L, new FileStateHandle(new Path(file.toUri()), 4096 + sizeDifference));

        // a read that succeeded would create a new internal coordinator
        waitUntilCondition(
                () -> context.isJobFailed() || provider.getCreatedCoordinators().size() > 1);
        assertThat(provider.getCreatedCoordinators()).hasSize(1);
        assertThat(context.getJobFailureReason())
                .hasMessageContaining("Could not read the coordinator state of checkpoint 1")
                .hasMessageContaining(file.getFileName().toString())
                .hasRootCauseInstanceOf(IOException.class)
                .hasRootCauseMessage(
                        sizeDifference > 0
                                ? "Premature EOF from inputStream"
                                : "The state is larger than its recorded size of 4095 bytes.");
    }

    @Test
    void testCallsDuringTheReadOfStateAreAppliedAfterTheReset() throws Exception {
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(
                        new TestingCoordinatorProvider(null),
                        new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS));
        final BlockingStateHandle slowState = new BlockingStateHandle(new byte[] {7}, false);
        final TestOperatorEvent event = new TestOperatorEvent(1);
        final CompletableFuture<byte[]> checkpoint = new CompletableFuture<>();

        coordinator.resetToCheckpointFromHandle(1L, slowState);
        slowState.reading.await();
        coordinator.handleEventFromOperator(0, 0, event);
        coordinator.checkpointCoordinator(2L, checkpoint);
        coordinator.notifyCheckpointComplete(1L);
        assertThat(checkpoint).isNotDone();
        slowState.release();

        final TestingOperatorCoordinator afterReset = getInternalCoordinator(coordinator);
        assertThat(afterReset.getLastRestoredCheckpointState()).containsExactly(7);
        assertThat(afterReset.getNextReceivedOperatorEvent()).isSameAs(event);
        afterReset.getLastTriggeredCheckpoint().complete(new byte[] {8});
        assertThat(checkpoint).isDone();
        assertThat(checkpoint.get()).containsExactly(8);
        assertThat(afterReset.getLastCheckpointComplete()).isEqualTo(1L);
    }

    @Test
    void testResetToCheckpointFailsJobIfStateCannotBeRead(@TempDir java.nio.file.Path tmp)
            throws Exception {
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(new TestingCoordinatorProvider(null), context);

        coordinator.resetToCheckpointFromHandle(
                1L, new FileStateHandle(new Path(tmp.resolve("absent").toUri()), 4096));

        waitUntilCondition(context::isJobFailed);
        assertThat(context.getJobFailureReason())
                .hasMessageContaining("Could not read the coordinator state of checkpoint 1")
                .hasCauseInstanceOf(FileNotFoundException.class);
    }

    @ParameterizedTest
    @CsvSource({"Java heap space, false", "Java heap space, true", "Metaspace, false"})
    void testErrorWhileReadingStateFailsOnlyTheJob(String errorMessage, boolean wrapped)
            throws Exception {
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(new TestingCoordinatorProvider(null), context);
        final OutOfMemoryError error = new OutOfMemoryError(errorMessage);
        // as a file system client may wrap it
        final FailingStateHandle state =
                new FailingStateHandle(wrapped ? new IOException("Read failed", error) : error);

        coordinator.resetToCheckpointFromHandle(1L, state);

        waitUntilCondition(context::isJobFailed);
        assertThat(context.getJobFailureReason())
                .hasMessageContaining("Could not read the coordinator state of checkpoint 1")
                .hasRootCause(error);
        final String heapHint =
                "The JobManager ran out of heap memory, which can be increased with"
                        + " 'jobmanager.memory.heap.size'.";
        if (errorMessage.equals("Java heap space")) {
            assertThat(context.getJobFailureReason()).hasMessageContaining(heapHint);
        } else {
            assertThat(context.getJobFailureReason()).hasMessageNotContaining(heapHint);
        }
        // a thread without a handler of its own reports to its group, which passes the error to
        // the process's default handler, and that can end the process, as
        // FatalExitExceptionHandler does
        assertThat(state.handlerOfReadingThread)
                .isNotNull()
                .isNotInstanceOf(ThreadGroup.class)
                .isNotSameAs(FatalExitExceptionHandler.INSTANCE);
        // the reader logs the error after failing the job, so let it finish within this test
        state.readingThread.join();
    }

    @Test
    void testStateWithARecordedSizeBeyondTheArrayLimitIsReadToItsEnd(
            @TempDir java.nio.file.Path tmp) throws Exception {
        final byte[] stateToRestore = new byte[4096];
        new Random(42).nextBytes(stateToRestore);
        final java.nio.file.Path file = tmp.resolve("coordinator-state");
        Files.write(file, stateToRestore);
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final TestingCoordinatorProvider provider = new TestingCoordinatorProvider(null);
        final RecreateOnResetOperatorCoordinator coordinator = createCoordinator(provider, context);

        coordinator.resetToCheckpointFromHandle(
                1L, new FileStateHandle(new Path(file.toUri()), 4L * 1024 * 1024 * 1024));

        waitUntilCondition(
                () -> context.isJobFailed() || provider.getCreatedCoordinators().size() > 1);
        assertThat(context.isJobFailed()).isFalse();
        assertThat(getInternalCoordinator(coordinator).getLastRestoredCheckpointState())
                .isEqualTo(stateToRestore);
    }

    @ParameterizedTest(name = "while the reset creates the coordinator: {0}")
    @ValueSource(booleans = {true, false})
    void testErrorWhileRestoringTheCoordinatorFailsTheJob(boolean whileCreating) throws Exception {
        // such as an OutOfMemoryError while the coordinator deserializes a large state
        final OutOfMemoryError error = new OutOfMemoryError("Java heap space");
        final ErrorOnRestoreCoordinatorProvider provider =
                new ErrorOnRestoreCoordinatorProvider(error, whileCreating);
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final RecreateOnResetOperatorCoordinator coordinator = createCoordinator(provider, context);

        coordinator.resetToCheckpoint(1L, new byte[0]);

        waitUntilCondition(() -> provider.failingThread != null);
        // the restore runs on the thread that closed the initial coordinator, which then ends,
        // unless the closing finished before the reset returned
        if (provider.failingThread != Thread.currentThread()) {
            provider.failingThread.join();
        }
        assertThat(context.isJobFailed()).isTrue();
        assertThat(context.getJobFailureReason()).isSameAs(error);
    }

    @Test
    void testErrorWhileCreatingTheInitialCoordinatorReachesTheCaller() {
        final NoClassDefFoundError error = new NoClassDefFoundError("SplitSerializer");
        final RecreateOnResetOperatorCoordinator.Provider provider =
                new RecreateOnResetOperatorCoordinator.Provider(OPERATOR_ID) {
                    private static final long serialVersionUID = 1L;

                    @Override
                    protected OperatorCoordinator getCoordinator(
                            OperatorCoordinator.Context context) {
                        throw error;
                    }
                };

        assertThatThrownBy(
                        () ->
                                provider.create(
                                        new MockOperatorCoordinatorContext(
                                                OPERATOR_ID, NUM_SUBTASKS)))
                .isSameAs(error);
    }

    @Test
    void testResetToCheckpointDoesNotWaitForTheStateToBeRead() throws Exception {
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(
                        new TestingCoordinatorProvider(null),
                        new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS));
        final BlockingStateHandle slowState = new BlockingStateHandle(new byte[] {7}, false);

        coordinator.resetToCheckpointFromHandle(1L, slowState);
        waitUntilCondition(() -> slowState.readingThread != null);
        assertThat(slowState.readingThread).isNotSameAs(Thread.currentThread());
        slowState.release();

        assertThat(getInternalCoordinator(coordinator).getLastRestoredCheckpointState())
                .containsExactly(7);
        assertThat(slowState.closed.getCount()).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLaterResetWinsOverEarlierResetStillReadingItsState(boolean earlierReadFails)
            throws Exception {
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(new TestingCoordinatorProvider(null), context);
        final BlockingStateHandle slowState =
                new BlockingStateHandle(new byte[] {1}, earlierReadFails);
        final TestingOperatorCoordinator initial = getInternalCoordinator(coordinator);

        coordinator.resetToCheckpointFromHandle(1L, slowState);
        // the reset continues on whichever of the read and the closing of the initial coordinator
        // finishes last, so waiting for the closing makes it continue on the reading thread,
        // which the checks at the end cover
        waitUntilCondition(initial::isClosed);
        slowState.reading.await();
        coordinator.resetToCheckpoint(2L, new byte[] {2});
        final TestingOperatorCoordinator afterLaterReset = getInternalCoordinator(coordinator);
        slowState.release();

        assertThat(afterLaterReset.getLastRestoredCheckpointState()).containsExactly(2);
        assertThat(getInternalCoordinator(coordinator)).isSameAs(afterLaterReset);
        assertThat(context.isJobFailed()).isFalse();
        // the replaced reset ends without a warning or an error of its own
        assertThat(log.getMessages()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReadStartsOnceThePreviousResetHasFinished(boolean middleResetReadsFromFile)
            throws Exception {
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final RecordingThreadFactory threads = new RecordingThreadFactory();
        final RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator)
                        new TestingCoordinatorProvider(null)
                                .create(context, Long.MAX_VALUE, threads);
        // returns only when released, not when its stream is closed
        final BlockingStateHandle slowState = new BlockingStateHandle(new byte[] {1}, false);
        final ThreadRecordingStateHandle replacedState =
                new ThreadRecordingStateHandle(
                        new ByteStreamStateHandle("replaced", new byte[] {2}));
        final ThreadRecordingStateHandle latestState =
                new ThreadRecordingStateHandle(new ByteStreamStateHandle("latest", new byte[] {3}));

        coordinator.resetToCheckpointFromHandle(1L, slowState);
        slowState.reading.await();
        if (middleResetReadsFromFile) {
            // so that only the flag set when it is stopped keeps the replaced reset from reading
            threads.holdBack(4);
            coordinator.resetToCheckpointFromHandle(2L, replacedState);
        } else {
            coordinator.resetToCheckpoint(2L, new byte[] {2});
        }
        coordinator.resetToCheckpointFromHandle(3L, latestState);
        // each reset starts its reader, then the thread that stops the read it replaces
        final Thread latestReader = threads.get(middleResetReadsFromFile ? 3 : 2);
        if (middleResetReadsFromFile) {
            final Thread replacedReader = threads.get(1);
            waitUntilCondition(
                    () ->
                            isWaitingForPreviousReset(replacedReader)
                                    || replacedState.readingThread != null);
        }
        waitUntilCondition(
                () -> isWaitingForPreviousReset(latestReader) || latestState.readingThread != null);
        assertThat(replacedState.readingThread).isNull();
        assertThat(latestState.readingThread).isNull();

        slowState.release();
        latestReader.join();

        assertThat(latestState.readingThread).isSameAs(latestReader);
        assertThat(replacedState.readingThread).isNull();
        assertThat(getInternalCoordinator(coordinator).getLastRestoredCheckpointState())
                .containsExactly(3);
        assertThat(context.isJobFailed()).isFalse();
        assertThat(log.getMessages()).isEmpty();
        threads.release();
    }

    @Test
    void testReadIsNotStartedIfTheCoordinatorIsClosedWhileItWaits() throws Exception {
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final RecordingThreadFactory threads = new RecordingThreadFactory();
        final RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator)
                        new TestingCoordinatorProvider(null)
                                .create(context, Long.MAX_VALUE, threads);
        final BlockingStateHandle slowState = new BlockingStateHandle(new byte[] {1}, false);
        final ThreadRecordingStateHandle latestState =
                new ThreadRecordingStateHandle(new ByteStreamStateHandle("latest", new byte[] {2}));

        coordinator.resetToCheckpointFromHandle(1L, slowState);
        slowState.reading.await();
        coordinator.resetToCheckpointFromHandle(2L, latestState);
        final Thread latestReader = threads.get(1);
        waitUntilCondition(
                () -> isWaitingForPreviousReset(latestReader) || latestState.readingThread != null);
        // so that only the flag set when it is stopped keeps the reset from reading
        threads.holdBack(3);
        coordinator.close();
        slowState.release();
        latestReader.join();

        assertThat(latestState.readingThread).isNull();
        assertThat(context.isJobFailed()).isFalse();
        assertThat(log.getMessages()).isEmpty();
        threads.release();
    }

    @Test
    void testInterruptedWaitFailsTheReadAndKeepsTheInterrupt() throws Exception {
        final AtomicBoolean interruptedWhenFailing = new AtomicBoolean();
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS) {
                    @Override
                    public void failJob(Throwable cause) {
                        interruptedWhenFailing.set(Thread.currentThread().isInterrupted());
                        super.failJob(cause);
                    }
                };
        final RecordingThreadFactory threads = new RecordingThreadFactory();
        final RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator)
                        new TestingCoordinatorProvider(null)
                                .create(context, Long.MAX_VALUE, threads);
        final BlockingStateHandle slowState = new BlockingStateHandle(new byte[] {1}, false);
        final ThreadRecordingStateHandle latestState =
                new ThreadRecordingStateHandle(new ByteStreamStateHandle("latest", new byte[] {2}));

        coordinator.resetToCheckpointFromHandle(1L, slowState);
        slowState.reading.await();
        coordinator.resetToCheckpointFromHandle(2L, latestState);
        final Thread latestReader = threads.get(1);
        waitUntilCondition(() -> isWaitingForPreviousReset(latestReader));
        latestReader.interrupt();
        latestReader.join();

        assertThat(context.getJobFailureReason())
                .hasMessageContaining("Could not read the coordinator state of checkpoint 2")
                .hasRootCauseInstanceOf(InterruptedException.class);
        assertThat(interruptedWhenFailing).isTrue();
        assertThat(latestState.readingThread).isNull();
        slowState.release();
    }

    @Test
    void testReadWaitsForThePreviousResetWhileItRestoresTheCoordinator() throws Exception {
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final CountDownLatch restoring = new CountDownLatch(1);
        final CountDownLatch restored = new CountDownLatch(1);
        final RecordingThreadFactory threads = new RecordingThreadFactory();
        final RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator)
                        new BlockingRestoreCoordinatorProvider(1L, restoring, restored)
                                .create(context, Long.MAX_VALUE, threads);
        final BlockingStateHandle previousState = new BlockingStateHandle(new byte[] {1}, false);
        final ThreadRecordingStateHandle latestState =
                new ThreadRecordingStateHandle(new ByteStreamStateHandle("latest", new byte[] {2}));

        coordinator.resetToCheckpointFromHandle(1L, previousState);
        // the read returns only now, so the restore does not run on this thread
        previousState.released.countDown();
        restoring.await();
        coordinator.resetToCheckpointFromHandle(2L, latestState);
        final Thread latestReader = threads.get(1);
        waitUntilCondition(
                () -> isWaitingForPreviousReset(latestReader) || latestState.readingThread != null);
        assertThat(latestState.readingThread).isNull();

        restored.countDown();
        latestReader.join();

        assertThat(latestState.readingThread).isSameAs(latestReader);
        assertThat(getInternalCoordinator(coordinator).getLastRestoredCheckpointState())
                .containsExactly(2);
        assertThat(context.isJobFailed()).isFalse();
    }

    @Test
    void testReadWaitsForThePreviousResetAlsoAfterItsReadHasReturned() throws Exception {
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final CountDownLatch closing = new CountDownLatch(1);
        final RecordingThreadFactory threads = new RecordingThreadFactory();
        final RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator)
                        new TestingCoordinatorProvider(closing)
                                .create(context, Long.MAX_VALUE, threads);
        final ThreadRecordingStateHandle latestState =
                new ThreadRecordingStateHandle(new ByteStreamStateHandle("latest", new byte[] {2}));

        coordinator.resetToCheckpointFromHandle(
                1L, new ByteStreamStateHandle("previous", new byte[] {1}));
        // the read has returned, and its reset holds the state until the old coordinator closes
        threads.get(0).join();
        coordinator.resetToCheckpointFromHandle(2L, latestState);
        final Thread latestReader = threads.get(1);
        waitUntilCondition(
                () -> isWaitingForPreviousReset(latestReader) || latestState.readingThread != null);
        assertThat(latestState.readingThread).isNull();

        closing.countDown();
        latestReader.join();

        assertThat(getInternalCoordinator(coordinator).getLastRestoredCheckpointState())
                .containsExactly(2);
        assertThat(context.isJobFailed()).isFalse();
        assertThat(log.getMessages()).isEmpty();
    }

    @Test
    void testReadStartsAfterTheClosingTimeoutIfThePreviousResetHasNotFinished() throws Exception {
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final RecordingThreadFactory threads = new RecordingThreadFactory();
        final RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator)
                        new TestingCoordinatorProvider(null).create(context, 1000L, threads);
        final BlockingStateHandle slowState = new BlockingStateHandle(new byte[] {1}, false);

        coordinator.resetToCheckpointFromHandle(1L, slowState);
        slowState.reading.await();
        coordinator.resetToCheckpointFromHandle(
                2L, new ByteStreamStateHandle("latest", new byte[] {2}));
        threads.get(1).join();

        assertThat(getInternalCoordinator(coordinator).getLastRestoredCheckpointState())
                .containsExactly(2);
        assertThat(context.isJobFailed()).isFalse();
        assertThat(log.getMessages()).hasSize(1);
        assertThat(log.getMessages().get(0))
                .startsWith(
                        "The previous reset of the coordinator of operator "
                                + OPERATOR_ID
                                + " has not finished after 1000 ms")
                .contains("The reset to checkpoint 2");
        slowState.release();
    }

    @ParameterizedTest(name = "replacedByLaterReset = {0}, stoppedWhileOpening = {1}")
    @CsvSource({"true, false", "false, false", "true, true", "false, true"})
    void testReadOfStateIsStoppedWhenTheResetIsReplacedOrClosed(
            boolean replacedByLaterReset, boolean stoppedWhileOpening) throws Exception {
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final RecordingThreadFactory threads = new RecordingThreadFactory();
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(new TestingCoordinatorProvider(null), context, threads);
        final StoppableStateHandle slowState = new StoppableStateHandle();
        final TestingOperatorCoordinator initial = getInternalCoordinator(coordinator);

        coordinator.resetToCheckpointFromHandle(1L, slowState);
        // wait for the closing, so that the reset continues on the thread that read
        waitUntilCondition(initial::isClosed);
        waitUntilCondition(() -> slowState.readingThread != null);
        if (!stoppedWhileOpening) {
            slowState.opened.countDown();
            slowState.stream.reading.await();
        }
        if (replacedByLaterReset) {
            coordinator.resetToCheckpoint(2L, new byte[] {2});
        } else {
            coordinator.close();
        }
        if (stoppedWhileOpening) {
            // the stop has closed the registry before the stream is open
            waitUntilCondition(() -> threads.size() == 2);
            threads.get(1).join();
            slowState.opened.countDown();
        }

        slowState.stream.closed.await();
        slowState.readingThread.join();
        assertThat(slowState.readingThread).isSameAs(threads.get(0));
        if (stoppedWhileOpening) {
            assertThat(slowState.stream.closingThread).isSameAs(slowState.readingThread);
        } else {
            assertThat(slowState.stream.closingThread).isSameAs(threads.get(1));
        }
        if (replacedByLaterReset) {
            assertThat(getInternalCoordinator(coordinator).getLastRestoredCheckpointState())
                    .containsExactly(2);
        }
        assertThat(context.isJobFailed()).isFalse();
    }

    @Test
    void testResetToCheckpointTimeout() throws Exception {
        final long closingTimeoutMs = 1L;
        // Let the user coordinator block on close.
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider(new CountDownLatch(1));
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) provider.create(context, closingTimeoutMs);

        coordinator.resetToCheckpoint(2L, new byte[0]);
        CommonTestUtils.waitUtil(
                context::isJobFailed,
                Duration.ofSeconds(5),
                "The job should fail due to resetToCheckpoint() timeout.");
    }

    @Test
    void testMethodCallsOnLongResetToCheckpoint() throws Exception {
        final long closingTimeoutMs = Long.MAX_VALUE;
        final CountDownLatch blockOnCloseLatch = new CountDownLatch(1);
        // Let the user coordinator block on close.
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider(blockOnCloseLatch);
        MockOperatorCoordinatorContext context = new MockOperatorCoordinatorContext(OPERATOR_ID, 2);
        RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) provider.create(context, closingTimeoutMs);

        // Set up the testing variables.
        final byte[] restoredState = new byte[0];
        final TestingEvent testingEvent = new TestingEvent();
        final long completedCheckpointId = 1234L;

        // Reset the coordinator which closes the current internal coordinator
        // and then create a new one. The closing of the current internal
        // coordinator will block until the blockOnCloseLatch is pulled.
        coordinator.resetToCheckpoint(2L, restoredState);

        // The following method calls should be applied to the new internal
        // coordinator asynchronously because the current coordinator has not
        // been successfully closed yet.
        coordinator.handleEventFromOperator(1, 0, testingEvent);
        coordinator.executionAttemptFailed(1, 0, new Exception("Subtask Failure Exception."));
        coordinator.notifyCheckpointComplete(completedCheckpointId);

        // The new coordinator should not have been created because the resetToCheckpoint()
        // should block on closing the current coordinator.
        assertThat(provider.getCreatedCoordinators()).hasSize(1);

        // Now unblock the closing of the current coordinator.
        blockOnCloseLatch.countDown();

        // Take a checkpoint on the coordinator after reset.
        CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        coordinator.checkpointCoordinator(5678L, checkpointFuture);
        coordinator.waitForAllAsyncCallsFinish();

        // Verify that the methods calls have been made against the new coordinator.
        TestingOperatorCoordinator internalCoordinatorAfterReset =
                getInternalCoordinator(coordinator);
        // The internal coordinator after reset should have triggered a new checkpoint.
        assertThat(internalCoordinatorAfterReset.getLastTriggeredCheckpoint())
                .isEqualTo(checkpointFuture);
        // The internal coordinator after reset should be the second coordinator created by the
        // provider.
        assertThat(internalCoordinatorAfterReset)
                .isEqualTo(provider.getCreatedCoordinators().get(1));
        // The internal coordinator after reset should have been reset to the restored state.
        assertThat(internalCoordinatorAfterReset.getLastRestoredCheckpointState())
                .isEqualTo(restoredState);
        // The internal coordinator after reset should have received the testing event.
        assertThat(internalCoordinatorAfterReset.getNextReceivedOperatorEvent())
                .isEqualTo(testingEvent);
        // The internal coordinator after reset should have handled the failure of subtask 1.
        assertThat(internalCoordinatorAfterReset.getFailedTasks())
                .isEqualTo(Collections.singletonList(1));
        // The internal coordinator after reset should have the completedCheckpointId.
        assertThat(internalCoordinatorAfterReset.getLastCheckpointComplete())
                .isEqualTo(completedCheckpointId);
    }

    @Test
    @Timeout(30)
    void testConsecutiveResetToCheckpoint() throws Exception {
        final long closingTimeoutMs = Long.MAX_VALUE;
        final int numResets = 1000;
        // Let the user coordinator block on close.
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider();
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) provider.create(context, closingTimeoutMs);

        // Loop to get some interleaved method invocations on multiple instances
        // of active coordinators.
        for (int i = 0; i < numResets; i++) {
            coordinator.handleEventFromOperator(1, 0, new TestingEvent(i));
            coordinator.executionAttemptFailed(i, 0, new Exception());
            CompletableFuture<byte[]> future = CompletableFuture.completedFuture(new byte[i]);
            coordinator.checkpointCoordinator(i, future);
            final int loop = i;
            future.thenRun(() -> coordinator.notifyCheckpointComplete(loop));
            // The reset bytes has a length of i+1 here because this will be reset to the
            // next internal coordinator.
            coordinator.resetToCheckpoint(i, new byte[i + 1]);
        }

        coordinator.waitForAllAsyncCallsFinish();

        // Verify that the methods calls have been made against the coordinators.
        for (TestingOperatorCoordinator internalCoordinator : provider.getCreatedCoordinators()) {
            // The indexOfCoordinator is set to 0 by default because:
            // 1. For the initial internal coordinator, its index is 0.
            // 2. For all the subsequent internal coordinators, there are two cases:
            //    a. they have processed at least one method call. In that case the coordinator
            //       must have been restored to the given state. So the indexOfCoordinator will
            //       be updated correctly.
            //    b. no method call was processed. In this case the indexOfCoordinator does not
            //       matter because all the fields will either be empty or null.
            int indexOfCoordinator = 0;
            byte[] lastRestoredState = internalCoordinator.getLastRestoredCheckpointState();
            if (lastRestoredState != null) {
                indexOfCoordinator = lastRestoredState.length;
            }
            TestingEvent testingEvent =
                    (TestingEvent) internalCoordinator.getNextReceivedOperatorEvent();
            List<Integer> failedTasks = internalCoordinator.getFailedTasks();

            final int finalIndexOfCoordinator = indexOfCoordinator;
            assertThat(testingEvent)
                    .satisfiesAnyOf(
                            x -> assertThat(x).isNull(),
                            x -> assertThat(x.getId()).isEqualTo(finalIndexOfCoordinator));
            assertThat(failedTasks)
                    .satisfiesAnyOf(
                            x -> assertThat(x).isEmpty(),
                            x ->
                                    assertThat(x)
                                            .hasSize(1)
                                            .element(0)
                                            .isEqualTo(finalIndexOfCoordinator));
            assertThat(internalCoordinator)
                    .satisfiesAnyOf(
                            x -> assertThat(x.hasCompleteCheckpoint()).isFalse(),
                            x ->
                                    assertThat(x.getLastCheckpointComplete())
                                            .isEqualTo(finalIndexOfCoordinator));
            assertThat(internalCoordinator)
                    .satisfiesAnyOf(
                            x -> assertThat(x.hasTriggeredCheckpoint()).isFalse(),
                            x ->
                                    assertThat(x.getLastTriggeredCheckpoint().get())
                                            .hasSize(finalIndexOfCoordinator));
        }
        coordinator.close();
        TestingOperatorCoordinator internalCoordinator = getInternalCoordinator(coordinator);
        CommonTestUtils.waitUtil(
                internalCoordinator::isClosed,
                Duration.ofSeconds(5),
                "Timed out when waiting for the coordinator to close.");
    }

    @Test
    void testNotifyCheckpointAbortedSuccess() throws Exception {
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider(null);
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator coordinator = createCoordinator(provider, context);
        TestingOperatorCoordinator internalCoordinatorAfterReset =
                getInternalCoordinator(coordinator);

        long checkpointId = 10L;
        coordinator.notifyCheckpointAborted(checkpointId);
        assertThat(internalCoordinatorAfterReset.getLastCheckpointAborted())
                .isEqualTo(checkpointId);
    }

    // ---------------

    private RecreateOnResetOperatorCoordinator createCoordinator(
            RecreateOnResetOperatorCoordinator.Provider provider,
            OperatorCoordinator.Context context)
            throws Exception {
        return (RecreateOnResetOperatorCoordinator) provider.create(context);
    }

    private RecreateOnResetOperatorCoordinator createCoordinator(
            TestingCoordinatorProvider provider,
            OperatorCoordinator.Context context,
            ThreadFactory stateReaderThreads)
            throws Exception {
        return (RecreateOnResetOperatorCoordinator)
                provider.create(context, 60_000L, stateReaderThreads);
    }

    /** Creates the threads of the state reader and records them in the order they are created. */
    private static class RecordingThreadFactory implements ThreadFactory {
        private final List<Thread> threads = new CopyOnWriteArrayList<>();
        private final CountDownLatch heldBack = new CountDownLatch(1);
        private volatile int heldBackIndex = -1;

        @Override
        public synchronized Thread newThread(Runnable runnable) {
            final Runnable task =
                    threads.size() == heldBackIndex
                            ? () -> {
                                try {
                                    heldBack.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                                runnable.run();
                            }
                            : runnable;
            final Thread thread = new Thread(task);
            // like the coordinator's own reader threads, so that a failed test leaves none behind
            thread.setDaemon(true);
            threads.add(thread);
            return thread;
        }

        /** Makes the thread created with the given index wait for {@link #release()} to run. */
        private void holdBack(int index) {
            heldBackIndex = index;
        }

        private void release() {
            heldBack.countDown();
        }

        private int size() {
            return threads.size();
        }

        private Thread get(int index) {
            return threads.get(index);
        }
    }

    /** Whether the reader is parked waiting for the previous reset to finish. */
    private static boolean isWaitingForPreviousReset(Thread reader) {
        final Thread.State state = reader.getState();
        return (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
                && Arrays.stream(reader.getStackTrace())
                        .anyMatch(frame -> frame.getMethodName().equals("awaitPreviousReset"));
    }

    private TestingOperatorCoordinator getInternalCoordinator(
            RecreateOnResetOperatorCoordinator coordinator) throws Exception {
        return (TestingOperatorCoordinator) coordinator.getInternalCoordinator();
    }

    // ---------------

    /** Creates coordinators whose restore to the given checkpoint blocks until released. */
    private static class BlockingRestoreCoordinatorProvider
            extends RecreateOnResetOperatorCoordinator.Provider {

        private static final long serialVersionUID = 1L;

        private final long blockedCheckpointId;
        private final CountDownLatch restoring;
        private final CountDownLatch restored;

        private BlockingRestoreCoordinatorProvider(
                long blockedCheckpointId, CountDownLatch restoring, CountDownLatch restored) {
            super(OPERATOR_ID);
            this.blockedCheckpointId = blockedCheckpointId;
            this.restoring = restoring;
            this.restored = restored;
        }

        @Override
        protected OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
            final TestingOperatorCoordinator coordinator =
                    new TestingOperatorCoordinator(context, null);
            coordinator.setResetToCheckpointConsumer(
                    (checkpointId, ignored) -> {
                        if (checkpointId == blockedCheckpointId) {
                            restoring.countDown();
                            try {
                                restored.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    });
            return coordinator;
        }
    }

    /**
     * Creates coordinators that throw the given error when one is created again, or otherwise when
     * one is restored.
     */
    private static class ErrorOnRestoreCoordinatorProvider
            extends RecreateOnResetOperatorCoordinator.Provider {

        private static final long serialVersionUID = 1L;

        private final Error error;
        private final boolean whileCreating;
        private final AtomicInteger created = new AtomicInteger();
        private transient volatile Thread failingThread;

        private ErrorOnRestoreCoordinatorProvider(Error error, boolean whileCreating) {
            super(OPERATOR_ID);
            this.error = error;
            this.whileCreating = whileCreating;
        }

        @Override
        protected OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
            if (created.getAndIncrement() > 0 && whileCreating) {
                failingThread = Thread.currentThread();
                throw error;
            }
            final TestingOperatorCoordinator coordinator =
                    new TestingOperatorCoordinator(context, null);
            coordinator.setResetToCheckpointConsumer(
                    (checkpointId, ignored) -> {
                        failingThread = Thread.currentThread();
                        throw error;
                    });
            return coordinator;
        }
    }

    @SuppressWarnings("serial")
    private static class TestingCoordinatorProvider
            extends RecreateOnResetOperatorCoordinator.Provider {

        private final CountDownLatch blockOnCloseLatch;
        private final List<TestingOperatorCoordinator> createdCoordinators;

        public TestingCoordinatorProvider() {
            this(null);
        }

        public TestingCoordinatorProvider(CountDownLatch blockOnCloseLatch) {
            super(OPERATOR_ID);
            this.blockOnCloseLatch = blockOnCloseLatch;
            this.createdCoordinators = new CopyOnWriteArrayList<>();
        }

        @Override
        protected OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
            TestingOperatorCoordinator testingCoordinator =
                    new TestingOperatorCoordinator(context, blockOnCloseLatch);
            createdCoordinators.add(testingCoordinator);
            return testingCoordinator;
        }

        private List<TestingOperatorCoordinator> getCreatedCoordinators() {
            return createdCoordinators;
        }
    }

    /**
     * State whose reads block until released, even if the stream is closed, and then return the
     * given bytes or fail.
     */
    private static class BlockingStateHandle implements StreamStateHandle {
        private static final long serialVersionUID = 1L;
        private final byte[] bytes;
        private final boolean fail;
        private final CountDownLatch reading = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        private volatile Thread readingThread;

        private BlockingStateHandle(byte[] bytes, boolean fail) {
            this.bytes = bytes;
            this.fail = fail;
        }

        /** Lets the read finish and waits until the thread that read has finished. */
        private void release() throws Exception {
            waitUntilCondition(() -> readingThread != null);
            released.countDown();
            readingThread.join();
        }

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            readingThread = Thread.currentThread();
            final FSDataInputStream data =
                    new ByteStreamStateHandle("state", bytes).openInputStream();
            return new FSDataInputStream() {
                @Override
                public int read() throws IOException {
                    reading.countDown();
                    try {
                        released.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    if (fail) {
                        throw new IOException("Expected test failure");
                    }
                    return data.read();
                }

                @Override
                public void close() {
                    closed.countDown();
                }

                @Override
                public void seek(long desired) throws IOException {
                    data.seek(desired);
                }

                @Override
                public long getPos() throws IOException {
                    return data.getPos();
                }
            };
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        @Override
        public PhysicalStateHandleID getStreamStateHandleID() {
            return new PhysicalStateHandleID("blocking");
        }

        @Override
        public void discardState() {}

        @Override
        public long getStateSize() {
            return bytes.length;
        }
    }

    /** State whose stream opens once allowed to, and whose reads block until it is closed. */
    private static class StoppableStateHandle implements StreamStateHandle {
        private static final long serialVersionUID = 1L;
        private final CountDownLatch opened = new CountDownLatch(1);
        private final BlockingInputStream stream = new BlockingInputStream();
        private volatile Thread readingThread;

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            readingThread = Thread.currentThread();
            try {
                opened.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            return stream;
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        @Override
        public PhysicalStateHandleID getStreamStateHandleID() {
            return new PhysicalStateHandleID("stoppable");
        }

        @Override
        public void discardState() {}

        @Override
        public long getStateSize() {
            return 0;
        }
    }

    /** A stream whose reads block until it is closed, and then fail. */
    private static class BlockingInputStream extends FSDataInputStream {
        private final CountDownLatch reading = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        private volatile Thread closingThread;

        @Override
        public int read() throws IOException {
            reading.countDown();
            try {
                closed.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            throw new IOException("The stream is closed.");
        }

        @Override
        public void close() {
            closingThread = Thread.currentThread();
            closed.countDown();
        }

        @Override
        public void seek(long desired) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPos() {
            return 0;
        }
    }

    /** A state handle whose stream cannot be opened because of the given error. */
    private static class FailingStateHandle implements StreamStateHandle {
        private static final long serialVersionUID = 1L;
        private final Throwable error;
        private volatile Thread.UncaughtExceptionHandler handlerOfReadingThread;
        private volatile Thread readingThread;

        private FailingStateHandle(Throwable error) {
            this.error = error;
        }

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            // a thread drops its handler when it ends, so record it while the thread runs
            handlerOfReadingThread = Thread.currentThread().getUncaughtExceptionHandler();
            readingThread = Thread.currentThread();
            if (error instanceof IOException) {
                throw (IOException) error;
            }
            throw (Error) error;
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        @Override
        public PhysicalStateHandleID getStreamStateHandleID() {
            return new PhysicalStateHandleID("failing-state");
        }

        @Override
        public void discardState() {}

        @Override
        public long getStateSize() {
            return 4096;
        }
    }

    /** Records the thread that reads the state. */
    private static class ThreadRecordingStateHandle implements StreamStateHandle {
        private static final long serialVersionUID = 1L;
        private final StreamStateHandle delegate;
        private volatile Thread readingThread;

        private ThreadRecordingStateHandle(StreamStateHandle delegate) {
            this.delegate = delegate;
        }

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            readingThread = Thread.currentThread();
            return delegate.openInputStream();
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        @Override
        public PhysicalStateHandleID getStreamStateHandleID() {
            return delegate.getStreamStateHandleID();
        }

        @Override
        public void discardState() throws Exception {
            delegate.discardState();
        }

        @Override
        public long getStateSize() {
            return delegate.getStateSize();
        }
    }

    private static class TestingEvent implements OperatorEvent {
        private static final long serialVersionUID = -3289352911927668275L;
        private final int id;

        private TestingEvent() {
            this(-1);
        }

        private TestingEvent(int id) {
            this.id = id;
        }

        private int getId() {
            return id;
        }
    }
}
