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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGatewayDecoratorBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.function.TriFunction;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;

/**
 * A test suite for source enumerator (operator coordinator) for situations where RPC calls for
 * split assignments (operator events) fails from time to time.
 */
public class OperatorEventSendingCheckpointITCase extends TestLogger {

    private static final int PARALLELISM = 1;
    private static MiniCluster flinkCluster;

    @BeforeClass
    public static void setupMiniClusterAndEnv() throws Exception {
        Configuration config = new Configuration();
        // uncomment to run test with adaptive scheduler
        // config.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        flinkCluster = new MiniClusterWithRpcIntercepting(PARALLELISM, config);
        flinkCluster.start();
        TestStreamEnvironment.setAsContext(flinkCluster, PARALLELISM);
    }

    @AfterClass
    public static void clearEnvAndStopMiniCluster() throws Exception {
        TestStreamEnvironment.unsetAsContext();
        if (flinkCluster != null) {
            flinkCluster.close();
            flinkCluster = null;
        }
    }

    // ------------------------------------------------------------------------
    //  tests
    // ------------------------------------------------------------------------

    /**
     * Every second assign split event is lost. Eventually, the enumerator must recognize that an
     * event was lost and trigger recovery to prevent data loss. Data loss would manifest in a
     * stalled test, because we could wait forever to collect the required number of events back.
     */
    @Test
    public void testOperatorEventLostNoReaderFailure() throws Exception {
        final int[] eventsToLose = new int[] {2, 4, 6};

        OpEventRpcInterceptor.currentHandler =
                new OperatorEventRpcHandler(
                        (task, operator, event, originalRpcHandler) -> askTimeoutFuture(),
                        eventsToLose);

        runTest(false);
    }

    /**
     * First and third assign split events are lost. In the middle of all events being processed
     * (which is after the second successful event delivery, the fourth event), there is
     * additionally a failure on the reader that triggers recovery.
     */
    @Test
    public void testOperatorEventLostWithReaderFailure() throws Exception {
        final int[] eventsToLose = new int[] {1, 3};

        OpEventRpcInterceptor.currentHandler =
                new OperatorEventRpcHandler(
                        (task, operator, event, originalRpcHandler) -> askTimeoutFuture(),
                        eventsToLose);

        runTest(true);
    }

    /**
     * This test the case that the enumerator must handle the case of presumably lost splits that
     * were actually delivered.
     *
     * <p>Some split assignment events happen normally, but for some their acknowledgement never
     * comes back. The enumerator must assume the assignments were unsuccessful, even though the
     * split assignment was received by the reader.
     */
    @Test
    public void testOperatorEventAckLost() throws Exception {
        final int[] eventsWithLostAck = new int[] {2, 4};

        OpEventRpcInterceptor.currentHandler =
                new OperatorEventRpcHandler(
                        (task, operator, event, originalRpcHandler) -> {
                            // forward call
                            originalRpcHandler.apply(task, operator, event);
                            // but return an ack future that times out to simulate lost response
                            return askTimeoutFuture();
                        },
                        eventsWithLostAck);

        runTest(false);
    }

    /**
     * This tests the case where the status of an assignment remains unknown across checkpoints.
     *
     * <p>Some split assignment events happen normally, but for some their acknowledgement comes
     * very late, so that we expect multiple checkpoints would have normally happened in the
     * meantime. We trigger a failure (which happens after the second split)
     */
    @Test
    public void testOperatorEventAckDelay() throws Exception {
        final int[] eventsWithLateAck = new int[] {2, 4};

        OpEventRpcInterceptor.currentHandler =
                new OperatorEventRpcHandler(
                        (task, operator, event, originalRpcHandler) -> {
                            // forward call
                            final CompletableFuture<Acknowledge> result =
                                    originalRpcHandler.apply(task, operator, event);
                            // but return an ack future that completes late, after
                            // multiple checkpoints should have happened
                            final CompletableFuture<Acknowledge> late = lateFuture();
                            return result.thenCompose((v) -> late);
                        },
                        eventsWithLateAck);

        runTest(false);
    }

    /**
     * Runs the test program, which uses a single reader (parallelism = 1) and has three splits of
     * data, to be assigned to the same reader.
     *
     * <p>If an intermittent failure should happen, it will happen after the second split was
     * assigned.
     */
    private void runTest(boolean intermittentFailure) throws Exception {
        final int numElements = 100;
        final int failAt = intermittentFailure ? numElements / 2 : numElements * 2;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(50);

        // This test depends on checkpoints persisting progress from the source before the
        // artificial exception gets triggered. Otherwise, the job will run for a long time (or
        // forever) because the exception will be thrown before any checkpoint successfully
        // completes.
        //
        // Checkpoints are triggered once the checkpoint scheduler gets started + a random initial
        // delay. For DefaultScheduler, this mechanism is fine, because DS starts the checkpoint
        // coordinator, then requests the required slots and then deploys the tasks. These
        // operations take enough time to have a checkpoint triggered by the time the task starts
        // running. AdaptiveScheduler starts the CheckpointCoordinator right before deploying tasks
        // (when slots are available already), hence tasks will start running almost immediately,
        // and the checkpoint gets triggered too late (it won't be able to complete before the
        // artificial failure from this test)
        // Therefore, the TestingNumberSequenceSource waits for a checkpoint before emitting all
        // required messages.

        final DataStream<Long> numbers =
                env.fromSource(
                                new TestingNumberSequenceSource(1L, numElements, 3),
                                WatermarkStrategy.noWatermarks(),
                                "numbers")
                        .map(
                                new MapFunction<Long, Long>() {
                                    private int num;

                                    @Override
                                    public Long map(Long value) throws Exception {
                                        if (++num > failAt) {
                                            throw new Exception("Artificial intermittent failure.");
                                        }
                                        return value;
                                    }
                                });

        final List<Long> sequence = numbers.executeAndCollect(numElements);
        // the recovery may change the order of splits, so the sequence might be out-of-order
        sequence.sort(Long::compareTo);

        final List<Long> expectedSequence =
                LongStream.rangeClosed(1L, numElements).boxed().collect(Collectors.toList());

        assertEquals(expectedSequence, sequence);
    }

    private static CompletableFuture<Acknowledge> askTimeoutFuture() {
        final CompletableFuture<Acknowledge> future = new CompletableFuture<>();
        FutureUtils.orTimeout(future, 500, TimeUnit.MILLISECONDS);
        return future;
    }

    private static CompletableFuture<Acknowledge> lateFuture() {
        final CompletableFuture<Acknowledge> future = new CompletableFuture<>();
        FutureUtils.completeDelayed(future, Acknowledge.get(), Duration.ofMillis(500));
        return future;
    }

    // ------------------------------------------------------------------------
    //  Specialized Source
    // ------------------------------------------------------------------------

    /**
     * This is an enumerator for the {@link NumberSequenceSource}, which only responds to the split
     * requests after the next checkpoint is complete. That way, we naturally draw the split
     * processing across checkpoints without artificial sleep statements.
     */
    private static final class AssignAfterCheckpointEnumerator<
                    SplitT extends IteratorSourceSplit<?, ?>>
            extends IteratorSourceEnumerator<SplitT> {
        private final Queue<Integer> pendingRequests = new ArrayDeque<>();
        private final SplitEnumeratorContext<?> context;

        public AssignAfterCheckpointEnumerator(
                SplitEnumeratorContext<SplitT> context, Collection<SplitT> splits) {
            super(context, splits);
            this.context = context;
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            pendingRequests.add(subtaskId);
        }

        @Override
        public Collection<SplitT> snapshotState(long checkpointId) throws Exception {
            // this will be enqueued in the enumerator thread, so it will actually run after this
            // method (the snapshot operation) is complete!
            context.runInCoordinatorThread(this::fullFillPendingRequests);

            return super.snapshotState(checkpointId);
        }

        private void fullFillPendingRequests() {
            for (int subtask : pendingRequests) {
                // respond only to requests for which we still have registered readers
                if (!context.registeredReaders().containsKey(subtask)) {
                    continue;
                }
                super.handleSplitRequest(subtask, null);
            }
            pendingRequests.clear();
        }
    }

    private static class TestingNumberSequenceSource extends NumberSequenceSource {
        private static final long serialVersionUID = 1L;

        private final int numSplits;
        private final long numAllowedMessageBeforeCheckpoint;

        public TestingNumberSequenceSource(long from, long to, int numSplits) {
            super(from, to);
            this.numSplits = numSplits;
            this.numAllowedMessageBeforeCheckpoint = (to - from) / numSplits;
        }

        @Override
        public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>>
                createEnumerator(final SplitEnumeratorContext<NumberSequenceSplit> enumContext) {
            final List<NumberSequenceSplit> splits =
                    splitNumberRange(getFrom(), getTo(), numSplits);
            return new AssignAfterCheckpointEnumerator<>(enumContext, splits);
        }

        @Override
        public SourceReader<Long, NumberSequenceSplit> createReader(
                SourceReaderContext readerContext) {
            return new CheckpointListeningIteratorSourceReader<>(
                    readerContext, numAllowedMessageBeforeCheckpoint);
        }
    }

    private static class CheckpointListeningIteratorSourceReader<
                    E, IterT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IterT>>
            extends IteratorSourceReader<E, IterT, SplitT> {
        private boolean checkpointed = false;
        private long messagesProduced = 0;
        private final long numAllowedMessageBeforeCheckpoint;

        public CheckpointListeningIteratorSourceReader(
                SourceReaderContext context, long waitForCheckpointAfterMessages) {
            super(context);
            this.numAllowedMessageBeforeCheckpoint = waitForCheckpointAfterMessages;
        }

        @Override
        public InputStatus pollNext(ReaderOutput<E> output) {
            if (messagesProduced < numAllowedMessageBeforeCheckpoint || checkpointed) {
                messagesProduced++;
                return super.pollNext(output);
            } else {
                return InputStatus.NOTHING_AVAILABLE;
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            checkpointed = true;
        }
    }

    // ------------------------------------------------------------------------
    //  Source Operator Event specific intercepting
    // ------------------------------------------------------------------------

    private static class OperatorEventRpcHandler {

        private final FilteredRpcAction actionForFilteredEvent;
        private final Set<Integer> eventsToFilter;
        private int eventNum;

        OperatorEventRpcHandler(FilteredRpcAction actionForFilteredEvent, int... eventsToFilter) {
            this(
                    actionForFilteredEvent,
                    IntStream.of(eventsToFilter).boxed().collect(Collectors.toSet()));
        }

        OperatorEventRpcHandler(
                FilteredRpcAction actionForFilteredEvent, Set<Integer> eventsToFilter) {
            this.actionForFilteredEvent = actionForFilteredEvent;
            this.eventsToFilter = eventsToFilter;
        }

        CompletableFuture<Acknowledge> filterCall(
                ExecutionAttemptID task,
                OperatorID operator,
                SerializedValue<OperatorEvent> evt,
                TriFunction<
                                ExecutionAttemptID,
                                OperatorID,
                                SerializedValue<OperatorEvent>,
                                CompletableFuture<Acknowledge>>
                        rpcHandler) {

            final Object o;
            try {
                o = evt.deserializeValue(getClass().getClassLoader());
            } catch (Exception e) {
                throw new Error(e); // should never happen
            }

            if (o instanceof AddSplitEvent || o instanceof NoMoreSplitsEvent) {
                // only deal with split related events here
                if (eventsToFilter.contains(++eventNum)) {
                    return actionForFilteredEvent.handleEvent(task, operator, evt, rpcHandler);
                }
            }

            return rpcHandler.apply(task, operator, evt);
        }

        interface FilteredRpcAction {

            CompletableFuture<Acknowledge> handleEvent(
                    ExecutionAttemptID task,
                    OperatorID operator,
                    SerializedValue<OperatorEvent> evt,
                    TriFunction<
                                    ExecutionAttemptID,
                                    OperatorID,
                                    SerializedValue<OperatorEvent>,
                                    CompletableFuture<Acknowledge>>
                            rpcHandler);
        }
    }

    // ------------------------------------------------------------------------
    //  Utils for MiniCluster RPC intercepting
    // ------------------------------------------------------------------------

    private static final class OpEventRpcInterceptor extends TaskExecutorGatewayDecoratorBase {

        // initialize with a handler that filters nothing
        static OperatorEventRpcHandler currentHandler =
                new OperatorEventRpcHandler((task, id, evt, rpc) -> null, Collections.emptySet());

        OpEventRpcInterceptor(TaskExecutorGateway originalGateway) {
            super(originalGateway);
        }

        @Override
        public CompletableFuture<Acknowledge> sendOperatorEventToTask(
                ExecutionAttemptID task, OperatorID operator, SerializedValue<OperatorEvent> evt) {
            return currentHandler.filterCall(task, operator, evt, super::sendOperatorEventToTask);
        }
    }

    private static class InterceptingRpcService implements RpcService {

        private final RpcService rpcService;

        public InterceptingRpcService(RpcService rpcService) {
            this.rpcService = rpcService;
        }

        @Override
        public String getAddress() {
            return rpcService.getAddress();
        }

        @Override
        public int getPort() {
            return rpcService.getPort();
        }

        @Override
        public <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz) {
            final CompletableFuture<C> future = rpcService.connect(address, clazz);
            return clazz == TaskExecutorGateway.class ? decorateTmGateway(future) : future;
        }

        @Override
        public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
                String address, F fencingToken, Class<C> clazz) {
            return rpcService.connect(address, fencingToken, clazz);
        }

        @Override
        public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
            return rpcService.startServer(rpcEndpoint);
        }

        @Override
        public <F extends Serializable> RpcServer fenceRpcServer(
                RpcServer rpcServer, F fencingToken) {
            return rpcService.fenceRpcServer(rpcServer, fencingToken);
        }

        @Override
        public void stopServer(RpcServer selfGateway) {
            rpcService.stopServer(selfGateway);
        }

        @Override
        public CompletableFuture<Void> stopService() {
            return rpcService.stopService();
        }

        @Override
        public CompletableFuture<Void> getTerminationFuture() {
            return rpcService.getTerminationFuture();
        }

        @Override
        public ScheduledExecutor getScheduledExecutor() {
            return rpcService.getScheduledExecutor();
        }

        @Override
        public ScheduledFuture<?> scheduleRunnable(Runnable runnable, long delay, TimeUnit unit) {
            return rpcService.scheduleRunnable(runnable, delay, unit);
        }

        @Override
        public void execute(Runnable runnable) {
            rpcService.execute(runnable);
        }

        @Override
        public <T> CompletableFuture<T> execute(Callable<T> callable) {
            return rpcService.execute(callable);
        }

        @SuppressWarnings("unchecked")
        private <C extends RpcGateway> CompletableFuture<C> decorateTmGateway(
                CompletableFuture<C> future) {
            final CompletableFuture<TaskExecutorGateway> wrapped =
                    future.thenApply(
                            (gateway) -> new OpEventRpcInterceptor((TaskExecutorGateway) gateway));
            return (CompletableFuture<C>) wrapped;
        }
    }

    private static class MiniClusterWithRpcIntercepting extends MiniCluster {

        private boolean localRpcCreated;

        public MiniClusterWithRpcIntercepting(
                final int numSlots, final Configuration configuration) {
            super(
                    new MiniClusterConfiguration.Builder()
                            .setRpcServiceSharing(RpcServiceSharing.SHARED)
                            .setNumTaskManagers(1)
                            .setConfiguration(configuration)
                            .setNumSlotsPerTaskManager(numSlots)
                            .build());
        }

        @Override
        public void start() throws Exception {
            super.start();

            if (!localRpcCreated) {
                throw new Exception(
                        "MiniClusterWithRpcIntercepting is broken, the intercepting local RPC service was not created.");
            }
        }

        @Override
        protected RpcService createLocalRpcService(Configuration configuration, RpcSystem rpcSystem)
                throws Exception {
            localRpcCreated = true;

            return new InterceptingRpcService(
                    rpcSystem
                            .localServiceBuilder(configuration)
                            .withExecutorConfiguration(
                                    RpcUtils.getTestForkJoinExecutorConfiguration())
                            .createAndStart());
        }
    }
}
