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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.function.SerializableFunction;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/** A simple testing implementation of the {@link OperatorCoordinator}. */
class TestingOperatorCoordinator implements OperatorCoordinator {

    public static final byte[] NULL_RESTORE_VALUE = new byte[0];

    private final OperatorCoordinator.Context context;

    private final ArrayList<Integer> failedTasks = new ArrayList<>();
    private final ArrayList<SubtaskAndCheckpoint> restoredTasks = new ArrayList<>();

    private final CountDownLatch blockOnCloseLatch;

    @Nullable private byte[] lastRestoredCheckpointState;
    private long lastRestoredCheckpointId;

    private final BlockingQueue<CompletableFuture<byte[]>> triggeredCheckpoints;

    private final BlockingQueue<Long> lastCheckpointComplete;

    private final BlockingQueue<OperatorEvent> receivedOperatorEvents;

    private final Map<Integer, SubtaskGateway> subtaskGateways;

    private boolean started;
    private boolean closed;

    public TestingOperatorCoordinator(OperatorCoordinator.Context context) {
        this(context, null);
    }

    public TestingOperatorCoordinator(
            OperatorCoordinator.Context context, CountDownLatch blockOnCloseLatch) {
        this.context = context;
        this.triggeredCheckpoints = new LinkedBlockingQueue<>();
        this.lastCheckpointComplete = new LinkedBlockingQueue<>();
        this.receivedOperatorEvents = new LinkedBlockingQueue<>();
        this.blockOnCloseLatch = blockOnCloseLatch;
        this.subtaskGateways = new HashMap<>();
    }

    // ------------------------------------------------------------------------

    @Override
    public void start() throws Exception {
        started = true;
    }

    @Override
    public void close() throws InterruptedException {
        closed = true;
        if (blockOnCloseLatch != null) {
            blockOnCloseLatch.await();
        }
    }

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) {
        receivedOperatorEvents.add(event);
    }

    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {
        failedTasks.add(subtask);
        subtaskGateways.remove(subtask);
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        restoredTasks.add(new SubtaskAndCheckpoint(subtask, checkpointId));
    }

    @Override
    public void subtaskReady(int subtask, SubtaskGateway gateway) {
        subtaskGateways.put(subtask, gateway);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        boolean added = triggeredCheckpoints.offer(result);
        assert added; // guard the test assumptions
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        lastCheckpointComplete.offer(checkpointId);
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) {
        lastRestoredCheckpointId = checkpointId;
        lastRestoredCheckpointState = checkpointData == null ? NULL_RESTORE_VALUE : checkpointData;
    }

    // ------------------------------------------------------------------------

    public OperatorCoordinator.Context getContext() {
        return context;
    }

    public SubtaskGateway getSubtaskGateway(int subtask) {
        return subtaskGateways.get(subtask);
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isClosed() {
        return closed;
    }

    public List<Integer> getFailedTasks() {
        return failedTasks;
    }

    public List<SubtaskAndCheckpoint> getRestoredTasks() {
        return restoredTasks;
    }

    @Nullable
    public byte[] getLastRestoredCheckpointState() {
        return lastRestoredCheckpointState;
    }

    public long getLastRestoredCheckpointId() {
        return lastRestoredCheckpointId;
    }

    public CompletableFuture<byte[]> getLastTriggeredCheckpoint() throws InterruptedException {
        return triggeredCheckpoints.take();
    }

    public boolean hasTriggeredCheckpoint() {
        return !triggeredCheckpoints.isEmpty();
    }

    public long getLastCheckpointComplete() throws InterruptedException {
        return lastCheckpointComplete.take();
    }

    @Nullable
    public OperatorEvent getNextReceivedOperatorEvent() {
        return receivedOperatorEvents.poll();
    }

    public boolean hasCompleteCheckpoint() throws InterruptedException {
        return !lastCheckpointComplete.isEmpty();
    }

    // ------------------------------------------------------------------------

    public static final class SubtaskAndCheckpoint {

        public final int subtaskIndex;
        public final long checkpointId;

        public SubtaskAndCheckpoint(int subtaskIndex, long checkpointId) {
            this.subtaskIndex = subtaskIndex;
            this.checkpointId = checkpointId;
        }
    }

    // ------------------------------------------------------------------------
    //  The provider for this coordinator implementation
    // ------------------------------------------------------------------------

    /**
     * A testing stub for an {@link OperatorCoordinator.Provider} that creates a {@link
     * TestingOperatorCoordinator}.
     */
    public static final class Provider implements OperatorCoordinator.Provider {

        private static final long serialVersionUID = 1L;

        private final OperatorID operatorId;

        private final SerializableFunction<Context, TestingOperatorCoordinator> factory;

        public Provider(OperatorID operatorId) {
            this(operatorId, TestingOperatorCoordinator::new);
        }

        public Provider(
                OperatorID operatorId,
                SerializableFunction<Context, TestingOperatorCoordinator> factory) {
            this.operatorId = operatorId;
            this.factory = factory;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(OperatorCoordinator.Context context) {
            return factory.apply(context);
        }
    }
}
