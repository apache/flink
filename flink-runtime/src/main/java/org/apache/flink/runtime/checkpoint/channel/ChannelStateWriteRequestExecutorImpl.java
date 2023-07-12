/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Executes {@link ChannelStateWriteRequest}s in a separate thread. Any exception occurred during
 * execution causes this thread to stop and the exception to be re-thrown on any subsequent call.
 */
@ThreadSafe
class ChannelStateWriteRequestExecutorImpl implements ChannelStateWriteRequestExecutor {

    private static final Logger LOG =
            LoggerFactory.getLogger(ChannelStateWriteRequestExecutorImpl.class);

    private final Object lock = new Object();

    private final ChannelStateWriteRequestDispatcher dispatcher;
    private final Thread thread;

    private final int maxSubtasksPerChannelStateFile;

    @GuardedBy("lock")
    private final Deque<ChannelStateWriteRequest> deque;

    @GuardedBy("lock")
    private Exception thrown = null;

    @GuardedBy("lock")
    private boolean wasClosed = false;

    @GuardedBy("lock")
    private final Map<SubtaskID, Queue<ChannelStateWriteRequest>> unreadyQueues = new HashMap<>();

    @GuardedBy("lock")
    private final Set<SubtaskID> subtasks;

    /** Lock this before the {@link #lock} to avoid the deadlock. */
    private final Object registerLock;

    @GuardedBy("registerLock")
    private boolean isRegistering = true;

    @GuardedBy("registerLock")
    private final Consumer<ChannelStateWriteRequestExecutor> onRegistered;

    ChannelStateWriteRequestExecutorImpl(
            ChannelStateWriteRequestDispatcher dispatcher,
            int maxSubtasksPerChannelStateFile,
            Consumer<ChannelStateWriteRequestExecutor> onRegistered,
            Object registerLock) {
        this(
                dispatcher,
                new ArrayDeque<>(),
                maxSubtasksPerChannelStateFile,
                registerLock,
                onRegistered);
    }

    ChannelStateWriteRequestExecutorImpl(
            ChannelStateWriteRequestDispatcher dispatcher,
            Deque<ChannelStateWriteRequest> deque,
            int maxSubtasksPerChannelStateFile,
            Object registerLock,
            Consumer<ChannelStateWriteRequestExecutor> onRegistered) {
        this.dispatcher = dispatcher;
        this.deque = deque;
        this.maxSubtasksPerChannelStateFile = maxSubtasksPerChannelStateFile;
        this.registerLock = registerLock;
        this.onRegistered = onRegistered;
        this.thread = new Thread(this::run, "Channel state writer ");
        this.subtasks = new HashSet<>();
        this.thread.setDaemon(true);
    }

    @VisibleForTesting
    void run() {
        try {
            FileSystemSafetyNet.initializeSafetyNetForThread();
            loop();
        } catch (Exception ex) {
            thrown = ex;
        } finally {
            try {
                closeAll(
                        this::cleanupRequests,
                        () -> {
                            Throwable cause;
                            synchronized (lock) {
                                cause = thrown == null ? new CancellationException() : thrown;
                            }
                            dispatcher.fail(cause);
                        });
            } catch (Exception e) {
                synchronized (lock) {
                    //noinspection NonAtomicOperationOnVolatileField
                    thrown = ExceptionUtils.firstOrSuppressed(e, thrown);
                }
            }
            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
        }
        LOG.debug("loop terminated");
    }

    private void loop() throws Exception {
        while (true) {
            try {
                ChannelStateWriteRequest request;
                synchronized (lock) {
                    request = waitAndTakeUnsafe();
                    if (request == null) {
                        // The executor is closed, so return directly.
                        return;
                    }
                }
                // The executor will end the registration, when the start request comes.
                // Because the checkpoint can be started after all tasks are initiated.
                if (request instanceof CheckpointStartRequest) {
                    synchronized (registerLock) {
                        if (completeRegister()) {
                            onRegistered.accept(this);
                        }
                    }
                }
                dispatcher.dispatch(request);
            } catch (InterruptedException e) {
                synchronized (lock) {
                    if (!wasClosed) {
                        LOG.debug(
                                "Channel state executor is interrupted while waiting for a request (continue waiting)",
                                e);
                    } else {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    /**
     * Retrieves and removes the head request of the {@link #deque}, waiting if necessary until an
     * element becomes available.
     *
     * @return The head request, it can be null when the executor is closed.
     */
    @Nullable
    private ChannelStateWriteRequest waitAndTakeUnsafe() throws InterruptedException {
        ChannelStateWriteRequest request;
        while (!wasClosed) {
            request = deque.pollFirst();
            if (request == null) {
                lock.wait();
            } else {
                return request;
            }
        }
        return null;
    }

    private void cleanupRequests() throws Exception {
        List<ChannelStateWriteRequest> drained;
        Throwable cause;
        synchronized (lock) {
            cause = thrown == null ? new CancellationException() : thrown;
            drained = new ArrayList<>(deque);
            deque.clear();
            for (Queue<ChannelStateWriteRequest> unreadyQueue : unreadyQueues.values()) {
                while (!unreadyQueue.isEmpty()) {
                    drained.add(unreadyQueue.poll());
                }
            }
        }
        LOG.info("discarding {} drained requests", drained.size());
        closeAll(
                drained.stream()
                        .<AutoCloseable>map(request -> () -> request.cancel(cause))
                        .collect(Collectors.toList()));
    }

    @Override
    public void start() throws IllegalStateException {
        this.thread.start();
    }

    @Override
    public void submit(ChannelStateWriteRequest request) throws Exception {
        synchronized (lock) {
            Queue<ChannelStateWriteRequest> unreadyQueue =
                    unreadyQueues.get(
                            SubtaskID.of(request.getJobVertexID(), request.getSubtaskIndex()));
            checkArgument(unreadyQueue != null, "The subtask %s is not yet registered.");
            submitInternal(
                    request,
                    () -> {
                        // 1. unreadyQueue isn't empty, the new request must keep the order, so add
                        // the new request to the unreadyQueue tail.
                        if (!unreadyQueue.isEmpty()) {
                            unreadyQueue.add(request);
                            return;
                        }
                        // 2. unreadyQueue is empty, and new request is ready, so add it to the
                        // readyQueue directly.
                        if (request.getReadyFuture().isDone()) {
                            deque.add(request);
                            lock.notifyAll();
                            return;
                        }
                        // 3. unreadyQueue is empty, and new request isn't ready, so add it to the
                        // unreadyQueue, and register it as the first request.
                        unreadyQueue.add(request);
                        registerFirstRequestFuture(request, unreadyQueue);
                    });
        }
    }

    private void registerFirstRequestFuture(
            @Nonnull ChannelStateWriteRequest firstRequest,
            Queue<ChannelStateWriteRequest> unreadyQueue) {
        assert Thread.holdsLock(lock);
        checkState(firstRequest == unreadyQueue.peek(), "The request isn't the first request.");

        firstRequest
                .getReadyFuture()
                .thenAccept(
                        o -> {
                            synchronized (lock) {
                                moveReadyRequestToReadyQueue(unreadyQueue, firstRequest);
                            }
                        })
                .exceptionally(
                        throwable -> {
                            // When dataFuture is completed, just move the request to readyQueue.
                            // And the throwable doesn't need to be handled here, it will be handled
                            // in channel state writer thread later.
                            synchronized (lock) {
                                moveReadyRequestToReadyQueue(unreadyQueue, firstRequest);
                            }
                            return null;
                        });
    }

    private void moveReadyRequestToReadyQueue(
            Queue<ChannelStateWriteRequest> unreadyQueue, ChannelStateWriteRequest firstRequest) {
        assert Thread.holdsLock(lock);
        checkState(firstRequest == unreadyQueue.peek());
        while (!unreadyQueue.isEmpty()) {
            ChannelStateWriteRequest req = unreadyQueue.peek();
            if (!req.getReadyFuture().isDone()) {
                registerFirstRequestFuture(req, unreadyQueue);
                return;
            }
            deque.add(Objects.requireNonNull(unreadyQueue.poll()));
            lock.notifyAll();
        }
    }

    @Override
    public void submitPriority(ChannelStateWriteRequest request) throws Exception {
        synchronized (lock) {
            checkArgument(
                    unreadyQueues.containsKey(
                            SubtaskID.of(request.getJobVertexID(), request.getSubtaskIndex())),
                    "The subtask %s is not yet registered.");
            checkState(request.getReadyFuture().isDone(), "The priority request must be ready.");
            submitInternal(
                    request,
                    () -> {
                        deque.addFirst(request);
                        lock.notifyAll();
                    });
        }
    }

    private void submitInternal(ChannelStateWriteRequest request, RunnableWithException action)
            throws Exception {
        try {
            action.run();
        } catch (Exception ex) {
            request.cancel(ex);
            throw ex;
        }
        ensureRunning();
    }

    private void ensureRunning() throws Exception {
        assert Thread.holdsLock(lock);
        // this check should be performed *at least after* enqueuing a request
        // checking before is not enough because (check + enqueue) is not atomic
        if (wasClosed || !thread.isAlive()) {
            cleanupRequests();
            IllegalStateException exception = new IllegalStateException("not running");
            if (thrown != null) {
                exception.addSuppressed(thrown);
            }
            throw exception;
        }
    }

    @Override
    public void registerSubtask(JobVertexID jobVertexID, int subtaskIndex) {
        assert Thread.holdsLock(registerLock);

        SubtaskID subtaskID = SubtaskID.of(jobVertexID, subtaskIndex);
        synchronized (lock) {
            checkState(isRegistering(), "This executor has been registered.");
            checkState(
                    !subtasks.contains(subtaskID),
                    String.format("This subtask[%s] has already registered.", subtaskID));
            subtasks.add(subtaskID);
            deque.add(
                    ChannelStateWriteRequest.registerSubtask(
                            subtaskID.getJobVertexID(), subtaskID.getSubtaskIndex()));
            lock.notifyAll();
            unreadyQueues.put(subtaskID, new ArrayDeque<>());
            if (subtasks.size() == maxSubtasksPerChannelStateFile && completeRegister()) {
                onRegistered.accept(this);
            }
        }
    }

    @VisibleForTesting
    public boolean isRegistering() {
        synchronized (registerLock) {
            return isRegistering;
        }
    }

    private boolean completeRegister() {
        assert Thread.holdsLock(registerLock);
        if (isRegistering) {
            isRegistering = false;
            return true;
        }
        return false;
    }

    @Override
    public void releaseSubtask(JobVertexID jobVertexID, int subtaskIndex) throws IOException {
        synchronized (registerLock) {
            synchronized (lock) {
                if (completeRegister()) {
                    onRegistered.accept(this);
                }
                subtasks.remove(SubtaskID.of(jobVertexID, subtaskIndex));
                if (!subtasks.isEmpty()) {
                    return;
                }
                wasClosed = true;
                lock.notifyAll();
            }
        }
        while (thread.isAlive()) {
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                if (!thread.isAlive()) {
                    Thread.currentThread().interrupt();
                }
                LOG.debug(
                        "Channel state executor is interrupted while waiting for the writer thread to die",
                        e);
            }
        }
        synchronized (lock) {
            if (thrown != null) {
                throw new IOException(thrown);
            }
        }
    }

    @VisibleForTesting
    Thread getThread() {
        return thread;
    }
}
