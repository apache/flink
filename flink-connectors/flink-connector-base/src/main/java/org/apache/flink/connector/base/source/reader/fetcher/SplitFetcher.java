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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The internal fetcher runnable responsible for polling message from the external system. */
@Internal
public class SplitFetcher<E, SplitT extends SourceSplit> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SplitFetcher.class);

    private final int id;

    @GuardedBy("lock")
    private final Deque<SplitFetcherTask> taskQueue = new ArrayDeque<>();
    // track the assigned splits so we can suspend the reader when there is no splits assigned.
    private final Map<String, SplitT> assignedSplits = new HashMap<>();
    private final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue;
    private final SplitReader<E, SplitT> splitReader;
    private final Consumer<Throwable> errorHandler;
    private final Runnable shutdownHook;

    @GuardedBy("lock")
    private boolean closed;

    @GuardedBy("lock")
    private boolean paused;

    private final FetchTask<E, SplitT> fetchTask;

    @GuardedBy("lock")
    @Nullable
    private SplitFetcherTask runningTask = null;

    private final ReentrantLock lock = new ReentrantLock();

    @GuardedBy("lock")
    private final Condition nonEmpty = lock.newCondition();

    @GuardedBy("lock")
    private final Condition resumed = lock.newCondition();

    private final boolean allowUnalignedSourceSplits;

    private final Consumer<Collection<String>> splitFinishedHook;

    SplitFetcher(
            int id,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            SplitReader<E, SplitT> splitReader,
            Consumer<Throwable> errorHandler,
            Runnable shutdownHook,
            Consumer<Collection<String>> splitFinishedHook,
            boolean allowUnalignedSourceSplits) {
        this.id = id;
        this.elementsQueue = checkNotNull(elementsQueue);
        this.splitReader = checkNotNull(splitReader);
        this.errorHandler = checkNotNull(errorHandler);
        this.shutdownHook = checkNotNull(shutdownHook);
        this.allowUnalignedSourceSplits = allowUnalignedSourceSplits;
        this.splitFinishedHook = splitFinishedHook;

        this.fetchTask =
                new FetchTask<>(
                        splitReader,
                        elementsQueue,
                        ids -> {
                            ids.forEach(assignedSplits::remove);
                            splitFinishedHook.accept(ids);
                            LOG.info("Finished reading from splits {}", ids);
                        },
                        id);
    }

    @Override
    public void run() {
        LOG.info("Starting split fetcher {}", id);
        try {
            while (runOnce()) {
                // nothing to do, everything is inside #runOnce.
            }
        } catch (Throwable t) {
            errorHandler.accept(t);
        } finally {
            try {
                splitReader.close();
            } catch (Exception e) {
                errorHandler.accept(e);
            } finally {
                LOG.info("Split fetcher {} exited.", id);
                // This executes after possible errorHandler.accept(t). If these operations bear
                // a happens-before relation, then we can checking side effect of
                // errorHandler.accept(t)
                // to know whether it happened after observing side effect of shutdownHook.run().
                shutdownHook.run();
            }
        }
    }

    /** Package private method to help unit test. */
    boolean runOnce() {
        // first blocking call = get next task. blocks only if there are no active splits and queued
        // tasks.
        SplitFetcherTask task;
        lock.lock();
        try {
            if (closed) {
                return false;
            }

            task = getNextTaskUnsafe();
            if (task == null) {
                // (spurious) wakeup, so just repeat
                return true;
            }

            LOG.debug("Prepare to run {}", task);
            // store task for #wakeUp
            this.runningTask = task;
        } finally {
            lock.unlock();
        }

        // execute the task outside of lock, so that it can be woken up
        boolean taskFinished;
        try {
            taskFinished = task.run();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "SplitFetcher thread %d received unexpected exception while polling the records",
                            id),
                    e);
        }

        // re-acquire lock as all post-processing steps, need it
        lock.lock();
        try {
            this.runningTask = null;
            processTaskResultUnsafe(task, taskFinished);
        } finally {
            lock.unlock();
        }
        return true;
    }

    private void processTaskResultUnsafe(SplitFetcherTask task, boolean taskFinished) {
        assert lock.isHeldByCurrentThread();
        if (taskFinished) {
            LOG.debug("Finished running task {}", task);
            if (assignedSplits.isEmpty() && taskQueue.isEmpty()) {
                // because the method might get invoked past the point when the source reader
                // last checked the elements queue, we need to notify availability in the case
                // when we become idle
                elementsQueue.notifyAvailable();
            }
        } else if (task != fetchTask) {
            // task was woken up, so repeat
            taskQueue.addFirst(task);
            LOG.debug("Reenqueuing woken task {}", task);
        }
    }

    @Nullable
    private SplitFetcherTask getNextTaskUnsafe() {
        assert lock.isHeldByCurrentThread();
        try {
            if (paused) {
                resumed.await();
                // if it was paused, ensure that fetcher was not shutdown
                return null;
            }
            if (!taskQueue.isEmpty()) {
                // a specific task is avail, so take that in FIFO
                return taskQueue.poll();
            } else if (!assignedSplits.isEmpty()) {
                // use fallback task = fetch if there is at least one split
                return fetchTask;
            } else {
                // nothing to do, wait for signal
                nonEmpty.await();
                return taskQueue.poll();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new RuntimeException(
                    "The thread was interrupted while waiting for a fetcher task.");
        }
    }

    /**
     * Add splits to the split fetcher. This operation is asynchronous.
     *
     * @param splitsToAdd the splits to add.
     */
    public void addSplits(List<SplitT> splitsToAdd) {
        lock.lock();
        try {
            enqueueTaskUnsafe(new AddSplitsTask<>(splitReader, splitsToAdd, assignedSplits));
            wakeUpUnsafe(true);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Notice the split fetcher that some splits finished. This operation is asynchronous.
     *
     * @param splitsToRemove the splits need to be removed.
     */
    public void removeSplits(List<SplitT> splitsToRemove) {
        lock.lock();
        try {
            enqueueTaskUnsafe(
                    new RemoveSplitsTask<>(
                            splitReader, splitsToRemove, assignedSplits, splitFinishedHook));
            wakeUpUnsafe(true);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Called when some splits of this source instance progressed too much beyond the global
     * watermark of all subtasks. If the split reader implements {@link SplitReader}, it will relay
     * the information asynchronously through the split fetcher thread.
     *
     * @param splitsToPause the splits to pause
     * @param splitsToResume the splits to resume
     */
    public void pauseOrResumeSplits(
            Collection<SplitT> splitsToPause, Collection<SplitT> splitsToResume) {
        lock.lock();
        try {
            enqueueTaskUnsafe(
                    new PauseOrResumeSplitsTask<>(
                            splitReader,
                            splitsToPause,
                            splitsToResume,
                            allowUnalignedSourceSplits));
            wakeUpUnsafe(true);
        } finally {
            lock.unlock();
        }
    }

    public void enqueueTask(SplitFetcherTask task) {
        lock.lock();
        try {
            enqueueTaskUnsafe(task);
        } finally {
            lock.unlock();
        }
    }

    private void enqueueTaskUnsafe(SplitFetcherTask task) {
        assert lock.isHeldByCurrentThread();
        taskQueue.add(task);
        nonEmpty.signal();
    }

    public SplitReader<E, SplitT> getSplitReader() {
        return splitReader;
    }

    public int fetcherId() {
        return id;
    }

    /** Shutdown the split fetcher. */
    public void shutdown() {
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                paused = false;
                LOG.info("Shutting down split fetcher {}", id);
                wakeUpUnsafe(false);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Package private for unit test.
     *
     * @return the assigned splits.
     */
    Map<String, SplitT> assignedSplits() {
        return assignedSplits;
    }

    /**
     * Package private for unit test.
     *
     * @return true if task queue is empty, false otherwise.
     */
    boolean isIdle() {
        lock.lock();
        try {
            return assignedSplits.isEmpty() && taskQueue.isEmpty() && runningTask == null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Wake up the fetcher thread. There are only two blocking points in a running fetcher. 1.
     * Waiting for the next task in an idle fetcher. 2. Running a task.
     *
     * <p>They need to be waken up differently. If the fetcher is blocking waiting on the next task
     * in the task queue, we should just notify that a task is available. If the fetcher is running
     * the user split reader, we should call SplitReader.wakeUp() instead.
     *
     * <p>The correctness can be thought of in the following way. The purpose of wake up is to let
     * the fetcher thread go to the very beginning of the running loop.
     */
    void wakeUp(boolean taskOnly) {
        // Synchronize to make sure the wake up only works for the current invocation of runOnce().
        lock.lock();
        try {
            wakeUpUnsafe(taskOnly);
        } finally {
            lock.unlock();
        }
    }

    private void wakeUpUnsafe(boolean taskOnly) {
        assert lock.isHeldByCurrentThread();

        SplitFetcherTask currentTask = runningTask;
        if (currentTask != null) {
            // The running task may have missed our wakeUp flag and running, wake it up.
            LOG.debug("Waking up running task {}", currentTask);
            currentTask.wakeUp();
        } else if (!taskOnly) {
            // The task has not started running yet, and it will not run for this
            // runOnce() invocation due to the wakeUp flag. But we might have to
            // wake up the fetcher thread in case it is blocking on the task queue.
            // Only wake up when the thread has started and there is no running task.
            LOG.debug("Waking up fetcher thread.");
            nonEmpty.signal();
            resumed.signal();
        }
    }

    public void pause() {
        lock.lock();
        try {
            paused = true;
        } finally {
            lock.unlock();
        }
    }

    public void resume() {
        lock.lock();
        try {
            paused = false;
            resumed.signal();
        } finally {
            lock.unlock();
        }
    }
}
