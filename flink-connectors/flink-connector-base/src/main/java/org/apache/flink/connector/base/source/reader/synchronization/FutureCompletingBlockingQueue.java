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

package org.apache.flink.connector.base.source.reader.synchronization;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.concurrent.GuardedBy;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A custom implementation of blocking queue in combination with a {@link CompletableFuture} that is
 * used in the hand-over of data from a producing thread to a consuming thread. This
 * FutureCompletingBlockingQueue has the following features:
 *
 * <h3>Consumer Notifications</h3>
 *
 * <p>Rather than letting consumers block on the {@link #take()} method, or have them poll the
 * {@link #poll()} method, this queue offers a {@link CompletableFuture}, obtained via the {@link
 * #getAvailabilityFuture()} method) that gets completed whenever the queue is non-empty. A consumer
 * can thus subscribe to asynchronous notifications for availability by adding a handler to the
 * obtained {@code CompletableFuture}.
 *
 * <p>The future may also be completed by an explicit call to {@link #notifyAvailable()}. That way
 * the consumer may be notified of a situation/condition without adding an element to the queue.
 *
 * <p>Availability is reset when a call to {@link #poll()} (or {@link #take()} finds an empty queue
 * or results in an empty queue (takes the last element).
 *
 * <p>Note that this model generally assumes that <i>false positives</i> are okay, meaning that the
 * availability future completes despite there being no data availabile in the queue. The consumer
 * is responsible for polling data and obtaining another future to wait on. This is similar to the
 * way that Java's Monitors and Conditions can have the <i>spurious wakeup</i> of the waiting
 * threads and commonly need to be used in loop with the waiting condition.
 *
 * <h3>Producer Wakeup</h3>
 *
 * <p>The queue supports gracefully waking up producing threads that are blocked due to the queue
 * capacity limits, without interrupting the thread. This is done via the {@link
 * #wakeUpPuttingThread(int)} method.
 *
 * @param <T> the type of the elements in the queue.
 */
public class FutureCompletingBlockingQueue<T> {

    /**
     * A constant future that is complete, indicating availability. Using this constant in cases
     * that are guaranteed available helps short-circuiting some checks and avoiding volatile memory
     * operations.
     */
    public static final CompletableFuture<Void> AVAILABLE = getAvailableFuture();

    /** The default capacity for the queue. */
    private static final int DEFAULT_CAPACITY = 2;

    // ------------------------------------------------------------------------

    /** The maximum capacity of the queue. */
    private final int capacity;

    /**
     * The availability future. This doubles as a "non empty" condition. This value is never null.
     */
    private CompletableFuture<Void> currentFuture;

    /** The lock for synchronization. */
    private final Lock lock;

    /** The element queue. */
    @GuardedBy("lock")
    private final Queue<T> queue;

    /** The per-thread conditions that are waiting on putting elements. */
    @GuardedBy("lock")
    private final Queue<Condition> notFull;

    /** The per-thread conditions and wakeUp flags. */
    @GuardedBy("lock")
    private ConditionAndFlag[] putConditionAndFlags;

    public FutureCompletingBlockingQueue() {
        this(DEFAULT_CAPACITY);
    }

    public FutureCompletingBlockingQueue(int capacity) {
        checkArgument(capacity > 0, "capacity must be > 0");
        this.capacity = capacity;
        this.queue = new ArrayDeque<>(capacity);
        this.lock = new ReentrantLock();
        this.putConditionAndFlags = new ConditionAndFlag[1];
        this.notFull = new ArrayDeque<>();

        // initially the queue is empty and thus unavailable
        this.currentFuture = new CompletableFuture<>();
    }

    // ------------------------------------------------------------------------
    //  Future / Notification logic
    // ------------------------------------------------------------------------

    /**
     * Returns the availability future. If the queue is non-empty, then this future will already be
     * complete. Otherwise the obtained future is guaranteed to get completed the next time the
     * queue becomes non-empty, or a notification happens via {@link #notifyAvailable()}.
     *
     * <p>It is important that a completed future is no guarantee that the next call to {@link
     * #poll()} will return a non-null element. If there are concurrent consumer, another consumer
     * may have taken the available element. Or there was no element in the first place, because the
     * future was completed through a call to {@link #notifyAvailable()}.
     *
     * <p>For that reason, it is important to call this method (to obtain a new future) every time
     * again after {@link #poll()} returned null and you want to wait for data.
     */
    public CompletableFuture<Void> getAvailabilityFuture() {
        return currentFuture;
    }

    /**
     * Makes sure the availability future is complete, if it is not complete already. All futures
     * returned by previous calls to {@link #getAvailabilityFuture()} are guaranteed to be
     * completed.
     *
     * <p>All future calls to the method will return a completed future, until the point that the
     * availability is reset via calls to {@link #poll()} that leave the queue empty.
     */
    public void notifyAvailable() {
        lock.lock();
        try {
            moveToAvailable();
        } finally {
            lock.unlock();
        }
    }

    /** Internal utility to make sure that the current future futures are complete (until reset). */
    @GuardedBy("lock")
    private void moveToAvailable() {
        final CompletableFuture<Void> current = currentFuture;
        if (current != AVAILABLE) {
            currentFuture = AVAILABLE;
            current.complete(null);
        }
    }

    /** Makes sure the availability future is incomplete, if it was complete before. */
    @GuardedBy("lock")
    private void moveToUnAvailable() {
        if (currentFuture == AVAILABLE) {
            currentFuture = new CompletableFuture<>();
        }
    }

    // ------------------------------------------------------------------------
    //  Blocking Queue Logic
    // ------------------------------------------------------------------------

    /**
     * Put an element into the queue. The thread blocks if the queue is full.
     *
     * @param threadIndex the index of the thread.
     * @param element the element to put.
     * @return true if the element has been successfully put into the queue, false otherwise.
     * @throws InterruptedException when the thread is interrupted.
     */
    public boolean put(int threadIndex, T element) throws InterruptedException {
        if (element == null) {
            throw new NullPointerException();
        }
        lock.lockInterruptibly();
        try {
            while (queue.size() >= capacity) {
                if (getAndResetWakeUpFlag(threadIndex)) {
                    return false;
                }
                waitOnPut(threadIndex);
            }
            enqueue(element);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * <b>Warning:</b> This is a dangerous method and should only be used for testing convenience. A
     * method that blocks until availability does not go together well with the concept of
     * asynchronous notifications and non-blocking polling.
     *
     * <p>Get and remove the first element from the queue. The call blocks if the queue is empty.
     * The problem with this method is that it may loop internally until an element is available and
     * that way eagerly reset the availability future. If a consumer thread is blocked in taking an
     * element, it will receive availability notifications from {@link #notifyAvailable()} and
     * immediately reset them by calling {@link #poll()} and finding the queue empty.
     *
     * @return the first element in the queue.
     * @throws InterruptedException when the thread is interrupted.
     */
    @VisibleForTesting
    public T take() throws InterruptedException {
        T next;
        while ((next = poll()) == null) {
            // use the future to wait for availability to avoid busy waiting
            try {
                getAvailabilityFuture().get();
            } catch (ExecutionException | CompletionException e) {
                // this should never happen, but we propagate just in case
                throw new FlinkRuntimeException("exception in queue future completion", e);
            }
        }
        return next;
    }

    /**
     * Get and remove the first element from the queue. Null is returned if the queue is empty. If
     * this makes the queue empty (takes the last element) or finds the queue already empty, then
     * this resets the availability notifications. The next call to {@link #getAvailabilityFuture()}
     * will then return a non-complete future that completes only the next time that the queue
     * becomes non-empty or the {@link #notifyAvailable()} method is called.
     *
     * @return the first element from the queue, or Null if the queue is empty.
     */
    public T poll() {
        lock.lock();
        try {
            if (queue.size() == 0) {
                moveToUnAvailable();
                return null;
            }
            return dequeue();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the first element from the queue without removing it.
     *
     * @return the first element in the queue, or Null if the queue is empty.
     */
    public T peek() {
        lock.lock();
        try {
            return queue.peek();
        } finally {
            lock.unlock();
        }
    }

    /** Gets the size of the queue. */
    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    /** Checks whether the queue is empty. */
    public boolean isEmpty() {
        lock.lock();
        try {
            return queue.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Checks the remaining capacity in the queue. That is the difference between the maximum
     * capacity and the current number of elements in the queue.
     */
    public int remainingCapacity() {
        lock.lock();
        try {
            return capacity - queue.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gracefully wakes up the thread with the given {@code threadIndex} if it is blocked in adding
     * an element. to the queue. If the thread is blocked in {@link #put(int, Object)} it will
     * immediately return from the method with a return value of false.
     *
     * <p>If this method is called, the next time the thread with the given index is about to be
     * blocked in adding an element, it may immediately wake up and return.
     *
     * @param threadIndex The number identifying the thread.
     */
    public void wakeUpPuttingThread(int threadIndex) {
        lock.lock();
        try {
            maybeCreateCondition(threadIndex);
            ConditionAndFlag caf = putConditionAndFlags[threadIndex];
            if (caf != null) {
                caf.setWakeUp(true);
                caf.condition().signal();
            }
        } finally {
            lock.unlock();
        }
    }

    // --------------- private helpers -------------------------

    @GuardedBy("lock")
    private void enqueue(T element) {
        final int sizeBefore = queue.size();
        queue.add(element);
        if (sizeBefore == 0) {
            moveToAvailable();
        }
        if (sizeBefore < capacity - 1 && !notFull.isEmpty()) {
            signalNextPutter();
        }
    }

    @GuardedBy("lock")
    private T dequeue() {
        final int sizeBefore = queue.size();
        final T element = queue.poll();
        if (sizeBefore == capacity && !notFull.isEmpty()) {
            signalNextPutter();
        }
        if (queue.isEmpty()) {
            moveToUnAvailable();
        }
        return element;
    }

    @GuardedBy("lock")
    private void waitOnPut(int fetcherIndex) throws InterruptedException {
        maybeCreateCondition(fetcherIndex);
        Condition cond = putConditionAndFlags[fetcherIndex].condition();
        notFull.add(cond);
        cond.await();
    }

    @GuardedBy("lock")
    private void signalNextPutter() {
        if (!notFull.isEmpty()) {
            notFull.poll().signal();
        }
    }

    @GuardedBy("lock")
    private void maybeCreateCondition(int threadIndex) {
        if (putConditionAndFlags.length < threadIndex + 1) {
            putConditionAndFlags = Arrays.copyOf(putConditionAndFlags, threadIndex + 1);
        }

        if (putConditionAndFlags[threadIndex] == null) {
            putConditionAndFlags[threadIndex] = new ConditionAndFlag(lock.newCondition());
        }
    }

    @GuardedBy("lock")
    private boolean getAndResetWakeUpFlag(int threadIndex) {
        maybeCreateCondition(threadIndex);
        if (putConditionAndFlags[threadIndex].getWakeUp()) {
            putConditionAndFlags[threadIndex].setWakeUp(false);
            return true;
        }
        return false;
    }

    // --------------- private per thread state ------------

    private static class ConditionAndFlag {
        private final Condition cond;
        private boolean wakeUp;

        private ConditionAndFlag(Condition cond) {
            this.cond = cond;
            this.wakeUp = false;
        }

        private Condition condition() {
            return cond;
        }

        private boolean getWakeUp() {
            return wakeUp;
        }

        private void setWakeUp(boolean value) {
            wakeUp = value;
        }
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Void> getAvailableFuture() {
        // this is a way to obtain the AvailabilityProvider.AVAILABLE future until we decide to
        // move the class from the runtime module to the core module
        try {
            final Class<?> clazz =
                    Class.forName("org.apache.flink.runtime.io.AvailabilityProvider");
            final Field field = clazz.getDeclaredField("AVAILABLE");
            return (CompletableFuture<Void>) field.get(null);
        } catch (Throwable t) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
