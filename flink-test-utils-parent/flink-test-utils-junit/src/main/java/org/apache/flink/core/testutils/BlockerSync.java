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

package org.apache.flink.core.testutils;

/**
 * A utility to help synchronize two threads in cases where one of them is supposed to reach a
 * blocking state before the other may continue.
 *
 * <p>Use as follows:
 *
 * <pre>{@code
 * final BlockerSync sync = new BlockerSync();
 *
 * // thread to be blocked
 * Runnable toBeBlocked = () -> {
 *     // do something, like acquire a shared resource
 *     sync.blockNonInterruptible();
 *     // release resource
 * }
 *
 * new Thread(toBeBlocked).start();
 * sync.awaitBlocker();
 *
 * // do stuff that requires the other thread to still hold the resource
 * sync.releaseBlocker();
 * }</pre>
 */
public class BlockerSync {

    private final Object lock = new Object();

    private boolean blockerReady;

    private boolean blockerReleased;

    /**
     * Waits until the blocking thread has entered the method {@link #block()} or {@link
     * #blockNonInterruptible()}.
     */
    public void awaitBlocker() throws InterruptedException {
        synchronized (lock) {
            while (!blockerReady) {
                lock.wait();
            }
        }
    }

    /**
     * Blocks until {@link #releaseBlocker()} is called or this thread is interrupted. Notifies the
     * awaiting thread that waits in the method {@link #awaitBlocker()}.
     */
    public void block() throws InterruptedException {
        synchronized (lock) {
            blockerReady = true;
            lock.notifyAll();

            while (!blockerReleased) {
                lock.wait();
            }
        }
    }

    /**
     * Blocks until {@link #releaseBlocker()} is called. Notifies the awaiting thread that waits in
     * the method {@link #awaitBlocker()}.
     */
    public void blockNonInterruptible() {
        synchronized (lock) {
            blockerReady = true;
            lock.notifyAll();

            while (!blockerReleased) {
                try {
                    lock.wait();
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    /** Lets the blocked thread continue. */
    public void releaseBlocker() {
        synchronized (lock) {
            blockerReleased = true;
            lock.notifyAll();
        }
    }
}
