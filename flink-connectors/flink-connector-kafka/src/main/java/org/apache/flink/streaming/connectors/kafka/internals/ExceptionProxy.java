/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A proxy that communicates exceptions between threads. Typically used if an exception from a
 * spawned thread needs to be recognized by the "parent" (spawner) thread.
 *
 * <p>The spawned thread would set the exception via {@link #reportError(Throwable)}. The parent
 * would check (at certain points) for exceptions via {@link #checkAndThrowException()}. Optionally,
 * the parent can pass itself in the constructor to be interrupted as soon as an exception occurs.
 *
 * <pre>{@code
 * final ExceptionProxy errorProxy = new ExceptionProxy(Thread.currentThread());
 *
 * Thread subThread = new Thread() {
 *
 *     public void run() {
 *         try {
 *             doSomething();
 *         } catch (Throwable t) {
 *             errorProxy.reportError(
 *         } finally {
 *             doSomeCleanup();
 *         }
 *     }
 * };
 * subThread.start();
 *
 * doSomethingElse();
 * errorProxy.checkAndThrowException();
 *
 * doSomethingMore();
 * errorProxy.checkAndThrowException();
 *
 * try {
 *     subThread.join();
 * } catch (InterruptedException e) {
 *     errorProxy.checkAndThrowException();
 *     // restore interrupted status, if not caused by an exception
 *     Thread.currentThread().interrupt();
 * }
 * }</pre>
 */
@Internal
public class ExceptionProxy {

    /** The thread that should be interrupted when an exception occurs. */
    private final Thread toInterrupt;

    /** The exception to throw. */
    private final AtomicReference<Throwable> exception;

    /**
     * Creates an exception proxy that interrupts the given thread upon report of an exception. The
     * thread to interrupt may be null.
     *
     * @param toInterrupt The thread to interrupt upon an exception. May be null.
     */
    public ExceptionProxy(@Nullable Thread toInterrupt) {
        this.toInterrupt = toInterrupt;
        this.exception = new AtomicReference<>();
    }

    // ------------------------------------------------------------------------

    /**
     * Sets the exception and interrupts the target thread, if no other exception has occurred so
     * far.
     *
     * <p>The exception is only set (and the interruption is only triggered), if no other exception
     * was set before.
     *
     * @param t The exception that occurred
     */
    public void reportError(Throwable t) {
        // set the exception, if it is the first (and the exception is non null)
        if (t != null && exception.compareAndSet(null, t) && toInterrupt != null) {
            toInterrupt.interrupt();
        }
    }

    /**
     * Checks whether an exception has been set via {@link #reportError(Throwable)}. If yes, that
     * exception if re-thrown by this method.
     *
     * @throws Exception This method re-throws the exception, if set.
     */
    public void checkAndThrowException() throws Exception {
        Throwable t = exception.get();
        if (t != null) {
            if (t instanceof Exception) {
                throw (Exception) t;
            } else if (t instanceof Error) {
                throw (Error) t;
            } else {
                throw new Exception(t);
            }
        }
    }
}
