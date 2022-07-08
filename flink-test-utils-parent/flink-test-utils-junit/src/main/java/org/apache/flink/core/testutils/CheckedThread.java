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

import java.util.concurrent.TimeoutException;

/**
 * A thread that additionally catches exceptions and offers a joining method that re-throws the
 * exceptions.
 *
 * <p>This class needs to supply a {@link ThrowingRunnable} that may throw exceptions.
 *
 * <p>Exception from the {@link #runnable} are caught and re-thrown when joining this thread via the
 * {@link #sync()} method.
 */
public class CheckedThread extends Thread {

    /** The error thrown from the main work method. */
    private volatile Throwable error;

    /**
     * This field takes the role of {@link Runnable} in {@link Thread}, but should propagate
     * exceptions. The exceptions thrown by this field will be re-thrown in the {@link #sync()}
     * method.
     */
    private final ThrowingRunnable<Exception> runnable;

    // ------------------------------------------------------------------------

    /** Unnamed checked thread. */
    public CheckedThread(ThrowingRunnable<Exception> runnable) {
        if (runnable == null) {
            throw new NullPointerException("runnable must be not-null for CheckedThread.");
        }
        this.runnable = runnable;
    }

    /**
     * Checked thread with a name.
     *
     * @param name the name of the new thread
     * @see Thread#Thread(String)
     */
    public CheckedThread(ThrowingRunnable<Exception> runnable, final String name) {
        super(name);
        if (runnable == null) {
            throw new NullPointerException("runnable must be not-null for CheckedThread.");
        }
        this.runnable = runnable;
    }

    // ------------------------------------------------------------------------

    /** This method is final - thread work should go into the {@link #runnable} field instead. */
    @Override
    public final void run() {
        try {
            runnable.run();
        } catch (Throwable t) {
            error = t;
        }
    }

    /**
     * Waits until the thread is completed and checks whether any error occurred during the
     * execution.
     *
     * <p>This method blocks like {@link #join()}, but performs an additional check for exceptions
     * thrown from the {@link #runnable}.
     */
    public void sync() throws Exception {
        sync(0);
    }

    /**
     * Waits with timeout until the thread is completed and checks whether any error occurred during
     * the execution. In case of timeout an {@link Exception} is thrown.
     *
     * <p>This method blocks like {@link #join()}, but performs an additional check for exceptions
     * thrown from the {@link #runnable}.
     */
    public void sync(long timeout) throws Exception {
        trySync(timeout);
        checkFinished();
    }

    /**
     * Waits with timeout until the thread is completed and checks whether any error occurred during
     * the execution.
     *
     * <p>This method blocks like {@link #join()}, but performs an additional check for exceptions
     * thrown from the {@link #runnable}.
     */
    public void trySync(long timeout) throws Exception {
        join(timeout);
        checkError();
    }

    private void checkError() throws Exception {
        // propagate the error
        if (error != null) {
            if (error instanceof Error) {
                throw (Error) error;
            } else if (error instanceof Exception) {
                throw (Exception) error;
            } else {
                throw new Exception(error.getMessage(), error);
            }
        }
    }

    private void checkFinished() throws Exception {
        if (getState() != State.TERMINATED) {
            throw new TimeoutException(
                    String.format(
                            "%s[name = %s] has not finished!",
                            this.getClass().getSimpleName(), getName()));
        }
    }
}
