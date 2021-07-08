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
 * A thread that additionally catches exceptions and offers a joining method that re-throws the
 * exceptions.
 *
 * <p>Rather than overriding {@link Thread#run()} (or supplying a {@link Runnable}), one needs to
 * extends this class and implement the {@link #go()} method. That method may throw exceptions.
 *
 * <p>Exception from the {@link #go()} method are caught and re-thrown when joining this thread via
 * the {@link #sync()} method.
 */
public abstract class CheckedThread extends Thread {

    /** The error thrown from the main work method. */
    private volatile Throwable error;

    // ------------------------------------------------------------------------

    /** Unnamed checked thread. */
    public CheckedThread() {
        super();
    }

    /**
     * Checked thread with a name.
     *
     * @param name the name of the new thread
     * @see Thread#Thread(String)
     */
    public CheckedThread(final String name) {
        super(name);
    }

    /**
     * This method needs to be overwritten to contain the main work logic. It takes the role of
     * {@link Thread#run()}, but should propagate exceptions.
     *
     * @throws Exception The exceptions thrown here will be re-thrown in the {@link #sync()} method.
     */
    public abstract void go() throws Exception;

    // ------------------------------------------------------------------------

    /** This method is final - thread work should go into the {@link #go()} method instead. */
    @Override
    public final void run() {
        try {
            go();
        } catch (Throwable t) {
            error = t;
        }
    }

    /**
     * Waits until the thread is completed and checks whether any error occurred during the
     * execution.
     *
     * <p>This method blocks like {@link #join()}, but performs an additional check for exceptions
     * thrown from the {@link #go()} method.
     */
    public void sync() throws Exception {
        sync(0);
    }

    /**
     * Waits with timeout until the thread is completed and checks whether any error occurred during
     * the execution. In case of timeout an {@link Exception} is thrown.
     *
     * <p>This method blocks like {@link #join()}, but performs an additional check for exceptions
     * thrown from the {@link #go()} method.
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
     * thrown from the {@link #go()} method.
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
            throw new Exception(
                    String.format(
                            "%s[name = %s] has not finished!",
                            this.getClass().getSimpleName(), getName()));
        }
    }
}
