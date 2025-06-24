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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Base class which provides some convenience functions for testing purposes of {@link
 * LeaderContender}.
 */
public class TestingLeaderBase {
    // The queues will be offered by subclasses
    protected final BlockingQueue<LeaderInformation> leaderEventQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Throwable> errorQueue = new LinkedBlockingQueue<>();

    private boolean isLeader = false;
    private Throwable error;

    public void waitForLeader() throws Exception {
        throwExceptionIfNotNull();

        CommonTestUtils.waitUntilCondition(
                () -> {
                    final LeaderInformation leader = leaderEventQueue.take();
                    return !leader.isEmpty();
                });

        isLeader = true;
    }

    public void waitForRevokeLeader() throws Exception {
        throwExceptionIfNotNull();

        CommonTestUtils.waitUntilCondition(
                () -> {
                    final LeaderInformation leader = leaderEventQueue.take();
                    return leader.isEmpty();
                });

        isLeader = false;
    }

    public void waitForError() throws Exception {
        error = errorQueue.take();
    }

    public void clearError() {
        error = null;
    }

    public void handleError(Throwable ex) {
        errorQueue.offer(ex);
    }

    /**
     * Please use {@link #waitForError} before get the error.
     *
     * @return the error has been handled.
     */
    @Nullable
    public Throwable getError() {
        return error == null ? errorQueue.poll() : error;
    }

    /**
     * Method for exposing errors that were caught during the test execution and need to be exposed
     * within the test.
     *
     * @throws AssertionError with the actual unhandled error as the cause if such an error was
     *     caught during the test code execution.
     */
    public void throwErrorIfPresent() {
        final String assertionErrorMessage = "An unhandled error was caught during test execution.";
        if (error != null) {
            throw new AssertionError(assertionErrorMessage, error);
        }

        if (!errorQueue.isEmpty()) {
            throw new AssertionError(assertionErrorMessage, errorQueue.poll());
        }
    }

    public boolean isLeader() {
        return isLeader;
    }

    private void throwExceptionIfNotNull() throws Exception {
        if (error != null) {
            ExceptionUtils.rethrowException(error);
        }
    }
}
