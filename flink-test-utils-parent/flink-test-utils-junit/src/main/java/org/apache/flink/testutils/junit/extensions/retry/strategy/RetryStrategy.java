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

package org.apache.flink.testutils.junit.extensions.retry.strategy;

/** Retry strategy for executing retry tests. */
public interface RetryStrategy {
    /** Return the next attempt should execute or not. */
    boolean hasNextAttempt();

    /** Stop the following attempts when test succeed or failed. */
    void stopFollowingAttempts();

    /**
     * Handle an exception that occurred during the annotated test attempt.
     *
     * <p>This method can swallow the exception to pass the test.
     *
     * @param testName the test name
     * @param attemptIndex test attempt index that starts from 1
     * @param throwable the throwable that the test case throws
     * @throws org.opentest4j.TestAbortedException When handling a test attempt failure, throwing
     *     this exception indicates another attempt should be made.
     * @throws Throwable Propagating the original exception, or throwing any other exception
     *     indicates that the test has definitively failed and no further attempts should be made.
     */
    void handleException(String testName, int attemptIndex, Throwable throwable) throws Throwable;
}
