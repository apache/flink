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

import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A retry strategy that will ignore a specific type of exception and retry a test if it occurs, up
 * to a fixed number of times.
 */
public class RetryOnExceptionStrategy extends AbstractRetryStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(RetryOnExceptionStrategy.class);

    private final Class<? extends Throwable> repeatableException;

    public RetryOnExceptionStrategy(
            int retryTimes, Class<? extends Throwable> repeatableException) {
        super(retryTimes, true);
        this.repeatableException = repeatableException;
    }

    @Override
    public void handleException(String testName, int attemptIndex, Throwable throwable)
            throws Throwable {
        // Failed when reach the total retry times
        if (attemptIndex >= totalTimes) {
            LOG.error("Test Failed at the last retry.", throwable);
            throw throwable;
        }

        if (repeatableException.isAssignableFrom(throwable.getClass())) {
            // continue retrying when get some repeatable exceptions
            String retryMsg =
                    String.format(
                            "Retry test %s[%d/%d] failed with repeatable exception, continue retrying.",
                            testName, attemptIndex, totalTimes);
            LOG.warn(retryMsg, throwable);
            throw new TestAbortedException(retryMsg);
        } else {
            // stop retrying when get an unrepeatable exception
            stopFollowingAttempts();
            LOG.error(
                    String.format(
                            "Retry test %s[%d/%d] failed with unrepeatable exception, stop retrying.",
                            testName, attemptIndex, totalTimes),
                    throwable);
            throw throwable;
        }
    }
}
