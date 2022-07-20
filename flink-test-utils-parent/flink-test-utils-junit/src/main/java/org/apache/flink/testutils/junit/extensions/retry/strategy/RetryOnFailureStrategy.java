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

/** Retry strategy that retry fixed times. */
public class RetryOnFailureStrategy extends AbstractRetryStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(RetryOnFailureStrategy.class);

    public RetryOnFailureStrategy(int retryTimes) {
        super(retryTimes, true);
    }

    @Override
    public void handleException(String testName, int attemptIndex, Throwable throwable)
            throws Throwable {
        // Failed when reach the total retry times
        if (attemptIndex >= totalTimes) {
            LOG.error("Test Failed at the last retry.", throwable);
            throw throwable;
        }

        // continue retrying
        String retryMsg =
                String.format(
                        "Retry test %s[%d/%d] failed, continue retrying.",
                        testName, attemptIndex, totalTimes);
        LOG.error(retryMsg, throwable);
        throw new TestAbortedException(retryMsg);
    }
}
