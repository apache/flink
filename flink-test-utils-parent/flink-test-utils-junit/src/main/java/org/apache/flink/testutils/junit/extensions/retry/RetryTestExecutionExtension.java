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

package org.apache.flink.testutils.junit.extensions.retry;

import org.apache.flink.testutils.junit.extensions.retry.strategy.RetryStrategy;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.flink.testutils.junit.extensions.retry.RetryExtension.RETRY_KEY;
import static org.apache.flink.testutils.junit.extensions.retry.RetryExtension.RETRY_NAMESPACE;
import static org.apache.flink.testutils.junit.extensions.retry.RetryExtension.getTestMethodKey;

/** Extension to decide whether a retry test should run. */
public class RetryTestExecutionExtension
        implements ExecutionCondition, TestExecutionExceptionHandler, AfterEachCallback {
    private static final Logger LOG = LoggerFactory.getLogger(RetryTestExecutionExtension.class);
    private final int retryIndex;
    private final int totalTimes;

    public RetryTestExecutionExtension(int retryIndex, int totalTimes) {
        this.retryIndex = retryIndex;
        this.totalTimes = totalTimes;
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        RetryStrategy retryStrategy = getRetryStrategyInStore(context);
        String method = getTestMethodKey(context);
        if (!retryStrategy.hasNextAttempt()) {
            return ConditionEvaluationResult.disabled(method + "has already passed or failed.");
        }
        return ConditionEvaluationResult.enabled(
                String.format("Test %s[%d/%d]", method, retryIndex, totalTimes));
    }

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable)
            throws Throwable {
        RetryStrategy retryStrategy = getRetryStrategyInStore(context);
        String method = getTestMethodKey(context);
        retryStrategy.handleException(method, retryIndex, throwable);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        Throwable exception = context.getExecutionException().orElse(null);
        if (exception == null) {
            RetryStrategy retryStrategy = getRetryStrategyInStore(context);
            String method = getTestMethodKey(context);
            retryStrategy.stopFollowingAttempts();
            LOG.trace(
                    String.format(
                            "Retry test %s[%d/%d] passed, stop retrying.",
                            method, retryIndex, totalTimes));
        }
    }

    private RetryStrategy getRetryStrategyInStore(ExtensionContext context) {
        Map<String, RetryStrategy> retryStrategies =
                (Map<String, RetryStrategy>) context.getStore(RETRY_NAMESPACE).get(RETRY_KEY);
        String method = getTestMethodKey(context);
        return retryStrategies.get(method);
    }
}
