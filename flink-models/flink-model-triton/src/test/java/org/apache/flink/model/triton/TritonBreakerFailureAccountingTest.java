/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.model.triton;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link
 * TritonInferenceModelFunction#shouldRecordBreakerFailureOnFinalCompletion(boolean, int)}.
 *
 * <p>Guards the "one logical request, one breaker entry" contract: a breaker-worthy failure
 * deferred on attempt 0 must be flushed even when the retry lands on a non-breaker-worthy branch
 * (4xx / parse / request-build bug).
 */
class TritonBreakerFailureAccountingTest {

    @Test
    void testStandaloneBreakerWorthyFailureIsRecorded() {
        // 5xx / network on first attempt with no retries: record directly.
        assertThat(
                        TritonInferenceModelFunction.shouldRecordBreakerFailureOnFinalCompletion(
                                true, 0))
                .isTrue();
    }

    @Test
    void testStandaloneNonBreakerFailureIsNotRecorded() {
        // 4xx / parse error on attempt 0: not a backend-health signal.
        assertThat(
                        TritonInferenceModelFunction.shouldRecordBreakerFailureOnFinalCompletion(
                                false, 0))
                .isFalse();
    }

    @Test
    void testExhaustedRetriesStillRecord() {
        // Multiple retries all breaker-worthy: record once.
        assertThat(
                        TritonInferenceModelFunction.shouldRecordBreakerFailureOnFinalCompletion(
                                true, 3))
                .isTrue();
    }

    @Test
    void testRetryLandingOnNonRetryableStillRecordsDeferred() {
        // Regression for the bug this fix addresses: 5xx on attempt 0 → retry → attempt 1
        // fails with 4xx / parse error. countAsBreakerFailure=false for the final attempt, but
        // the prior 5xx was deferred and must still be recorded.
        assertThat(
                        TritonInferenceModelFunction.shouldRecordBreakerFailureOnFinalCompletion(
                                false, 1))
                .isTrue();
    }
}
