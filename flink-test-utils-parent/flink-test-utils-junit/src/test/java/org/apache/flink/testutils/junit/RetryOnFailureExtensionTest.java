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

package org.apache.flink.testutils.junit;

import org.apache.flink.testutils.junit.extensions.retry.RetryExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for the {@link RetryOnFailure} annotation on JUnit5 {@link RetryExtension}. */
@ExtendWith(RetryExtension.class)
class RetryOnFailureExtensionTest {

    private static final int NUMBER_OF_RUNS = 5;

    private static int numberOfFailedRuns;

    private static int numberOfSuccessfulRuns;

    private static boolean firstRun = true;

    @AfterAll
    static void verify() {
        assertEquals(NUMBER_OF_RUNS + 1, numberOfFailedRuns);
        assertEquals(3, numberOfSuccessfulRuns);
    }

    @TestTemplate
    @RetryOnFailure(times = NUMBER_OF_RUNS)
    void testRetryOnFailure() {
        // All but the (expected) last run should be successful
        if (numberOfFailedRuns < NUMBER_OF_RUNS) {
            numberOfFailedRuns++;
            throw new RuntimeException("Expected test exception");
        } else {
            numberOfSuccessfulRuns++;
        }
    }

    @TestTemplate
    @RetryOnFailure(times = NUMBER_OF_RUNS)
    void testRetryOnceOnFailure() {
        if (firstRun) {
            numberOfFailedRuns++;
            firstRun = false;
            throw new RuntimeException("Expected test exception");
        } else {
            numberOfSuccessfulRuns++;
        }
    }

    @TestTemplate
    @RetryOnFailure(times = NUMBER_OF_RUNS)
    void testNotRetryOnSuccess() {
        numberOfSuccessfulRuns++;
    }
}
