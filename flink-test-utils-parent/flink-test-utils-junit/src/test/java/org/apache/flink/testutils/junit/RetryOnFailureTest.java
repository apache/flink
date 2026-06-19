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

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link RetryOnFailure} annotation on JUnit4 {@link RetryRule}. */
public class RetryOnFailureTest {

    @Rule public RetryRule retryRule = new RetryRule();

    private static final int NUMBER_OF_RUNS = 5;

    private static int numberOfFailedRuns;

    private static int numberOfSuccessfulRuns;

    private static boolean firstRun = true;

    @AfterClass
    public static void verify() throws Exception {
        assertThat(numberOfFailedRuns).isEqualTo(NUMBER_OF_RUNS + 1);
        assertThat(numberOfSuccessfulRuns).isEqualTo(3);
    }

    @Test
    @RetryOnFailure(times = NUMBER_OF_RUNS)
    public void testRetryOnFailure() throws Exception {
        // All but the (expected) last run should be successful
        if (numberOfFailedRuns < NUMBER_OF_RUNS) {
            numberOfFailedRuns++;
            throw new RuntimeException("Expected test exception");
        } else {
            numberOfSuccessfulRuns++;
        }
    }

    @Test
    @RetryOnFailure(times = NUMBER_OF_RUNS)
    public void testRetryOnceOnFailure() throws Exception {
        if (firstRun) {
            numberOfFailedRuns++;
            firstRun = false;
            throw new RuntimeException("Expected test exception");
        } else {
            numberOfSuccessfulRuns++;
        }
    }

    @Test
    @RetryOnFailure(times = NUMBER_OF_RUNS)
    public void testDontRetryOnSuccess() throws Exception {
        numberOfSuccessfulRuns++;
    }
}
