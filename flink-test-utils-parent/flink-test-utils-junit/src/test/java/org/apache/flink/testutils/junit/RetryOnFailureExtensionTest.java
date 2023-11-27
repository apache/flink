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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link RetryOnFailure} annotation on JUnit5 {@link RetryExtension}. */
@ExtendWith(RetryExtension.class)
class RetryOnFailureExtensionTest {

    private static final int NUMBER_OF_RETRIES = 5;

    private static final Map<String, Integer> methodRunCount = new HashMap<>();

    private static final Map<String, Runnable> verificationCallbackRegistry = new HashMap<>();

    @BeforeEach
    void incrementMethodRunCount(TestInfo testInfo) {
        // Set or increment the run count for the unit test method, by the method short name.
        // This starts at 1 and is incremented before the test starts.
        testInfo.getTestMethod()
                .ifPresent(
                        method ->
                                methodRunCount.compute(
                                        method.getName(), (k, v) -> (v == null) ? 1 : v + 1));
    }

    private static int assertAndReturnRunCount(TestInfo testInfo) {
        return methodRunCount.get(assertAndReturnTestMethodName(testInfo));
    }

    private static void registerCallbackForTest(TestInfo testInfo, Consumer<Integer> verification) {
        verificationCallbackRegistry.putIfAbsent(
                assertAndReturnTestMethodName(testInfo),
                () -> verification.accept(assertAndReturnRunCount(testInfo)));
    }

    private static String assertAndReturnTestMethodName(TestInfo testInfo) {
        return testInfo.getTestMethod()
                .orElseThrow(() -> new AssertionError("No test method is provided."))
                .getName();
    }

    @AfterAll
    static void verify() {
        for (Runnable verificationCallback : verificationCallbackRegistry.values()) {
            verificationCallback.run();
        }
    }

    @TestTemplate
    @RetryOnFailure(times = NUMBER_OF_RETRIES)
    void testRetryOnFailure(TestInfo testInfo) {
        registerCallbackForTest(
                testInfo, total -> assertThat(total).isEqualTo(NUMBER_OF_RETRIES + 1));

        // All but the (expected) last run should be successful
        if (assertAndReturnRunCount(testInfo) <= NUMBER_OF_RETRIES) {
            throw new RuntimeException("Expected test exception");
        }
    }

    @TestTemplate
    @RetryOnFailure(times = NUMBER_OF_RETRIES)
    void testRetryOnceOnFailure(TestInfo testInfo) {
        registerCallbackForTest(testInfo, total -> assertThat(total).isEqualTo(2));
        if (assertAndReturnRunCount(testInfo) == 1) {
            throw new RuntimeException("Expected test exception");
        }
    }

    @TestTemplate
    @RetryOnFailure(times = NUMBER_OF_RETRIES)
    void testNotRetryOnSuccess(TestInfo testInfo) {
        registerCallbackForTest(testInfo, total -> assertThat(total).isOne());
    }
}
