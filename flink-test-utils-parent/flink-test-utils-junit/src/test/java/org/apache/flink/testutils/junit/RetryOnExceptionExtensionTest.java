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
import org.apache.flink.testutils.junit.extensions.retry.strategy.RetryOnExceptionStrategy;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.TestAbortedException;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link RetryOnException} annotation on JUnit5 {@link RetryExtension}. */
@ExtendWith(RetryExtension.class)
class RetryOnExceptionExtensionTest {

    private static final int NUMBER_OF_RUNS = 3;

    private static int runsForSuccessfulTest = 0;

    private static int runsForTestWithMatchingException = 0;

    private static int runsForTestWithSubclassException = 0;

    private static int runsForPassAfterOneFailure = 0;

    @AfterAll
    static void verify() {
        assertThat(runsForTestWithMatchingException).isEqualTo(NUMBER_OF_RUNS + 1);
        assertThat(runsForTestWithSubclassException).isEqualTo(NUMBER_OF_RUNS + 1);
        assertThat(runsForSuccessfulTest).isOne();
        assertThat(runsForPassAfterOneFailure).isEqualTo(2);
    }

    @TestTemplate
    @RetryOnException(times = NUMBER_OF_RUNS, exception = IllegalArgumentException.class)
    void testSuccessfulTest() {
        runsForSuccessfulTest++;
    }

    @TestTemplate
    @RetryOnException(times = NUMBER_OF_RUNS, exception = IllegalArgumentException.class)
    void testMatchingException() {
        runsForTestWithMatchingException++;
        if (runsForTestWithMatchingException <= NUMBER_OF_RUNS) {
            throw new IllegalArgumentException();
        }
    }

    @TestTemplate
    @RetryOnException(times = NUMBER_OF_RUNS, exception = RuntimeException.class)
    void testSubclassException() {
        runsForTestWithSubclassException++;
        if (runsForTestWithSubclassException <= NUMBER_OF_RUNS) {
            throw new IllegalArgumentException();
        }
    }

    @TestTemplate
    @RetryOnException(times = NUMBER_OF_RUNS, exception = IllegalArgumentException.class)
    void testPassAfterOneFailure() {
        runsForPassAfterOneFailure++;
        if (runsForPassAfterOneFailure == 1) {
            throw new IllegalArgumentException();
        }
    }

    @ParameterizedTest(name = "Retrying with {0}")
    @MethodSource("retryTestProvider")
    void testRetryFailsWithExpectedExceptionAfterNumberOfRetries(
            final Throwable expectedException) {
        final int numberOfRetries = 1;
        RetryOnExceptionStrategy retryOnExceptionStrategy =
                new RetryOnExceptionStrategy(numberOfRetries, expectedException.getClass());
        // All attempts that permit a retry should be a TestAbortedException.  When retries are no
        // longer permitted, the handled exception should be propagated.
        for (int j = 0; j <= numberOfRetries; j++) {
            final int attemptIndex = j;
            assertThatThrownBy(
                            () ->
                                    retryOnExceptionStrategy.handleException(
                                            "Any test name", attemptIndex, expectedException))
                    .isInstanceOf(
                            j == numberOfRetries
                                    ? expectedException.getClass()
                                    : TestAbortedException.class);
        }
    }

    static class RetryTestError extends Error {}

    static class RetryTestException extends Exception {}

    static class RetryTestRuntimeException extends RuntimeException {}

    static class RetryTestThrowable extends Throwable {}

    static Stream<Throwable> retryTestProvider() {
        return Stream.of(
                new RetryTestError(),
                new RetryTestException(),
                new RetryTestRuntimeException(),
                new RetryTestThrowable());
    }
}
