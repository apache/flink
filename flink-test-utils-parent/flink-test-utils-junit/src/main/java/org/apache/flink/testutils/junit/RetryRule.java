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

import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * A rule to retry failed tests for a fixed number of times.
 *
 * <p>Add the {@link RetryRule} to your test class and annotate the class and/or tests with either
 * {@link RetryOnFailure} or {@link RetryOnException}. If both the class and test are annotated,
 * then only the latter annotation is taken into account.
 *
 * <pre>
 * {@literal @}RetryOnFailure(times=1)
 * public class YourTest {
 *
 *     {@literal @}Rule
 *     public RetryRule retryRule = new RetryRule();
 *
 *     {@literal @}Test
 *     public void yourTest() {
 *         // This will be retried 1 time (total runs 2) before failing the test.
 *         throw new Exception("Failing test");
 *     }
 *
 *     {@literal @}Test
 *     {@literal @}RetryOnFailure(times=2)
 *     public void yourTest() {
 *         // This will be retried 2 time (total runs 3) before failing the test.
 *         throw new Exception("Failing test");
 *     }
 * }
 * </pre>
 */
public class RetryRule implements TestRule {

    public static final Logger LOG = LoggerFactory.getLogger(RetryRule.class);

    @Override
    public Statement apply(Statement statement, Description description) {
        RetryOnFailure retryOnFailure = description.getAnnotation(RetryOnFailure.class);
        RetryOnException retryOnException = description.getAnnotation(RetryOnException.class);

        if (retryOnFailure == null && retryOnException == null) {
            // if nothing is specified on the test method, fall back to annotations on the class
            retryOnFailure = description.getTestClass().getAnnotation(RetryOnFailure.class);
            retryOnException = description.getTestClass().getAnnotation(RetryOnException.class);
        }

        // sanity check that we don't use both annotations
        if (retryOnFailure != null && retryOnException != null) {
            throw new IllegalArgumentException(
                    "You cannot combine the RetryOnFailure and RetryOnException annotations.");
        }

        final Class<? extends Throwable> whitelistedException = getExpectedException(description);

        if (retryOnFailure != null) {
            return new RetryOnFailureStatement(
                    retryOnFailure.times(), statement, whitelistedException);
        } else if (retryOnException != null) {
            return new RetryOnExceptionStatement(
                    retryOnException.times(),
                    retryOnException.exception(),
                    statement,
                    whitelistedException);
        } else {
            return statement;
        }
    }

    @Nullable
    private static Class<? extends Throwable> getExpectedException(Description description) {
        Test test = description.getAnnotation(Test.class);
        if (test.expected() != Test.None.class) {
            return test.expected();
        }

        return null;
    }

    /** Retries a test in case of a failure. */
    private static class RetryOnFailureStatement extends Statement {

        private final int timesOnFailure;

        private int currentRun;

        private final Statement statement;
        @Nullable private final Class<? extends Throwable> expectedException;

        private RetryOnFailureStatement(
                int timesOnFailure,
                Statement statement,
                @Nullable Class<? extends Throwable> expectedException) {
            this.expectedException = expectedException;
            if (timesOnFailure < 0) {
                throw new IllegalArgumentException("Negatives number of retries on failure");
            }
            this.timesOnFailure = timesOnFailure;
            this.statement = statement;
        }

        /**
         * Retry a test in case of a failure.
         *
         * @throws Throwable
         */
        @Override
        public void evaluate() throws Throwable {
            for (currentRun = 0; currentRun <= timesOnFailure; currentRun++) {
                try {
                    statement.evaluate();
                    break; // success
                } catch (Throwable t) {
                    if (expectedException != null
                            && expectedException.isAssignableFrom(t.getClass())) {
                        throw t;
                    }

                    LOG.warn(
                            String.format(
                                    "Test run failed (%d/%d).", currentRun, timesOnFailure + 1),
                            t);

                    // Throw the failure if retried too often
                    if (currentRun == timesOnFailure) {
                        throw t;
                    }
                }
            }
        }
    }

    /** Retries a test in case of a failure. */
    private static class RetryOnExceptionStatement extends Statement {

        private final Class<? extends Throwable> exceptionClass;
        private final int timesOnFailure;
        private final Statement statement;
        @Nullable private final Class<? extends Throwable> expectedException;

        private int currentRun;

        private RetryOnExceptionStatement(
                int timesOnFailure,
                Class<? extends Throwable> exceptionClass,
                Statement statement,
                @Nullable Class<? extends Throwable> expectedException) {
            if (timesOnFailure < 0) {
                throw new IllegalArgumentException("Negatives number of retries on failure");
            }
            if (exceptionClass == null) {
                throw new NullPointerException("exceptionClass");
            }

            this.exceptionClass = (exceptionClass);
            this.timesOnFailure = timesOnFailure;
            this.statement = statement;
            this.expectedException = expectedException;
        }

        /**
         * Retry a test in case of a failure with a specific exception.
         *
         * @throws Throwable
         */
        @Override
        public void evaluate() throws Throwable {
            for (currentRun = 0; currentRun <= timesOnFailure; currentRun++) {
                try {
                    statement.evaluate();
                    break; // success
                } catch (Throwable t) {
                    if (expectedException != null
                            && expectedException.isAssignableFrom(t.getClass())) {
                        throw t;
                    }

                    LOG.warn(
                            String.format(
                                    "Test run failed (%d/%d).", currentRun, timesOnFailure + 1),
                            t);

                    if (!exceptionClass.isAssignableFrom(t.getClass())
                            || currentRun >= timesOnFailure) {
                        // Throw the failure if retried too often, or if it is the wrong exception
                        throw t;
                    }
                }
            }
        }
    }
}
