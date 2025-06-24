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

package org.apache.flink.testutils.junit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link RetryRule}. */
class RetryRuleTest {
    private static final RetryRule RETRY_RULE = new RetryRule();

    @Test
    void testExpectedExceptionIgnored() throws Throwable {
        final int numEvaluationsToFail = 1;

        final Description testDescription =
                Description.createTestDescription(
                        TestClassWithTestExpectingRuntimeException.class,
                        "test",
                        TestClassWithTestExpectingRuntimeException.class
                                .getMethod("test")
                                .getAnnotations());

        final TestStatement statement = new TestStatement(numEvaluationsToFail);

        assertThatThrownBy(() -> RETRY_RULE.apply(statement, testDescription).evaluate())
                .withFailMessage("Should have failed.")
                .isInstanceOf(RuntimeException.class);

        assertThat(statement.getNumEvaluations()).isEqualTo(numEvaluationsToFail);
    }

    @Disabled // we don't want to actually this run as a test
    private static class TestClassWithTestExpectingRuntimeException {
        @RetryOnFailure(times = 2)
        @org.junit.Test(expected = RuntimeException.class)
        public void test() {}
    }

    @Test
    void testNoAnnotationResultsInZeroRetries() throws Throwable {
        final int numEvaluationsToFail = 1;

        final Description testDescription =
                Description.createTestDescription(
                        TestClassWithoutAnnotation.class,
                        "test",
                        TestClassWithAnnotation.class.getMethod("test").getAnnotations());

        final TestStatement statement = new TestStatement(numEvaluationsToFail);

        assertThatThrownBy(() -> RETRY_RULE.apply(statement, testDescription).evaluate())
                .withFailMessage("Should have failed.")
                .isInstanceOf(RuntimeException.class);

        assertThat(statement.getNumEvaluations()).isEqualTo(numEvaluationsToFail);
    }

    @Disabled // we don't want to actually this run as a test
    private static class TestClassWithoutAnnotation {
        @org.junit.Test
        public void test() {}
    }

    @Test
    void testAnnotationOnClassUsedAsFallback() throws Throwable {
        final int numEvaluationsToFail = 1;

        final Description testDescription =
                Description.createTestDescription(
                        TestClassWithAnnotation.class,
                        "test",
                        TestClassWithAnnotation.class.getMethod("test").getAnnotations());

        final TestStatement statement = new TestStatement(numEvaluationsToFail);

        RETRY_RULE.apply(statement, testDescription).evaluate();

        assertThat(statement.getNumEvaluations()).isEqualTo(numEvaluationsToFail + 1);
    }

    @Disabled // we don't want to actually this run as a test
    @RetryOnFailure(times = 1)
    private static class TestClassWithAnnotation {
        @org.junit.Test
        public void test() {}
    }

    @Test
    void testAnnotationOnMethodTakesPrecedence() throws Throwable {
        final int numEvaluationsToFail = 2;

        final Description testDescription =
                Description.createTestDescription(
                        TestClassWithAnnotationOnMethod.class,
                        "test",
                        TestClassWithAnnotationOnMethod.class.getMethod("test").getAnnotations());

        final TestStatement statement = new TestStatement(numEvaluationsToFail);

        RETRY_RULE.apply(statement, testDescription).evaluate();

        assertThat(statement.getNumEvaluations()).isEqualTo(numEvaluationsToFail + 1);
    }

    @Disabled // we don't want to actually this run as a test
    @RetryOnFailure(times = 1)
    private static class TestClassWithAnnotationOnMethod {
        @RetryOnFailure(times = 2)
        @org.junit.Test
        public void test() {}
    }

    private static class TestStatement extends Statement {
        private final int numEvaluationsToFail;

        private int numEvaluations = 0;

        private TestStatement(int numEvaluationsToFail) {
            this.numEvaluationsToFail = numEvaluationsToFail;
        }

        @Override
        public void evaluate() throws Throwable {
            try {
                if (numEvaluations < numEvaluationsToFail) {
                    throw new RuntimeException("test exception");
                }
            } finally {
                numEvaluations++;
            }
        }

        public int getNumEvaluations() {
            return numEvaluations;
        }
    }
}
