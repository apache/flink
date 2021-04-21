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

import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link RetryRule}. */
public class RetryRuleTest extends TestLogger {

    @Test
    public void testExpectedExceptionIgnored() throws Throwable {
        final RetryRule retryRule = new RetryRule();

        final Description testDescription =
                Description.createTestDescription(
                        TestClassWithTestExpectingRuntimeException.class,
                        "test",
                        TestClassWithTestExpectingRuntimeException.class
                                .getMethod("test")
                                .getAnnotations());

        final TestStatement statement = new TestStatement(1);

        try {
            retryRule.apply(statement, testDescription).evaluate();
            Assert.fail("Should have failed.");
        } catch (RuntimeException expected) {
        }

        assertThat(statement.getNumEvaluations(), is(1));
    }

    @Ignore // we don't want to actually this run as a test
    private static class TestClassWithTestExpectingRuntimeException {
        @RetryOnFailure(times = 2)
        @Test(expected = RuntimeException.class)
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
