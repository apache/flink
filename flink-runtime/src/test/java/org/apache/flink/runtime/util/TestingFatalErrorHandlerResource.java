/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.util.Preconditions;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.annotation.Nullable;

/**
 * Resource which provides a {@link TestingFatalErrorHandler} and checks whether no exception has
 * been caught when calling {@link #after()}.
 */
public final class TestingFatalErrorHandlerResource implements TestRule {

    @Nullable private TestingFatalErrorHandler testingFatalErrorHandler;

    public TestingFatalErrorHandlerResource() {
        testingFatalErrorHandler = null;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return statement(base);
    }

    private Statement statement(Statement base) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                before();

                // using try-with-resources in order to properly report exceptions also in case
                // that after throws an exception (suppressed exception)
                try (CloseableStatement closeableStatement = new CloseableStatement(base)) {
                    closeableStatement.evaluate();
                } finally {
                    after();
                }
            }
        };
    }

    public TestingFatalErrorHandler getFatalErrorHandler() {
        return Preconditions.checkNotNull(
                testingFatalErrorHandler,
                "The %s has not been properly initialized.",
                TestingFatalErrorHandlerResource.class.getSimpleName());
    }

    private void before() {
        testingFatalErrorHandler = new TestingFatalErrorHandler();
    }

    private void after() throws AssertionError {
        final Throwable exception = getFatalErrorHandler().getException();

        if (exception != null) {
            throw new AssertionError(
                    "The TestingFatalErrorHandler caught an exception.", exception);
        }
    }

    private static final class CloseableStatement implements AutoCloseable {
        private final Statement statement;

        private CloseableStatement(Statement statement) {
            this.statement = statement;
        }

        private void evaluate() throws Throwable {
            statement.evaluate();
        }

        @Override
        public void close() {}
    }
}
