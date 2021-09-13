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

package org.apache.flink.connector.pulsar.testutils;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connectors.test.common.TestResource;
import org.apache.flink.connectors.test.common.junit.annotations.ExternalSystem;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.List;

/**
 * A JUnit 5 {@link Extension} for supporting running a pulsar instance before executing tests. This
 * class is also a {@link ExternalSystem} for {@code flink-connector-testing} tools.
 *
 * <p>Some old flink tests are based on JUint 4, this class is also support it. The follow code
 * snippet shows how to use this class in JUnit 4.
 *
 * <pre>{@code
 * @ClassRule
 * public static PulsarContainerEnvironment environment = new PulsarContainerEnvironment(MOCK);
 * }</pre>
 *
 * <p>If you want to use this class in JUnit 5, just simply extends {@link PulsarTestSuiteBase}, all
 * the helper methods in {@code PulsarContainerOperator} is also exposed there.
 */
public class PulsarTestEnvironment
        implements BeforeAllCallback, AfterAllCallback, TestResource, TestRule {

    private final PulsarRuntime runtime;

    public PulsarTestEnvironment(PulsarRuntime runtime) {
        this.runtime = runtime;
    }

    /** JUnit 4 Rule based test logic. */
    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                runtime.startUp();

                List<Throwable> errors = new ArrayList<>();
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    try {
                        runtime.tearDown();
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                }
                MultipleFailureException.assertEmpty(errors);
            }
        };
    }

    /** JUnit 5 Extension setup method. */
    @Override
    public void beforeAll(ExtensionContext context) {
        runtime.startUp();
    }

    /** flink-connector-testing setup method. */
    @Override
    public void startUp() {
        runtime.startUp();
    }

    /** JUnit 5 Extension shutdown method. */
    @Override
    public void afterAll(ExtensionContext context) {
        runtime.tearDown();
    }

    /** flink-connector-testing shutdown method. */
    @Override
    public void tearDown() {
        runtime.tearDown();
    }

    /** Get a common supported set of method for operating pulsar which is in container. */
    public PulsarRuntimeOperator operator() {
        return runtime.operator();
    }
}
