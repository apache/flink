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
import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The base class for the all Pulsar related test sites. It brings up:
 *
 * <ul>
 *   <li>A Zookeeper cluster.
 *   <li>Pulsar Broker.
 *   <li>A Bookkeeper cluster.
 * </ul>
 *
 * <p>You just need to write a JUnit 5 test class and extends this suite class. All the helper
 * method list below would be ready.
 *
 * <p>{@code PulsarSourceEnumeratorTest} would be a test example for how to use this base class. If
 * you have some setup logic, such as create topic or send message, just place then in a setup
 * method with annotation {@code @BeforeAll}. This setup method would not require {@code static}.
 *
 * @see PulsarRuntimeOperator for how to use the helper methods in this class.
 */
@ExtendWith(TestLoggerExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class PulsarTestSuiteBase {

    @RegisterExtension
    final PulsarTestEnvironment environment = new PulsarTestEnvironment(runtime());

    /**
     * Choose the desired pulsar runtime as the test backend. The default test backend is a mocked
     * pulsar broker. Override this method when needs.
     */
    protected PulsarRuntime runtime() {
        return PulsarRuntime.mock();
    }

    /** Operate pulsar by acquiring a runtime operator. */
    protected PulsarRuntimeOperator operator() {
        return environment.operator();
    }
}
