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

package org.apache.flink.connector.pulsar.testutils.runtime;

import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.runtime.container.PulsarContainerRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.embedded.PulsarEmbeddedRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.mock.PulsarMockRuntime;

import org.testcontainers.containers.GenericContainer;

/**
 * An abstraction for different pulsar runtimes. Providing the common methods for {@link
 * PulsarTestEnvironment}.
 *
 * <p>All the Pulsar runtime should enable the transaction by default.
 */
public interface PulsarRuntime {

    /** Start up this pulsar runtime, block the thread until everytime is ready for this runtime. */
    void startUp();

    /** Shutdown this pulsar runtime. */
    void tearDown();

    /**
     * Return an operator for operating this pulsar runtime. This operator predefined a set of
     * extremely useful methods for Pulsar. You can easily add new methods in this operator.
     */
    PulsarRuntimeOperator operator();

    /** Create a Pulsar instance which would mock all the backends. */
    static PulsarRuntime mock() {
        return new PulsarMockRuntime();
    }

    /**
     * Create a standalone Pulsar instance in test thread. We would start an embedded zookeeper and
     * bookkeeper. The stream storage for bookkeeper is disabled. The function worker is disabled on
     * Pulsar broker.
     *
     * <p>This runtime would be faster than {@link #container()} and behaves the same as the {@link
     * #container()}.
     */
    static PulsarRuntime embedded() {
        return new PulsarEmbeddedRuntime();
    }

    /**
     * Create a Pulsar instance in docker. We would start a standalone Pulsar in TestContainers.
     * This runtime is often used in end-to-end tests. The performance may be a bit of slower than
     * {@link #embedded()}. The stream storage for bookkeeper is disabled. The function worker is
     * disabled on Pulsar broker.
     */
    static PulsarRuntime container() {
        return new PulsarContainerRuntime();
    }

    /**
     * Create a Pulsar instance in docker. We would start a standalone Pulsar in TestContainers.
     * This runtime is often used in end-to-end tests. The performance may be a bit of slower than
     * {@link #embedded()}. The stream storage for bookkeeper is disabled. The function worker is
     * disabled on Pulsar broker.
     *
     * <p>We would link the created Pulsar docker instance with the given flink instance. This would
     * enable the connection for Pulsar and Flink in docker environment.
     */
    static PulsarRuntime container(GenericContainer<?> flinkContainer) {
        return new PulsarContainerRuntime().bindWithFlinkContainer(flinkContainer);
    }
}
