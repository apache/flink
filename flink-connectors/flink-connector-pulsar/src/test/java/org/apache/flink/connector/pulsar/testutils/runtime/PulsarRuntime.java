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
import org.apache.flink.connector.pulsar.testutils.runtime.mock.PulsarMockRuntime;

import org.testcontainers.containers.GenericContainer;

/**
 * An abstraction for different pulsar runtimes. Providing the common methods for {@link
 * PulsarTestEnvironment}.
 */
public interface PulsarRuntime {

    /** Start up this pulsar runtime, block the thread until everytime is ready for this runtime. */
    void startUp();

    /** Shutdown this pulsar runtime. */
    void tearDown();

    /** Return a operator for operating this pulsar runtime. */
    PulsarRuntimeOperator operator();

    static PulsarRuntime mock() {
        return new PulsarMockRuntime();
    }

    static PulsarRuntime container() {
        return new PulsarContainerRuntime();
    }

    static PulsarRuntime container(GenericContainer<?> flinkContainer) {
        return new PulsarContainerRuntime().bindWithFlinkContainer(flinkContainer);
    }
}
