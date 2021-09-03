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

import org.apache.flink.connector.pulsar.testutils.runtime.container.PulsarContainerProvider;
import org.apache.flink.connector.pulsar.testutils.runtime.mock.PulsarMockProvider;

import java.util.function.Supplier;

/**
 * A enum class for providing a operable pulsar runtime. We support two types of runtime, the
 * container and mock.
 */
public enum PulsarRuntime {

    /**
     * The whole pulsar cluster would run in a docker container, provide the full fledged test
     * backend.
     */
    CONTAINER(PulsarContainerProvider::new),

    /** The bookkeeper and zookeeper would use a mock backend, and start a single pulsar broker. */
    MOCK(PulsarMockProvider::new);

    private final Supplier<PulsarRuntimeProvider> provider;

    PulsarRuntime(Supplier<PulsarRuntimeProvider> provider) {
        this.provider = provider;
    }

    public PulsarRuntimeProvider provider() {
        return provider.get();
    }
}
