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

import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.runtime.util.SerializableFunction;

/**
 * Factory for creating all the test context that extends {@link PulsarContainerContext}. Test
 * context class should have a constructor with {@link PulsarContainerEnvironment} arg.
 */
public class PulsarContainerContextFactory<F, T extends PulsarContainerContext<F>>
        implements ExternalContext.Factory<F> {

    private final PulsarContainerEnvironment environment;
    private final SerializableFunction<PulsarContainerEnvironment, T> contextFactory;

    public PulsarContainerContextFactory(
            PulsarContainerEnvironment environment,
            SerializableFunction<PulsarContainerEnvironment, T> contextFactory) {
        this.environment = environment;
        this.contextFactory = contextFactory;
    }

    @Override
    public ExternalContext<F> createExternalContext() {
        return contextFactory.apply(environment);
    }
}
