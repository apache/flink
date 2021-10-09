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

import java.util.function.Function;

/**
 * Factory for creating all the test context that extends {@link PulsarTestContext}. Test context
 * class should have a constructor with {@link PulsarTestEnvironment} arg.
 */
public class PulsarTestContextFactory<F, T extends PulsarTestContext<F>>
        implements ExternalContext.Factory<F> {

    private final PulsarTestEnvironment environment;
    private final Function<PulsarTestEnvironment, T> contextFactory;

    public PulsarTestContextFactory(
            PulsarTestEnvironment environment, Function<PulsarTestEnvironment, T> contextFactory) {
        this.environment = environment;
        this.contextFactory = contextFactory;
    }

    @Override
    public ExternalContext<F> createExternalContext() {
        return contextFactory.apply(environment);
    }
}
