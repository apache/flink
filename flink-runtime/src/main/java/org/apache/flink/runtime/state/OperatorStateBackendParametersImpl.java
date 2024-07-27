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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.StateBackend.CustomInitializationMetrics;

import javax.annotation.Nonnull;

import java.util.Collection;

/**
 * Internal POJO implementing {@link
 * org.apache.flink.runtime.state.StateBackend.OperatorStateBackendParameters}
 *
 * @param <K>
 */
@Internal
public class OperatorStateBackendParametersImpl
        implements StateBackend.OperatorStateBackendParameters {
    private final Environment env;
    private final String operatorIdentifier;

    private final Collection<OperatorStateHandle> stateHandles;
    private final CustomInitializationMetrics customInitializationMetrics;
    private final CloseableRegistry cancelStreamRegistry;

    public OperatorStateBackendParametersImpl(
            Environment env,
            String operatorIdentifier,
            Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry) {
        this(env, operatorIdentifier, stateHandles, (name, value) -> {}, cancelStreamRegistry);
    }

    public OperatorStateBackendParametersImpl(
            Environment env,
            String operatorIdentifier,
            Collection<OperatorStateHandle> stateHandles,
            CustomInitializationMetrics customInitializationMetrics,
            CloseableRegistry cancelStreamRegistry) {
        this.env = env;
        this.operatorIdentifier = operatorIdentifier;
        this.customInitializationMetrics = customInitializationMetrics;
        this.stateHandles = stateHandles;
        this.cancelStreamRegistry = cancelStreamRegistry;
    }

    @Override
    public Environment getEnv() {
        return env;
    }

    @Override
    public String getOperatorIdentifier() {
        return operatorIdentifier;
    }

    @Nonnull
    @Override
    public Collection<OperatorStateHandle> getStateHandles() {
        return stateHandles;
    }

    @Override
    public CloseableRegistry getCancelStreamRegistry() {
        return cancelStreamRegistry;
    }

    @Override
    public CustomInitializationMetrics getCustomInitializationMetrics() {
        return customInitializationMetrics;
    }
}
