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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.StateBackend.CustomInitializationMetrics;
import org.apache.flink.runtime.state.StateBackend.KeyedStateBackendParameters;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.util.Collection;

/**
 * Internal POJO implementing {@link KeyedStateBackendParameters}
 *
 * @param <K>
 */
@Internal
public class KeyedStateBackendParametersImpl<K> implements KeyedStateBackendParameters<K> {
    private final Environment env;
    private final JobID jobID;
    private final String operatorIdentifier;
    private final TypeSerializer<K> keySerializer;
    private final int numberOfKeyGroups;
    private final KeyGroupRange keyGroupRange;
    private final TaskKvStateRegistry kvStateRegistry;
    private TtlTimeProvider ttlTimeProvider;
    private final MetricGroup metricGroup;
    private final CustomInitializationMetrics customInitializationMetrics;
    private Collection<KeyedStateHandle> stateHandles;
    private final CloseableRegistry cancelStreamRegistry;
    private final double managedMemoryFraction;

    public KeyedStateBackendParametersImpl(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry) {
        this(
                env,
                jobID,
                operatorIdentifier,
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                kvStateRegistry,
                ttlTimeProvider,
                metricGroup,
                (name, value) -> {},
                stateHandles,
                cancelStreamRegistry,
                1.0);
    }

    public KeyedStateBackendParametersImpl(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            CustomInitializationMetrics customInitializationMetrics,
            Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry,
            double managedMemoryFraction) {
        this.env = env;
        this.jobID = jobID;
        this.operatorIdentifier = operatorIdentifier;
        this.keySerializer = keySerializer;
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.keyGroupRange = keyGroupRange;
        this.kvStateRegistry = kvStateRegistry;
        this.ttlTimeProvider = ttlTimeProvider;
        this.metricGroup = metricGroup;
        this.customInitializationMetrics = customInitializationMetrics;
        this.stateHandles = stateHandles;
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.managedMemoryFraction = managedMemoryFraction;
    }

    public KeyedStateBackendParametersImpl(KeyedStateBackendParameters<K> parameters) {
        this(
                parameters.getEnv(),
                parameters.getJobID(),
                parameters.getOperatorIdentifier(),
                parameters.getKeySerializer(),
                parameters.getNumberOfKeyGroups(),
                parameters.getKeyGroupRange(),
                parameters.getKvStateRegistry(),
                parameters.getTtlTimeProvider(),
                parameters.getMetricGroup(),
                parameters.getCustomInitializationMetrics(),
                parameters.getStateHandles(),
                parameters.getCancelStreamRegistry(),
                parameters.getManagedMemoryFraction());
    }

    @Override
    public Environment getEnv() {
        return env;
    }

    @Override
    public JobID getJobID() {
        return jobID;
    }

    @Override
    public String getOperatorIdentifier() {
        return operatorIdentifier;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public int getNumberOfKeyGroups() {
        return numberOfKeyGroups;
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Override
    public TaskKvStateRegistry getKvStateRegistry() {
        return kvStateRegistry;
    }

    @Override
    public TtlTimeProvider getTtlTimeProvider() {
        return ttlTimeProvider;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    @Nonnull
    @Override
    public Collection<KeyedStateHandle> getStateHandles() {
        return stateHandles;
    }

    @Override
    public CloseableRegistry getCancelStreamRegistry() {
        return cancelStreamRegistry;
    }

    @Override
    public double getManagedMemoryFraction() {
        return managedMemoryFraction;
    }

    @Override
    public CustomInitializationMetrics getCustomInitializationMetrics() {
        return customInitializationMetrics;
    }

    public KeyedStateBackendParametersImpl<K> setStateHandles(
            Collection<KeyedStateHandle> stateHandles) {
        this.stateHandles = stateHandles;
        return this;
    }

    public KeyedStateBackendParametersImpl<K> setTtlTimeProvider(TtlTimeProvider ttlTimeProvider) {
        this.ttlTimeProvider = ttlTimeProvider;
        return this;
    }
}
