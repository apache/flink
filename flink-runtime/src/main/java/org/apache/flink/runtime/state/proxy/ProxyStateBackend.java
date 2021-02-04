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

package org.apache.flink.runtime.state.proxy;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Collection;

/** */
public class ProxyStateBackend implements StateBackend {
    private static final Logger LOG = LoggerFactory.getLogger(ProxyStateBackend.class);

    final StateBackend proxiedStateBackend;

    public ProxyStateBackend(StateBackend stateBackend) {
        this.proxiedStateBackend = stateBackend;
    }

    @Override
    // this is called when StreamTask create KeyedStateBackend
    public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {
        LOG.debug("createKeyedStateBackend is called");
        AbstractKeyedStateBackend<K> keyedStateBackend =
                (AbstractKeyedStateBackend<K>)
                        proxiedStateBackend.createKeyedStateBackend(
                                env,
                                jobID,
                                operatorIdentifier,
                                keySerializer,
                                numberOfKeyGroups,
                                keyGroupRange,
                                kvStateRegistry,
                                ttlTimeProvider,
                                metricGroup,
                                stateHandles,
                                cancelStreamRegistry);
        return new ProxyKeyedStateBackend<>(
                keyedStateBackend, env.getExecutionConfig(), ttlTimeProvider);
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            @Nonnull Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {
        LOG.debug("createOperatorStateBackend is called");
        return proxiedStateBackend.createOperatorStateBackend(
                env, operatorIdentifier, stateHandles, cancelStreamRegistry);
    }

    public StateBackend getProxiedStateBackend() {
        return proxiedStateBackend;
    }
}
