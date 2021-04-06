/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public abstract class HeapStateBackendTestBase {

    @Parameterized.Parameters
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Parameterized.Parameter public boolean async;

    public HeapKeyedStateBackend<String> createKeyedBackend(
            Collection<KeyedStateHandle> stateHandles) throws Exception {
        return createKeyedBackend(StringSerializer.INSTANCE, stateHandles);
    }

    public <K> HeapKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer, Collection<KeyedStateHandle> stateHandles)
            throws Exception {
        final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 15);
        final int numKeyGroups = keyGroupRange.getNumberOfKeyGroups();
        ExecutionConfig executionConfig = new ExecutionConfig();

        return new HeapKeyedStateBackendBuilder<>(
                        mock(TaskKvStateRegistry.class),
                        keySerializer,
                        HeapStateBackendTestBase.class.getClassLoader(),
                        numKeyGroups,
                        keyGroupRange,
                        executionConfig,
                        TtlTimeProvider.DEFAULT,
                        LatencyTrackingStateConfig.disabled(),
                        stateHandles,
                        AbstractStateBackend.getCompressionDecorator(executionConfig),
                        TestLocalRecoveryConfig.disabled(),
                        new HeapPriorityQueueSetFactory(keyGroupRange, numKeyGroups, 128),
                        async,
                        new CloseableRegistry())
                .build();
    }
}
