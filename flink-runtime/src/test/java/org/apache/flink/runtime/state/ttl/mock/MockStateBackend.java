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

package org.apache.flink.runtime.state.ttl.mock;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackend.MockSnapshotSupplier;

import javax.annotation.Nonnull;

import java.util.HashMap;

/** mack state backend. */
public class MockStateBackend extends AbstractStateBackend {
    private static final long serialVersionUID = 995676510267499393L;
    private MockSnapshotSupplier snapshotSupplier;

    public MockStateBackend() {
        this(MockSnapshotSupplier.DEFAULT);
    }

    public MockStateBackend(MockSnapshotSupplier snapshotSupplier) {
        this.snapshotSupplier = snapshotSupplier;
    }

    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) {
        return new MockKeyedStateBackendBuilder<>(
                        new KvStateRegistry()
                                .createTaskRegistry(parameters.getJobID(), new JobVertexID()),
                        parameters.getKeySerializer(),
                        parameters.getEnv().getUserCodeClassLoader().asClassLoader(),
                        parameters.getNumberOfKeyGroups(),
                        parameters.getKeyGroupRange(),
                        parameters.getEnv().getExecutionConfig(),
                        parameters.getTtlTimeProvider(),
                        LatencyTrackingStateConfig.disabled(),
                        parameters.getStateHandles(),
                        AbstractStateBackend.getCompressionDecorator(
                                parameters.getEnv().getExecutionConfig()),
                        parameters.getCancelStreamRegistry(),
                        snapshotSupplier)
                .build();
    }

    public OperatorStateBackend createOperatorStateBackend(
            OperatorStateBackendParameters parameters) {
        return new DefaultOperatorStateBackend(
                parameters.getEnv().getExecutionConfig(),
                parameters.getCancelStreamRegistry(),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new SnapshotStrategyRunner<>(
                        "",
                        new SnapshotStrategy<OperatorStateHandle, SnapshotResources>() {
                            @Override
                            public SnapshotResources syncPrepareResources(long checkpointId) {
                                return () -> {};
                            }

                            @Override
                            public SnapshotResultSupplier<OperatorStateHandle> asyncSnapshot(
                                    SnapshotResources syncPartResource,
                                    long checkpointId,
                                    long timestamp,
                                    @Nonnull CheckpointStreamFactory streamFactory,
                                    @Nonnull CheckpointOptions checkpointOptions) {
                                return snapshotCloseableRegistry -> SnapshotResult.empty();
                            }
                        },
                        new CloseableRegistry(),
                        SnapshotExecutionType.ASYNCHRONOUS));
    }
}
