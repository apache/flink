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

package org.apache.flink.streaming.tests;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A stub implementation of the {@link StateBackend} that allows the use of a custom {@link
 * TtlTimeProvider}.
 */
final class StubStateBackend implements StateBackend {

    private static final long serialVersionUID = 1L;

    private final TtlTimeProvider ttlTimeProvider;

    private final StateBackend backend;

    StubStateBackend(final StateBackend wrappedBackend, final TtlTimeProvider ttlTimeProvider) {
        this.backend = checkNotNull(wrappedBackend);
        this.ttlTimeProvider = checkNotNull(ttlTimeProvider);
    }

    @Override
    public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws Exception {
        return backend.createKeyedStateBackend(
                new KeyedStateBackendParametersImpl<>(parameters)
                        .setTtlTimeProvider(ttlTimeProvider));
    }

    @Override
    public boolean useManagedMemory() {
        return backend.useManagedMemory();
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            OperatorStateBackendParameters parameters) throws Exception {
        return backend.createOperatorStateBackend(parameters);
    }

    @Override
    public boolean supportsNoClaimRestoreMode() {
        return backend.supportsNoClaimRestoreMode();
    }

    @Override
    public boolean supportsSavepointFormat(SavepointFormatType formatType) {
        return true;
    }
}
