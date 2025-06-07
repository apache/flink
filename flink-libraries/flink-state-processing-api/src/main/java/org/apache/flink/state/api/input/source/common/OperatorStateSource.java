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

package org.apache.flink.state.api.input.source.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.input.operator.enumerator.OperatorStateSourceSplitEnumerator;
import org.apache.flink.state.api.input.splits.OperatorStateSourceSplit;
import org.apache.flink.state.api.input.splits.OperatorStateSourceSplitSerializer;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

@Internal
public abstract class OperatorStateSource<OUT>
        implements Source<OUT, OperatorStateSourceSplit, NoOpEnumState> {

    protected final @Nullable StateBackend stateBackend;

    protected final OperatorState operatorState;

    protected final Configuration configuration;

    protected final ExecutionConfig executionConfig;

    public OperatorStateSource(
            @Nullable StateBackend stateBackend,
            OperatorState operatorState,
            Configuration configuration,
            ExecutionConfig executionConfig) {
        this.stateBackend = stateBackend;
        this.operatorState =
                Preconditions.checkNotNull(operatorState, "The operator state cannot be null");
        this.configuration =
                Preconditions.checkNotNull(configuration, "The configuration cannot be null");
        this.executionConfig =
                Preconditions.checkNotNull(executionConfig, "The executionConfig cannot be null");
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<OperatorStateSourceSplit, NoOpEnumState> createEnumerator(
            SplitEnumeratorContext<OperatorStateSourceSplit> enumContext) {
        return new OperatorStateSourceSplitEnumerator(enumContext, operatorState);
    }

    @Override
    public SplitEnumerator<OperatorStateSourceSplit, NoOpEnumState> restoreEnumerator(
            SplitEnumeratorContext<OperatorStateSourceSplit> enumContext,
            NoOpEnumState checkpoint) {
        return new OperatorStateSourceSplitEnumerator(enumContext, operatorState);
    }

    @Override
    public SimpleVersionedSerializer<OperatorStateSourceSplit> getSplitSerializer() {
        return new OperatorStateSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<NoOpEnumState> getEnumeratorCheckpointSerializer() {
        return NoOpEnumStateSerializer.INSTANCE;
    }
}
