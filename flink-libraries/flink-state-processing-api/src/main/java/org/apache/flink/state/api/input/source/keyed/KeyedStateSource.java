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

package org.apache.flink.state.api.input.source.keyed;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.RichSourceReaderContext;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.input.operator.StateReaderOperator;
import org.apache.flink.state.api.input.operator.enumerator.KeyGroupRangeSourceSplitEnumerator;
import org.apache.flink.state.api.input.source.common.NoOpEnumState;
import org.apache.flink.state.api.input.source.common.NoOpEnumStateSerializer;
import org.apache.flink.state.api.input.splits.KeyGroupRangeSourceSplit;
import org.apache.flink.state.api.input.splits.KeyGroupRangeSourceSplitSerializer;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;

public class KeyedStateSource<K, N, OUT>
        implements Source<OUT, KeyGroupRangeSourceSplit, NoOpEnumState> {

    private final OperatorState operatorState;

    private final @Nullable StateBackend stateBackend;

    private final Configuration configuration;

    private final ExecutionConfig executionConfig;

    private final StateReaderOperator<?, K, N, OUT> operator;

    public KeyedStateSource(
            @Nullable StateBackend stateBackend,
            OperatorState operatorState,
            Configuration configuration,
            ExecutionConfig executionConfig,
            StateReaderOperator<?, K, N, OUT> operator) {
        this.stateBackend = stateBackend;
        this.operatorState =
                Preconditions.checkNotNull(operatorState, "The operator state cannot be null");
        this.configuration =
                Preconditions.checkNotNull(configuration, "The configuration cannot be null");
        this.executionConfig =
                Preconditions.checkNotNull(executionConfig, "The execution config cannot be null");
        this.operator = Preconditions.checkNotNull(operator, "The operator cannot be null");
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<OUT, KeyGroupRangeSourceSplit> createReader(
            SourceReaderContext readerContext) throws IOException {
        Preconditions.checkState(
                readerContext instanceof RichSourceReaderContext,
                "Source reader context must be an instance of RichSourceReaderContext");
        return new KeyedStateSourceReader<>(
                (RichSourceReaderContext) readerContext,
                stateBackend,
                operatorState,
                configuration,
                executionConfig,
                operator);
    }

    @Override
    public SplitEnumerator<KeyGroupRangeSourceSplit, NoOpEnumState> createEnumerator(
            SplitEnumeratorContext<KeyGroupRangeSourceSplit> enumContext) {
        return new KeyGroupRangeSourceSplitEnumerator(enumContext, operatorState);
    }

    @Override
    public SplitEnumerator<KeyGroupRangeSourceSplit, NoOpEnumState> restoreEnumerator(
            SplitEnumeratorContext<KeyGroupRangeSourceSplit> enumContext,
            NoOpEnumState checkpoint) {
        return new KeyGroupRangeSourceSplitEnumerator(enumContext, operatorState);
    }

    @Override
    public SimpleVersionedSerializer<KeyGroupRangeSourceSplit> getSplitSerializer() {
        return new KeyGroupRangeSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<NoOpEnumState> getEnumeratorCheckpointSerializer() {
        return NoOpEnumStateSerializer.INSTANCE;
    }
}
