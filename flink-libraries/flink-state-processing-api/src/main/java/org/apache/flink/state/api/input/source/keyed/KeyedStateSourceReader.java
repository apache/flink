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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.RichSourceReaderContext;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.input.operator.StateReaderOperator;
import org.apache.flink.state.api.input.source.common.BufferingCollector;
import org.apache.flink.state.api.input.source.common.StreamOperatorContextBuilder;
import org.apache.flink.state.api.input.splits.KeyGroupRangeSourceSplit;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.StreamOperatorStateContext;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/** A {@link SourceReader} implementation that reads data from keyed state. */
public class KeyedStateSourceReader<K, N, OUT>
        implements SourceReader<OUT, KeyGroupRangeSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyedStateSourceReader.class);

    private final RichSourceReaderContext sourceReaderContext;

    private final @Nullable StateBackend stateBackend;

    private final OperatorState operatorState;

    private final Configuration configuration;

    private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

    private final StateReaderOperator<?, K, N, OUT> operator;

    private CloseableRegistry registry;

    private CloseableIterator<Tuple2<K, N>> keysAndNamespaces;

    private final Queue<KeyGroupRangeSourceSplit> remainingSplits;

    private boolean noMoreSplits;

    private final BufferingCollector<OUT> outBufferingCollector;

    public KeyedStateSourceReader(
            RichSourceReaderContext sourceReaderContext,
            @Nullable StateBackend stateBackend,
            OperatorState operatorState,
            Configuration configuration,
            ExecutionConfig executionConfig,
            StateReaderOperator<?, K, N, OUT> operator)
            throws IOException {
        this.sourceReaderContext = sourceReaderContext;
        this.stateBackend = stateBackend;
        this.operatorState = operatorState;
        this.configuration = configuration;
        this.serializedExecutionConfig = new SerializedValue<>(executionConfig);
        this.operator = operator;
        this.remainingSplits = new ArrayDeque<>();
        this.noMoreSplits = false;
        this.outBufferingCollector = new BufferingCollector<>();
    }

    @Override
    public void start() {
        sourceReaderContext.sendSplitRequest();
    }

    @SuppressWarnings("unchecked")
    @Override
    public InputStatus pollNext(ReaderOutput<OUT> output) throws Exception {
        // If no keys and namespaces then try to open it
        if (keysAndNamespaces == null) {
            KeyGroupRangeSourceSplit split = remainingSplits.poll();
            if (split != null) {
                registry = new CloseableRegistry();
                RuntimeContext runtimeContext = sourceReaderContext.getRuntimeContext();
                ExecutionConfig executionConfig;
                try {
                    executionConfig =
                            serializedExecutionConfig.deserializeValue(
                                    runtimeContext.getUserCodeClassLoader());
                } catch (ClassNotFoundException | IOException e) {
                    throw new RuntimeException("Could not deserialize ExecutionConfig.", e);
                }
                final StreamOperatorStateContext context =
                        new StreamOperatorContextBuilder(
                                        runtimeContext,
                                        configuration,
                                        operatorState,
                                        split,
                                        registry,
                                        stateBackend,
                                        executionConfig)
                                .withMaxParallelism(split.getNumKeyGroups())
                                .withKey(
                                        operator,
                                        runtimeContext.createSerializer(operator.getKeyType()))
                                .build(LOG);

                AbstractKeyedStateBackend<K> keyedStateBackend =
                        (AbstractKeyedStateBackend<K>) context.keyedStateBackend();

                final DefaultKeyedStateStore keyedStateStore =
                        new DefaultKeyedStateStore(
                                keyedStateBackend, runtimeContext::createSerializer);
                SavepointRuntimeContext ctx =
                        new SavepointRuntimeContext(runtimeContext, keyedStateStore);

                InternalTimeServiceManager<K> timeServiceManager =
                        (InternalTimeServiceManager<K>) context.internalTimerServiceManager();
                try {
                    operator.setup(
                            runtimeContext::createSerializer,
                            keyedStateBackend,
                            timeServiceManager,
                            ctx);
                    operator.open();
                    keysAndNamespaces = operator.getKeysAndNamespaces(ctx);
                } catch (Exception e) {
                    throw new IOException("Failed to restore state", e);
                }

                // We send a request for more
                if (remainingSplits.isEmpty() && !noMoreSplits) {
                    sourceReaderContext.sendSplitRequest();
                }
            }
        }

        // If keys and namespaces exists then fill the buffer
        if (keysAndNamespaces != null && keysAndNamespaces.hasNext()) {
            final Tuple2<K, N> keyAndNamespace = keysAndNamespaces.next();
            operator.setCurrentKey(keyAndNamespace.f0);

            try {
                operator.processElement(
                        keyAndNamespace.f0, keyAndNamespace.f1, outBufferingCollector);
            } catch (Exception e) {
                throw new IOException(
                        "User defined function KeyedStateReaderFunction#readKey threw an exception",
                        e);
            }
        } else {
            IOUtils.closeQuietly(keysAndNamespaces);
            IOUtils.closeQuietly(operator);
            IOUtils.closeQuietly(registry);
            keysAndNamespaces = null;
            registry = null;
        }

        // If out buffer is already filled by the operator then just collect a value
        if (outBufferingCollector.hasNext()) {
            output.collect(outBufferingCollector.next());
            return InputStatus.MORE_AVAILABLE;
        }

        // Here we have nothing to collect
        if (remainingSplits.isEmpty()) {
            if (noMoreSplits) {
                // No further data so signal end
                return InputStatus.END_OF_INPUT;
            } else {
                // When there are splits remote then we signal nothing available
                return InputStatus.NOTHING_AVAILABLE;
            }
        } else {
            // When we have splits locally then we just need to process them in the next round
            return InputStatus.MORE_AVAILABLE;
        }
    }

    @Override
    public List<KeyGroupRangeSourceSplit> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<KeyGroupRangeSourceSplit> splits) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(keysAndNamespaces);
        IOUtils.closeQuietly(operator);
        IOUtils.closeQuietly(registry);
    }
}
