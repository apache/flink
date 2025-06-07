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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.RichSourceReaderContext;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.input.splits.OperatorStateSourceSplit;
import org.apache.flink.streaming.api.operators.StreamOperatorStateContext;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/** A {@link SourceReader} implementation that reads data from operator state. */
@Internal
public abstract class OperatorStateSourceReader<OUT>
        implements SourceReader<OUT, OperatorStateSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorStateSourceReader.class);

    private final RichSourceReaderContext sourceReaderContext;

    private final @Nullable StateBackend stateBackend;

    private final OperatorState operatorState;

    private final Configuration configuration;

    private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

    private CloseableRegistry registry;

    private OperatorStateBackend restoredBackend;

    private final Queue<OperatorStateSourceSplit> remainingSplits;

    private boolean noMoreSplits;

    private Iterator<OUT> elementsIterator;

    public OperatorStateSourceReader(
            RichSourceReaderContext sourceReaderContext,
            @Nullable StateBackend stateBackend,
            OperatorState operatorState,
            Configuration configuration,
            ExecutionConfig executionConfig)
            throws IOException {
        this.sourceReaderContext = sourceReaderContext;
        this.operatorState = operatorState;
        this.stateBackend = stateBackend;
        this.configuration = configuration;
        this.serializedExecutionConfig = new SerializedValue<>(executionConfig);
        this.remainingSplits = new ArrayDeque<>();
    }

    @Override
    public void start() {
        sourceReaderContext.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OUT> output) throws Exception {
        // If no iterator then try to open it
        if (elementsIterator == null) {
            OperatorStateSourceSplit split = remainingSplits.poll();
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
                                .build(LOG);

                restoredBackend = context.operatorStateBackend();

                try {
                    elementsIterator = getElements(restoredBackend).iterator();
                } catch (Exception e) {
                    throw new IOException(
                            "Failed to read operator state from restored state backend", e);
                }

                // We send a request for more
                if (remainingSplits.isEmpty() && !noMoreSplits) {
                    sourceReaderContext.sendSplitRequest();
                }
            }
        }

        // If keys and namespaces exists then just collect a value
        if (elementsIterator != null && elementsIterator.hasNext()) {
            output.collect(elementsIterator.next());
            return InputStatus.MORE_AVAILABLE;
        } else {
            if (registry != null) {
                registry.unregisterCloseable(restoredBackend);
            }
            IOUtils.closeQuietly(restoredBackend);
            IOUtils.closeQuietly(registry);
            elementsIterator = null;
            restoredBackend = null;
            registry = null;
        }

        // Here we have nothing to collect
        if (noMoreSplits) {
            // No further data so signal end
            return InputStatus.END_OF_INPUT;
        } else {
            // When there are splits remote then we signal nothing available
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    protected abstract Iterable<OUT> getElements(OperatorStateBackend restoredBackend)
            throws Exception;

    @Override
    public List<OperatorStateSourceSplit> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<OperatorStateSourceSplit> splits) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void close() throws Exception {
        if (registry != null) {
            registry.unregisterCloseable(restoredBackend);
            IOUtils.closeQuietly(restoredBackend);
            IOUtils.closeQuietly(registry);
        }
    }
}
