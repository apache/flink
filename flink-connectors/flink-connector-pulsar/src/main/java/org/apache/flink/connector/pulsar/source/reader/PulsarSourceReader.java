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

package org.apache.flink.connector.pulsar.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.pulsar.source.reader.fetcher.PulsarSourceFetcherManager;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitState;
import org.apache.flink.util.function.RunnableWithException;

import java.util.Map;
import java.util.function.Supplier;

/** The source reader for pulsar subscription. */
public class PulsarSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                PulsarMessage<T>, T, PulsarPartitionSplit, PulsarPartitionSplitState> {

    private final RunnableWithException closeCallback;

    public PulsarSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<PulsarMessage<T>>> elementsQueue,
            Supplier<PulsarPartitionSplitReader<T>> splitReaderSupplier,
            RecordEmitter<PulsarMessage<T>, T, PulsarPartitionSplitState> recordEmitter,
            Configuration configuration,
            SourceReaderContext context,
            RunnableWithException closeCallback) {

        // Since pulsar have its own configuration class, this Configuration class is used less and
        // hard to use. We just pass a default instance instead.
        super(
                elementsQueue,
                new PulsarSourceFetcherManager<>(elementsQueue, splitReaderSupplier::get),
                recordEmitter,
                configuration,
                context);

        this.closeCallback = closeCallback;
    }

    @Override
    protected void onSplitFinished(Map<String, PulsarPartitionSplitState> finishedSplitIds) {}

    @Override
    protected PulsarPartitionSplitState initializedState(PulsarPartitionSplit split) {
        return null;
    }

    @Override
    protected PulsarPartitionSplit toSplitType(
            String splitId, PulsarPartitionSplitState splitState) {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {}

    @Override
    public void close() throws Exception {
        super.close();
        closeCallback.run();
    }
}
