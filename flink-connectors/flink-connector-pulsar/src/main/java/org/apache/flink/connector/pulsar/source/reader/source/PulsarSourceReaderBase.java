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

package org.apache.flink.connector.pulsar.source.reader.source;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.reader.emitter.PulsarRecordEmitter;
import org.apache.flink.connector.pulsar.source.reader.fetcher.PulsarFetcherManagerBase;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitState;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;

/**
 * The common pulsar source reader for both ordered & unordered message consuming.
 *
 * @param <OUT> The output message type for flink.
 */
abstract class PulsarSourceReaderBase<OUT>
        extends SingleThreadMultiplexSourceReaderBase<
                PulsarMessage<OUT>, OUT, PulsarPartitionSplit, PulsarPartitionSplitState> {

    protected final SourceConfiguration sourceConfiguration;
    protected final PulsarClient pulsarClient;
    protected final PulsarAdmin pulsarAdmin;

    protected PulsarSourceReaderBase(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<PulsarMessage<OUT>>> elementsQueue,
            PulsarFetcherManagerBase<OUT> splitFetcherManager,
            Configuration configuration,
            SourceReaderContext context,
            SourceConfiguration sourceConfiguration,
            PulsarClient pulsarClient,
            PulsarAdmin pulsarAdmin) {
        super(
                elementsQueue,
                splitFetcherManager,
                new PulsarRecordEmitter<>(),
                configuration,
                context);

        this.sourceConfiguration = sourceConfiguration;
        this.pulsarClient = pulsarClient;
        this.pulsarAdmin = pulsarAdmin;
    }

    @Override
    protected PulsarPartitionSplitState initializedState(PulsarPartitionSplit split) {
        return new PulsarPartitionSplitState(split);
    }

    @Override
    protected PulsarPartitionSplit toSplitType(
            String splitId, PulsarPartitionSplitState splitState) {
        return splitState.toPulsarPartitionSplit();
    }

    @Override
    public void close() throws Exception {
        // Close the all the consumers first.
        super.close();

        // Close shared pulsar resources.
        pulsarClient.shutdown();
        pulsarAdmin.close();
    }
}
