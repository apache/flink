/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplitState;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Supplier;

/**
 * The common mongo source reader for both ordered & unordered message consuming.
 *
 * @param <OUT> The output message type for flink.
 */
public class MongoSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<
                BsonDocument, OUT, MongoSourceSplit, MongoSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoSourceReader.class);

    public MongoSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<BsonDocument>> elementQueue,
            Supplier<SplitReader<BsonDocument, MongoSourceSplit>> splitReaderSupplier,
            RecordEmitter<BsonDocument, OUT, MongoSourceSplitState> recordEmitter,
            SourceReaderContext readerContext) {
        super(
                elementQueue,
                new SingleThreadFetcherManager<>(
                        elementQueue, splitReaderSupplier, readerContext.getConfiguration()),
                recordEmitter,
                readerContext.getConfiguration(),
                readerContext);
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, MongoSourceSplitState> finishedSplitIds) {
        for (MongoSourceSplitState splitState : finishedSplitIds.values()) {
            MongoSourceSplit sourceSplit = splitState.toMongoSourceSplit();
            LOG.info("Split {} is finished.", sourceSplit.splitId());
        }
        context.sendSplitRequest();
    }

    @Override
    protected MongoSourceSplitState initializedState(MongoSourceSplit split) {
        return new MongoSourceSplitState(split);
    }

    @Override
    protected MongoSourceSplit toSplitType(String splitId, MongoSourceSplitState splitState) {
        return splitState.toMongoSourceSplit();
    }
}
