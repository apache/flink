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

package org.apache.flink.connector.base.source.reader.mocks;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Map;
import java.util.function.Supplier;

/** A mock SourceReader class. */
public class MockSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
                int[], Integer, MockSourceSplit, MockSplitState> {

    public MockSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue,
            Supplier<SplitReader<int[], MockSourceSplit>> splitFetcherSupplier,
            Configuration config,
            SourceReaderContext context) {
        super(
                elementsQueue,
                splitFetcherSupplier,
                new MockRecordEmitter(context.metricGroup()),
                config,
                context);
    }

    public MockSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue,
            SingleThreadFetcherManager<int[], MockSourceSplit> splitSplitFetcherManager,
            Configuration config,
            SourceReaderContext context) {
        super(
                elementsQueue,
                splitSplitFetcherManager,
                new MockRecordEmitter(context.metricGroup()),
                config,
                context);
    }

    @Override
    protected void onSplitFinished(Map<String, MockSplitState> finishedSplitIds) {}

    @Override
    protected MockSplitState initializedState(MockSourceSplit split) {
        return new MockSplitState(split.index(), split.endIndex());
    }

    @Override
    protected MockSourceSplit toSplitType(String splitId, MockSplitState splitState) {
        return new MockSourceSplit(Integer.parseInt(splitId), splitState.getRecordIndex());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
