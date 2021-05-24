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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** A {@link Source} class for unit test of base implementation of source connector. */
public class MockBaseSource implements Source<Integer, MockSourceSplit, List<MockSourceSplit>> {
    private static final long serialVersionUID = 4445067705639284175L;

    private final int numSplits;
    private final int numRecordsPerSplit;
    private final int startingValue;
    private final Boundedness boundedness;

    public MockBaseSource(int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
        this(numSplits, numRecordsPerSplit, 0, boundedness);
    }

    public MockBaseSource(
            int numSplits, int numRecordsPerSplit, int startingValue, Boundedness boundedness) {
        this.numSplits = numSplits;
        this.numRecordsPerSplit = numRecordsPerSplit;
        this.startingValue = startingValue;
        this.boundedness = boundedness;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<Integer, MockSourceSplit> createReader(SourceReaderContext readerContext) {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        Configuration config = new Configuration();
        config.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
        config.setLong(SourceReaderOptions.SOURCE_READER_CLOSE_TIMEOUT, 30000L);
        return new MockSourceReader(
                elementsQueue, () -> new MockSplitReader(2, true), config, readerContext);
    }

    @Override
    public SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> createEnumerator(
            SplitEnumeratorContext<MockSourceSplit> enumContext) {
        List<MockSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            int endIndex =
                    boundedness == Boundedness.BOUNDED ? numRecordsPerSplit : Integer.MAX_VALUE;
            MockSourceSplit split = new MockSourceSplit(i, 0, endIndex);
            for (int j = 0; j < numRecordsPerSplit; j++) {
                split.addRecord(startingValue + i * numRecordsPerSplit + j);
            }
            splits.add(split);
        }
        return new MockSplitEnumerator(splits, enumContext);
    }

    @Override
    public SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<MockSourceSplit> enumContext, List<MockSourceSplit> checkpoint)
            throws IOException {
        return new MockSplitEnumerator(checkpoint, enumContext);
    }

    @Override
    public SimpleVersionedSerializer<MockSourceSplit> getSplitSerializer() {
        return new MockSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<List<MockSourceSplit>> getEnumeratorCheckpointSerializer() {
        return new SimpleVersionedSerializer<List<MockSourceSplit>>() {
            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(List<MockSourceSplit> obj) throws IOException {
                return InstantiationUtil.serializeObject(obj.toArray());
            }

            @Override
            public List<MockSourceSplit> deserialize(int version, byte[] serialized)
                    throws IOException {
                MockSourceSplit[] splitArray;
                try {
                    splitArray =
                            InstantiationUtil.deserializeObject(
                                    serialized, getClass().getClassLoader());
                } catch (ClassNotFoundException e) {
                    throw new IOException("Failed to deserialize the source split.");
                }
                return new ArrayList<>(Arrays.asList(splitArray));
            }
        };
    }
}
