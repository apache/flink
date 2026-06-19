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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * A mock {@link RecordEmitter} that works with the {@link MockSplitReader} and {@link
 * MockSourceReader}.
 */
public class MockRecordEmitter implements RecordEmitter<int[], Integer, MockSplitState> {
    public static final int RECORD_SIZE_IN_BYTES = 10;
    private final SourceReaderMetricGroup metricGroup;
    private final Set<MockSplitState> knownSplits =
            Collections.newSetFromMap(new IdentityHashMap<>());

    MockRecordEmitter(SourceReaderMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.metricGroup.setPendingBytesGauge(
                () ->
                        knownSplits.stream().mapToLong(MockSplitState::getPendingRecords).sum()
                                * RECORD_SIZE_IN_BYTES);
        this.metricGroup.setPendingRecordsGauge(
                () -> knownSplits.stream().mapToLong(MockSplitState::getPendingRecords).sum());
    }

    @Override
    public void emitRecord(int[] record, SourceOutput<Integer> output, MockSplitState splitState) {
        knownSplits.add(splitState);
        if (record[0] % 2 == 0) {
            this.metricGroup.getNumRecordsInErrorsCounter().inc();
        }
        this.metricGroup.getIOMetricGroup().getNumBytesInCounter().inc(RECORD_SIZE_IN_BYTES);
        this.metricGroup.getIOMetricGroup().getNumBytesOutCounter().inc(RECORD_SIZE_IN_BYTES);

        // The value is the first element.
        output.collect(record[0]);
        // The state will be next index.
        splitState.setRecordIndex(record[1] + 1);
    }
}
