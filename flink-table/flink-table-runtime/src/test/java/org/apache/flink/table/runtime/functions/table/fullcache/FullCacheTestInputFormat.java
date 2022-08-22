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

package org.apache.flink.table.runtime.functions.table.fullcache;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** TestInputFormat that reads data from (2 + delta) splits which share the same {@code queue}. */
public class FullCacheTestInputFormat
        extends RichInputFormat<RowData, FullCacheTestInputFormat.QueueInputSplit> {

    private static final int DEFAULT_NUM_SPLITS = 2;
    private static final AtomicInteger OPEN_CLOSED_COUNTER = new AtomicInteger(0);

    // RowData is not serializable, so we store Rows
    private final Collection<Row> dataRows;
    private final DataFormatConverters.RowConverter rowConverter;
    private final int deltaNumSplits;
    private final transient Consumer<Collection<RowData>> secondLoadDataChange;

    private transient ConcurrentLinkedQueue<RowData> queue;
    private int loadCounter;
    private int maxReadRecords;
    private int readRecords;

    private int numOpens;

    public FullCacheTestInputFormat(
            Collection<Row> dataRows,
            DataFormatConverters.RowConverter rowConverter,
            int deltaNumSplits,
            Consumer<Collection<RowData>> secondLoadDataChange) {
        // for unit tests
        this.dataRows = dataRows;
        this.rowConverter = rowConverter;
        this.deltaNumSplits = deltaNumSplits;
        this.secondLoadDataChange = secondLoadDataChange;
    }

    public FullCacheTestInputFormat(
            Collection<Row> dataRows, DataFormatConverters.RowConverter rowConverter) {
        // for integration tests
        this.dataRows = dataRows;
        this.rowConverter = rowConverter;
        this.deltaNumSplits = 0;
        this.secondLoadDataChange = null;
    }

    @Override
    public QueueInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        int delta = loadCounter > 0 ? deltaNumSplits : 0;
        int numSplits = DEFAULT_NUM_SPLITS + delta;
        ConcurrentLinkedQueue<RowData> queue = new ConcurrentLinkedQueue<>();
        QueueInputSplit[] splits = new QueueInputSplit[numSplits];
        IntStream.range(0, numSplits).forEach(i -> splits[i] = new QueueInputSplit(queue, i));
        dataRows.forEach(row -> queue.add(rowConverter.toInternal(row)));
        // divide data evenly between InputFormat copies
        loadCounter++;
        if (loadCounter == 2 && secondLoadDataChange != null) {
            secondLoadDataChange.accept(queue);
        }
        maxReadRecords = (int) Math.ceil((double) queue.size() / numSplits);
        return splits;
    }

    @Override
    public void openInputFormat() {
        numOpens++;
        OPEN_CLOSED_COUNTER.incrementAndGet();
    }

    @Override
    public void open(QueueInputSplit split) throws IOException {
        this.queue = split.getQueue();
        this.readRecords = 0;
        numOpens++;
        OPEN_CLOSED_COUNTER.incrementAndGet();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return queue.isEmpty();
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        assertThat(numOpens).isEqualTo(2);
        if (readRecords == maxReadRecords) {
            return null;
        }
        readRecords++;
        return queue.poll();
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(QueueInputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {
        OPEN_CLOSED_COUNTER.decrementAndGet();
    }

    @Override
    public void closeInputFormat() {
        OPEN_CLOSED_COUNTER.decrementAndGet();
    }

    public boolean isClosed() {
        return OPEN_CLOSED_COUNTER.get() == 0;
    }

    /** {@link InputSplit} that provides queue to {@link InputFormat}. */
    public static class QueueInputSplit implements InputSplit {

        private final transient ConcurrentLinkedQueue<RowData> queue;
        private final int splitNumber;

        public QueueInputSplit(ConcurrentLinkedQueue<RowData> queue, int splitNumber) {
            this.queue = queue;
            this.splitNumber = splitNumber;
        }

        @Override
        public int getSplitNumber() {
            return splitNumber;
        }

        public ConcurrentLinkedQueue<RowData> getQueue() {
            return queue;
        }
    }
}
