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

package org.apache.flink.table.runtime.functions.table.fullcache.inputformat;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TestInputFormat that reads data from {@link #numSplits} splits on first load and ({@link
 * #numSplits} + {@link #deltaNumSplits}) splits on next load. Splits share the same {@code queue}.
 */
public class FullCacheTestInputFormat
        extends RichInputFormat<RowData, FullCacheTestInputFormat.QueueInputSplit> {

    public static final AtomicInteger OPEN_CLOSED_COUNTER = new AtomicInteger(0);
    private static final int DEFAULT_NUM_SPLITS = 2;
    private static final int DEFAULT_DELTA_NUM_SPLITS = 0;

    // RowData is not serializable, so we store Rows
    private final Collection<Row> dataRows;
    private final DataFormatConverters.RowConverter rowConverter;
    private final GeneratedProjection generatedProjection;
    private final boolean projectable;
    private final int numSplits;
    private final int deltaNumSplits;

    private transient ConcurrentLinkedQueue<RowData> queue;
    private transient Projection<RowData, GenericRowData> projection;

    private int loadCounter;
    private int maxReadRecords;
    private int readRecords;

    private int numOpens;

    public FullCacheTestInputFormat(
            Collection<Row> dataRows,
            Optional<GeneratedProjection> generatedProjection,
            DataFormatConverters.RowConverter rowConverter,
            int numSplits,
            int deltaNumSplits) {
        // for unit tests
        this.dataRows = dataRows;
        this.projectable = generatedProjection.isPresent();
        this.generatedProjection = generatedProjection.orElse(null);
        this.rowConverter = rowConverter;
        this.numSplits = numSplits;
        this.deltaNumSplits = deltaNumSplits;
    }

    public FullCacheTestInputFormat(
            Collection<Row> dataRows,
            Optional<GeneratedProjection> generatedProjection,
            DataFormatConverters.RowConverter rowConverter) {
        // for integration tests
        this(
                dataRows,
                generatedProjection,
                rowConverter,
                DEFAULT_NUM_SPLITS,
                DEFAULT_DELTA_NUM_SPLITS);
    }

    @Override
    public QueueInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        int delta = loadCounter > 0 ? deltaNumSplits : 0;
        int currentSplits = numSplits + delta;
        ConcurrentLinkedQueue<RowData> queue = new ConcurrentLinkedQueue<>();
        QueueInputSplit[] splits = new QueueInputSplit[currentSplits];
        IntStream.range(0, currentSplits).forEach(i -> splits[i] = new QueueInputSplit(queue, i));
        dataRows.forEach(row -> queue.add(rowConverter.toInternal(row)));
        // divide data evenly between InputFormat copies
        loadCounter++;
        maxReadRecords = (int) Math.ceil((double) queue.size() / currentSplits);
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
        if (projectable) {
            projection =
                    generatedProjection.newInstance(Thread.currentThread().getContextClassLoader());
        }
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
        RowData rowData = queue.poll();
        if (rowData != null && projectable) {
            // InputSplitCacheLoadTask will do copy work
            return projection.apply(rowData);
        }
        return rowData;
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
