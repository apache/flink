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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaRecordSerializationSchema.createProjectedRow;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

class ReducingUpsertWriter<WriterState> implements SinkWriter<RowData, Void, WriterState> {

    private final SinkWriter<RowData, ?, WriterState> wrappedWriter;
    private final WrappedContext wrappedContext = new WrappedContext();
    private final int batchMaxRowNums;
    private final Function<RowData, RowData> valueCopyFunction;
    private final Map<RowData, Tuple2<RowData, Long>> reduceBuffer = new HashMap<>();
    private final Function<RowData, RowData> keyExtractor;
    private final Sink.ProcessingTimeService timeService;
    private final long batchIntervalMs;

    private boolean closed = false;
    private long lastFlush = System.currentTimeMillis();

    ReducingUpsertWriter(
            SinkWriter<RowData, ?, WriterState> wrappedWriter,
            DataType physicalDataType,
            int[] keyProjection,
            SinkBufferFlushMode bufferFlushMode,
            Sink.ProcessingTimeService timeService,
            Function<RowData, RowData> valueCopyFunction) {
        checkArgument(bufferFlushMode != null && bufferFlushMode.isEnabled());
        this.wrappedWriter = checkNotNull(wrappedWriter);
        this.timeService = checkNotNull(timeService);
        this.batchMaxRowNums = bufferFlushMode.getBatchSize();
        this.batchIntervalMs = bufferFlushMode.getBatchIntervalMs();
        registerFlush();
        List<LogicalType> fields = physicalDataType.getLogicalType().getChildren();
        final RowData.FieldGetter[] keyFieldGetters =
                Arrays.stream(keyProjection)
                        .mapToObj(
                                targetField ->
                                        RowData.createFieldGetter(
                                                fields.get(targetField), targetField))
                        .toArray(RowData.FieldGetter[]::new);
        this.keyExtractor = rowData -> createProjectedRow(rowData, RowKind.INSERT, keyFieldGetters);
        this.valueCopyFunction = valueCopyFunction;
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        wrappedContext.setContext(context);
        addToBuffer(element, context.timestamp());
    }

    @Override
    public List<Void> prepareCommit(boolean flush) throws IOException, InterruptedException {
        flush();
        return Collections.emptyList();
    }

    @Override
    public List<WriterState> snapshotState(long checkpointId) throws IOException {
        return wrappedWriter.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
        if (!closed) {
            closed = true;
            wrappedWriter.close();
        }
    }

    private void addToBuffer(RowData row, Long timestamp) throws IOException, InterruptedException {
        RowData key = keyExtractor.apply(row);
        RowData value = valueCopyFunction.apply(row);
        reduceBuffer.put(key, new Tuple2<>(changeFlag(value), timestamp));

        if (reduceBuffer.size() >= batchMaxRowNums) {
            flush();
        }
    }

    private void registerFlush() {
        if (closed) {
            return;
        }
        timeService.registerProcessingTimer(
                lastFlush + batchIntervalMs,
                (t) -> {
                    if (t >= lastFlush + batchIntervalMs) {
                        flush();
                    }
                    registerFlush();
                });
    }

    private RowData changeFlag(RowData value) {
        switch (value.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                value.setRowKind(UPDATE_AFTER);
                break;
            case UPDATE_BEFORE:
            case DELETE:
                value.setRowKind(DELETE);
        }
        return value;
    }

    private void flush() throws IOException, InterruptedException {
        for (Tuple2<RowData, Long> value : reduceBuffer.values()) {
            wrappedContext.setTimestamp(value.f1);
            wrappedWriter.write(value.f0, wrappedContext);
        }
        lastFlush = System.currentTimeMillis();
        reduceBuffer.clear();
    }

    /**
     * Wrapper of {@link SinkWriter.Context}.
     *
     * <p>When records arrives, the {@link ReducingUpsertWriter} updates the current {@link
     * SinkWriter.Context} and memorize the timestamp with the records. When flushing, the {@link
     * ReducingUpsertWriter} will emit the records in the buffer with memorized timestamp.
     */
    private static class WrappedContext implements SinkWriter.Context {
        private long timestamp;
        private SinkWriter.Context context;

        @Override
        public long currentWatermark() {
            checkNotNull(context, "context must be set before retrieving it.");
            return context.currentWatermark();
        }

        @Override
        public Long timestamp() {
            checkNotNull(timestamp, "timestamp must to be set before retrieving it.");
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public void setContext(SinkWriter.Context context) {
            this.context = context;
        }
    }
}
