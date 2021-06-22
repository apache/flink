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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaSerializationSchema.createProjectedRow;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The wrapper of the {@link RichSinkFunction}. It will buffer the data and emit when the buffer is
 * full or timer is triggered or checkpointing.
 */
public class BufferedUpsertSinkFunction extends RichSinkFunction<RowData>
        implements CheckpointedFunction, CheckpointListener {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(BufferedUpsertSinkFunction.class);

    // --------------------------------------------------------------------------------------------
    // Config
    // --------------------------------------------------------------------------------------------

    private final RichSinkFunction<RowData> producer;
    private final int batchMaxRowNums;
    private final long batchIntervalMs;
    private final DataType physicalDataType;
    private final int[] keyProjection;
    private final TypeInformation<RowData> consumedRowDataTypeInfo;
    private boolean closed;

    // --------------------------------------------------------------------------------------------
    // Writer and buffer
    // --------------------------------------------------------------------------------------------

    private int batchCount = 0;
    private transient Map<RowData, Tuple2<RowData, Long>> reduceBuffer;
    private transient WrappedContext wrappedContext;
    private transient Function<RowData, RowData> keyExtractor;
    private transient Function<RowData, RowData> valueCopier;

    // --------------------------------------------------------------------------------------------
    // Timer attributes
    // --------------------------------------------------------------------------------------------

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;

    public BufferedUpsertSinkFunction(
            RichSinkFunction<RowData> producer,
            DataType physicalDataType,
            int[] keyProjection,
            TypeInformation<RowData> consumedRowDataTypeInfo,
            SinkBufferFlushMode bufferFlushMode) {
        checkArgument(bufferFlushMode != null && bufferFlushMode.isEnabled());
        this.producer = checkNotNull(producer, "Producer must not be null.");
        this.physicalDataType =
                checkNotNull(physicalDataType, "Physical data type must not be null.");
        this.keyProjection = checkNotNull(keyProjection, "key projection must not be null.");
        this.consumedRowDataTypeInfo = consumedRowDataTypeInfo;
        this.batchMaxRowNums = bufferFlushMode.getBatchSize();
        this.batchIntervalMs = bufferFlushMode.getBatchIntervalMs();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // init variable
        reduceBuffer = new HashMap<>();
        wrappedContext = new WrappedContext();
        closed = false;

        // create keyExtractor and value copier
        List<LogicalType> fields = physicalDataType.getLogicalType().getChildren();
        final RowData.FieldGetter[] keyFieldGetters =
                Arrays.stream(keyProjection)
                        .mapToObj(
                                targetField ->
                                        RowData.createFieldGetter(
                                                fields.get(targetField), targetField))
                        .toArray(RowData.FieldGetter[]::new);
        this.keyExtractor = rowData -> createProjectedRow(rowData, RowKind.INSERT, keyFieldGetters);

        TypeSerializer<RowData> typeSerializer =
                consumedRowDataTypeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
        this.valueCopier =
                getRuntimeContext().getExecutionConfig().isObjectReuseEnabled()
                        ? typeSerializer::copy
                        : Function.identity();

        // register timer
        this.scheduler =
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("upsert-kafka-sink-function"));
        this.scheduledFuture =
                this.scheduler.scheduleWithFixedDelay(
                        () -> {
                            synchronized (BufferedUpsertSinkFunction.this) {
                                if (!closed) {
                                    try {
                                        flush();
                                    } catch (Exception e) {
                                        flushException = e;
                                    }
                                }
                            }
                        },
                        batchIntervalMs,
                        batchIntervalMs,
                        TimeUnit.MILLISECONDS);

        producer.open(parameters);
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        producer.setRuntimeContext(t);
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return producer.getRuntimeContext();
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        wrappedContext.setContext(context);
        addToBuffer(value, context.timestamp());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (producer instanceof CheckpointListener) {
            ((CheckpointListener) producer).notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (producer instanceof CheckpointListener) {
            ((CheckpointListener) producer).notifyCheckpointAborted(checkpointId);
        }
    }

    @Override
    public synchronized void close() throws Exception {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            if (batchCount > 0) {
                try {
                    flush();
                } catch (Exception e) {
                    LOG.warn("Writing records to kafka failed.", e);
                    throw new RuntimeException("Writing records to kafka failed.", e);
                }
            }

            producer.close();
        }
        super.close();
        checkFlushException();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flush();
        if (producer instanceof CheckpointedFunction) {
            ((CheckpointedFunction) producer).snapshotState(context);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (producer instanceof CheckpointedFunction) {
            ((CheckpointedFunction) producer).initializeState(context);
        }
    }

    // --------------------------------------------------------------------------------------------

    private synchronized void addToBuffer(RowData row, Long timestamp) throws Exception {
        checkFlushException();

        RowData key = keyExtractor.apply(row);
        RowData value = valueCopier.apply(row);
        reduceBuffer.put(key, new Tuple2<>(changeFlag(value), timestamp));
        batchCount++;

        if (batchCount >= batchMaxRowNums) {
            flush();
        }
    }

    private synchronized void flush() throws Exception {
        checkFlushException();
        for (Tuple2<RowData, Long> value : reduceBuffer.values()) {
            wrappedContext.setTimestamp(value.f1);
            producer.invoke(value.f0, wrappedContext);
        }
        reduceBuffer.clear();
        batchCount = 0;
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

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to JDBC failed.", flushException);
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Wrapper of {@link Context}.
     *
     * <p>When records arrives, the {@link BufferedUpsertSinkFunction} updates the current {@link
     * Context} and memorize the timestamp with the records. When flushing, the {@link
     * BufferedUpsertSinkFunction} will emit the records in the buffer with memorized timestamp.
     */
    private static class WrappedContext implements Context {

        private Long timestamp;
        private Context context;

        void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        void setContext(Context context) {
            this.context = context;
        }

        @Override
        public long currentProcessingTime() {
            return context.currentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return context.currentWatermark();
        }

        @Override
        public Long timestamp() {
            return timestamp;
        }
    }
}
