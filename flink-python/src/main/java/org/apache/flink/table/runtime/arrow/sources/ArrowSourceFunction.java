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

package org.apache.flink.table.runtime.arrow.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayDeque;
import java.util.Deque;

/** An Arrow {@link SourceFunction} which takes the serialized arrow record batch data as input. */
@Internal
public class ArrowSourceFunction extends RichParallelSourceFunction<RowData>
        implements ResultTypeQueryable<RowData>, CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ArrowSourceFunction.class);

    static {
        ArrowUtils.checkArrowUsable();
    }

    /** The type of the records produced by this source. */
    private final DataType dataType;

    /**
     * The array of byte array of the source data. Each element is an array representing an arrow
     * batch.
     */
    private final byte[][] arrowData;

    /** Allocator which is used for byte buffer allocation. */
    private transient BufferAllocator allocator;

    /** Container that holds a set of vectors for the source data to emit. */
    private transient VectorSchemaRoot root;

    private transient volatile boolean running;

    /**
     * The indexes of the collection of source data to emit. Each element is a tuple of the index of
     * the arrow batch and the staring index inside the arrow batch.
     */
    private transient Deque<Tuple2<Integer, Integer>> indexesToEmit;

    /** The indexes of the source data which have not been emitted. */
    private transient ListState<Tuple2<Integer, Integer>> checkpointedState;

    ArrowSourceFunction(DataType dataType, byte[][] arrowData) {
        this.dataType = Preconditions.checkNotNull(dataType);
        this.arrowData = Preconditions.checkNotNull(arrowData);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        allocator =
                ArrowUtils.getRootAllocator()
                        .newChildAllocator("ArrowSourceFunction", 0, Long.MAX_VALUE);
        root =
                VectorSchemaRoot.create(
                        ArrowUtils.toArrowSchema((RowType) dataType.getLogicalType()), allocator);
        running = true;
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
        } finally {
            if (root != null) {
                root.close();
                root = null;
            }
            if (allocator != null) {
                allocator.close();
                allocator = null;
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        this.checkpointedState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "arrow-source-state",
                                        new TupleSerializer<>(
                                                (Class<Tuple2<Integer, Integer>>)
                                                        (Class<?>) Tuple2.class,
                                                new TypeSerializer[] {
                                                    IntSerializer.INSTANCE, IntSerializer.INSTANCE
                                                })));

        this.indexesToEmit = new ArrayDeque<>();
        if (context.isRestored()) {
            // upon restoring
            for (Tuple2<Integer, Integer> v : this.checkpointedState.get()) {
                this.indexesToEmit.add(v);
            }
            LOG.info(
                    "Subtask {} restored state: {}.",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    indexesToEmit);
        } else {
            // the first time the job is executed
            final int stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
            final int taskIdx = getRuntimeContext().getIndexOfThisSubtask();

            for (int i = taskIdx; i < arrowData.length; i += stepSize) {
                this.indexesToEmit.add(Tuple2.of(i, 0));
            }
            LOG.info(
                    "Subtask {} has no restore state, initialized with {}.",
                    taskIdx,
                    indexesToEmit);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " state has not been properly initialized.");

        this.checkpointedState.clear();
        for (Tuple2<Integer, Integer> v : indexesToEmit) {
            this.checkpointedState.add(v);
        }
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        VectorLoader vectorLoader = new VectorLoader(root);
        while (running && !indexesToEmit.isEmpty()) {
            Tuple2<Integer, Integer> indexToEmit = indexesToEmit.peek();
            ArrowRecordBatch arrowRecordBatch = loadBatch(indexToEmit.f0);
            vectorLoader.load(arrowRecordBatch);
            arrowRecordBatch.close();

            ArrowReader arrowReader = createArrowReader(root);
            int rowCount = root.getRowCount();
            int nextRowId = indexToEmit.f1;
            while (nextRowId < rowCount) {
                RowData element = arrowReader.read(nextRowId);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(element);
                    indexToEmit.setField(++nextRowId, 1);
                }
            }

            synchronized (ctx.getCheckpointLock()) {
                indexesToEmit.pop();
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return (TypeInformation<RowData>)
                TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(dataType);
    }

    private ArrowReader createArrowReader(VectorSchemaRoot root) {
        return ArrowUtils.createArrowReader(root, (RowType) dataType.getLogicalType());
    }

    /** Load the specified batch of data to process. */
    private ArrowRecordBatch loadBatch(int nextIndexOfArrowDataToProcess) throws IOException {
        ByteArrayInputStream bais =
                new ByteArrayInputStream(arrowData[nextIndexOfArrowDataToProcess]);
        return MessageSerializer.deserializeRecordBatch(
                new ReadChannel(Channels.newChannel(bais)), allocator);
    }
}
