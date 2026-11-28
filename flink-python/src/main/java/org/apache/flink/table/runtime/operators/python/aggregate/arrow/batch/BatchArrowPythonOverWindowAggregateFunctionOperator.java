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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.util.ProtoUtils.createOverWindowArrowTypeCoderInfoDescriptorProto;
import static org.apache.flink.python.util.ProtoUtils.createUserDefinedFunctionProto;

/** The Batch Arrow Python {@link AggregateFunction} Operator for Over Window Aggregation. */
@Internal
public class BatchArrowPythonOverWindowAggregateFunctionOperator
        extends AbstractBatchArrowPythonAggregateFunctionOperator {

    private static final long serialVersionUID = 1L;

    private static final String PANDAS_BATCH_OVER_WINDOW_AGG_FUNCTION_URN =
            "flink:transform:batch_over_window_aggregate_function:arrow:v1";

    /** Used to serialize the boundary of range window. */
    private static final IntSerializer windowBoundarySerializer = IntSerializer.INSTANCE;

    /** Window lower boundary. e.g. Long.MIN_VALUE means unbounded preceding. */
    private final long[] lowerBoundary;

    /** Window upper boundary. e.g. Long.MAX_VALUE means unbounded following. */
    private final long[] upperBoundary;

    /** Whether the specified position window is a range window. */
    private final boolean[] isRangeWindows;

    /** The window index of the specified aggregate function belonging to. */
    private final int[] aggWindowIndex;

    /** Finds the lower bound of each bounded RANGE window; null for other windows. */
    private final GeneratedRecordComparator[] genLowerBoundComparators;

    /** Finds the upper bound of each bounded RANGE window; null for other windows. */
    private final GeneratedRecordComparator[] genUpperBoundComparators;

    private transient RecordComparator[] lowerBoundComparators;

    private transient RecordComparator[] upperBoundComparators;

    /** The type serializer for the forwarded fields. */
    private transient RowDataSerializer forwardedInputSerializer;

    /** Stores the start position of the last key data in forwardedInputQueue. */
    private transient int lastKeyDataStartPos;

    /** Reusable OutputStream used to holding the window boundary with input elements. */
    private transient ByteArrayOutputStreamWithPos windowBoundaryWithDataBaos;

    /** OutputStream Wrapper. */
    private transient DataOutputViewStreamWrapper windowBoundaryWithDataWrapper;

    /** Stores bounded range window boundaries. */
    private transient List<List<Integer>> boundedRangeWindowBoundaries;

    /** Stores index of the bounded range window. */
    private transient ArrayList<Integer> boundedRangeWindowIndex;

    public BatchArrowPythonOverWindowAggregateFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] pandasAggFunctions,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            long[] lowerBoundary,
            long[] upperBoundary,
            boolean[] isRangeWindows,
            int[] aggWindowIndex,
            GeneratedRecordComparator[] genLowerBoundComparators,
            GeneratedRecordComparator[] genUpperBoundComparators,
            GeneratedProjection inputGeneratedProjection,
            GeneratedProjection groupKeyGeneratedProjection,
            GeneratedProjection groupSetGeneratedProjection) {
        super(
                config,
                pandasAggFunctions,
                inputType,
                udfInputType,
                udfOutputType,
                inputGeneratedProjection,
                groupKeyGeneratedProjection,
                groupSetGeneratedProjection);
        this.lowerBoundary = lowerBoundary;
        this.upperBoundary = upperBoundary;
        this.isRangeWindows = isRangeWindows;
        this.aggWindowIndex = aggWindowIndex;
        this.genLowerBoundComparators = genLowerBoundComparators;
        this.genUpperBoundComparators = genUpperBoundComparators;
    }

    @Override
    public void open() throws Exception {
        super.open();
        forwardedInputSerializer = new RowDataSerializer(inputType);
        this.lastKeyDataStartPos = 0;
        windowBoundaryWithDataBaos = new ByteArrayOutputStreamWithPos();
        windowBoundaryWithDataWrapper = new DataOutputViewStreamWrapper(windowBoundaryWithDataBaos);
        boundedRangeWindowBoundaries = new ArrayList<>(lowerBoundary.length);
        boundedRangeWindowIndex = new ArrayList<>();
        ClassLoader userCodeClassLoader = getRuntimeContext().getUserCodeClassLoader();
        lowerBoundComparators = new RecordComparator[lowerBoundary.length];
        upperBoundComparators = new RecordComparator[upperBoundary.length];
        for (int i = 0; i < lowerBoundary.length; i++) {
            // range window with bounded preceding or bounded following
            if (isRangeWindows[i]
                    && (lowerBoundary[i] != Long.MIN_VALUE || upperBoundary[i] != Long.MAX_VALUE)) {
                boundedRangeWindowIndex.add(i);
                boundedRangeWindowBoundaries.add(new ArrayList<>());
            }
            if (genLowerBoundComparators[i] != null) {
                lowerBoundComparators[i] =
                        genLowerBoundComparators[i].newInstance(userCodeClassLoader);
            }
            if (genUpperBoundComparators[i] != null) {
                upperBoundComparators[i] =
                        genUpperBoundComparators[i].newInstance(userCodeClassLoader);
            }
        }
    }

    @Override
    public void bufferInput(RowData input) throws Exception {
        BinaryRowData currentKey = groupKeyProjection.apply(input).copy();
        if (isNewKey(currentKey)) {
            if (lastGroupKey != null) {
                invokeCurrentBatch();
            }
            lastGroupKey = currentKey;
            lastGroupSet = groupSetProjection.apply(input).copy();
        }
        RowData forwardedFields = forwardedInputSerializer.copy(input);
        forwardedInputQueue.add(forwardedFields);
    }

    @Override
    protected void invokeCurrentBatch() throws Exception {
        if (currentBatchCount > 0) {
            arrowSerializer.finishCurrentBatch();
            ListIterator<RowData> iter = forwardedInputQueue.listIterator(lastKeyDataStartPos);
            int[] lowerBoundaryPos = new int[boundedRangeWindowIndex.size()];
            int[] upperBoundaryPos = new int[boundedRangeWindowIndex.size()];
            for (int i = 0; i < lowerBoundaryPos.length; i++) {
                lowerBoundaryPos[i] = lastKeyDataStartPos;
                upperBoundaryPos[i] = lastKeyDataStartPos;
            }
            while (iter.hasNext()) {
                RowData curData = iter.next();
                // loop every bounded range window
                for (int j = 0; j < boundedRangeWindowIndex.size(); j++) {
                    int windowPos = boundedRangeWindowIndex.get(j);
                    List<Integer> curWindowBoundary = boundedRangeWindowBoundaries.get(j);
                    // A FOLLOWING lower bound can lie beyond the last row of the partition, or
                    // beyond the upper boundary after a NULL key. As in RangeSlidingOverFrame,
                    // the lower boundary never moves past the upper one, so find that first.
                    int lowerBoundaryLimit = forwardedInputQueue.size();
                    // bounded following
                    if (upperBoundary[windowPos] != Long.MAX_VALUE) {
                        RecordComparator upperBoundComparator = upperBoundComparators[windowPos];
                        int curUpperBoundaryPos = upperBoundaryPos[j];
                        while (curUpperBoundaryPos < forwardedInputQueue.size()
                                && upperBoundComparator.compare(
                                                forwardedInputQueue.get(curUpperBoundaryPos),
                                                curData)
                                        <= 0) {
                            curUpperBoundaryPos += 1;
                        }
                        upperBoundaryPos[j] = curUpperBoundaryPos;
                        lowerBoundaryLimit = curUpperBoundaryPos;
                    }
                    // bounded preceding
                    if (lowerBoundary[windowPos] != Long.MIN_VALUE) {
                        RecordComparator lowerBoundComparator = lowerBoundComparators[windowPos];
                        int curLowerBoundaryPos = lowerBoundaryPos[j];
                        while (curLowerBoundaryPos < lowerBoundaryLimit
                                && lowerBoundComparator.compare(
                                                forwardedInputQueue.get(curLowerBoundaryPos),
                                                curData)
                                        < 0) {
                            curLowerBoundaryPos += 1;
                        }
                        lowerBoundaryPos[j] = curLowerBoundaryPos;
                        curWindowBoundary.add(curLowerBoundaryPos - lastKeyDataStartPos);
                    }
                    if (upperBoundary[windowPos] != Long.MAX_VALUE) {
                        curWindowBoundary.add(upperBoundaryPos[j] - lastKeyDataStartPos);
                    }
                }
            }
            // serialize the num of bounded range window.
            windowBoundarySerializer.serialize(
                    boundedRangeWindowBoundaries.size(), windowBoundaryWithDataWrapper);
            // serialize every bounded range window boundaries.
            for (List<Integer> boundedRangeWindowBoundary : boundedRangeWindowBoundaries) {
                windowBoundarySerializer.serialize(
                        boundedRangeWindowBoundary.size(), windowBoundaryWithDataWrapper);
                for (int ele : boundedRangeWindowBoundary) {
                    windowBoundarySerializer.serialize(ele, windowBoundaryWithDataWrapper);
                }
                boundedRangeWindowBoundary.clear();
            }
            // write arrow format data.
            windowBoundaryWithDataBaos.write(baos.toByteArray());
            baos.reset();
            pythonFunctionRunner.process(windowBoundaryWithDataBaos.toByteArray());
            windowBoundaryWithDataBaos.reset();
            elementCount += currentBatchCount;
            checkInvokeFinishBundleByCount();
            currentBatchCount = 0;
            arrowSerializer.resetWriter();
        }
        lastKeyDataStartPos = forwardedInputQueue.size();
    }

    @Override
    public void processElementInternal(RowData value) {
        arrowSerializer.write(getFunctionInput(value));
        currentBatchCount++;
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public void emitResult(Tuple3<String, byte[], Integer> resultTuple) throws Exception {
        byte[] udafResult = resultTuple.f1;
        int length = resultTuple.f2;
        bais.setBuffer(udafResult, 0, length);
        int rowCount = arrowSerializer.load();
        for (int i = 0; i < rowCount; i++) {
            RowData input = forwardedInputQueue.poll();
            lastKeyDataStartPos--;
            reuseJoinedRow.setRowKind(input.getRowKind());
            rowDataWrapper.collect(reuseJoinedRow.replace(input, arrowSerializer.read(i)));
        }
        arrowSerializer.resetReader();
    }

    @Override
    public FlinkFnApi.UserDefinedFunctions createUserDefinedFunctionsProto() {
        FlinkFnApi.UserDefinedFunctions.Builder builder =
                FlinkFnApi.UserDefinedFunctions.newBuilder();
        // add udaf proto
        for (int i = 0; i < pandasAggFunctions.length; i++) {
            FlinkFnApi.UserDefinedFunction.Builder functionBuilder =
                    createUserDefinedFunctionProto(pandasAggFunctions[i]).toBuilder();
            functionBuilder.setWindowIndex(aggWindowIndex[i]);
            builder.addUdfs(functionBuilder);
        }
        builder.setMetricEnabled(config.get(PYTHON_METRIC_ENABLED));
        builder.setProfileEnabled(config.get(PYTHON_PROFILE_ENABLED));
        builder.addAllJobParameters(
                getRuntimeContext().getGlobalJobParameters().entrySet().stream()
                        .map(
                                entry ->
                                        FlinkFnApi.JobParameter.newBuilder()
                                                .setKey(entry.getKey())
                                                .setValue(entry.getValue())
                                                .build())
                        .collect(Collectors.toList()));
        builder.setRuntimeContext(
                FlinkFnApi.UserDefinedDataStreamFunction.RuntimeContext.newBuilder()
                        .setTaskName(getRuntimeContext().getTaskInfo().getTaskName())
                        .setTaskNameWithSubtasks(
                                getRuntimeContext().getTaskInfo().getTaskNameWithSubtasks())
                        .setNumberOfParallelSubtasks(
                                getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks())
                        .setMaxNumberOfParallelSubtasks(
                                getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks())
                        .setIndexOfThisSubtask(
                                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask())
                        .setAttemptNumber(getRuntimeContext().getTaskInfo().getAttemptNumber())
                        .build());
        // add windows
        for (int i = 0; i < lowerBoundary.length; i++) {
            FlinkFnApi.OverWindow.Builder windowBuilder = FlinkFnApi.OverWindow.newBuilder();
            if (isRangeWindows[i]) {
                // range window
                if (lowerBoundary[i] != Long.MIN_VALUE) {
                    if (upperBoundary[i] != Long.MAX_VALUE) {
                        // range sliding window
                        windowBuilder.setWindowType(FlinkFnApi.OverWindow.WindowType.RANGE_SLIDING);
                    } else {
                        // range unbounded following window
                        windowBuilder.setWindowType(
                                FlinkFnApi.OverWindow.WindowType.RANGE_UNBOUNDED_FOLLOWING);
                    }
                } else {
                    if (upperBoundary[i] != Long.MAX_VALUE) {
                        // range unbounded preceding window
                        windowBuilder.setWindowType(
                                FlinkFnApi.OverWindow.WindowType.RANGE_UNBOUNDED_PRECEDING);
                    } else {
                        // range unbounded window
                        windowBuilder.setWindowType(
                                FlinkFnApi.OverWindow.WindowType.RANGE_UNBOUNDED);
                    }
                }
            } else {
                // row window
                if (lowerBoundary[i] != Long.MIN_VALUE) {
                    windowBuilder.setLowerBoundary(lowerBoundary[i]);
                    if (upperBoundary[i] != Long.MAX_VALUE) {
                        // row sliding window
                        windowBuilder.setUpperBoundary(upperBoundary[i]);
                        windowBuilder.setWindowType(FlinkFnApi.OverWindow.WindowType.ROW_SLIDING);
                    } else {
                        // row unbounded following window
                        windowBuilder.setWindowType(
                                FlinkFnApi.OverWindow.WindowType.ROW_UNBOUNDED_FOLLOWING);
                    }
                } else {
                    if (upperBoundary[i] != Long.MAX_VALUE) {
                        // row unbounded preceding window
                        windowBuilder.setUpperBoundary(upperBoundary[i]);
                        windowBuilder.setWindowType(
                                FlinkFnApi.OverWindow.WindowType.ROW_UNBOUNDED_PRECEDING);
                    } else {
                        // row unbounded window
                        windowBuilder.setWindowType(FlinkFnApi.OverWindow.WindowType.ROW_UNBOUNDED);
                    }
                }
            }
            builder.addWindows(windowBuilder);
        }
        return builder.build();
    }

    @Override
    public String getFunctionUrn() {
        return PANDAS_BATCH_OVER_WINDOW_AGG_FUNCTION_URN;
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createInputCoderInfoDescriptor(RowType runnerInputType) {
        return createOverWindowArrowTypeCoderInfoDescriptorProto(
                runnerInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false);
    }
}
