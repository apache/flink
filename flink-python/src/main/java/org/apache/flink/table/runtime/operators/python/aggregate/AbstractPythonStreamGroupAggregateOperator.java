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

package org.apache.flink.table.runtime.operators.python.aggregate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.UpdatableRowData;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.runtime.functions.CleanupState;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for {@link PythonStreamGroupAggregateOperator} and {@link
 * PythonStreamGroupTableAggregateOperator}.
 */
@Internal
public abstract class AbstractPythonStreamGroupAggregateOperator
        extends AbstractPythonStreamAggregateOperator
        implements Triggerable<RowData, VoidNamespace>, CleanupState {

    private static final long serialVersionUID = 1L;

    /** The minimum time in milliseconds until state which was not updated will be retained. */
    private final long minRetentionTime;

    /** The maximum time in milliseconds until state which was not updated will be retained. */
    private final long maxRetentionTime;

    /**
     * Indicates whether state cleaning is enabled. Can be calculated from the `minRetentionTime`.
     */
    private final boolean stateCleaningEnabled;

    private transient TimerService timerService;

    // holds the latest registered cleanup timer
    private transient ValueState<Long> cleanupTimeState;

    private transient UpdatableRowData reuseRowData;

    private transient UpdatableRowData reuseTimerRowData;

    public AbstractPythonStreamGroupAggregateOperator(
            Configuration config,
            RowType inputType,
            RowType outputType,
            PythonAggregateFunctionInfo[] aggregateFunctions,
            DataViewUtils.DataViewSpec[][] dataViewSpecs,
            int[] grouping,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            long minRetentionTime,
            long maxRetentionTime) {
        super(
                config,
                inputType,
                outputType,
                aggregateFunctions,
                dataViewSpecs,
                grouping,
                indexOfCountStar,
                generateUpdateBefore);
        this.minRetentionTime = minRetentionTime;
        this.maxRetentionTime = maxRetentionTime;
        this.stateCleaningEnabled = minRetentionTime > 1;
    }

    @Override
    public void open() throws Exception {
        // The structure is:  [type]|[normal record]|[timestamp of timer]|[row key]
        // If the type is 'NORMAL_RECORD', store the RowData object in the 2nd column.
        // If the type is 'TRIGGER_TIMER', store the timestamp in 3rd column and the row key
        reuseRowData = new UpdatableRowData(GenericRowData.of(NORMAL_RECORD, null, null, null), 4);
        reuseTimerRowData =
                new UpdatableRowData(GenericRowData.of(TRIGGER_TIMER, null, null, null), 4);
        timerService =
                new SimpleTimerService(
                        getInternalTimerService(
                                "state-clean-timer", VoidNamespaceSerializer.INSTANCE, this));
        initCleanupTimeState();
        super.open();
    }

    /** Invoked when an event-time timer fires. */
    @Override
    public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) {}

    /** Invoked when a processing-time timer fires. */
    @Override
    public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
        if (stateCleaningEnabled) {
            RowData key = timer.getKey();
            long timestamp = timer.getTimestamp();
            reuseTimerRowData.setLong(2, timestamp);
            reuseTimerRowData.setField(3, key);
            udfInputTypeSerializer.serialize(reuseTimerRowData, baosWrapper);
            pythonFunctionRunner.process(baos.toByteArray());
            baos.reset();
            elementCount++;
        }
    }

    @Override
    public void processElementInternal(RowData value) throws Exception {
        long currentTime = timerService.currentProcessingTime();
        registerProcessingCleanupTimer(currentTime);
        reuseRowData.setField(1, value);
        udfInputTypeSerializer.serialize(reuseRowData, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
    }

    @Override
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] rawUdfResult = resultTuple.f0;
        int length = resultTuple.f1;
        bais.setBuffer(rawUdfResult, 0, length);
        RowData udfResult = udfOutputTypeSerializer.deserialize(baisWrapper);
        rowDataWrapper.collect(udfResult);
    }

    @Override
    public RowType createUserDefinedFunctionInputType() {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("record_type", new TinyIntType()));
        fields.add(new RowType.RowField("row", inputType));
        fields.add(new RowType.RowField("timestamp", new BigIntType()));
        fields.add(new RowType.RowField("key", getKeyType()));
        return new RowType(fields);
    }

    @Override
    public RowType createUserDefinedFunctionOutputType() {
        return outputType;
    }

    @Override
    protected FlinkFnApi.UserDefinedAggregateFunctions getUserDefinedFunctionsProto() {
        FlinkFnApi.UserDefinedAggregateFunctions.Builder builder =
                super.getUserDefinedFunctionsProto().toBuilder();
        builder.setStateCleaningEnabled(stateCleaningEnabled);
        return builder.build();
    }

    private void initCleanupTimeState() {
        if (stateCleaningEnabled) {
            ValueStateDescriptor<Long> inputCntDescriptor =
                    new ValueStateDescriptor<>("PythonAggregateCleanupTime", Types.LONG);
            cleanupTimeState = getRuntimeContext().getState(inputCntDescriptor);
        }
    }

    private void registerProcessingCleanupTimer(long currentTime) throws Exception {
        if (stateCleaningEnabled) {
            synchronized (getKeyedStateBackend()) {
                getKeyedStateBackend().setCurrentKey(getCurrentKey());
                registerProcessingCleanupTimer(
                        cleanupTimeState,
                        currentTime,
                        minRetentionTime,
                        maxRetentionTime,
                        timerService);
            }
        }
    }
}
