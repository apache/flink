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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.operators.python.AbstractOneInputPythonFunctionOperator;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.UpdatableRowData;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
import org.apache.flink.table.runtime.runners.python.beam.BeamTableStatefulPythonFunctionRunner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.typeutils.PythonTypeUtils.toProtoType;

/**
 * Base class for {@link AbstractPythonStreamGroupAggregateOperator} and {@link
 * PythonStreamGroupWindowAggregateOperator}.
 */
@Internal
public abstract class AbstractPythonStreamAggregateOperator
        extends AbstractOneInputPythonFunctionOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    @VisibleForTesting
    protected static final String FLINK_AGGREGATE_FUNCTION_SCHEMA_CODER_URN =
            "flink:coder:schema:aggregate_function:v1";

    @VisibleForTesting static final byte NORMAL_RECORD = 0;

    @VisibleForTesting static final byte TRIGGER_TIMER = 1;

    private final PythonAggregateFunctionInfo[] aggregateFunctions;

    private final DataViewUtils.DataViewSpec[][] dataViewSpecs;

    /** The input logical type. */
    protected final RowType inputType;

    /** The output logical type. */
    protected final RowType outputType;

    /** The options used to configure the Python worker process. */
    private final Map<String, String> jobOptions;

    /** The array of the key indexes. */
    private final int[] grouping;

    /** The index of a count aggregate used to calculate the number of accumulated rows. */
    private final int indexOfCountStar;

    /** Generate retract messages if true. */
    private final boolean generateUpdateBefore;

    /** The maximum NUMBER of the states cached in Python side. */
    private final int stateCacheSize;

    /** The maximum number of cached entries in a single Python MapState. */
    private final int mapStateReadCacheSize;

    private final int mapStateWriteCacheSize;

    private transient Object keyForTimerService;

    /** The user-defined function input logical type. */
    protected transient RowType userDefinedFunctionInputType;

    /** The user-defined function output logical type. */
    protected transient RowType userDefinedFunctionOutputType;

    /** The TypeSerializer for udf execution results. */
    transient TypeSerializer<RowData> udfOutputTypeSerializer;

    /** The TypeSerializer for udf input elements. */
    transient TypeSerializer<RowData> udfInputTypeSerializer;

    /** Reusable InputStream used to holding the execution results to be deserialized. */
    protected transient ByteArrayInputStreamWithPos bais;

    /** InputStream Wrapper. */
    protected transient DataInputViewStreamWrapper baisWrapper;

    /** Reusable OutputStream used to holding the serialized input elements. */
    protected transient ByteArrayOutputStreamWithPos baos;

    /** OutputStream Wrapper. */
    protected transient DataOutputViewStreamWrapper baosWrapper;

    protected transient UpdatableRowData reuseRowData;

    protected transient UpdatableRowData reuseTimerRowData;

    /** The collector used to collect records. */
    protected transient StreamRecordRowDataWrappingCollector rowDataWrapper;

    public AbstractPythonStreamAggregateOperator(
            Configuration config,
            RowType inputType,
            RowType outputType,
            PythonAggregateFunctionInfo[] aggregateFunctions,
            DataViewUtils.DataViewSpec[][] dataViewSpecs,
            int[] grouping,
            int indexOfCountStar,
            boolean generateUpdateBefore) {
        super(config);
        this.inputType = Preconditions.checkNotNull(inputType);
        this.outputType = Preconditions.checkNotNull(outputType);
        this.aggregateFunctions = aggregateFunctions;
        this.dataViewSpecs = dataViewSpecs;
        this.jobOptions = buildJobOptions(config);
        this.grouping = grouping;
        this.indexOfCountStar = indexOfCountStar;
        this.generateUpdateBefore = generateUpdateBefore;
        this.stateCacheSize = config.get(PythonOptions.STATE_CACHE_SIZE);
        this.mapStateReadCacheSize = config.get(PythonOptions.MAP_STATE_READ_CACHE_SIZE);
        this.mapStateWriteCacheSize = config.get(PythonOptions.MAP_STATE_WRITE_CACHE_SIZE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open() throws Exception {
        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(baos);
        userDefinedFunctionInputType = getUserDefinedFunctionInputType();
        udfInputTypeSerializer =
                PythonTypeUtils.toBlinkTypeSerializer(userDefinedFunctionInputType);
        userDefinedFunctionOutputType = getUserDefinedFunctionOutputType();
        udfOutputTypeSerializer =
                PythonTypeUtils.toBlinkTypeSerializer(userDefinedFunctionOutputType);
        // The structure is:  [type]|[normal record]|[timestamp of timer]|[row key /timer data]
        // If the type is 'NORMAL_RECORD', store the RowData object in the 2nd column.
        // If the type is 'TRIGGER_TIMER', store the timestamp in 3rd column and the row key/timer
        // data in 4th column.
        reuseRowData = new UpdatableRowData(GenericRowData.of(NORMAL_RECORD, null, null, null), 4);
        reuseTimerRowData =
                new UpdatableRowData(GenericRowData.of(TRIGGER_TIMER, null, null, null), 4);
        rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
        super.open();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData value = element.getValue();
        processElementInternal(value);
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        return new BeamTableStatefulPythonFunctionRunner(
                getRuntimeContext().getTaskName(),
                createPythonEnvironmentManager(),
                userDefinedFunctionInputType,
                userDefinedFunctionOutputType,
                getFunctionUrn(),
                getUserDefinedFunctionsProto(),
                FLINK_AGGREGATE_FUNCTION_SCHEMA_CODER_URN,
                jobOptions,
                getFlinkMetricContainer(),
                getKeyedStateBackend(),
                getKeySerializer(),
                getContainingTask().getEnvironment().getMemoryManager(),
                getOperatorConfig()
                        .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.PYTHON,
                                getContainingTask()
                                        .getEnvironment()
                                        .getTaskManagerInfo()
                                        .getConfiguration(),
                                getContainingTask()
                                        .getEnvironment()
                                        .getUserCodeClassLoader()
                                        .asClassLoader()));
    }

    /**
     * As the beam state gRPC service will access the KeyedStateBackend in parallel with this
     * operator, we must override this method to prevent changing the current key of the
     * KeyedStateBackend while the beam service is handling requests.
     */
    @Override
    public void setCurrentKey(Object key) {
        keyForTimerService = key;
    }

    @Override
    public Object getCurrentKey() {
        return keyForTimerService;
    }

    @Override
    public PythonEnv getPythonEnv() {
        return aggregateFunctions[0].getPythonFunction().getPythonEnv();
    }

    @VisibleForTesting
    TypeSerializer getKeySerializer() {
        return PythonTypeUtils.toBlinkTypeSerializer(getKeyType());
    }

    protected RowType getKeyType() {
        RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(grouping, InternalTypeInfo.of(inputType));
        return selector.getProducedType().toRowType();
    }

    /**
     * Gets the proto representation of the Python user-defined aggregate functions to be executed.
     */
    protected FlinkFnApi.UserDefinedAggregateFunctions getUserDefinedFunctionsProto() {
        FlinkFnApi.UserDefinedAggregateFunctions.Builder builder =
                FlinkFnApi.UserDefinedAggregateFunctions.newBuilder();
        builder.setMetricEnabled(getPythonConfig().isMetricEnabled());
        builder.addAllGrouping(Arrays.stream(grouping).boxed().collect(Collectors.toList()));
        builder.setGenerateUpdateBefore(generateUpdateBefore);
        builder.setIndexOfCountStar(indexOfCountStar);
        builder.setKeyType(toProtoType(getKeyType()));
        builder.setStateCacheSize(stateCacheSize);
        builder.setMapStateReadCacheSize(mapStateReadCacheSize);
        builder.setMapStateWriteCacheSize(mapStateWriteCacheSize);
        for (int i = 0; i < aggregateFunctions.length; i++) {
            DataViewUtils.DataViewSpec[] specs = null;
            if (i < dataViewSpecs.length) {
                specs = dataViewSpecs[i];
            }
            builder.addUdfs(
                    PythonOperatorUtils.getUserDefinedAggregateFunctionProto(
                            aggregateFunctions[i], specs));
        }
        return builder.build();
    }

    public abstract String getFunctionUrn();

    public abstract void processElementInternal(RowData value) throws Exception;

    public abstract RowType getUserDefinedFunctionInputType();

    public abstract RowType getUserDefinedFunctionOutputType();

    private Map<String, String> buildJobOptions(Configuration config) {
        Map<String, String> jobOptions = new HashMap<>();
        if (config.containsKey("table.exec.timezone")) {
            jobOptions.put("table.exec.timezone", config.getString("table.exec.timezone", null));
        }
        jobOptions.put(
                PythonOptions.STATE_CACHE_SIZE.key(),
                String.valueOf(config.get(PythonOptions.STATE_CACHE_SIZE)));
        jobOptions.put(
                PythonOptions.MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE.key(),
                String.valueOf(config.get(PythonOptions.MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE)));
        return jobOptions;
    }
}
