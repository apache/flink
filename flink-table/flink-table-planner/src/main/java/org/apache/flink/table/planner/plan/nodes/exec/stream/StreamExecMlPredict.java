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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.ModelProviderSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link StreamExecNode} for ML model prediction. */
@ExecNodeMetadata(
        name = "stream-exec-ml-predict",
        version = 1,
        producedTransformations = StreamExecMlPredict.ML_PREDICT_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecMlPredict implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String ML_PREDICT_TRANSFORMATION = "ml-predict";
    public static final String FIELD_NAME_INPUT_COLUMNS = "inputColumns";
    public static final String FIELD_NAME_MODEL_PROVIDER = "modelProvider";
    public static final String FIELD_NAME_ASYNC_OPTIONS = "asyncOptions";

    @JsonProperty(FIELD_NAME_INPUT_COLUMNS)
    private final List<String> inputColumns;

    @JsonProperty(FIELD_NAME_MODEL_PROVIDER)
    private final ModelProviderSpec modelProviderSpec;

    @JsonProperty(FIELD_NAME_ASYNC_OPTIONS)
    @Nullable
    private final AsyncOptions asyncOptions;

    private final RowType inputRowType;
    private final RowType outputRowType;

    @JsonCreator
    public StreamExecMlPredict(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_INPUT_COLUMNS) List<String> inputColumns,
            @JsonProperty(FIELD_NAME_MODEL_PROVIDER) ModelProviderSpec modelProviderSpec,
            @JsonProperty(FIELD_NAME_ASYNC_OPTIONS) @Nullable AsyncOptions asyncOptions,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        
        this.inputColumns = checkNotNull(inputColumns);
        this.modelProviderSpec = checkNotNull(modelProviderSpec);
        this.asyncOptions = asyncOptions;
        this.inputRowType = (RowType) inputProperties.get(0).getType();
        this.outputRowType = outputType;
    }

    @Override
    public Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        final Transformation<RowData> inputTransformation = 
            getInputEdges().get(0).translateToPlan(planner);
        final ClassLoader classLoader = planner.getExecEnv().getUserClassLoader();

        final Transformation<RowData> predictTransformation;
        if (asyncOptions != null) {
            predictTransformation = createAsyncPredictTransformation(
                inputTransformation, classLoader, config);
        } else {
            predictTransformation = createSyncPredictTransformation(
                inputTransformation, classLoader, config);
        }

        return predictTransformation;
    }

    private Transformation<RowData> createSyncPredictTransformation(
            Transformation<RowData> inputTransformation,
            ClassLoader classLoader,
            ExecNodeConfig config) {
        ProcessFunction<RowData, RowData> processFunction = 
            createSyncPredictFunction(classLoader, config);

        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(processFunction);

        return ExecNodeUtil.createOneInputTransformation(
                inputTransformation,
                createTransformationMeta(ML_PREDICT_TRANSFORMATION, config),
                operator,
                InternalTypeInfo.of(outputRowType),
                inputTransformation.getParallelism());
    }

    private Transformation<RowData> createAsyncPredictTransformation(
            Transformation<RowData> inputTransformation,
            ClassLoader classLoader,
            ExecNodeConfig config) {
        AsyncFunction<RowData, RowData> asyncFunction = 
            createAsyncPredictFunction(classLoader, config);

        TypeSerializer<RowData> inputSerializer =
                InternalTypeInfo.of(inputRowType)
                        .createSerializer(new ExecutionConfig());

        AsyncWaitOperator<RowData, RowData> operator =
                new AsyncWaitOperator<>(
                        asyncFunction,
                        asyncOptions.getTimeout(),
                        asyncOptions.getCapacity(),
                        asyncOptions.getOutputMode(),
                        inputSerializer);

        return ExecNodeUtil.createOneInputTransformation(
                inputTransformation,
                createTransformationMeta(ML_PREDICT_TRANSFORMATION, config),
                operator,
                InternalTypeInfo.of(outputRowType),
                inputTransformation.getParallelism());
    }

    private ProcessFunction<RowData, RowData> createSyncPredictFunction(
            ClassLoader classLoader, ExecNodeConfig config) {
        final PredictRuntimeProvider runtimeProvider = 
                modelProviderSpec.createPredictRuntimeProvider(
                        getContextResolvedModel(config),
                        classLoader);
        
        return runtimeProvider.createPredictFunction(
                inputColumns,
                inputRowType,
                outputRowType,
                config.getConfiguration());
    }

    private AsyncFunction<RowData, RowData> createAsyncPredictFunction(
            ClassLoader classLoader, ExecNodeConfig config) {
        final PredictRuntimeProvider runtimeProvider = 
                modelProviderSpec.createPredictRuntimeProvider(
                        getContextResolvedModel(config),
                        classLoader);
        
        return runtimeProvider.createAsyncPredictFunction(
                inputColumns,
                inputRowType,
                outputRowType,
                config.getConfiguration());
    }

    /** Async options for ML prediction. */
    public static class AsyncOptions {
        private final long timeout;
        private final int capacity;
        private final String outputMode;

        @JsonCreator
        public AsyncOptions(
                @JsonProperty("timeout") long timeout,
                @JsonProperty("capacity") int capacity,
                @JsonProperty("outputMode") String outputMode) {
            this.timeout = timeout;
            this.capacity = capacity;
            this.outputMode = outputMode;
        }

        public long getTimeout() {
            return timeout;
        }

        public int getCapacity() {
            return capacity;
        }

        public String getOutputMode() {
            return outputMode;
        }
    }
} 