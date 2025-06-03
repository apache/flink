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

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.AsyncOutputMode;
import org.apache.flink.table.api.config.ExecutionConfigOptions.AsyncRetryStrategies;
import org.apache.flink.table.api.config.ExecutionConfigOptions.RetryStrategy;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.ModelProviderSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.time.Duration;
import java.util.List;

import javax.annotation.Nullable;

/** {@link StreamExecNode} for ML model prediction. */
@ExecNodeMetadata(
        name = "stream-exec-ml-predict",
        version = 1,
        producedTransformations = StreamExecMlPredict.ML_PREDICT_TRANSFORMATION,
        consumedOptions = {
            "table.exec.async-ml-predict.buffer-capacity",
            "table.exec.async-ml-predict.timeout",
            "table.exec.async-ml-predict.output-mode",
            "table.exec.async-ml-predict.retry-strategy",
            "table.exec.async-ml-predict.retry-delay",
            "table.exec.async-ml-predict.max-attempts"
        },
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecMlPredict extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

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
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.inputColumns = checkNotNull(inputColumns);
        this.modelProviderSpec = checkNotNull(modelProviderSpec);
        this.asyncOptions = asyncOptions;
        this.inputRowType = (RowType) inputProperties.get(0).getType();
    }

    public StreamExecMlPredict(
            ReadableConfig tableConfig,
            ModelProviderSpec modelProviderSpec,
            List<String> inputColumns,
            @Nullable AsyncOptions asyncOptions,
            ChangelogMode inputChangelogMode,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecMlPredict.class),
                ExecNodeContext.newPersistedConfig(StreamExecLookupJoin.class, tableConfig),
                inputColumns,
                modelProviderSpec,
                asyncOptions,
                inputProperties,
                outputType,
                description);
    }

    @Override
    public Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final Transformation<RowData> inputTransformation =
                getInputEdges().get(0).translateToPlan(planner);
        final ClassLoader classLoader = planner.getExecEnv().getUserClassLoader();

        final StreamOperatorFactory<RowData> operatorFactory;
        if (asyncOptions != null) {
            operatorFactory = createAsyncPredictOperatorFactory(classLoader, config);
        } else {
            operatorFactory = createSyncPredictOperatorFactory(classLoader, config);
        }

        return ExecNodeUtil.createOneInputTransformation(
                inputTransformation,
                createTransformationMeta(ML_PREDICT_TRANSFORMATION, config),
                operatorFactory,
                InternalTypeInfo.of(getOutputType()),
                inputTransformation.getParallelism());
    }

    private StreamOperatorFactory<RowData> createSyncPredictOperatorFactory(
            ClassLoader classLoader, ExecNodeConfig config) {
        ProcessFunction<RowData, RowData> processFunction =
                createSyncPredictFunction(classLoader, config);

        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(processFunction);

        return SimpleOperatorFactory.of(operator);
    }

    private StreamOperatorFactory<RowData> createAsyncPredictOperatorFactory(
            ClassLoader classLoader, ExecNodeConfig config, AsyncOptions asyncOptions) {
        AsyncFunction<RowData, RowData> asyncFunction =
                createAsyncPredictFunction(classLoader, config);

        return new AsyncWaitOperatorFactory<>(
                asyncFunction,
                asyncOptions.timeout,
                asyncOptions.capacity,
                asyncOptions.outputMode);

        return AsyncWaitOperatorFactory.createOperatorFactory(
                asyncFunction,
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_TIMEOUT).toMillis(),
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_BUFFER_CAPACITY),
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_OUTPUT_MODE),
                inputSerializer,
                AsyncRetryStrategies.createRetryStrategy(
                        config.get(
                                ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_RETRY_STRATEGY),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_RETRY_DELAY),
                        config.get(
                                ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_MAX_ATTEMPTS)));
    }

    private ProcessFunction<RowData, RowData> createSyncPredictFunction(
            ClassLoader classLoader, ExecNodeConfig config) {
        final PredictRuntimeProvider runtimeProvider =
                modelProviderSpec.getPredictRuntimeProvider(
                        getContextResolvedModel(config), classLoader);

        return runtimeProvider.createPredictFunction(
                inputColumns, inputRowType, getOutputType(), config.getConfiguration());
    }

    private AsyncFunction<RowData, RowData> createAsyncPredictFunction(
            ClassLoader classLoader, ExecNodeConfig config) {
        final PredictRuntimeProvider runtimeProvider =
                modelProviderSpec.getPredictRuntimeProvider(
                        getContextResolvedModel(config), classLoader);

        return runtimeProvider.createAsyncPredictFunction(
                inputColumns, inputRowType, getOutputType(), config.getConfiguration());
    }

    /** Async options for ML prediction. */
    public static class AsyncOptions {
        private final long timeout;
        private final int capacity;
        private final AsyncDataStream.OutputMode outputMode;
        private final RetryStrategy retryStrategy;
        private final Duration retryDelay;
        private final int maxAttempts;

        @JsonCreator
        public AsyncOptions(
                @JsonProperty("timeout") long timeout,
                @JsonProperty("capacity") int capacity,
                @JsonProperty("outputMode") OutputMode outputMode,
                @JsonProperty("retryStrategy") RetryStrategy retryStrategy,
                @JsonProperty("retryDelay") Duration retryDelay,
                @JsonProperty("maxAttempts") int maxAttempts) {
            this.timeout = timeout;
            this.capacity = capacity;
            this.outputMode = outputMode;
            this.retryStrategy = retryStrategy;
            this.retryDelay = retryDelay;
            this.maxAttempts = maxAttempts;
        }

        public Duration getTimeout() {
            return timeout;
        }

        public int getCapacity() {
            return capacity;
        }

        public AsyncOutputMode getOutputMode() {
            return outputMode;
        }

        public RetryStrategy getRetryStrategy() {
            return retryStrategy;
        }

        public Duration getRetryDelay() {
            return retryDelay;
        }

        public int getMaxAttempts() {
            return maxAttempts;
        }
    }
}
