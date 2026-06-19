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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.functions.PredictFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.FunctionCallCodeGenerator;
import org.apache.flink.table.planner.codegen.MLPredictCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.MLPredictSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.ModelSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.functions.ml.ModelPredictRuntimeProviderContext;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.ml.AsyncMLPredictRunner;
import org.apache.flink.table.runtime.operators.ml.MLPredictRunner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;

/** Common ExecNode for {@code ML_PREDICT}. */
public abstract class CommonExecMLPredictTableFunction extends ExecNodeBase<RowData> {

    public static final String ML_PREDICT_TRANSFORMATION = "ml-predict-table-function";

    protected static final String FIELD_NAME_ML_PREDICT_SPEC = "mlPredictSpec";
    protected static final String FIELD_NAME_MODEL_SPEC = "modelSpec";
    protected static final String FIELD_NAME_ASYNC_OPTIONS = "asyncOptions";

    @JsonProperty(FIELD_NAME_ML_PREDICT_SPEC)
    protected final MLPredictSpec mlPredictSpec;

    @JsonProperty(FIELD_NAME_MODEL_SPEC)
    protected final ModelSpec modelSpec;

    @JsonProperty(FIELD_NAME_ASYNC_OPTIONS)
    protected final @Nullable FunctionCallUtil.AsyncOptions asyncOptions;

    protected CommonExecMLPredictTableFunction(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            MLPredictSpec mlPredictSpec,
            ModelSpec modelSpec,
            @Nullable FunctionCallUtil.AsyncOptions asyncOptions,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.mlPredictSpec = mlPredictSpec;
        this.modelSpec = modelSpec;
        this.asyncOptions = asyncOptions;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        Transformation<RowData> inputTransformation =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);

        ModelProvider provider = modelSpec.getModelProvider(planner.getFlinkContext());
        boolean async = asyncOptions != null;
        UserDefinedFunction predictFunction = findModelFunction(provider, async);
        FlinkContext context = planner.getFlinkContext();
        DataTypeFactory dataTypeFactory = context.getCatalogManager().getDataTypeFactory();

        RowType inputType = (RowType) getInputEdges().get(0).getOutputType();
        RowType modelOutputType =
                (RowType)
                        modelSpec
                                .getContextResolvedModel()
                                .getResolvedModel()
                                .getResolvedOutputSchema()
                                .toPhysicalRowDataType()
                                .getLogicalType();
        return async
                ? createAsyncModelPredict(
                        inputTransformation,
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        dataTypeFactory,
                        inputType,
                        modelOutputType,
                        (RowType) getOutputType(),
                        (AsyncPredictFunction) predictFunction)
                : createModelPredict(
                        inputTransformation,
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        dataTypeFactory,
                        inputType,
                        modelOutputType,
                        (RowType) getOutputType(),
                        (PredictFunction) predictFunction);
    }

    private Transformation<RowData> createModelPredict(
            Transformation<RowData> inputTransformation,
            ExecNodeConfig config,
            ClassLoader classLoader,
            DataTypeFactory dataTypeFactory,
            RowType inputRowType,
            RowType modelOutputType,
            RowType resultRowType,
            PredictFunction predictFunction) {
        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher =
                MLPredictCodeGenerator.generateSyncPredictFunction(
                        config,
                        classLoader,
                        dataTypeFactory,
                        inputRowType,
                        modelOutputType,
                        resultRowType,
                        mlPredictSpec.getFeatures(),
                        predictFunction,
                        modelSpec.getContextResolvedModel().getIdentifier().asSummaryString(),
                        config.get(PipelineOptions.OBJECT_REUSE));
        GeneratedCollector<ListenableCollector<RowData>> generatedCollector =
                MLPredictCodeGenerator.generateCollector(
                        new CodeGeneratorContext(config, classLoader),
                        inputRowType,
                        modelOutputType,
                        (RowType) getOutputType());
        MLPredictRunner mlPredictRunner = new MLPredictRunner(generatedFetcher, generatedCollector);
        SimpleOperatorFactory<RowData> operatorFactory =
                SimpleOperatorFactory.of(new ProcessOperator<>(mlPredictRunner));
        return ExecNodeUtil.createOneInputTransformation(
                inputTransformation,
                createTransformationMeta(ML_PREDICT_TRANSFORMATION, config),
                operatorFactory,
                InternalTypeInfo.of(getOutputType()),
                inputTransformation.getParallelism(),
                false);
    }

    @SuppressWarnings("unchecked")
    private Transformation<RowData> createAsyncModelPredict(
            Transformation<RowData> inputTransformation,
            ExecNodeConfig config,
            ClassLoader classLoader,
            DataTypeFactory dataTypeFactory,
            RowType inputRowType,
            RowType modelOutputType,
            RowType resultRowType,
            AsyncPredictFunction asyncPredictFunction) {
        FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
                generatedFuncWithType =
                        MLPredictCodeGenerator.generateAsyncPredictFunction(
                                config,
                                classLoader,
                                dataTypeFactory,
                                inputRowType,
                                modelOutputType,
                                resultRowType,
                                mlPredictSpec.getFeatures(),
                                asyncPredictFunction,
                                modelSpec
                                        .getContextResolvedModel()
                                        .getIdentifier()
                                        .asSummaryString());
        AsyncFunction<RowData, RowData> asyncFunc =
                new AsyncMLPredictRunner(
                        (GeneratedFunction) generatedFuncWithType.tableFunc(),
                        Preconditions.checkNotNull(asyncOptions).asyncBufferCapacity);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransformation,
                createTransformationMeta(ML_PREDICT_TRANSFORMATION, config),
                new AsyncWaitOperatorFactory<>(
                        asyncFunc,
                        asyncOptions.asyncTimeout,
                        asyncOptions.asyncBufferCapacity,
                        asyncOptions.asyncOutputMode),
                InternalTypeInfo.of(getOutputType()),
                inputTransformation.getParallelism(),
                false);
    }

    private UserDefinedFunction findModelFunction(ModelProvider provider, boolean async) {
        ModelPredictRuntimeProviderContext context =
                new ModelPredictRuntimeProviderContext(
                        modelSpec.getContextResolvedModel().getResolvedModel(),
                        Configuration.fromMap(mlPredictSpec.getRuntimeConfig()));
        if (async) {
            if (provider instanceof AsyncPredictRuntimeProvider) {
                return ((AsyncPredictRuntimeProvider) provider).createAsyncPredictFunction(context);
            }
        } else {
            if (provider instanceof PredictRuntimeProvider) {
                return ((PredictRuntimeProvider) provider).createPredictFunction(context);
            }
        }

        throw new TableException(
                "Required "
                        + (async ? "async" : "sync")
                        + " model function by planner, but ModelProvider "
                        + "does not offer a valid model function.");
    }
}
