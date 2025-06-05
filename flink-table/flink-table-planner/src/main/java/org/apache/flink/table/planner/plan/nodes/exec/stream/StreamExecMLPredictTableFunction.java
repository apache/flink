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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.functions.PredictFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.FilterCodeGenerator;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.MLPredictSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.ModelSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.functions.ml.ModelPredictRuntimeProviderContext;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.IntStream;

/** Stream {@link ExecNode} for {@code ML_PREDICT}. */
@ExecNodeMetadata(
        name = "stream-exec-ml-predict-table-function",
        version = 1,
        producedTransformations = StreamExecMLPredictTableFunction.ML_PREDICT_TRANSFORMATION,
        minPlanVersion = FlinkVersion.V2_1,
        minStateVersion = FlinkVersion.V2_1)
public class StreamExecMLPredictTableFunction extends ExecNodeBase<RowData>
        implements MultipleTransformationTranslator<RowData>, StreamExecNode<RowData> {

    public static final String PARTITIONER_TRANSFORMATION = "partitioner";

    public static final String ML_PREDICT_TRANSFORMATION = "ml-predict-table-function";

    private final MLPredictSpec mlPredictSpec;
    private final ModelSpec modelSpec;
    private final @Nullable LookupJoinUtil.AsyncLookupOptions asyncLookupOptions;
    private final @Nullable int[] inputUpsertKeys;

    public StreamExecMLPredictTableFunction(
            ReadableConfig persistedConfig,
            MLPredictSpec mlPredictSpec,
            ModelSpec modelSpec,
            @Nullable LookupJoinUtil.AsyncLookupOptions asyncLookupOptions,
            @Nullable int[] inputUpsertKeys,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecMLPredictTableFunction.class),
                persistedConfig,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.mlPredictSpec = mlPredictSpec;
        this.modelSpec = modelSpec;
        this.asyncLookupOptions = asyncLookupOptions;
        this.inputUpsertKeys = inputUpsertKeys;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        Transformation<RowData> inputTransformation =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);

        ModelProvider provider = modelSpec.getModelProvider();
        boolean async = asyncLookupOptions != null;
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
                LookupJoinCodeGenerator.generateSyncLookupFunction(
                        config,
                        classLoader,
                        dataTypeFactory,
                        inputRowType,
                        modelOutputType,
                        resultRowType,
                        mlPredictSpec.getFeatures(),
                        predictFunction,
                        "MLPredict",
                        true);
        GeneratedCollector<ListenableCollector<RowData>> generatedCollector =
                LookupJoinCodeGenerator.generateCollector(
                        new CodeGeneratorContext(config, classLoader),
                        inputRowType,
                        modelOutputType,
                        (RowType) getOutputType(),
                        JavaScalaConversionUtil.toScala(Optional.empty()),
                        JavaScalaConversionUtil.toScala(Optional.empty()),
                        true);
        LookupJoinRunner mlPredictRunner =
                new LookupJoinRunner(
                        generatedFetcher,
                        generatedCollector,
                        FilterCodeGenerator.generateFilterCondition(
                                config, classLoader, null, inputRowType),
                        false,
                        modelOutputType.getFieldCount());
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
        LookupJoinCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
                generatedFuncWithType =
                        LookupJoinCodeGenerator.generateAsyncLookupFunction(
                                config,
                                classLoader,
                                dataTypeFactory,
                                inputRowType,
                                modelOutputType,
                                resultRowType,
                                mlPredictSpec.getFeatures(),
                                asyncPredictFunction,
                                "AsyncMLPredict");

        GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture =
                LookupJoinCodeGenerator.generateTableAsyncCollector(
                        config,
                        classLoader,
                        "TableFunctionResultFuture",
                        inputRowType,
                        modelOutputType,
                        JavaScalaConversionUtil.toScala(Optional.empty()));

        DataStructureConverter<?, ?> fetcherConverter =
                DataStructureConverters.getConverter(generatedFuncWithType.dataType());
        AsyncFunction<RowData, RowData> asyncFunc =
                new AsyncLookupJoinRunner(
                        generatedFuncWithType.tableFunc(),
                        (DataStructureConverter<RowData, Object>) fetcherConverter,
                        generatedResultFuture,
                        FilterCodeGenerator.generateFilterCondition(
                                config, classLoader, null, inputRowType),
                        InternalSerializers.create(modelOutputType),
                        false,
                        asyncLookupOptions.asyncBufferCapacity);
        if (asyncLookupOptions.asyncOutputMode == AsyncDataStream.OutputMode.UNORDERED) {
            // The input stream is insert-only.
            return ExecNodeUtil.createOneInputTransformation(
                    inputTransformation,
                    createTransformationMeta(ML_PREDICT_TRANSFORMATION, config),
                    new AsyncWaitOperatorFactory<>(
                            asyncFunc,
                            asyncLookupOptions.asyncTimeout,
                            asyncLookupOptions.asyncBufferCapacity,
                            asyncLookupOptions.asyncOutputMode),
                    InternalTypeInfo.of(getOutputType()),
                    inputTransformation.getParallelism(),
                    false);
        } else if (asyncLookupOptions.asyncOutputMode == AsyncDataStream.OutputMode.ORDERED) {
            // The input stream is cdc-stream.
            int[] shuffleKeys = inputUpsertKeys;
            // If no upset key is specified, use the whole row
            if (shuffleKeys == null || shuffleKeys.length == 0) {
                shuffleKeys = IntStream.range(0, inputRowType.getFieldCount()).toArray();
            }
            Arrays.sort(shuffleKeys);

            // Shuffle the data
            RowDataKeySelector keySelector =
                    KeySelectorUtil.getRowDataSelector(
                            classLoader, shuffleKeys, InternalTypeInfo.of(inputRowType));
            final KeyGroupStreamPartitioner<RowData, RowData> partitioner =
                    new KeyGroupStreamPartitioner<>(
                            keySelector,
                            KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
            Transformation<RowData> partitionedTransform =
                    new PartitionTransformation<>(inputTransformation, partitioner);
            createTransformationMeta(
                            PARTITIONER_TRANSFORMATION, "Partitioner", "Partitioner", config)
                    .fill(partitionedTransform);

            // Add the operator. AsyncOperator emit data order is same as the data enter the
            // operator order.
            OneInputTransformation<RowData, RowData> transformation =
                    ExecNodeUtil.createOneInputTransformation(
                            partitionedTransform,
                            createTransformationMeta(ML_PREDICT_TRANSFORMATION, config),
                            new AsyncWaitOperatorFactory<>(
                                    asyncFunc,
                                    asyncLookupOptions.asyncTimeout,
                                    asyncLookupOptions.asyncBufferCapacity,
                                    asyncLookupOptions.asyncOutputMode),
                            InternalTypeInfo.of(getOutputType()),
                            inputTransformation.getParallelism(),
                            false);
            transformation.setStateKeySelector(keySelector);
            transformation.setStateKeyType(keySelector.getProducedType());
            return transformation;
        } else {
            throw new TableException(
                    String.format("Unknown output mode: %s.", asyncLookupOptions.asyncOutputMode));
        }
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
