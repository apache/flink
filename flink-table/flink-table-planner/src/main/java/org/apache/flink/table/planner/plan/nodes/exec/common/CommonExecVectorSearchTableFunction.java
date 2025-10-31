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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.source.VectorSearchTableSource;
import org.apache.flink.table.connector.source.search.AsyncVectorSearchFunctionProvider;
import org.apache.flink.table.connector.source.search.VectorSearchFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncVectorSearchFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.functions.VectorSearchFunction;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.FunctionCallCodeGenerator;
import org.apache.flink.table.planner.codegen.VectorSearchCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.VectorSearchSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.VectorSearchTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.VectorSearchUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.search.AsyncVectorSearchRunner;
import org.apache.flink.table.runtime.operators.search.VectorSearchRunner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.JoinRelType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Common ExecNode for {@code VECTOR_SEARCH} and {@code VECTOR_SEARCH_AGG}. */
public abstract class CommonExecVectorSearchTableFunction extends ExecNodeBase<RowData> {

    public static final String VECTOR_SEARCH_TRANSFORMATION = "vector-search-table-function";

    protected static final String FIELD_NAME_TABLE_SOURCE_SPEC = "tableSourceSpec";
    protected static final String FIELD_NAME_VECTOR_SEARCH_SPEC = "vectorSearchSpec";
    protected static final String FIELD_NAME_ASYNC_OPTIONS = "asyncOptions";

    @JsonProperty(FIELD_NAME_TABLE_SOURCE_SPEC)
    protected final VectorSearchTableSourceSpec tableSourceSpec;

    @JsonProperty(FIELD_NAME_VECTOR_SEARCH_SPEC)
    protected final VectorSearchSpec vectorSearchSpec;

    @JsonProperty(FIELD_NAME_ASYNC_OPTIONS)
    protected final @Nullable FunctionCallUtil.AsyncOptions asyncOptions;

    protected CommonExecVectorSearchTableFunction(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            VectorSearchTableSourceSpec tableSourceSpec,
            VectorSearchSpec vectorSearchSpec,
            @Nullable FunctionCallUtil.AsyncOptions asyncOptions,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.tableSourceSpec = tableSourceSpec;
        this.vectorSearchSpec = vectorSearchSpec;
        this.asyncOptions = asyncOptions;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        // 1. translate input node
        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> inputTransformation =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        // 2. extract search function
        TableSourceTable searchTable =
                tableSourceSpec.getSearchTable(planner.getFlinkContext(), planner.getTypeFactory());
        boolean isAsyncEnabled = asyncOptions != null;
        UserDefinedFunction vectorSearchFunction =
                findVectorSearchFunction(
                        VectorSearchUtil.createVectorSearchRuntimeProvider(
                                searchTable,
                                vectorSearchSpec.getSearchColumns().keySet(),
                                Configuration.fromMap(
                                        Optional.ofNullable(vectorSearchSpec.getRuntimeConfig())
                                                .orElse(Collections.emptyMap()))),
                        isAsyncEnabled);
        UserDefinedFunctionHelper.prepareInstance(config, vectorSearchFunction);
        // 3. build the operator
        RowType inputType = (RowType) inputEdge.getOutputType();
        RowType outputType = (RowType) getOutputType();
        DataTypeFactory dataTypeFactory =
                ShortcutUtils.unwrapContext(planner.getFlinkContext())
                        .getCatalogManager()
                        .getDataTypeFactory();
        StreamOperatorFactory<RowData> operatorFactory =
                isAsyncEnabled
                        ? createAsyncVectorSearchOperator(
                                searchTable,
                                config,
                                planner.getFlinkContext().getClassLoader(),
                                (AsyncVectorSearchFunction) vectorSearchFunction,
                                dataTypeFactory,
                                inputType,
                                vectorSearchSpec.getOutputType(),
                                outputType)
                        : createSyncVectorSearchOperator(
                                searchTable,
                                config,
                                planner.getFlinkContext().getClassLoader(),
                                (VectorSearchFunction) vectorSearchFunction,
                                dataTypeFactory,
                                inputType,
                                vectorSearchSpec.getOutputType(),
                                outputType);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransformation,
                createTransformationMeta(VECTOR_SEARCH_TRANSFORMATION, config),
                operatorFactory,
                InternalTypeInfo.of(outputType),
                inputTransformation.getParallelism(),
                false);
    }

    // ~ Utilities --------------------------------------------------------------

    private UserDefinedFunction findVectorSearchFunction(
            VectorSearchTableSource.VectorSearchRuntimeProvider provider, boolean async) {
        if (async) {
            if (provider instanceof AsyncVectorSearchFunctionProvider) {
                return ((AsyncVectorSearchFunctionProvider) provider)
                        .createAsyncVectorSearchFunction();
            }
        } else {
            if (provider instanceof VectorSearchFunctionProvider) {
                return ((VectorSearchFunctionProvider) provider).createVectorSearchFunction();
            }
        }

        throw new TableException(
                "Required "
                        + (async ? "async" : "sync")
                        + " vector search function by planner, but VectorSearchRuntimeProvider "
                        + "does not offer a valid vector search function.");
    }

    private StreamOperatorFactory<RowData> createSyncVectorSearchOperator(
            RelOptTable searchTable,
            ExecNodeConfig config,
            ClassLoader jobClassLoader,
            VectorSearchFunction vectorSearchFunction,
            DataTypeFactory dataTypeFactory,
            RowType inputType,
            RowType searchOutputType,
            RowType outputType) {
        return SimpleOperatorFactory.of(
                new ProcessOperator<>(
                        createSyncVectorSearchFunction(
                                searchTable,
                                config,
                                jobClassLoader,
                                vectorSearchFunction,
                                dataTypeFactory,
                                inputType,
                                searchOutputType,
                                outputType)));
    }

    private ProcessFunction<RowData, RowData> createSyncVectorSearchFunction(
            RelOptTable searchTable,
            ExecNodeConfig config,
            ClassLoader jobClassLoader,
            VectorSearchFunction vectorSearchFunction,
            DataTypeFactory dataTypeFactory,
            RowType inputType,
            RowType searchOutputType,
            RowType outputType) {
        ArrayList<FunctionCallUtil.FunctionParam> parameters =
                new ArrayList<>(1 + vectorSearchSpec.getSearchColumns().size());
        parameters.add(vectorSearchSpec.getTopK());
        parameters.addAll(vectorSearchSpec.getSearchColumns().values());
        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher =
                VectorSearchCodeGenerator.generateSyncVectorSearchFunction(
                        config,
                        jobClassLoader,
                        dataTypeFactory,
                        inputType,
                        searchOutputType,
                        outputType,
                        parameters,
                        vectorSearchFunction,
                        ((TableSourceTable) searchTable)
                                .contextResolvedTable()
                                .getIdentifier()
                                .asSummaryString(),
                        config.get(PipelineOptions.OBJECT_REUSE));
        GeneratedCollector<ListenableCollector<RowData>> generatedCollector =
                VectorSearchCodeGenerator.generateCollector(
                        new CodeGeneratorContext(config, jobClassLoader),
                        inputType,
                        searchOutputType,
                        outputType);
        boolean isLeftOuterJoin = vectorSearchSpec.getJoinType() == JoinRelType.LEFT;
        return new VectorSearchRunner(
                generatedFetcher,
                generatedCollector,
                isLeftOuterJoin,
                searchOutputType.getFieldCount());
    }

    @SuppressWarnings("unchecked")
    private StreamOperatorFactory<RowData> createAsyncVectorSearchOperator(
            RelOptTable searchTable,
            ExecNodeConfig config,
            ClassLoader jobClassLoader,
            AsyncVectorSearchFunction vectorSearchFunction,
            DataTypeFactory dataTypeFactory,
            RowType inputType,
            RowType searchOutputType,
            RowType outputType) {
        ArrayList<FunctionCallUtil.FunctionParam> parameters =
                new ArrayList<>(1 + vectorSearchSpec.getSearchColumns().size());
        parameters.add(vectorSearchSpec.getTopK());
        parameters.addAll(vectorSearchSpec.getSearchColumns().values());

        FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
                generatedFetcher =
                        VectorSearchCodeGenerator.generateAsyncVectorSearchFunction(
                                config,
                                jobClassLoader,
                                dataTypeFactory,
                                inputType,
                                searchOutputType,
                                outputType,
                                parameters,
                                vectorSearchFunction,
                                ((TableSourceTable) searchTable)
                                        .contextResolvedTable()
                                        .getIdentifier()
                                        .asSummaryString());

        boolean isLeftOuterJoin = vectorSearchSpec.getJoinType() == JoinRelType.LEFT;

        Preconditions.checkNotNull(asyncOptions, "Async Options can not be null.");

        return new AsyncWaitOperatorFactory<>(
                new AsyncVectorSearchRunner(
                        (GeneratedFunction) generatedFetcher.tableFunc(),
                        isLeftOuterJoin,
                        asyncOptions.asyncBufferCapacity,
                        searchOutputType.getFieldCount()),
                asyncOptions.asyncTimeout,
                asyncOptions.asyncBufferCapacity,
                asyncOptions.asyncOutputMode);
    }
}
