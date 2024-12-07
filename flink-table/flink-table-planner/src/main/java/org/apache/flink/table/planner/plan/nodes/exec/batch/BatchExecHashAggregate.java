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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.batch.AggWithoutKeysCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.batch.HashAggCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.generator.OneInputOpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.spec.HashAggFusionCodegenSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedOperator;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Batch {@link ExecNode} for hash-based aggregate operator. */
@ExecNodeMetadata(
        name = "batch-exec-hash-aggregate",
        version = 1,
        producedTransformations = BatchExecHashAggregate.HASH_AGGREGATE_TRANSFORMATION,
        consumedOptions = {
            "table.exec.resource.hash-agg.memory",
            "table.exec.sort.max-num-file-handles",
            "table.exec.spill-compression.enabled",
            "table.exec.spill-compression.block-size"
        },
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecHashAggregate extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String HASH_AGGREGATE_TRANSFORMATION = "hash-aggregate";
    public static final String FIELD_NAME_GROUPING = "grouping";
    public static final String FIELD_NAME_AUX_GROUPING = "auxGrouping";
    public static final String FIELD_NAME_AGG_CALLS = "aggCalls";
    public static final String FIELD_NAME_AGG_INPUT_ROW_TYPE = "aggInputRowType";
    public static final String FIELD_NAME_IS_MERGE = "isMerge";
    public static final String FIELD_NAME_IS_FINAL = "isFinal";
    public static final String FIELD_NAME_SUPPORT_ADAPTIVE_LOCAL_HASH_AGG =
            "supportAdaptiveLocalHashAgg";

    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AUX_GROUPING)
    private final int[] auxGrouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    @JsonProperty(FIELD_NAME_AGG_INPUT_ROW_TYPE)
    private final RowType aggInputRowType;

    @JsonProperty(FIELD_NAME_IS_MERGE)
    private final boolean isMerge;

    @JsonProperty(FIELD_NAME_IS_FINAL)
    private final boolean isFinal;

    @JsonProperty(FIELD_NAME_SUPPORT_ADAPTIVE_LOCAL_HASH_AGG)
    private final boolean supportAdaptiveLocalHashAgg;

    public BatchExecHashAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            int[] auxGrouping,
            AggregateCall[] aggCalls,
            RowType aggInputRowType,
            boolean isMerge,
            boolean isFinal,
            boolean supportAdaptiveLocalHashAgg,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecHashAggregate.class),
                ExecNodeContext.newPersistedConfig(BatchExecHashAggregate.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.grouping = grouping;
        this.auxGrouping = auxGrouping;
        this.aggCalls = aggCalls;
        this.aggInputRowType = aggInputRowType;
        this.isMerge = isMerge;
        this.isFinal = isFinal;
        this.supportAdaptiveLocalHashAgg = supportAdaptiveLocalHashAgg;
    }

    @JsonCreator
    public BatchExecHashAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AUX_GROUPING) int[] auxGrouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_AGG_INPUT_ROW_TYPE) RowType aggInputRowType,
            @JsonProperty(FIELD_NAME_IS_MERGE) boolean isMerge,
            @JsonProperty(FIELD_NAME_IS_FINAL) boolean isFinal,
            @JsonProperty(FIELD_NAME_SUPPORT_ADAPTIVE_LOCAL_HASH_AGG)
                    boolean supportAdaptiveLocalHashAgg,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.grouping = grouping;
        this.auxGrouping = auxGrouping;
        this.aggCalls = aggCalls;
        this.aggInputRowType = aggInputRowType;
        this.isMerge = isMerge;
        this.isFinal = isFinal;
        this.supportAdaptiveLocalHashAgg = supportAdaptiveLocalHashAgg;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final RowType outputRowType = (RowType) getOutputType();

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader());

        final AggregateInfoList aggInfos =
                AggregateUtil.transformToBatchAggregateInfoList(
                        planner.getTypeFactory(),
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        null, // aggCallNeedRetractions
                        null); // orderKeyIndexes

        final long managedMemory;
        final GeneratedOperator<OneInputStreamOperator<RowData, RowData>> generatedOperator;
        if (grouping.length == 0) {
            managedMemory = 0L;
            generatedOperator =
                    AggWithoutKeysCodeGenerator.genWithoutKeys(
                            ctx,
                            planner.createRelBuilder(),
                            aggInfos,
                            inputRowType,
                            outputRowType,
                            isMerge,
                            isFinal,
                            "NoGrouping");
        } else {
            managedMemory =
                    config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_AGG_MEMORY)
                            .getBytes();
            generatedOperator =
                    HashAggCodeGenerator.genWithKeys(
                            ctx,
                            planner.createRelBuilder(),
                            aggInfos,
                            inputRowType,
                            outputRowType,
                            grouping,
                            auxGrouping,
                            isMerge,
                            isFinal,
                            supportAdaptiveLocalHashAgg,
                            config.get(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES),
                            config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED),
                            (int)
                                    config.get(
                                                    ExecutionConfigOptions
                                                            .TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                            .getBytes());
        }

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(HASH_AGGREGATE_TRANSFORMATION, config),
                new CodeGenOperatorFactory<>(generatedOperator),
                InternalTypeInfo.of(outputRowType),
                inputTransform.getParallelism(),
                managedMemory,
                false);
    }

    @Override
    public boolean supportFusionCodegen() {
        return true;
    }

    @Override
    protected OpFusionCodegenSpecGenerator translateToFusionCodegenSpecInternal(
            PlannerBase planner, ExecNodeConfig config, CodeGeneratorContext parentCtx) {
        OpFusionCodegenSpecGenerator input =
                getInputEdges().get(0).translateToFusionCodegenSpec(planner, parentCtx);

        final AggregateInfoList aggInfos =
                AggregateUtil.transformToBatchAggregateInfoList(
                        planner.getTypeFactory(),
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        null, // aggCallNeedRetractions
                        null); // orderKeyIndexes
        long managedMemory =
                config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_AGG_MEMORY).getBytes();
        if (grouping.length == 0) {
            managedMemory = 0L;
        }

        OpFusionCodegenSpecGenerator hashAggSpecGenerator =
                new OneInputOpFusionCodegenSpecGenerator(
                        input,
                        managedMemory,
                        (RowType) getOutputType(),
                        new HashAggFusionCodegenSpec(
                                new CodeGeneratorContext(
                                        config,
                                        planner.getFlinkContext().getClassLoader(),
                                        parentCtx),
                                planner.createRelBuilder(),
                                aggInfos,
                                grouping,
                                auxGrouping,
                                isFinal,
                                isMerge,
                                supportAdaptiveLocalHashAgg,
                                config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES),
                                config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_SPILL_COMPRESSION_ENABLED),
                                (int)
                                        config.get(
                                                        ExecutionConfigOptions
                                                                .TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                                .getBytes()));
        input.addOutput(1, hashAggSpecGenerator);
        return hashAggSpecGenerator;
    }
}
