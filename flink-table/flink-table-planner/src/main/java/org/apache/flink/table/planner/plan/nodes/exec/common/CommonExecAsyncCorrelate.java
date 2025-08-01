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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.AsyncCorrelateCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AsyncTableUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.correlate.async.AsyncCorrelateRunner;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexCall;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Base class for exec Async Correlate. */
public class CommonExecAsyncCorrelate extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    public static final String ASYNC_CORRELATE_TRANSFORMATION = "async-correlate";

    public static final String FIELD_NAME_JOIN_TYPE = "joinType";
    public static final String FIELD_NAME_FUNCTION_CALL = "functionCall";

    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType joinType;

    @JsonProperty(FIELD_NAME_FUNCTION_CALL)
    private final RexCall invocation;

    public CommonExecAsyncCorrelate(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            FlinkJoinType joinType,
            RexCall invocation,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.joinType = joinType;
        this.invocation = invocation;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final OneInputTransformation<RowData, RowData> transform =
                createAsyncOneInputTransformation(
                        inputTransform, config, planner.getFlinkContext().getClassLoader());

        return transform;
    }

    private OneInputTransformation<RowData, RowData> createAsyncOneInputTransformation(
            Transformation<RowData> inputTransform,
            ExecNodeConfig config,
            ClassLoader classLoader) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        RowType inputRowType =
                RowType.of(inputEdge.getOutputType().getChildren().toArray(new LogicalType[0]));

        InternalTypeInfo<RowData> asyncOperatorResultTypeInfo =
                InternalTypeInfo.of(getOutputType());
        OneInputStreamOperatorFactory<RowData, RowData> factory =
                getAsyncFunctionOperator(config, classLoader, inputRowType);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(ASYNC_CORRELATE_TRANSFORMATION, config),
                factory,
                asyncOperatorResultTypeInfo,
                inputTransform.getParallelism(),
                false);
    }

    private OneInputStreamOperatorFactory<RowData, RowData> getAsyncFunctionOperator(
            ExecNodeConfig config, ClassLoader classLoader, RowType inputRowType) {

        RowType resultTypeInfo = (RowType) FlinkTypeFactory.toLogicalType(invocation.getType());

        GeneratedFunction<AsyncFunction<RowData, Object>> generatedFunction =
                AsyncCorrelateCodeGenerator.generateFunction(
                        "AsyncTableFunction",
                        inputRowType,
                        resultTypeInfo,
                        invocation,
                        config,
                        classLoader);
        DataStructureConverter<RowData, Object> fetcherConverter =
                cast(
                        DataStructureConverters.getConverter(
                                TypeConversions.fromLogicalToDataType(
                                        FlinkTypeFactory.toLogicalType(invocation.getType()))));
        AsyncCorrelateRunner func = new AsyncCorrelateRunner(generatedFunction, fetcherConverter);
        FunctionCallUtil.AsyncOptions options = AsyncTableUtil.getAsyncOptions(config);
        return new AsyncWaitOperatorFactory<>(
                func,
                options.asyncTimeout,
                options.asyncBufferCapacity,
                options.asyncOutputMode,
                AsyncTableUtil.getResultRetryStrategy(config));
    }

    @SuppressWarnings("unchecked")
    private DataStructureConverter<RowData, Object> cast(
            DataStructureConverter<Object, Object> converter) {
        return (DataStructureConverter<RowData, Object>) (Object) converter;
    }
}
