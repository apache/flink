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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.CorrelateCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base {@link ExecNode} which matches along with join a Java/Scala user defined table function. */
public abstract class CommonExecCorrelate extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    public static final String CORRELATE_TRANSFORMATION = "correlate";

    public static final String FIELD_NAME_JOIN_TYPE = "joinType";
    public static final String FIELD_NAME_FUNCTION_CALL = "functionCall";
    public static final String FIELD_NAME_CONDITION = "condition";

    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType joinType;

    @JsonProperty(FIELD_NAME_FUNCTION_CALL)
    private final RexCall invocation;

    @JsonProperty(FIELD_NAME_CONDITION)
    private final @Nullable RexNode condition;

    private final Class<?> operatorBaseClass;
    private final boolean retainHeader;

    public CommonExecCorrelate(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            FlinkJoinType joinType,
            RexCall invocation,
            @Nullable RexNode condition,
            Class<?> operatorBaseClass,
            boolean retainHeader,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.joinType = checkNotNull(joinType);
        this.invocation = checkNotNull(invocation);
        this.condition = condition;
        this.operatorBaseClass = checkNotNull(operatorBaseClass);
        this.retainHeader = retainHeader;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(operatorBaseClass);
        return CorrelateCodeGenerator.generateCorrelateTransformation(
                config,
                ctx,
                inputTransform,
                (RowType) inputEdge.getOutputType(),
                invocation,
                JavaScalaConversionUtil.toScala(Optional.ofNullable(condition)),
                (RowType) getOutputType(),
                joinType,
                inputTransform.getParallelism(),
                retainHeader,
                getClass().getSimpleName(),
                createTransformationMeta(CORRELATE_TRANSFORMATION, config),
                false);
    }
}
