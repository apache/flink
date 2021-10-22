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
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectCodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base class for exec Calc. */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class CommonExecCalc extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    public static final String FIELD_NAME_EXP_LIST = "expList";
    public static final String FIELD_NAME_LOCAL_REFS = "localRefs";
    public static final String FIELD_NAME_PROJECTION = "projection";
    public static final String FIELD_NAME_CONDITION = "condition";

    @JsonProperty(FIELD_NAME_EXP_LIST)
    private final List<RexNode> expList;

    @JsonProperty(FIELD_NAME_LOCAL_REFS)
    private final List<RexNode> localRefs;

    @JsonProperty(FIELD_NAME_PROJECTION)
    private final List<RexNode> projection;

    @JsonProperty(FIELD_NAME_CONDITION)
    private final @Nullable RexNode condition;

    @JsonIgnore private final Class<?> operatorBaseClass;
    @JsonIgnore private final boolean retainHeader;

    protected CommonExecCalc(
            List<RexNode> expList,
            List<RexNode> localRefs,
            List<RexNode> projection,
            @Nullable RexNode condition,
            Class<?> operatorBaseClass,
            boolean retainHeader,
            int id,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.expList = checkNotNull(expList);
        this.localRefs = localRefs;
        this.projection = projection;
        this.condition = condition;
        this.operatorBaseClass = checkNotNull(operatorBaseClass);
        this.retainHeader = retainHeader;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final CodeGeneratorContext ctx =
                new ProjectCodeGeneratorContext(
                                planner.getTableConfig(),
                                expList,
                                JavaScalaConversionUtil.toScala(localRefs),
                                JavaScalaConversionUtil.toScala(projection))
                        .setOperatorBaseClass(operatorBaseClass);

        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
                CalcCodeGenerator.generateCalcOperator(
                        ctx,
                        inputTransform,
                        (RowType) getOutputType(),
                        JavaScalaConversionUtil.toScala(expList),
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(this.condition)),
                        retainHeader,
                        getClass().getSimpleName());
        return new OneInputTransformation<>(
                inputTransform,
                getDescription(),
                substituteStreamOperator,
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism());
    }
}
