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
import org.apache.flink.table.planner.codegen.ExpandCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base {@link ExecNode} that can expand one row to multiple rows based on given projects. */
public abstract class CommonExecExpand extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    public static final String EXPAND_TRANSFORMATION = "expand";

    public static final String FIELD_NAME_PROJECTS = "projects";

    @JsonProperty(FIELD_NAME_PROJECTS)
    private final List<List<RexNode>> projects;

    private final boolean retainHeader;

    public CommonExecExpand(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<List<RexNode>> projects,
            boolean retainHeader,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.projects = checkNotNull(projects);
        checkArgument(
                projects.size() > 0
                        && projects.get(0).size() > 0
                        && projects.stream().map(List::size).distinct().count() == 1);
        this.retainHeader = retainHeader;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final CodeGenOperatorFactory<RowData> operatorFactory =
                ExpandCodeGenerator.generateExpandOperator(
                        new CodeGeneratorContext(
                                config, planner.getFlinkContext().getClassLoader()),
                        (RowType) inputEdge.getOutputType(),
                        (RowType) getOutputType(),
                        projects,
                        retainHeader,
                        getClass().getSimpleName());

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(EXPAND_TRANSFORMATION, config),
                operatorFactory,
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism(),
                false);
    }
}
