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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.CorrelateCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Optional;

/**
 * Base {@link ExecNode} which matches along with join a Java/Scala user defined table function.
 */
public abstract class CommonExecCorrelate extends ExecNodeBase<RowData> {
	private final FlinkJoinType joinType;
	@Nullable
	private final RexProgram project;
	private final RexCall invocation;
	@Nullable
	private final RexNode condition;
	private final Class<?> operatorBaseClass;
	private final boolean retainHeader;

	public CommonExecCorrelate(
			FlinkJoinType joinType,
			@Nullable RexProgram project,
			RexCall invocation,
			@Nullable RexNode condition,
			Class<?> operatorBaseClass,
			boolean retainHeader,
			ExecEdge inputEdge,
			RowType outputType,
			String description) {
		super(Collections.singletonList(inputEdge), outputType, description);
		this.joinType = joinType;
		this.project = project;
		this.invocation = invocation;
		this.condition = condition;
		this.operatorBaseClass = operatorBaseClass;
		this.retainHeader = retainHeader;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
		final ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
		final Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);
		final CodeGeneratorContext ctx = new CodeGeneratorContext(planner.getTableConfig())
				.setOperatorBaseClass(operatorBaseClass);
		final Transformation<RowData> transform = CorrelateCodeGenerator.generateCorrelateTransformation(
				planner.getTableConfig(),
				ctx,
				inputTransform,
				(RowType) inputNode.getOutputType(),
				JavaScalaConversionUtil.toScala(Optional.ofNullable(project)),
				invocation,
				JavaScalaConversionUtil.toScala(Optional.ofNullable(condition)),
				(RowType) getOutputType(),
				joinType,
				inputTransform.getParallelism(),
				retainHeader,
				getClass().getSimpleName(),
				getDesc());

		if (inputsContainSingleton()) {
			transform.setParallelism(1);
			transform.setMaxParallelism(1);
		}
		return transform;
	}
}
