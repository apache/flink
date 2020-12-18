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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.WatermarkGeneratorCodeGenerator;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;
import org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.Optional;

/**
 * Stream exec node which generates watermark based on the input elements.
 */
public class StreamExecWatermarkAssigner extends StreamExecNode<RowData> {
	private final RexNode watermarkExpr;
	private final int rowtimeFieldIndex;

	public StreamExecWatermarkAssigner(
			RexNode watermarkExpr,
			int rowtimeFieldIndex,
			ExecEdge inputEdge,
			RowType outputType,
			String description) {
		super(Collections.singletonList(inputEdge), outputType, description);
		this.watermarkExpr = watermarkExpr;
		this.rowtimeFieldIndex = rowtimeFieldIndex;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Transformation<RowData> translateToPlanInternal(StreamPlanner planner) {
		final ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
		final Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);

		final TableConfig tableConfig = planner.getTableConfig();

		final GeneratedWatermarkGenerator watermarkGenerator =
				WatermarkGeneratorCodeGenerator.generateWatermarkGenerator(
						tableConfig,
						(RowType) inputNode.getOutputType(),
						watermarkExpr,
						JavaScalaConversionUtil.toScala(Optional.empty()));

		final long idleTimeout = tableConfig.getConfiguration().get(
				ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT).toMillis();

		final WatermarkAssignerOperatorFactory operatorFactory = new WatermarkAssignerOperatorFactory(
				rowtimeFieldIndex,
				idleTimeout,
				watermarkGenerator);

		return new OneInputTransformation<>(
				inputTransform,
				getDesc(),
				operatorFactory,
				InternalTypeInfo.of(getOutputType()),
				inputTransform.getParallelism());
	}
}
