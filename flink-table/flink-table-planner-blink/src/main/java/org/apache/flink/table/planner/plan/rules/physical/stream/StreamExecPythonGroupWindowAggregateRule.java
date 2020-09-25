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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecPythonGroupWindowAggregate;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.PythonUtil;
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

import scala.collection.JavaConverters;

/**
 * The physical rule is responsible for converting {@link FlinkLogicalWindowAggregate} to
 * {@link StreamExecPythonGroupWindowAggregate}.
 */
public class StreamExecPythonGroupWindowAggregateRule extends ConverterRule {

	public static final StreamExecPythonGroupWindowAggregateRule INSTANCE = new StreamExecPythonGroupWindowAggregateRule();

	private StreamExecPythonGroupWindowAggregateRule() {
		super(FlinkLogicalWindowAggregate.class,
			FlinkConventions.LOGICAL(),
			FlinkConventions.STREAM_PHYSICAL(),
			"StreamExecPythonGroupWindowAggregateRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		FlinkLogicalWindowAggregate agg = call.rel(0);
		List<AggregateCall> aggCalls = agg.getAggCallList();

		// check if we have grouping sets
		if (agg.getGroupType() != Aggregate.Group.SIMPLE || agg.indicator) {
			throw new TableException("GROUPING SETS are currently not supported.");
		}

		boolean existGeneralPythonFunction =
			aggCalls.stream().anyMatch(x -> PythonUtil.isPythonAggregate(x, PythonFunctionKind.GENERAL));
		boolean existPandasFunction =
			aggCalls.stream().anyMatch(x -> PythonUtil.isPythonAggregate(x, PythonFunctionKind.PANDAS));
		boolean existJavaFunction =
			aggCalls.stream().anyMatch(x -> !PythonUtil.isPythonAggregate(x, null));
		if (existPandasFunction || existGeneralPythonFunction) {
			if (existGeneralPythonFunction) {
				throw new TableException("non-Pandas UDAFs are not supported in stream mode currently.");
			}
			if (existJavaFunction) {
				throw new TableException("Python UDAF and Java/Scala UDAF cannot be used together.");
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	public RelNode convert(RelNode rel) {
		FlinkLogicalWindowAggregate agg = (FlinkLogicalWindowAggregate) rel;
		LogicalWindow window = agg.getWindow();

		if (window instanceof SessionGroupWindow) {
			throw new TableException("Session Group Window is currently not supported.");
		}
		RelNode input = agg.getInput();
		RelDataType inputRowType = input.getRowType();
		RelOptCluster cluster = rel.getCluster();
		FlinkRelDistribution requiredDistribution;
		if (agg.getGroupCount() != 0) {
			requiredDistribution = FlinkRelDistribution.hash(agg.getGroupSet().asList(), true);
		} else {
			requiredDistribution = FlinkRelDistribution.SINGLETON();
		}
		RelTraitSet requiredTraitSet = input.getTraitSet()
			.replace(FlinkConventions.STREAM_PHYSICAL())
			.replace(requiredDistribution);

		RelTraitSet providedTraitSet = rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
		RelNode newInput = RelOptRule.convert(input, requiredTraitSet);
		TableConfig config = cluster.getPlanner().getContext().unwrap(FlinkContext.class).getTableConfig();
		WindowEmitStrategy emitStrategy = WindowEmitStrategy.apply(config, agg.getWindow());

		FieldReferenceExpression timeField = agg.getWindow().timeAttribute();

		int inputTimestampIndex;
		if (AggregateUtil.isRowtimeAttribute(timeField)) {
			inputTimestampIndex = AggregateUtil.timeFieldIndex(
				inputRowType, relBuilderFactory.create(cluster, null), timeField);
		} else {
			inputTimestampIndex = -1;
		}

		return new StreamExecPythonGroupWindowAggregate(
			cluster,
			providedTraitSet,
			newInput,
			rel.getRowType(),
			inputRowType,
			agg.getGroupSet().toArray(),
			JavaConverters.asScalaIteratorConverter(
				agg.getAggCallList().iterator()).asScala().toSeq(),
			agg.getWindow(),
			agg.getNamedProperties(),
			inputTimestampIndex,
			emitStrategy);
	}
}
