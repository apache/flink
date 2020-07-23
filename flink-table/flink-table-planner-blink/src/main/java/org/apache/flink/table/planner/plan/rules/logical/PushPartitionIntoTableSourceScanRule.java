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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.plan.utils.PartitionPruner;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Planner rule that tries to push partition evaluated by filter condition into a {@link LogicalTableScan}.
*/
public class PushPartitionIntoTableSourceScanRule extends RelOptRule {
	public static final PushPartitionIntoTableSourceScanRule INSTANCE = new PushPartitionIntoTableSourceScanRule();

	public PushPartitionIntoTableSourceScanRule(){
		super(operand(Filter.class,
				operand(LogicalTableScan.class, none())),
			"PushPartitionTableSourceScanRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		Filter filter = call.rel(0);
		if (filter.getCondition() == null) {
			return false;
		}
		TableSourceTable tableSourceTable = call.rel(1).getTable().unwrap(TableSourceTable.class);
		if (tableSourceTable == null){
			return false;
		}
		DynamicTableSource dynamicTableSource = tableSourceTable.tableSource();
		if (!(dynamicTableSource instanceof SupportsPartitionPushDown)) {
			return false;
		}
		Optional<List<Map<String, String>>> partitions = ((SupportsPartitionPushDown) dynamicTableSource).listPartitions();
		return partitions.isPresent()
			&& !partitions.get().isEmpty()
			&& !Arrays.stream(tableSourceTable.extraDigests()).anyMatch(digest -> digest.startsWith("source: [partitions="));
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		Filter filter = call.rel(0);
		LogicalTableScan scan = call.rel(1);
		Context context = call.getPlanner().getContext().unwrap(FlinkContext.class);
		TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		DynamicTableSource dynamicTableSource = tableSourceTable.tableSource();
		RelDataType inputFieldType = filter.getInput().getRowType();
		List<String> inputFieldName = inputFieldType.getFieldNames();

		List<String> partitionedFieldNames = tableSourceTable.catalogTable().getPartitionKeys();

		RelBuilder relBuilder = call.builder();
		RexBuilder rexBuilder = relBuilder.getRexBuilder();

		Tuple2<Seq<RexNode>, Seq<RexNode>> predicate = RexNodeExtractor.extractPartitionPredicateList(
			filter.getCondition(),
			FlinkRelOptUtil.getMaxCnfNodeCount(scan),
			inputFieldName.toArray(new String[inputFieldName.size()]),
			rexBuilder,
			partitionedFieldNames.toArray(new String[partitionedFieldNames.size()])
			);

		RexNode partitionPredicate = RexUtil.composeConjunction(rexBuilder, JavaConversions.seqAsJavaList(predicate._1));

		if (partitionPredicate.isAlwaysTrue()){
			return;
		}

		List<LogicalType> partitionFieldType = partitionedFieldNames.stream().map(name -> {
			int index = inputFieldName.indexOf(name);
			if (index < 0) {
				throw new RuntimeException(String.format("Partitioned key '%s' isn't found in input columns. " +
					"Validator should have checked that.", name));
			}
			return inputFieldType.getFieldList().get(index).getType(); })
			.map(FlinkTypeFactory::toLogicalType).collect(Collectors.toList());

		// checked in matches(call) and listPartitions() is not empty.
		List<Map<String, String>> allPartitions = ((SupportsPartitionPushDown) dynamicTableSource).listPartitions().get();
		RexNode finalPartitionPredicate = adjustPartitionPredicate(inputFieldName, partitionedFieldNames, partitionPredicate);
		// get partitions
		List<Map<String, String>> remainingPartitions = PartitionPruner.prunePartitions(
			((FlinkContext) context).getTableConfig(),
			partitionedFieldNames.toArray(new String[partitionedFieldNames.size()]),
			partitionFieldType.toArray(new LogicalType[partitionFieldType.size()]),
			allPartitions,
			finalPartitionPredicate
			);
		((SupportsPartitionPushDown) dynamicTableSource).applyPartitions(remainingPartitions);

		// build new table scan
		FlinkStatistic statistic = tableSourceTable.getStatistic();
		String extraDigest = "source: [partitions=" +
			String.join(", ", ((SupportsPartitionPushDown) dynamicTableSource).listPartitions()
				.get()
				.stream()
				.map(partition -> partition.toString())
				.collect(Collectors.toList())
				.toArray(new String[1])) +
			"]";
		TableSourceTable newTableSourceTable = tableSourceTable.copy(dynamicTableSource, statistic, new String[]{extraDigest});
		LogicalTableScan newScan = new LogicalTableScan(
			scan.getCluster(), scan.getTraitSet(), scan.getHints(), newTableSourceTable);

		RexNode nonPartitionPredicate = RexUtil.composeConjunction(rexBuilder, JavaConversions.seqAsJavaList(predicate._2()));
		if (nonPartitionPredicate.isAlwaysTrue()) {
			call.transformTo(newScan);
		} else {
			call.transformTo(filter.copy(filter.getTraitSet(), newScan, nonPartitionPredicate));
		}
	}

	private RexNode adjustPartitionPredicate(List<String> inputFieldNames, List<String> partitionFieldNames, RexNode partitionPredicate) {
		return partitionPredicate.accept(new RexShuttle(){
			@Override
			public RexNode visitInputRef(RexInputRef inputRef) {
				int index = inputRef.getIndex();
				String fieldName = inputFieldNames.get(index);
				int newIndex = partitionFieldNames.indexOf(fieldName);
				if (newIndex < 0) {
					throw new RuntimeException(String.format("Field name '%s' isn't found in partitioned columns." +
						" Validator should have checked that.", fieldName));
				}
				if (newIndex == index){
					return inputRef;
				} else {
					return new RexInputRef(newIndex, inputRef.getType());
				}
			}
		});
	}
}
