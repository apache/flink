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
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSort;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

/**
 * Planner rule that tries to push limit into a [[FlinkLogicalTableSourceScan]].
 * The original limit will still be retained.
 * The reasons why the limit still be retained:
 * 1.If the source is required to return the exact number of limit number, the implementation
 * of the source is highly required. The source is required to accurately control the record
 * number of split, and the parallelism setting also need to be adjusted accordingly.
 * 2.When remove the limit, maybe filter will be pushed down to the source after limit pushed
 * down. The source need know it should do limit first and do the filter later, it is hard to
 * implement.
 * 3.We can support limit with offset, we can push down offset + fetch to table source.
 */
public class PushLimitIntoTableSourceScanRule extends RelOptRule {
	public static final PushLimitIntoTableSourceScanRule INSTANCE = new PushLimitIntoTableSourceScanRule();

	public PushLimitIntoTableSourceScanRule() {
		super(operand(FlinkLogicalSort.class,
			operand(FlinkLogicalTableSourceScan.class, none())),
			"PushLimitIntoTableSourceScanRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		Sort sort = call.rel(0);
		boolean onlyLimit = sort.getCollation().getFieldCollations().isEmpty() && sort.fetch != null;
		if (onlyLimit) {
			TableSourceTable tableSourceTable = call.rel(1).getTable().unwrap(TableSourceTable.class);
			if (tableSourceTable != null && tableSourceTable.tableSource() instanceof SupportsLimitPushDown  && tableSourceTable.extraDigests().length == 0) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		Sort sort = call.rel(0);
		FlinkLogicalTableSourceScan scan = call.rel(1);
		TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		int offset = sort.offset == null ? 0 : RexLiteral.intValue(sort.offset);
		int limit = offset + RexLiteral.intValue(sort.fetch);

		TableSourceTable newTableSourceTable = applyLimit((long) limit, tableSourceTable);

		FlinkLogicalTableSourceScan newScan = scan.copy(scan.getTraitSet(), newTableSourceTable);
		call.transformTo(sort.copy(sort.getTraitSet(), Collections.singletonList(newScan)));
	}

	private TableSourceTable applyLimit(
		Long limit,
		FlinkPreparingTableBase relOptTable) {
		TableSourceTable tableSourceTable = relOptTable.unwrap(TableSourceTable.class);
		DynamicTableSource newTableSource = tableSourceTable.tableSource().copy();
		((SupportsLimitPushDown) newTableSource).applyLimit(limit);

		FlinkStatistic statistic = relOptTable.getStatistic();
		long newRowCount = 0;
		if (statistic.getRowCount() != null) {
			newRowCount = Math.min(limit, statistic.getRowCount().longValue());
		} else {
			newRowCount = limit;
		}
		// Update TableStats after limit push down
		TableStats newTableStats = new TableStats(newRowCount);
		FlinkStatistic newStatistic = FlinkStatistic.builder()
			.statistic(statistic)
			.tableStats(newTableStats)
			.build();

		//Update extraDigests
		String[] oldExtraDigests = tableSourceTable.extraDigests();
		String[] newExtraDigests = null;
		if (limit > 0) {
			String extraDigests = "limit=[" + limit + "]";
			newExtraDigests = Stream.concat(Arrays.stream(oldExtraDigests), Arrays.stream(new String[]{extraDigests})).toArray(String[]::new);
		} else {
			newExtraDigests = oldExtraDigests;
		}

		return new TableSourceTable(
			tableSourceTable.getRelOptSchema(),
			tableSourceTable.tableIdentifier(),
			tableSourceTable.getRowType(),
			newStatistic,
			newTableSource,
			tableSourceTable.isStreamingMode(),
			tableSourceTable.catalogTable(),
			tableSourceTable.dynamicOptions(),
			newExtraDigests
		);
	}
}
