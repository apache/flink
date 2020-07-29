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
import org.apache.flink.table.connector.source.abilities.SupportsStatisticsReport;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Planner rule that tries to calculate table statistics from a {@link LogicalTableScan},
 * which table is a {@link TableSourceTable}. And the table source in the table is a
 * {@link SupportsStatisticsReport}.
 */
public class SourceReportStatisticsRule extends RelOptRule {

	public static final SourceReportStatisticsRule INSTANCE = new SourceReportStatisticsRule();
	public static final String REPORTED_DIGEST = "statistics-reported=true";

	public SourceReportStatisticsRule() {
		super(operand(LogicalTableScan.class, none()), SourceReportStatisticsRule.class.getSimpleName());
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		LogicalTableScan scan = call.rel(0);
		TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		// we can not push filter twice
		return tableSourceTable != null
			&& tableSourceTable.tableSource() instanceof SupportsStatisticsReport
			&& Arrays.stream(tableSourceTable.extraDigests()).noneMatch(REPORTED_DIGEST::equals);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		LogicalTableScan scan = call.rel(0);
		TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
		report(call, scan, table);
	}

	private void report(
		RelOptRuleCall call,
		LogicalTableScan scan,
		FlinkPreparingTableBase relOptTable) {
		TableSourceTable oldTableSourceTable = relOptTable.unwrap(TableSourceTable.class);
		DynamicTableSource newTableSource = oldTableSourceTable.tableSource().copy();

		TableSourceTable newTableSourceTable = oldTableSourceTable.copy(
			newTableSource,
			getNewFlinkStatistic(oldTableSourceTable.getStatistic(), (SupportsStatisticsReport) newTableSource),
			getNewExtraDigests(oldTableSourceTable)
		);
		TableScan newScan = LogicalTableScan.create(scan.getCluster(), newTableSourceTable, scan.getHints());
		call.transformTo(newScan);
	}

	private FlinkStatistic getNewFlinkStatistic(FlinkStatistic oldStatistic, SupportsStatisticsReport report) {
		return FlinkStatistic.builder()
				.statistic(oldStatistic)
				.tableStats(report.reportTableStatistics(oldStatistic.getTableStats()))
				.build();
	}

	private String[] getNewExtraDigests(TableSourceTable tableSourceTable) {
		return Stream.concat(
				Arrays.stream(tableSourceTable.extraDigests()),
				Arrays.stream(new String[]{REPORTED_DIGEST}))
			.toArray(String[]::new);
	}
}
