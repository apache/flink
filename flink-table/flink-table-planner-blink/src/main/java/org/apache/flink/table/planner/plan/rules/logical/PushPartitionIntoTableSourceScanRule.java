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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.plan.utils.PartitionPruner;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.planner.plan.utils.RexNodeToExpressionConverter;
import org.apache.flink.table.planner.utils.CatalogTableStatisticsConverter;
import org.apache.flink.table.types.logical.LogicalType;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;

import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Planner rule that tries to push partition evaluated by filter condition into a {@link LogicalTableScan}.
*/
public class PushPartitionIntoTableSourceScanRule extends RelOptRule {
	public static final PushPartitionIntoTableSourceScanRule INSTANCE = new PushPartitionIntoTableSourceScanRule();

	public PushPartitionIntoTableSourceScanRule() {
		super(operand(Filter.class,
				operand(LogicalTableScan.class, none())),
			"PushPartitionIntoTableSourceScanRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		Filter filter = call.rel(0);
		if (filter.getCondition() == null) {
			return false;
		}
		TableSourceTable tableSourceTable = call.rel(1).getTable().unwrap(TableSourceTable.class);
		if (tableSourceTable == null) {
			return false;
		}
		DynamicTableSource dynamicTableSource = tableSourceTable.tableSource();
		if (!(dynamicTableSource instanceof SupportsPartitionPushDown)) {
			return false;
		}
		CatalogTable catalogTable = tableSourceTable.catalogTable();
		if (!catalogTable.isPartitioned() || catalogTable.getPartitionKeys().isEmpty()) {
			return false;
		}
		return Arrays.stream(tableSourceTable.extraDigests()).noneMatch(digest -> digest.startsWith("partitions=["));
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		Filter filter = call.rel(0);
		LogicalTableScan scan = call.rel(1);
		TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);

		RelDataType inputFieldTypes = filter.getInput().getRowType();
		List<String> inputFieldNames = inputFieldTypes.getFieldNames();
		List<String> partitionFieldNames = tableSourceTable.catalogTable().getPartitionKeys();
		// extract partition predicates
		RelBuilder relBuilder = call.builder();
		RexBuilder rexBuilder = relBuilder.getRexBuilder();
		Tuple2<Seq<RexNode>, Seq<RexNode>> allPredicates = RexNodeExtractor.extractPartitionPredicateList(
			filter.getCondition(),
			FlinkRelOptUtil.getMaxCnfNodeCount(scan),
			inputFieldNames.toArray(new String[0]),
			rexBuilder,
			partitionFieldNames.toArray(new String[0]));
		RexNode partitionPredicate = RexUtil.composeConjunction(rexBuilder, JavaConversions.seqAsJavaList(allPredicates._1));

		if (partitionPredicate.isAlwaysTrue()) {
			return;
		}
		// build pruner
		LogicalType[] partitionFieldTypes = partitionFieldNames.stream()
			.map(name -> {
				int index  = inputFieldNames.indexOf(name);
				if (index < 0) {
					throw new TableException(String.format("Partitioned key '%s' isn't found in input columns. " +
						"Validator should have checked that.", name));
				}
				return inputFieldTypes.getFieldList().get(index).getType(); })
			.map(FlinkTypeFactory::toLogicalType)
			.toArray(LogicalType[]::new);
		RexNode finalPartitionPredicate = adjustPartitionPredicate(inputFieldNames, partitionFieldNames, partitionPredicate);
		FlinkContext context = call.getPlanner().getContext().unwrap(FlinkContext.class);
		Function<List<Map<String, String>>, List<Map<String, String>>> defaultPruner = partitions -> PartitionPruner.prunePartitions(
			context.getTableConfig(),
			partitionFieldNames.toArray(new String[0]),
			partitionFieldTypes,
			partitions,
			finalPartitionPredicate);
		// prune partitions
		List<Map<String, String>> remainingPartitions =
			readPartitionsAndPrune(
					rexBuilder,
					context,
					tableSourceTable,
					defaultPruner,
					allPredicates._1(),
					inputFieldNames);
		// apply push down
		DynamicTableSource dynamicTableSource = tableSourceTable.tableSource().copy();
		((SupportsPartitionPushDown) dynamicTableSource).applyPartitions(remainingPartitions);

		// build new statistic
		TableStats newTableStat = null;
		ObjectIdentifier identifier = tableSourceTable.tableIdentifier();
		ObjectPath tablePath = identifier.toObjectPath();
		Optional<Catalog> catalogOptional = context.getCatalogManager().getCatalog(identifier.getCatalogName());
		Optional<TableStats> partitionStats;
		if (catalogOptional.isPresent()) {
			for (Map<String, String> partition: remainingPartitions) {
				partitionStats = getPartitionStats(catalogOptional.get(), tablePath, partition);
				if (!partitionStats.isPresent()) {
					// clear all information before
					newTableStat = null;
					break;
				} else {
					newTableStat = newTableStat == null ? partitionStats.get() : newTableStat.merge(partitionStats.get());
				}
			}
		}
		FlinkStatistic newStatistic = FlinkStatistic.builder()
			.statistic(tableSourceTable.getStatistic())
			.tableStats(newTableStat)
			.build();

		String extraDigest = "partitions=[" +
			String.join(", ", remainingPartitions
				.stream()
				.map(Object::toString)
				.toArray(String[]::new)) +
			"]";
		TableSourceTable newTableSourceTable = tableSourceTable.copy(dynamicTableSource, newStatistic, new String[]{extraDigest});
		LogicalTableScan newScan = LogicalTableScan.create(scan.getCluster(), newTableSourceTable, scan.getHints());

		// transform to new node
		RexNode nonPartitionPredicate = RexUtil.composeConjunction(rexBuilder, JavaConversions.seqAsJavaList(allPredicates._2()));
		if (nonPartitionPredicate.isAlwaysTrue()) {
			call.transformTo(newScan);
		} else {
			Filter newFilter = filter.copy(filter.getTraitSet(), newScan, nonPartitionPredicate);
			call.transformTo(newFilter);
		}
	}

	/**
	 * adjust the partition field reference index to evaluate the partition values.
	 * e.g. the original input fields is: a, b, c, p, and p is partition field. the partition values
	 * are: List(Map("p"->"1"), Map("p" -> "2"), Map("p" -> "3")). If the original partition
	 * predicate is $3 > 1. after adjusting, the new predicate is ($0 > 1).
	 * and use ($0 > 1) to evaluate partition values (row(1), row(2), row(3)).
	 */
	private RexNode adjustPartitionPredicate(List<String> inputFieldNames, List<String> partitionFieldNames, RexNode partitionPredicate) {
		return partitionPredicate.accept(new RexShuttle() {
			@Override
			public RexNode visitInputRef(RexInputRef inputRef) {
				int index = inputRef.getIndex();
				String fieldName = inputFieldNames.get(index);
				int newIndex = partitionFieldNames.indexOf(fieldName);
				if (newIndex < 0) {
					throw new TableException(String.format("Field name '%s' isn't found in partitioned columns." +
						" Validator should have checked that.", fieldName));
				}
				if (newIndex == index) {
					return inputRef;
				} else {
					return new RexInputRef(newIndex, inputRef.getType());
				}
			}
		});
	}

	private List<Map<String, String>> readPartitionsAndPrune(
			RexBuilder rexBuilder,
			FlinkContext context,
			TableSourceTable tableSourceTable,
			Function<List<Map<String, String>>, List<Map<String, String>>> pruner,
			Seq<RexNode> partitionPredicate,
			List<String> inputFieldNames) {
		// get partitions from table/catalog and prune
		Optional<Catalog> catalogOptional = context.getCatalogManager().getCatalog(tableSourceTable.tableIdentifier().getCatalogName());

		DynamicTableSource dynamicTableSource = tableSourceTable.tableSource();
		ObjectIdentifier identifier = tableSourceTable.tableIdentifier();
		Optional<List<Map<String, String>>> optionalPartitions = ((SupportsPartitionPushDown) dynamicTableSource).listPartitions();
		if (optionalPartitions.isPresent()) {
			return pruner.apply(optionalPartitions.get());
		} else {
			// check catalog whether is available
			// we will read partitions from catalog if table doesn't support listPartitions.
			if (!catalogOptional.isPresent()) {
				throw new TableException(
					String.format("Table %s must from a catalog, but %s is not a catalog",
						identifier.asSummaryString(), identifier.getCatalogName()));
			}
			try {
				return readPartitionFromCatalogAndPrune(
					rexBuilder,
					context,
					catalogOptional.get(),
					identifier,
					inputFieldNames,
					partitionPredicate,
					pruner);
			} catch (TableNotExistException tableNotExistException) {
				throw new TableException(String.format("Table %s is not found in catalog.", identifier.asSummaryString()));
			} catch (TableNotPartitionedException tableNotPartitionedException) {
				throw new TableException(
					String.format("Table %s is not a partitionable source. Validator should have checked it.", identifier.asSummaryString()),
					tableNotPartitionedException);
			}
		}
	}

	private List<Map<String, String>> readPartitionFromCatalogAndPrune(
			RexBuilder rexBuilder,
			FlinkContext context,
			Catalog catalog,
			ObjectIdentifier tableIdentifier,
			List<String> allFieldNames,
			Seq<RexNode> partitionPredicate,
			Function<List<Map<String, String>>, List<Map<String, String>>> pruner)
			throws TableNotExistException, TableNotPartitionedException {
		ObjectPath tablePath = tableIdentifier.toObjectPath();
		// build filters
		RexNodeToExpressionConverter converter = new RexNodeToExpressionConverter(
			rexBuilder,
			allFieldNames.toArray(new String[0]),
			context.getFunctionCatalog(),
			context.getCatalogManager(),
			TimeZone.getTimeZone(context.getTableConfig().getLocalTimeZone()));
		ArrayList<Expression> partitionFilters = new ArrayList<>();
		Option<ResolvedExpression> subExpr;
		for (RexNode node: JavaConversions.seqAsJavaList(partitionPredicate)) {
			subExpr = node.accept(converter);
			if (!subExpr.isEmpty()) {
				partitionFilters.add(subExpr.get());
			} else {
				// if part of expr is unresolved, we read all partitions and prune.
				return readPartitionFromCatalogWithoutFilterAndPrune(catalog, tablePath, pruner);
			}
		}
		try {
			return catalog.listPartitionsByFilter(tablePath, partitionFilters)
				.stream()
				.map(CatalogPartitionSpec::getPartitionSpec)
				.collect(Collectors.toList());
		} catch (UnsupportedOperationException e) {
			return readPartitionFromCatalogWithoutFilterAndPrune(catalog, tablePath, pruner);
		}
	}

	private List<Map<String, String>> readPartitionFromCatalogWithoutFilterAndPrune(
			Catalog catalog,
			ObjectPath tablePath,
			Function<List<Map<String, String>>, List<Map<String, String>>> pruner)
			throws TableNotExistException, CatalogException, TableNotPartitionedException {
		List<Map<String, String>> allPartitions = catalog.listPartitions(tablePath)
			.stream()
			.map(CatalogPartitionSpec::getPartitionSpec)
			.collect(Collectors.toList());
		// prune partitions
		return pruner.apply(allPartitions);
	}

	private Optional<TableStats> getPartitionStats(Catalog catalog, ObjectPath tablePath, Map<String, String> partition) {
		try {
			CatalogPartitionSpec spec = new CatalogPartitionSpec(partition);
			CatalogTableStatistics partitionStat = catalog.getPartitionStatistics(tablePath, spec);
			CatalogColumnStatistics partitionColStat = catalog.getPartitionColumnStatistics(tablePath, spec);
			TableStats stats = CatalogTableStatisticsConverter.convertToTableStats(partitionStat, partitionColStat);
			return Optional.of(stats);
		} catch (PartitionNotExistException e) {
			return Optional.empty();
		}
	}
}
