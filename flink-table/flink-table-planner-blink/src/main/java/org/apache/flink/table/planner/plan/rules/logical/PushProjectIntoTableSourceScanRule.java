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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.planner.plan.utils.RexNodeRewriter;
import org.apache.flink.table.planner.plan.utils.ScanUtil;
import org.apache.flink.table.planner.sources.DynamicSourceUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Planner rule that pushes a {@link LogicalProject} into a {@link LogicalTableScan}
 * which wraps a {@link SupportsProjectionPushDown} dynamic table source.
 *
 * <p>NOTES: This rule does not support nested fields push down now,
 * instead it will push the top-level column down just like non-nested fields.
 */
public class PushProjectIntoTableSourceScanRule extends RelOptRule {
	public static final PushProjectIntoTableSourceScanRule INSTANCE = new PushProjectIntoTableSourceScanRule();

	public PushProjectIntoTableSourceScanRule() {
		super(operand(LogicalProject.class,
				operand(LogicalTableScan.class, none())),
				"PushProjectIntoTableSourceScanRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		LogicalTableScan scan = call.rel(1);
		TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		if (tableSourceTable == null || !(tableSourceTable.tableSource() instanceof SupportsProjectionPushDown)) {
			return false;
		}
		SupportsProjectionPushDown pushDownSource = (SupportsProjectionPushDown) tableSourceTable.tableSource();
		if (pushDownSource.supportsNestedProjection()) {
			throw new TableException("Nested projection push down is unsupported now. \n" +
					"Please disable nested projection (SupportsProjectionPushDown#supportsNestedProjection returns false), " +
					"planner will push down the top-level columns.");
		} else {
			return true;
		}
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		final LogicalProject project = call.rel(0);
		final LogicalTableScan scan = call.rel(1);

		final List<String> fieldNames = scan.getRowType().getFieldNames();
		final int fieldCount = fieldNames.size();

		final int[] refFields = RexNodeExtractor.extractRefInputFields(project.getProjects());
		final int[] usedFields;

		TableSourceTable oldTableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		if (isUpsertSource(oldTableSourceTable)) {
			// primary key fields are needed for upsert source
			List<String> keyFields = oldTableSourceTable.catalogTable().getSchema()
				.getPrimaryKey().get().getColumns();
			// we should get source fields from scan node instead of CatalogTable,
			// because projection may have been pushed down
			List<String> sourceFields = scan.getRowType().getFieldNames();
			int[] primaryKey = ScanUtil.getPrimaryKeyIndices(sourceFields, keyFields);
			usedFields = mergeFields(refFields, primaryKey);
		} else {
			usedFields = refFields;
		}
		// if no fields can be projected, we keep the original plan.
		if (usedFields.length == fieldCount) {
			return;
		}

		final List<String> projectedFieldNames = IntStream.of(usedFields)
			.mapToObj(fieldNames::get)
			.collect(Collectors.toList());

		final TableSchema oldSchema = oldTableSourceTable.catalogTable().getSchema();
		final DynamicTableSource oldSource = oldTableSourceTable.tableSource();
		final List<String> metadataKeys = DynamicSourceUtils.createRequiredMetadataKeys(oldSchema, oldSource);
		final int physicalFieldCount = fieldCount - metadataKeys.size();
		final DynamicTableSource newSource = oldSource.copy();

		// remove metadata columns from the projection push down and store it in a separate list
		// the projection push down itself happens purely on physical columns
		final int[] usedPhysicalFields;
		final List<String> usedMetadataKeys;
		if (newSource instanceof SupportsReadingMetadata) {
			usedPhysicalFields = IntStream.of(usedFields)
				// select only physical columns
				.filter(i -> i < physicalFieldCount)
				.toArray();
			final List<String> usedMetadataKeysUnordered = IntStream.of(usedFields)
				// select only metadata columns
				.filter(i -> i >= physicalFieldCount)
				// map the indices to keys
				.mapToObj(i -> metadataKeys.get(fieldCount - i - 1))
				.collect(Collectors.toList());
			// order the keys according to the source's declaration
			usedMetadataKeys = metadataKeys
				.stream()
				.filter(usedMetadataKeysUnordered::contains)
				.collect(Collectors.toList());
		} else {
			usedPhysicalFields = usedFields;
			usedMetadataKeys = Collections.emptyList();
		}

		final int[][] projectedPhysicalFields = IntStream.of(usedPhysicalFields)
			.mapToObj(i -> new int[]{ i })
			.toArray(int[][]::new);

		// push down physical projection
		((SupportsProjectionPushDown) newSource).applyProjection(projectedPhysicalFields);

		// push down metadata projection
		applyUpdatedMetadata(
			oldSource,
			oldSchema,
			newSource,
			metadataKeys,
			usedMetadataKeys,
			physicalFieldCount,
			projectedPhysicalFields);

		FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) oldTableSourceTable.getRelOptSchema().getTypeFactory();
		RelDataType newRowType = flinkTypeFactory.projectStructType(oldTableSourceTable.getRowType(), usedFields);

		// project push down does not change the statistic, we can reuse origin statistic
		TableSourceTable newTableSourceTable = oldTableSourceTable.copy(
				newSource, newRowType, new String[] { ("project=[" + String.join(", ", projectedFieldNames) + "]") });

		LogicalTableScan newScan = new LogicalTableScan(
				scan.getCluster(), scan.getTraitSet(), scan.getHints(), newTableSourceTable);
		// rewrite input field in projections
		List<RexNode> newProjects = RexNodeRewriter.rewriteWithNewFieldInput(project.getProjects(), usedFields);
		LogicalProject newProject = project.copy(
				project.getTraitSet(),
				newScan,
				newProjects,
				project.getRowType());

		if (ProjectRemoveRule.isTrivial(newProject)) {
			// drop project if the transformed program merely returns its input
			call.transformTo(newScan);
		} else {
			call.transformTo(newProject);
		}
	}

	private void applyUpdatedMetadata(
			DynamicTableSource oldSource,
			TableSchema oldSchema,
			DynamicTableSource newSource,
			List<String> metadataKeys,
			List<String> usedMetadataKeys,
			int physicalFieldCount,
			int[][] projectedPhysicalFields) {
		if (newSource instanceof SupportsReadingMetadata) {
			final DataType producedDataType = TypeConversions.fromLogicalToDataType(
				DynamicSourceUtils.createProducedType(oldSchema, oldSource));

			final int[][] projectedMetadataFields = usedMetadataKeys
				.stream()
				.map(metadataKeys::indexOf)
				.map(i -> new int[]{ physicalFieldCount + i })
				.toArray(int[][]::new);

			final int[][] projectedFields = Stream
				.concat(
					Stream.of(projectedPhysicalFields),
					Stream.of(projectedMetadataFields)
				)
				.toArray(int[][]::new);

			// create a new, final data type that includes all projections
			final DataType newProducedDataType = DataTypeUtils.projectRow(producedDataType, projectedFields);

			((SupportsReadingMetadata) newSource).applyReadableMetadata(usedMetadataKeys, newProducedDataType);
		}
	}

	/**
	 * Returns true if the table is a upsert source when it is works in scan mode.
	 */
	private static boolean isUpsertSource(TableSourceTable table) {
		TableSchema schema = table.catalogTable().getSchema();
		if (!schema.getPrimaryKey().isPresent()) {
			return false;
		}
		DynamicTableSource tableSource = table.tableSource();
		if (tableSource instanceof ScanTableSource) {
			ChangelogMode mode = ((ScanTableSource) tableSource).getChangelogMode();
			return mode.contains(RowKind.UPDATE_AFTER) && !mode.contains(RowKind.UPDATE_BEFORE);
		}
		return false;
	}

	private static int[] mergeFields(int[] fields1, int[] fields2) {
		List<Integer> results = Arrays.stream(fields1).boxed().collect(Collectors.toList());
		Arrays.stream(fields2).forEach(idx -> {
			if (!results.contains(idx)) {
				results.add(idx);
			}
		});
		return results.stream().mapToInt(Integer::intValue).toArray();
	}
}
