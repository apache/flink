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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
		return Arrays.stream(tableSourceTable.extraDigests()).noneMatch(digest -> digest.startsWith("project=["));
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		final LogicalProject project = call.rel(0);
		final LogicalTableScan scan = call.rel(1);

		TableSourceTable oldTableSourceTable = scan.getTable().unwrap(TableSourceTable.class);

		final boolean supportsNestedProjection =
				((SupportsProjectionPushDown) oldTableSourceTable.tableSource()).supportsNestedProjection();

		final List<String> fieldNames = scan.getRowType().getFieldNames();
		final int fieldCount = fieldNames.size();

		final int[] refFields = RexNodeExtractor.extractRefInputFields(project.getProjects());
		final int[] usedFields;

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
		if (!supportsNestedProjection && usedFields.length == fieldCount) {
			return;
		}

		final TableSchema oldSchema = oldTableSourceTable.catalogTable().getSchema();
		final DynamicTableSource oldSource = oldTableSourceTable.tableSource();
		final List<String> metadataKeys = DynamicSourceUtils.createRequiredMetadataKeys(oldSchema, oldSource);
		final int physicalFieldCount = fieldCount - metadataKeys.size();
		final DynamicTableSource newSource = oldSource.copy();

		final List<List<Integer>> usedFieldsCoordinates = new ArrayList<>();
		final Map<Integer, Map<List<String>, Integer>> fieldCoordinatesToOrder = new HashMap<>();

		if (supportsNestedProjection) {
			getExpandedFieldAndOrderMapping(
					project, oldSchema, usedFields, physicalFieldCount, usedFieldsCoordinates, fieldCoordinatesToOrder);
		} else {
			for (int usedField : usedFields) {
				// filter metadata columns
				if (usedField >= physicalFieldCount) {
					continue;
				}
				fieldCoordinatesToOrder.put(usedField,
						Collections.singletonMap(Collections.singletonList("*"), usedFieldsCoordinates.size()));
				usedFieldsCoordinates.add(Collections.singletonList(usedField));
			}
		}

		final int[][] projectedPhysicalFields = usedFieldsCoordinates
				.stream()
				.map(list -> list.stream().mapToInt(i -> i).toArray())
				.toArray(int[][]::new);

		// push down physical projection
		((SupportsProjectionPushDown) newSource).applyProjection(projectedPhysicalFields);

		DataType producedDataType = TypeConversions.fromLogicalToDataType(DynamicSourceUtils.createProducedType(oldSchema, oldSource));
		// add metadata information into coordinates && mapping

		final boolean supportsReadingMetaData = oldTableSourceTable.tableSource() instanceof SupportsReadingMetadata;
		DataType newProducedDataType = supportsReadingMetaData ?
				applyUpdateMetadataAndGetNewDataType(
						newSource, producedDataType,  metadataKeys, usedFields, physicalFieldCount, usedFieldsCoordinates, fieldCoordinatesToOrder) :
				DataTypeUtils.projectRow(producedDataType, projectedPhysicalFields);

		FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) oldTableSourceTable.getRelOptSchema().getTypeFactory();
		RelDataType newRowType = flinkTypeFactory.buildRelNodeRowType((RowType) newProducedDataType.getLogicalType());

		// project push down does not change the statistic, we can reuse origin statistic
		TableSourceTable newTableSourceTable = oldTableSourceTable.copy(
				newSource, newRowType, new String[] {
						("project=[" + String.join(", ", newRowType.getFieldNames()) + "]") });

		LogicalTableScan newScan = new LogicalTableScan(
				scan.getCluster(), scan.getTraitSet(), scan.getHints(), newTableSourceTable);
		// rewrite input field in projections
		List<RexNode> newProjects = RexNodeRewriter.rewriteNestedProjectionWithNewFieldInput(
				project.getProjects(),
				fieldCoordinatesToOrder,
				newRowType.getFieldList().stream().map(RelDataTypeField::getType).collect(Collectors.toList()),
				call.builder().getRexBuilder());

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

	private DataType applyUpdateMetadataAndGetNewDataType(
			DynamicTableSource newSource,
			DataType producedDataType,
			List<String> metadataKeys,
			int[] usedFields,
			int physicalFieldCount,
			List<List<Integer>> usedFieldsCoordinates,
			Map<Integer, Map<List<String>, Integer>> fieldCoordinatesToOrder) {
		//TODO: support nested projection for metadata
		final List<String> usedMetadataKeysUnordered = IntStream.of(usedFields)
				// select only metadata columns
				.filter(i -> i >= physicalFieldCount)
				// map the indices to keys
				.mapToObj(i -> metadataKeys.get(i - physicalFieldCount))
				.collect(Collectors.toList());
		// order the keys according to the source's declaration
		final List<String> usedMetadataKeys = metadataKeys
				.stream()
				.filter(usedMetadataKeysUnordered::contains)
				.collect(Collectors.toList());

		final List<List<Integer>> projectedMetadataFields = new ArrayList<>(usedMetadataKeys.size());
		for (String key: usedMetadataKeys) {
			int index = metadataKeys.indexOf(key);
			fieldCoordinatesToOrder.put(
					physicalFieldCount + index,
					Collections.singletonMap(Collections.singletonList("*"), fieldCoordinatesToOrder.size()));
			projectedMetadataFields.add(Collections.singletonList(physicalFieldCount + index));
		}
		usedFieldsCoordinates.addAll(projectedMetadataFields);

		int[][] projectedFields = usedFieldsCoordinates
				.stream()
				.map(coordinates -> coordinates.stream().mapToInt(i -> i).toArray())
				.toArray(int[][]::new);

		DataType newProducedDataType = DataTypeUtils.projectRow(producedDataType, projectedFields);

		((SupportsReadingMetadata) newSource).applyReadableMetadata(usedMetadataKeys, newProducedDataType);
		return newProducedDataType;
	}

	/**
	 * 	Get coordinates and mapping of the physicalColumn if scan supports nested projection push down.
	 * 	It will get the expanded the name of the reference and place the refs with the same top level
	 * 	in the same array. The order of the fields in the new schema is determined by the current.
	 *
	 * 	<p>NOTICE: Currently, to resolve the name conflicts we use list to restore the every level name.
	 */
	private void getExpandedFieldAndOrderMapping(
			LogicalProject project,
			TableSchema oldSchema,
			int[] usedFields,
			int physicalCount,
			List<List<Integer>> usedPhysicalFieldsCoordinates,
			Map<Integer, Map<List<String>, Integer>> fieldCoordinatesToOrder) {
		List<String>[][] accessFieldNames =
				RexNodeExtractor.extractRefNestedInputFields(project.getProjects(), usedFields);
		int order = 0;
		for (int index = 0; index < usedFields.length; index++) {
			// filter metadata columns
			if (usedFields[index] >= physicalCount) {
				continue;
			}
			int indexInOldSchema = usedFields[index];
			Map<List<String>, Integer> mapping = new HashMap<>();
			if (accessFieldNames[index][0].get(0).equals("*")) {
				usedPhysicalFieldsCoordinates.add(Collections.singletonList(indexInOldSchema));
				mapping.put(Collections.singletonList("*"), order++);
			} else {
				for (List<String> fields : accessFieldNames[index]) {
					LogicalType dataType = oldSchema.getFieldDataType(indexInOldSchema).get().getLogicalType();
					List<Integer> coordinates = new LinkedList<>();
					coordinates.add(indexInOldSchema);
					for (String subFieldName : fields) {
						RowType rowType = (RowType) dataType;
						int fieldsIndex = rowType.getFieldIndex(subFieldName);
						dataType = rowType.getTypeAt(fieldsIndex);
						coordinates.add(fieldsIndex);
					}
					usedPhysicalFieldsCoordinates.add(coordinates);
					mapping.put(fields, order++);
				}
			}
			fieldCoordinatesToOrder.put(indexInOldSchema, mapping);
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
