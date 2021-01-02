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

import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.NestedColumn;
import org.apache.flink.table.planner.plan.utils.NestedProjectionUtil;
import org.apache.flink.table.planner.plan.utils.NestedSchema;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.planner.sources.DynamicSourceUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Base planner rule for push projects into tableSource which implements {@link
 * SupportsProjectionPushDown}.
 */
abstract class PushProjectIntoTableSourceRuleBase extends RelOptRule {

    public PushProjectIntoTableSourceRuleBase(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    protected static TableSourceTable applyPushIntoTableSource(
            TableScan oldTableScan,
            List<RexNode> oldProjectExps,
            List<RexNode> newProjectExps,
            RelOptRuleCall call) {
        final int[] refFields = RexNodeExtractor.extractRefInputFields(oldProjectExps);
        TableSourceTable oldTableSourceTable =
                oldTableScan.getTable().unwrap(TableSourceTable.class);
        final TableSchema oldSchema = oldTableSourceTable.catalogTable().getSchema();
        final DynamicTableSource oldSource = oldTableSourceTable.tableSource();

        final boolean supportsNestedProjection =
                ((SupportsProjectionPushDown) oldTableSourceTable.tableSource())
                        .supportsNestedProjection();
        List<String> fieldNames = oldTableScan.getRowType().getFieldNames();

        final TableConfig config =
                ShortcutUtils.unwrapContext(call.getPlanner().getContext()).getTableConfig();

        if (!supportsNestedProjection && refFields.length == fieldNames.size()) {
            return null;
        }

        List<RexNode> oldProjectsWithPK = new ArrayList<>(oldProjectExps);
        FlinkTypeFactory flinkTypeFactory =
                (FlinkTypeFactory) oldTableSourceTable.getRelOptSchema().getTypeFactory();
        if (isPrimaryKeyFieldsRequired(oldTableSourceTable, config)) {
            // add pk into projects for upsert source
            oldSchema
                    .getPrimaryKey()
                    .ifPresent(
                            pks -> {
                                for (String name : pks.getColumns()) {
                                    int index = fieldNames.indexOf(name);
                                    TableColumn col = oldSchema.getTableColumn(index).get();
                                    oldProjectsWithPK.add(
                                            new RexInputRef(
                                                    index,
                                                    flinkTypeFactory.createFieldTypeFromLogicalType(
                                                            col.getType().getLogicalType())));
                                }
                            });
        }
        // build used schema tree
        RowType originType = DynamicSourceUtils.createProducedType(oldSchema, oldSource);
        NestedSchema nestedSchema =
                NestedProjectionUtil.build(
                        oldProjectsWithPK, flinkTypeFactory.buildRelNodeRowType(originType));
        if (!supportsNestedProjection) {
            // mark the fields in the top level as leaf
            for (NestedColumn column : nestedSchema.columns().values()) {
                column.markLeaf();
            }
        }
        DynamicTableSource newSource = oldSource.copy();
        DataType producedDataType = TypeConversions.fromLogicalToDataType(originType);

        DataType newProducedDataType = null;
        if (oldSource instanceof SupportsReadingMetadata) {
            List<String> metadataKeys =
                    DynamicSourceUtils.createRequiredMetadataKeys(oldSchema, oldSource);
            newProducedDataType =
                    applyPhysicalAndMetadataPushDown(
                            nestedSchema, metadataKeys, originType, newSource);
        } else {
            int[][] projectedFields = NestedProjectionUtil.convertToIndexArray(nestedSchema);
            ((SupportsProjectionPushDown) newSource).applyProjection(projectedFields);
            newProducedDataType = DataTypeUtils.projectRow(producedDataType, projectedFields);
        }

        RelDataType newRowType =
                flinkTypeFactory.buildRelNodeRowType(
                        (RowType) newProducedDataType.getLogicalType());

        // project push down does not change the statistic, we can reuse origin statistic
        TableSourceTable newTableSourceTable =
                oldTableSourceTable.copy(
                        newSource,
                        newRowType,
                        new String[] {
                            ("project=[" + String.join(", ", newRowType.getFieldNames()) + "]")
                        });
        // rewrite the input field in projections
        // the origin projections are enough. Because the upsert source only uses pk info
        // normalization node.
        List<RexNode> newProjects =
                NestedProjectionUtil.rewrite(
                        oldProjectExps, nestedSchema, call.builder().getRexBuilder());
        newProjectExps.addAll(newProjects);

        return newTableSourceTable;
    }

    /** Returns true if the primary key is required and should be retained. */
    private static boolean isPrimaryKeyFieldsRequired(TableSourceTable table, TableConfig config) {
        return DynamicSourceUtils.isUpsertSource(table.catalogTable(), table.tableSource())
                || DynamicSourceUtils.isSourceChangeEventsDuplicate(
                        table.catalogTable(), table.tableSource(), config);
    }

    /**
     * Push the used physical column and metadata into table source. The returned value is used to
     * build new table schema.
     */
    private static DataType applyPhysicalAndMetadataPushDown(
            NestedSchema nestedSchema,
            List<String> metadataKeys,
            RowType originType,
            DynamicTableSource newSource) {
        // TODO: supports nested projection for metadata
        List<NestedColumn> usedMetaDataFields = new LinkedList<>();
        int physicalCount = originType.getFieldCount() - metadataKeys.size();
        List<String> fieldNames = originType.getFieldNames();

        // rm metadata in the tree
        for (int i = 0; i < metadataKeys.size(); i++) {
            NestedColumn usedMetadata =
                    nestedSchema.columns().remove(fieldNames.get(i + physicalCount));
            if (usedMetadata != null) {
                usedMetaDataFields.add(usedMetadata);
            }
        }

        // get path of the used fields
        int[][] projectedPhysicalFields = NestedProjectionUtil.convertToIndexArray(nestedSchema);
        ((SupportsProjectionPushDown) newSource).applyProjection(projectedPhysicalFields);

        // push the metadata back for later rewrite and extract the location in the origin row
        int newIndex = projectedPhysicalFields.length;
        List<String> usedMetadataNames = new LinkedList<>();
        for (NestedColumn metadata : usedMetaDataFields) {
            metadata.setIndexOfLeafInNewSchema(newIndex++);
            nestedSchema.columns().put(metadata.name(), metadata);
            usedMetadataNames.add(metadataKeys.get(metadata.indexInOriginSchema() - physicalCount));
        }

        // apply metadata push down
        int[][] projectedFields =
                Stream.concat(
                                Stream.of(projectedPhysicalFields),
                                usedMetaDataFields.stream()
                                        .map(field -> new int[] {field.indexInOriginSchema()}))
                        .toArray(int[][]::new);
        DataType newProducedDataType =
                DataTypeUtils.projectRow(
                        TypeConversions.fromLogicalToDataType(originType), projectedFields);
        ((SupportsReadingMetadata) newSource)
                .applyReadableMetadata(usedMetadataNames, newProducedDataType);

        return newProducedDataType;
    }
}
