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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilityContext;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.NestedColumn;
import org.apache.flink.table.planner.plan.utils.NestedProjectionUtil;
import org.apache.flink.table.planner.plan.utils.NestedSchema;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.connectors.DynamicSourceUtils.createProducedType;
import static org.apache.flink.table.planner.connectors.DynamicSourceUtils.createRequiredMetadataKeys;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;

/**
 * Pushes a {@link LogicalProject} into a {@link LogicalTableScan}.
 *
 * <p>If the source implements {@link SupportsProjectionPushDown} this rule pushes the projection of
 * physical columns into the source.
 *
 * <p>If the source implements {@link SupportsReadingMetadata} this rule also pushes projected
 * metadata into the source. For sources implementing {@link SupportsReadingMetadata} but not {@link
 * SupportsProjectionPushDown} this is only done if {@link
 * SupportsReadingMetadata#supportsMetadataProjection()} returns {@code true}. This is important for
 * some sources which would not be re-usable if different instances (due to different projected
 * metadata) of the source were used together.
 */
@Internal
public class PushProjectIntoTableSourceScanRule
        extends RelRule<PushProjectIntoTableSourceScanRule.Config> {

    public static final RelOptRule INSTANCE =
            Config.EMPTY.as(Config.class).onProjectedScan().toRule();

    public PushProjectIntoTableSourceScanRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalTableScan scan = call.rel(1);
        final TableSourceTable sourceTable = scan.getTable().unwrap(TableSourceTable.class);
        if (sourceTable == null) {
            return false;
        }

        final DynamicTableSource source = sourceTable.tableSource();

        // The source supports projection push-down.
        if (supportsProjectionPushDown(source)) {
            return Arrays.stream(sourceTable.abilitySpecs())
                    .noneMatch(spec -> spec instanceof ProjectPushDownSpec);
        }

        // The source supports metadata and wants them to be projected even if projection push-down
        // (for physical columns) is not supported.
        if (supportsMetadata(source)) {
            if (Arrays.stream(sourceTable.abilitySpecs())
                    .anyMatch(spec -> spec instanceof ReadingMetadataSpec)) {
                return false;
            }

            return ((SupportsReadingMetadata) source).supportsMetadataProjection();
        }

        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final LogicalTableScan scan = call.rel(1);
        final TableSourceTable sourceTable = scan.getTable().unwrap(TableSourceTable.class);

        final boolean supportsNestedProjection =
                supportsNestedProjection(sourceTable.tableSource());

        final int[] refFields = RexNodeExtractor.extractRefInputFields(project.getProjects());
        if (!supportsNestedProjection && refFields.length == scan.getRowType().getFieldCount()) {
            // There is no top-level projection and nested projections aren't supported.
            return;
        }

        final FlinkTypeFactory typeFactory = unwrapTypeFactory(scan);
        final ResolvedSchema schema = sourceTable.catalogTable().getResolvedSchema();
        final RowType producedType = createProducedType(schema, sourceTable.tableSource());
        final NestedSchema projectedSchema =
                NestedProjectionUtil.build(
                        getProjections(project, scan),
                        typeFactory.buildRelNodeRowType(producedType));
        if (!supportsNestedProjection) {
            for (NestedColumn column : projectedSchema.columns().values()) {
                column.markLeaf();
            }
        }

        final List<SourceAbilitySpec> abilitySpecs = new ArrayList<>();
        final RowType newProducedType =
                performPushDown(sourceTable, projectedSchema, producedType, abilitySpecs);

        final DynamicTableSource newTableSource = sourceTable.tableSource().copy();
        final SourceAbilityContext context = SourceAbilityContext.from(scan);
        abilitySpecs.forEach(spec -> spec.apply(newTableSource, context));

        final RelDataType newRowType = typeFactory.buildRelNodeRowType(newProducedType);
        final TableSourceTable newSource =
                sourceTable.copy(
                        newTableSource,
                        newRowType,
                        getExtraDigests(abilitySpecs),
                        abilitySpecs.toArray(new SourceAbilitySpec[0]));
        final LogicalTableScan newScan =
                new LogicalTableScan(
                        scan.getCluster(), scan.getTraitSet(), scan.getHints(), newSource);

        final LogicalProject newProject =
                project.copy(
                        project.getTraitSet(),
                        newScan,
                        rewriteProjections(call, newSource, projectedSchema),
                        project.getRowType());
        if (ProjectRemoveRule.isTrivial(newProject)) {
            call.transformTo(newScan);
        } else {
            call.transformTo(newProject);
        }
    }

    private boolean supportsProjectionPushDown(DynamicTableSource tableSource) {
        return tableSource instanceof SupportsProjectionPushDown;
    }

    private boolean supportsMetadata(DynamicTableSource tableSource) {
        return tableSource instanceof SupportsReadingMetadata;
    }

    private boolean supportsNestedProjection(DynamicTableSource tableSource) {
        return supportsProjectionPushDown(tableSource)
                && ((SupportsProjectionPushDown) tableSource).supportsNestedProjection();
    }

    private List<RexNode> getProjections(LogicalProject project, LogicalTableScan scan) {
        final TableSourceTable source = scan.getTable().unwrap(TableSourceTable.class);
        final TableConfig tableConfig = unwrapContext(scan).getTableConfig();

        final List<RexNode> projections = new ArrayList<>(project.getProjects());
        if (supportsProjectionPushDown(source.tableSource())
                && requiresPrimaryKey(source, tableConfig)) {
            projections.addAll(getPrimaryKeyProjections(scan));
        }

        return projections;
    }

    private static boolean requiresPrimaryKey(TableSourceTable table, TableConfig config) {
        return DynamicSourceUtils.isUpsertSource(table.catalogTable(), table.tableSource())
                || DynamicSourceUtils.isSourceChangeEventsDuplicate(
                        table.catalogTable(), table.tableSource(), config);
    }

    private List<RexNode> getPrimaryKeyProjections(LogicalTableScan scan) {
        final TableSourceTable source = scan.getTable().unwrap(TableSourceTable.class);
        final ResolvedSchema schema = source.catalogTable().getResolvedSchema();
        if (!schema.getPrimaryKey().isPresent()) {
            return Collections.emptyList();
        }

        final FlinkTypeFactory typeFactory = unwrapTypeFactory(scan);
        final UniqueConstraint primaryKey = schema.getPrimaryKey().get();
        return primaryKey.getColumns().stream()
                .map(
                        columnName -> {
                            final int idx = scan.getRowType().getFieldNames().indexOf(columnName);
                            final Column column =
                                    schema.getColumn(idx)
                                            .orElseThrow(
                                                    () ->
                                                            new TableException(
                                                                    String.format(
                                                                            "Column at index %d not found.",
                                                                            idx)));
                            return new RexInputRef(
                                    idx,
                                    typeFactory.createFieldTypeFromLogicalType(
                                            column.getDataType().getLogicalType()));
                        })
                .collect(Collectors.toList());
    }

    private RowType performPushDown(
            TableSourceTable source,
            NestedSchema projectedSchema,
            RowType producedType,
            List<SourceAbilitySpec> abilitySpecs) {
        final int numPhysicalColumns;
        final List<NestedColumn> projectedMetadataColumns;
        if (supportsMetadata(source.tableSource())) {
            final List<String> declaredMetadataKeys =
                    createRequiredMetadataKeys(
                            source.catalogTable().getResolvedSchema(), source.tableSource());

            numPhysicalColumns = producedType.getFieldCount() - declaredMetadataKeys.size();

            projectedMetadataColumns =
                    IntStream.range(0, declaredMetadataKeys.size())
                            .mapToObj(i -> producedType.getFieldNames().get(numPhysicalColumns + i))
                            .map(fieldName -> projectedSchema.columns().get(fieldName))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
        } else {
            numPhysicalColumns = producedType.getFieldCount();
            projectedMetadataColumns = Collections.emptyList();
        }

        final int[][] physicalProjections;
        if (supportsProjectionPushDown(source.tableSource())) {
            projectedMetadataColumns.forEach(
                    metaColumn -> projectedSchema.columns().remove(metaColumn.name()));

            physicalProjections = NestedProjectionUtil.convertToIndexArray(projectedSchema);

            projectedMetadataColumns.forEach(
                    metaColumn -> projectedSchema.columns().put(metaColumn.name(), metaColumn));
        } else {
            physicalProjections =
                    IntStream.range(0, numPhysicalColumns)
                            .mapToObj(columnIndex -> new int[] {columnIndex})
                            .toArray(int[][]::new);
        }

        final int[][] projectedFields =
                Stream.concat(
                                Stream.of(physicalProjections),
                                projectedMetadataColumns.stream()
                                        .map(NestedColumn::indexInOriginSchema)
                                        .map(columnIndex -> new int[] {columnIndex}))
                        .toArray(int[][]::new);

        int newIndex = physicalProjections.length;
        for (NestedColumn metaColumn : projectedMetadataColumns) {
            metaColumn.setIndexOfLeafInNewSchema(newIndex++);
        }

        final RowType newProducedType =
                (RowType)
                        DataTypeUtils.projectRow(
                                        TypeConversions.fromLogicalToDataType(producedType),
                                        projectedFields)
                                .getLogicalType();

        if (supportsProjectionPushDown(source.tableSource())) {
            abilitySpecs.add(new ProjectPushDownSpec(physicalProjections, newProducedType));
        }

        if (supportsMetadata(source.tableSource())) {
            final List<String> projectedMetadataKeys =
                    projectedMetadataColumns.stream()
                            .map(NestedColumn::name)
                            .collect(Collectors.toList());

            abilitySpecs.add(new ReadingMetadataSpec(projectedMetadataKeys, newProducedType));
        }

        return newProducedType;
    }

    private List<RexNode> rewriteProjections(
            RelOptRuleCall call, TableSourceTable source, NestedSchema projectedSchema) {
        final LogicalProject project = call.rel(0);
        if (supportsProjectionPushDown(source.tableSource())) {
            return NestedProjectionUtil.rewrite(
                    project.getProjects(), projectedSchema, call.builder().getRexBuilder());
        } else {
            return project.getProjects();
        }
    }

    private static String[] getExtraDigests(List<SourceAbilitySpec> abilitySpecs) {
        final List<String> digests = new ArrayList<>();
        for (SourceAbilitySpec abilitySpec : abilitySpecs) {
            if (abilitySpec instanceof ProjectPushDownSpec) {
                digests.add(formatPushDownDigest((ProjectPushDownSpec) abilitySpec));
            } else if (abilitySpec instanceof ReadingMetadataSpec) {
                digests.add(formatMetadataDigest((ReadingMetadataSpec) abilitySpec));
            }
        }

        return digests.toArray(new String[0]);
    }

    private static String formatPushDownDigest(ProjectPushDownSpec pushDownSpec) {
        final List<String> fieldNames =
                pushDownSpec
                        .getProducedType()
                        .orElseThrow(() -> new TableException("Produced data type is not present."))
                        .getFieldNames();

        return String.format("project=[%s]", String.join(", ", fieldNames));
    }

    private static String formatMetadataDigest(ReadingMetadataSpec metadataSpec) {
        return String.format("metadata=[%s]", String.join(", ", metadataSpec.getMetadataKeys()));
    }

    // ---------------------------------------------------------------------------------------------

    /** Configuration for {@link PushProjectIntoTableSourceScanRule}. */
    public interface Config extends RelRule.Config {

        @Override
        default RelOptRule toRule() {
            return new PushProjectIntoTableSourceScanRule(this);
        }

        default Config onProjectedScan() {
            final RelRule.OperandTransform scanTransform =
                    operandBuilder -> operandBuilder.operand(LogicalTableScan.class).noInputs();

            final RelRule.OperandTransform projectTransform =
                    operandBuilder ->
                            operandBuilder.operand(LogicalProject.class).oneInput(scanTransform);

            return withOperandSupplier(projectTransform).as(Config.class);
        }
    }
}
