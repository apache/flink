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
import org.apache.flink.table.connector.Projection;
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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.connectors.DynamicSourceUtils.createProducedType;
import static org.apache.flink.table.planner.connectors.DynamicSourceUtils.createRequiredMetadataColumns;
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
@Value.Enclosing
public class PushProjectIntoTableSourceScanRule
        extends RelRule<PushProjectIntoTableSourceScanRule.Config> {

    public static final PushProjectIntoTableSourceScanRule INSTANCE =
            new PushProjectIntoTableSourceScanRule(
                    PushProjectIntoTableSourceScanRule.Config.DEFAULT);

    public PushProjectIntoTableSourceScanRule(PushProjectIntoTableSourceScanRule.Config config) {
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
        final ResolvedSchema schema = sourceTable.contextResolvedTable().getResolvedSchema();
        final RowType producedType = createProducedType(schema, sourceTable.tableSource());
        NestedSchema projectedSchema =
                NestedProjectionUtil.build(
                        getProjections(project, scan),
                        typeFactory.buildRelNodeRowType(producedType));
        // we can not perform an empty column query to the table scan, just choose the first column
        // in such case
        if (projectedSchema.columns().isEmpty()) {
            if (scan.getRowType().getFieldCount() == 0) {
                throw new TableException(
                        "Unexpected empty row type of source table:"
                                + String.join(".", scan.getTable().getQualifiedName()));
            }
            RexInputRef firstFieldRef = RexInputRef.of(0, scan.getRowType());
            projectedSchema =
                    NestedProjectionUtil.build(
                            Collections.singletonList(firstFieldRef),
                            typeFactory.buildRelNodeRowType(producedType));
        }

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
                        newTableSource, newRowType, abilitySpecs.toArray(new SourceAbilitySpec[0]));
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

    private static boolean requiresPrimaryKey(TableSourceTable table, TableConfig tableConfig) {
        return DynamicSourceUtils.isUpsertSource(
                        table.contextResolvedTable().getResolvedSchema(), table.tableSource())
                || DynamicSourceUtils.isSourceChangeEventsDuplicate(
                        table.contextResolvedTable().getResolvedSchema(),
                        table.tableSource(),
                        tableConfig);
    }

    private List<RexNode> getPrimaryKeyProjections(LogicalTableScan scan) {
        final TableSourceTable source = scan.getTable().unwrap(TableSourceTable.class);
        final ResolvedSchema schema = source.contextResolvedTable().getResolvedSchema();
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
                    createRequiredMetadataColumns(
                                    source.contextResolvedTable().getResolvedSchema(),
                                    source.tableSource())
                            .stream()
                            .map(col -> col.getMetadataKey().orElse(col.getName()))
                            .collect(Collectors.toList());

            numPhysicalColumns = producedType.getFieldCount() - declaredMetadataKeys.size();

            // the projected metadata column name
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

        if (supportsProjectionPushDown(source.tableSource())) {
            final RowType projectedPhysicalType =
                    (RowType) Projection.of(physicalProjections).project(producedType);
            abilitySpecs.add(new ProjectPushDownSpec(physicalProjections, projectedPhysicalType));
        }

        final RowType newProducedType =
                (RowType) Projection.of(projectedFields).project(producedType);

        if (supportsMetadata(source.tableSource())) {
            // Use the projected column name to get the metadata key
            final List<String> projectedMetadataKeys =
                    projectedMetadataColumns.stream()
                            .map(
                                    nestedColumn ->
                                            source.contextResolvedTable()
                                                    .getResolvedSchema()
                                                    .getColumn(nestedColumn.name())
                                                    .orElseThrow(
                                                            () ->
                                                                    new TableException(
                                                                            String.format(
                                                                                    "Can not find the column %s in the origin schema.",
                                                                                    nestedColumn
                                                                                            .name()))))
                            .map(Column.MetadataColumn.class::cast)
                            .map(col -> col.getMetadataKey().orElse(col.getName()))
                            .collect(Collectors.toList());

            abilitySpecs.add(new ReadingMetadataSpec(projectedMetadataKeys, newProducedType));
        }

        return newProducedType;
    }

    private List<RexNode> rewriteProjections(
            RelOptRuleCall call, TableSourceTable source, NestedSchema projectedSchema) {
        final LogicalProject project = call.rel(0);
        List<RexNode> newProjects = project.getProjects();

        if (supportsProjectionPushDown(source.tableSource())) {
            // if support project push down, then all input ref will be rewritten includes metadata
            // columns.
            newProjects =
                    NestedProjectionUtil.rewrite(
                            newProjects, projectedSchema, call.builder().getRexBuilder());
        } else if (supportsMetadata(source.tableSource())) {
            // supportsMetadataProjection only.
            // Note: why not reuse the NestedProjectionUtil to rewrite metadata projection? because
            // it only works for sources which support projection push down.
            List<Column.MetadataColumn> metadataColumns =
                    DynamicSourceUtils.extractMetadataColumns(
                            source.contextResolvedTable().getResolvedSchema());
            if (metadataColumns.size() > 0) {
                Set<String> metaCols =
                        metadataColumns.stream().map(Column::getName).collect(Collectors.toSet());

                MetadataOnlyProjectionRewriter rewriter =
                        new MetadataOnlyProjectionRewriter(
                                project.getInput().getRowType(), source.getRowType(), metaCols);

                newProjects =
                        newProjects.stream()
                                .map(p -> p.accept(rewriter))
                                .collect(Collectors.toList());
            }
        }

        return newProjects;
    }

    private static class MetadataOnlyProjectionRewriter extends RexShuttle {

        private final RelDataType oldInputRowType;

        private final RelDataType newInputRowType;

        private final Set<String> metaCols;

        public MetadataOnlyProjectionRewriter(
                RelDataType oldInputRowType, RelDataType newInputRowType, Set<String> metaCols) {
            this.oldInputRowType = oldInputRowType;
            this.newInputRowType = newInputRowType;
            this.metaCols = metaCols;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            int refIndex = inputRef.getIndex();
            if (refIndex > oldInputRowType.getFieldCount() - 1) {
                throw new TableException(
                        "Illegal field ref:" + refIndex + " over input row:" + oldInputRowType);
            }
            String refName = oldInputRowType.getFieldNames().get(refIndex);
            if (metaCols.contains(refName)) {
                int newIndex = newInputRowType.getFieldNames().indexOf(refName);
                if (newIndex == -1) {
                    throw new TableException(
                            "Illegal meta field:" + refName + " over input row:" + newInputRowType);
                }
                return new RexInputRef(newIndex, inputRef.getType());
            }
            return inputRef;
        }
    }

    // ---------------------------------------------------------------------------------------------

    /** Configuration for {@link PushProjectIntoTableSourceScanRule}. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutablePushProjectIntoTableSourceScanRule.Config.builder()
                        .build()
                        .onProjectedScan()
                        .as(Config.class);

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
