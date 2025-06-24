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

import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilityContext;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceWatermarkSpec;
import org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.watermark.WatermarkParams;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.factories.FactoryUtil.SOURCE_IDLE_TIMEOUT;
import static org.apache.flink.table.factories.FactoryUtil.WATERMARK_ALIGNMENT_GROUP;
import static org.apache.flink.table.factories.FactoryUtil.WATERMARK_ALIGNMENT_MAX_DRIFT;
import static org.apache.flink.table.factories.FactoryUtil.WATERMARK_ALIGNMENT_UPDATE_INTERVAL;
import static org.apache.flink.table.factories.FactoryUtil.WATERMARK_EMIT_STRATEGY;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapFunctionDefinition;

/**
 * Base rule for interface {@link SupportsWatermarkPushDown} and {@link SupportsSourceWatermark}. It
 * offers a util to push the {@link FlinkLogicalWatermarkAssigner} into the {@link
 * FlinkLogicalTableSourceScan}.
 */
public abstract class PushWatermarkIntoTableSourceScanRuleBase extends RelOptRule {

    public PushWatermarkIntoTableSourceScanRuleBase(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    /**
     * It uses the input watermark expression to generate the {@link WatermarkGeneratorSupplier}.
     * After the {@link WatermarkStrategy} is pushed into the scan, it will build a new scan.
     * However, when {@link FlinkLogicalWatermarkAssigner} is the parent of the {@link
     * FlinkLogicalTableSourceScan} it should modify the rowtime type to keep the type of plan is
     * consistent. In other cases, it just keep the data type of the scan as same as before and
     * leave the work when rewriting the projection.
     *
     * <p>NOTES: the row type of the scan is not always as same as the watermark assigner. Because
     * the scan will not add the rowtime column into the row when pushing the watermark assigner
     * into the scan. In some cases, query may have computed columns defined on rowtime column. If
     * modifying the type of the rowtime(with time attribute), it will also influence the type of
     * the computed column. Therefore, if the watermark assigner is not the parent of the scan, set
     * the type of the scan as before and leave the work to projection.
     */
    protected FlinkLogicalTableSourceScan getNewScan(
            FlinkLogicalWatermarkAssigner watermarkAssigner,
            RexNode watermarkExpr,
            FlinkLogicalTableSourceScan scan,
            TableConfig tableConfig,
            boolean useWatermarkAssignerRowType) {
        final TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
        final DynamicTableSource newDynamicTableSource = tableSourceTable.tableSource().copy();

        final boolean isSourceWatermark =
                newDynamicTableSource instanceof SupportsSourceWatermark
                        && hasSourceWatermarkDeclaration(watermarkExpr);

        final RelDataType newType;
        if (useWatermarkAssignerRowType) {
            // project is trivial and set rowtime type in scan
            newType = watermarkAssigner.getRowType();
        } else {
            // project add/delete columns and set the rowtime column type in project
            newType = scan.getRowType();
        }

        final RowType producedType = (RowType) FlinkTypeFactory.toLogicalType(newType);
        final SourceAbilityContext abilityContext = SourceAbilityContext.from(scan);

        final SourceAbilitySpec abilitySpec;
        if (isSourceWatermark) {
            final SourceWatermarkSpec sourceWatermarkSpec =
                    new SourceWatermarkSpec(true, producedType);
            sourceWatermarkSpec.apply(newDynamicTableSource, abilityContext);
            abilitySpec = sourceWatermarkSpec;
        } else {
            final Duration globalIdleTimeout =
                    tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT);
            final long globalIdleTimeoutMillis;
            if (!globalIdleTimeout.isZero() && !globalIdleTimeout.isNegative()) {
                globalIdleTimeoutMillis = globalIdleTimeout.toMillis();
            } else {
                globalIdleTimeoutMillis = -1L;
            }

            Optional<RelHint> optionsHintOptional =
                    scan.getHints().stream()
                            .filter(
                                    relHint ->
                                            relHint.hintName.equalsIgnoreCase(
                                                    FlinkHints.HINT_NAME_OPTIONS))
                            .findFirst();
            Configuration hintOptions =
                    optionsHintOptional
                            .map(relHint -> Configuration.fromMap(relHint.kvOptions))
                            .orElseGet(Configuration::new);
            RelOptTable table = scan.getTable();
            Configuration tableOptions =
                    Optional.of(table)
                            .filter(TableSourceTable.class::isInstance)
                            .map(
                                    t -> {
                                        Map<String, String> tableConfigs =
                                                ((TableSourceTable) t)
                                                        .contextResolvedTable()
                                                        .getResolvedTable()
                                                        .getOptions();
                                        return Configuration.fromMap(tableConfigs);
                                    })
                            .orElseGet(Configuration::new);
            WatermarkParams watermarkParams = parseWatermarkParams(hintOptions, tableOptions);

            final WatermarkPushDownSpec watermarkPushDownSpec =
                    new WatermarkPushDownSpec(
                            watermarkExpr, globalIdleTimeoutMillis, producedType, watermarkParams);
            watermarkPushDownSpec.apply(newDynamicTableSource, abilityContext);
            abilitySpec = watermarkPushDownSpec;
        }

        TableSourceTable newTableSourceTable =
                tableSourceTable.copy(
                        newDynamicTableSource, newType, new SourceAbilitySpec[] {abilitySpec});
        return FlinkLogicalTableSourceScan.create(
                scan.getCluster(), scan.getHints(), newTableSourceTable);
    }

    protected boolean supportsWatermarkPushDown(FlinkLogicalTableSourceScan scan) {
        TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
        if (tableSourceTable == null) {
            return false;
        }
        final DynamicTableSource tableSource = tableSourceTable.tableSource();
        return (tableSource instanceof SupportsWatermarkPushDown)
                || (tableSource instanceof SupportsSourceWatermark
                        && hasSourceWatermarkDeclaration(tableSourceTable));
    }

    private boolean hasSourceWatermarkDeclaration(TableSourceTable table) {
        final ResolvedSchema schema = table.contextResolvedTable().getResolvedSchema();
        final List<WatermarkSpec> specs = schema.getWatermarkSpecs();
        // we only support one watermark spec for now
        if (specs.size() != 1) {
            return false;
        }
        final ResolvedExpression watermarkExpr = specs.get(0).getWatermarkExpression();
        final FunctionDefinition function = unwrapFunctionDefinition(watermarkExpr);
        return function == BuiltInFunctionDefinitions.SOURCE_WATERMARK;
    }

    private boolean hasSourceWatermarkDeclaration(RexNode rexNode) {
        final FunctionDefinition function = unwrapFunctionDefinition(rexNode);
        return function == BuiltInFunctionDefinitions.SOURCE_WATERMARK;
    }

    private WatermarkParams parseWatermarkParams(
            Configuration hintOptions, Configuration tableOptions) {
        WatermarkParams.WatermarkParamsBuilder builder = WatermarkParams.builder();
        getOptions(WATERMARK_EMIT_STRATEGY, hintOptions, tableOptions)
                .ifPresent(builder::emitStrategy);
        getOptions(WATERMARK_ALIGNMENT_GROUP, hintOptions, tableOptions)
                .ifPresent(builder::alignGroupName);
        getOptions(WATERMARK_ALIGNMENT_MAX_DRIFT, hintOptions, tableOptions)
                .ifPresent(builder::alignMaxDrift);
        getOptions(WATERMARK_ALIGNMENT_UPDATE_INTERVAL, hintOptions, tableOptions)
                .ifPresent(builder::alignUpdateInterval);
        getOptions(SOURCE_IDLE_TIMEOUT, hintOptions, tableOptions)
                .ifPresent(timeout -> builder.sourceIdleTimeout(timeout.toMillis()));
        return builder.build();
    }

    private <T> Optional<T> getOptions(
            ConfigOption<T> option, Configuration priorityOptions, Configuration secondOptions) {
        Optional<T> result = priorityOptions.getOptional(option);
        return result.isPresent() ? result : secondOptions.getOptional(option);
    }
}
