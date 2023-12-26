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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelDeleteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelUpdateSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Batch {@link ExecNode} to write data into an external sink defined by a {@link DynamicTableSink}.
 */
public class BatchExecSink extends CommonExecSink implements BatchExecNode<Object> {
    public BatchExecSink(
            ReadableConfig tableConfig,
            DynamicTableSinkSpec tableSinkSpec,
            InputProperty inputProperty,
            LogicalType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecSink.class),
                ExecNodeContext.newPersistedConfig(BatchExecSink.class, tableConfig),
                tableSinkSpec,
                ChangelogMode.insertOnly(),
                true, // isBounded
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<Object> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        final DynamicTableSink tableSink = tableSinkSpec.getTableSink(planner.getFlinkContext());
        return createSinkTransformation(
                planner.getExecEnv(),
                config,
                planner.getFlinkContext().getClassLoader(),
                inputTransform,
                tableSink,
                -1,
                false,
                null);
    }

    @Override
    protected RowType getPhysicalRowType(ResolvedSchema schema) {
        // row-level modification may only write partial columns,
        // so we try to prune the RowType to get the real RowType containing
        // the physical columns to be written
        if (tableSinkSpec.getSinkAbilities() != null) {
            for (SinkAbilitySpec sinkAbilitySpec : tableSinkSpec.getSinkAbilities()) {
                if (sinkAbilitySpec instanceof RowLevelUpdateSpec) {
                    RowLevelUpdateSpec rowLevelUpdateSpec = (RowLevelUpdateSpec) sinkAbilitySpec;
                    return getPhysicalRowType(
                            schema, rowLevelUpdateSpec.getRequiredPhysicalColumnIndices());
                } else if (sinkAbilitySpec instanceof RowLevelDeleteSpec) {
                    RowLevelDeleteSpec rowLevelDeleteSpec = (RowLevelDeleteSpec) sinkAbilitySpec;
                    return getPhysicalRowType(
                            schema, rowLevelDeleteSpec.getRequiredPhysicalColumnIndices());
                }
            }
        }
        return (RowType) schema.toPhysicalRowDataType().getLogicalType();
    }

    @Override
    protected Transformation<RowData> applyUpsertMaterialize(
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            ExecNodeConfig config,
            ClassLoader classLoader,
            RowType physicalRowType,
            int[] inputUpsertKey) {
        return inputTransform;
    }

    @Override
    protected int[] getPrimaryKeyIndices(RowType sinkRowType, ResolvedSchema schema) {
        if (schema.getPrimaryKey().isPresent()) {
            UniqueConstraint uniqueConstraint = schema.getPrimaryKey().get();
            int[] primaryKeyIndices = new int[uniqueConstraint.getColumns().size()];
            // SinkRowType may not contain full primary keys in case of row-level update or delete.
            // In such case, return an empty array since the primary keys are not completed and
            // we consider such case as no primary keys
            // Note: this may happen if the required columns returned by
            // connector don't fully contain the primary keys. But it's not recommended to only
            // return partial primary keys
            // For example, a table has primary keys: a, b, c; but the connector only return a, b
            // in method SupportsRowLevelUpdate#applyRowLevelUpdate.
            for (int i = 0; i < uniqueConstraint.getColumns().size(); i++) {
                int fieldIndex = sinkRowType.getFieldIndex(uniqueConstraint.getColumns().get(i));
                if (fieldIndex == -1) {
                    return new int[0];
                }
                primaryKeyIndices[i] = fieldIndex;
            }
            return primaryKeyIndices;
        } else {
            return new int[0];
        }
    }

    /** Get the physical row type with given column indices. */
    private RowType getPhysicalRowType(ResolvedSchema schema, int[] columnIndices) {
        List<Column> columns = schema.getColumns();
        List<Column> requireColumns = new ArrayList<>();
        for (int columnIndex : columnIndices) {
            requireColumns.add(columns.get(columnIndex));
        }
        return (RowType) ResolvedSchema.of(requireColumns).toPhysicalRowDataType().getLogicalType();
    }
}
