/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.runtime.types.PlannerTypeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a wrapper for {@link ContextResolvedModel} in {@link
 * org.apache.calcite.schema.Schema}.
 */
public class CatalogSchemaModel {
    // ~ Instance fields --------------------------------------------------------

    private final ContextResolvedModel contextResolvedModel;
    private final FlinkStatistic statistic;
    private final boolean isStreamingMode;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Create a CatalogSchemaModel instance.
     *
     * @param contextResolvedModel A result of catalog lookup
     * @param statistic Model statistics
     * @param isStreaming If the model is for streaming mode
     */
    public CatalogSchemaModel(
            ContextResolvedModel contextResolvedModel,
            FlinkStatistic statistic,
            boolean isStreaming) {
        this.contextResolvedModel = contextResolvedModel;
        this.statistic = statistic;
        this.isStreamingMode = isStreaming;
    }

    // ~ Methods ----------------------------------------------------------------

    public ContextResolvedModel getContextResolvedModel() {
        return contextResolvedModel;
    }

    public boolean isTemporary() {
        return contextResolvedModel.isTemporary();
    }

    public boolean isStreamingMode() {
        return isStreamingMode;
    }

    public RelDataType getInputRowType(RelDataTypeFactory typeFactory) {
        final FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) typeFactory;
        final ResolvedSchema schema =
                contextResolvedModel.getResolvedModel().getResolvedInputSchema();

        final List<String> fieldNames = schema.getColumnNames();
        final List<LogicalType> fieldTypes =
                schema.getColumnDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .map(PlannerTypeUtils::removeLegacyTypes)
                        .collect(Collectors.toList());
        return flinkTypeFactory.buildRelNodeRowType(fieldNames, fieldTypes);
    }

    public RelDataType getOutputRowType(RelDataTypeFactory typeFactory) {
        final FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) typeFactory;
        final ResolvedSchema schema =
                contextResolvedModel.getResolvedModel().getResolvedOutputSchema();

        final List<String> fieldNames = schema.getColumnNames();
        final List<LogicalType> fieldTypes =
                schema.getColumnDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .map(PlannerTypeUtils::removeLegacyTypes)
                        .collect(Collectors.toList());
        return flinkTypeFactory.buildRelNodeRowType(fieldNames, fieldTypes);
    }

    public FlinkStatistic getStatistic() {
        return statistic;
    }
}
