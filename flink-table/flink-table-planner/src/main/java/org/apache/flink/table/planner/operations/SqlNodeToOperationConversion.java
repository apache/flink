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

package org.apache.flink.table.planner.operations;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverters;
import org.apache.flink.table.planner.utils.RowLevelModificationContextUtils;

import org.apache.calcite.sql.SqlNode;

import java.util.Optional;

/**
 * Mix-in tool class for {@code SqlNode} that allows DDL commands to be converted to {@link
 * Operation}.
 *
 * <p>For every kind of {@link SqlNode}, there needs to have a corresponding #convert(type) method,
 * the 'type' argument should be the subclass of the supported {@link SqlNode}.
 *
 * <p>Every #convert() should return a {@link Operation} which can be used in {@link
 * org.apache.flink.table.delegation.Planner}.
 */
public class SqlNodeToOperationConversion {
    private final FlinkPlannerImpl flinkPlanner;
    private final CatalogManager catalogManager;

    // ~ Constructors -----------------------------------------------------------

    private SqlNodeToOperationConversion(
            FlinkPlannerImpl flinkPlanner, CatalogManager catalogManager) {
        this.flinkPlanner = flinkPlanner;
        this.catalogManager = catalogManager;
    }

    /**
     * This is the main entrance for executing all kinds of DDL/DML {@code SqlNode}s, different
     * SqlNode will have its implementation in the #convert(type) method whose 'type' argument is
     * subclass of {@code SqlNode}.
     *
     * @param flinkPlanner FlinkPlannerImpl to convertCreateTable sql node to rel node
     * @param catalogManager CatalogManager to resolve full path for operations
     * @param sqlNode SqlNode to execute on
     */
    public static Optional<Operation> convert(
            FlinkPlannerImpl flinkPlanner, CatalogManager catalogManager, SqlNode sqlNode) {
        // validate the query
        final SqlNode validated = flinkPlanner.validate(sqlNode);
        return convertValidatedSqlNode(flinkPlanner, catalogManager, validated);
    }

    /** Convert a validated sql node to Operation. */
    private static Optional<Operation> convertValidatedSqlNode(
            FlinkPlannerImpl flinkPlanner, CatalogManager catalogManager, SqlNode validated) {
        beforeConversion();

        // delegate conversion to the registered converters
        SqlNodeConvertContext context = new SqlNodeConvertContext(flinkPlanner, catalogManager);
        Optional<Operation> operation = SqlNodeConverters.convertSqlNode(validated, context);
        if (operation.isPresent()) {
            return operation;
        }

        return Optional.empty();
    }

    public static Operation convertValidatedSqlNodeOrFail(
            FlinkPlannerImpl flinkPlanner, CatalogManager catalogManager, SqlNode validated) {
        return convertValidatedSqlNode(flinkPlanner, catalogManager, validated)
                .orElseThrow(
                        () ->
                                new TableException(
                                        "Unsupported node type "
                                                + validated.getClass().getSimpleName()));
    }

    // ~ Tools ------------------------------------------------------------------

    private static void beforeConversion() {
        // clear row-level modification context
        RowLevelModificationContextUtils.clearContext();
    }
}
