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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;

import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlUtil;

import java.util.List;
import java.util.Map;

public class RichSqlInsertConverter implements SqlNodeConverter<RichSqlInsert> {
    @Override
    public Operation convertSqlNode(RichSqlInsert node, ConvertContext context) {
        FlinkPlannerImpl flinkPlanner = context.getFlinkPlanner();
        CatalogManager catalogManager = context.getCatalogManager();
        // Get sink table name.
        List<String> targetTablePath = ((SqlIdentifier) node.getTargetTableID()).names;
        // Get sink table hints.
        HintStrategyTable hintStrategyTable =
                flinkPlanner.config().getSqlToRelConverterConfig().getHintStrategyTable();
        List<RelHint> tableHints = SqlUtil.getRelHint(hintStrategyTable, node.getTableHints());
        Map<String, String> dynamicOptions = FlinkHints.getHintedOptions(tableHints);

        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(targetTablePath);
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        // If it is materialized table, convert it to catalog table for query optimize
        ContextResolvedTable contextResolvedTable =
                catalogManager.getTableOrError(identifier).toCatalogTable();

        PlannerQueryOperation query =
                (PlannerQueryOperation)
                        SqlNodeToOperationConversion.convertValidatedSqlNodeOrFail(
                                flinkPlanner, catalogManager, node.getSource());
        // TODO calc target column list to index array, currently only simple SqlIdentifiers are
        // available, this should be updated after FLINK-31301 fixed
        int[][] columnIndices =
                SqlNodeConvertUtils.getTargetColumnIndices(
                        contextResolvedTable, node.getTargetColumnList());

        return new SinkModifyOperation(
                contextResolvedTable,
                query,
                node.getStaticPartitionKVs(),
                columnIndices,
                node.isOverwrite(),
                dynamicOptions);
    }
}
