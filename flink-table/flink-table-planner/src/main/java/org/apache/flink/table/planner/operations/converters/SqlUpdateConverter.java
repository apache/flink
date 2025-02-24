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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;
import org.apache.flink.table.operations.ModifyType;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.utils.RowLevelModificationContextUtils;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.sql.SqlUpdate;

public class SqlUpdateConverter implements SqlNodeConverter<SqlUpdate> {
    @Override
    public Operation convertSqlNode(SqlUpdate node, ConvertContext context) {
        FlinkPlannerImpl flinkPlanner = context.getFlinkPlanner();
        CatalogManager catalogManager = context.getCatalogManager();
        // set its update
        RowLevelModificationContextUtils.setModificationType(
                SupportsRowLevelModificationScan.RowLevelModificationType.UPDATE);
        RelRoot updateRelational = flinkPlanner.rel(node);
        // get target sink table
        LogicalTableModify tableModify = (LogicalTableModify) updateRelational.rel;
        UnresolvedIdentifier unresolvedTableIdentifier =
                UnresolvedIdentifier.of(tableModify.getTable().getQualifiedName());
        ContextResolvedTable contextResolvedTable =
                catalogManager.getTableOrError(
                        catalogManager.qualifyIdentifier(unresolvedTableIdentifier));
        // get query
        PlannerQueryOperation queryOperation =
                new PlannerQueryOperation(
                        tableModify,
                        () -> {
                            throw new TableException("Update statements are not SQL serializable.");
                        });

        // TODO calc target column list to index array, currently only simple SqlIdentifiers are
        // available, this should be updated after FLINK-31344 fixed
        int[][] columnIndices =
                SqlNodeConvertUtils.getTargetColumnIndices(
                        contextResolvedTable, node.getTargetColumnList());

        return new SinkModifyOperation(
                contextResolvedTable, queryOperation, columnIndices, ModifyType.UPDATE);
    }
}
