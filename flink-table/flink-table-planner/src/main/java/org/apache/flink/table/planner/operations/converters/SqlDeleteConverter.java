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
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.operations.DeleteFromFilterOperation;
import org.apache.flink.table.operations.ModifyType;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.operations.DeletePushDownUtils;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.utils.RowLevelModificationContextUtils;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.sql.SqlDelete;

import java.util.List;
import java.util.Optional;

public class SqlDeleteConverter implements SqlNodeConverter<SqlDelete> {
    @Override
    public Operation convertSqlNode(SqlDelete node, ConvertContext context) {
        // set it's delete
        RowLevelModificationContextUtils.setModificationType(
                SupportsRowLevelModificationScan.RowLevelModificationType.DELETE);
        RelRoot deleteRelational = context.getFlinkPlanner().rel(node);
        LogicalTableModify tableModify = (LogicalTableModify) deleteRelational.rel;
        UnresolvedIdentifier unresolvedTableIdentifier =
                UnresolvedIdentifier.of(tableModify.getTable().getQualifiedName());
        ContextResolvedTable contextResolvedTable =
                context.getCatalogManager()
                        .getTableOrError(
                                context.getCatalogManager()
                                        .qualifyIdentifier(unresolvedTableIdentifier));
        // try push down delete
        Optional<DynamicTableSink> optionalDynamicTableSink =
                DeletePushDownUtils.getDynamicTableSink(contextResolvedTable, tableModify);
        if (optionalDynamicTableSink.isPresent()) {
            DynamicTableSink dynamicTableSink = optionalDynamicTableSink.get();
            // if the table sink supports delete push down
            if (dynamicTableSink instanceof SupportsDeletePushDown) {
                SupportsDeletePushDown supportsDeletePushDownSink =
                        (SupportsDeletePushDown) dynamicTableSink;
                // get resolved filter expression
                Optional<List<ResolvedExpression>> filters =
                        DeletePushDownUtils.getResolvedFilterExpressions(tableModify);
                if (filters.isPresent()
                        && supportsDeletePushDownSink.applyDeleteFilters(filters.get())) {
                    return new DeleteFromFilterOperation(
                            contextResolvedTable, supportsDeletePushDownSink, filters.get());
                }
            }
        }
        // delete push down is not applicable, use row-level delete
        PlannerQueryOperation queryOperation =
                new PlannerQueryOperation(
                        tableModify,
                        () -> {
                            throw new TableException("Delete statements are not SQL serializable.");
                        });
        return new SinkModifyOperation(
                contextResolvedTable,
                queryOperation,
                null, // targetColumns
                ModifyType.DELETE);
    }
}
