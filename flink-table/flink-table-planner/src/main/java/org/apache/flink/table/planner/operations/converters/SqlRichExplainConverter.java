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

import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.sql.parser.ddl.SqlReplaceTableAs;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dml.SqlStatementSet;
import org.apache.flink.sql.parser.dql.SqlRichExplain;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

public class SqlRichExplainConverter implements SqlNodeConverter<SqlRichExplain> {
    @Override
    public Operation convertSqlNode(SqlRichExplain node, ConvertContext context) {
        FlinkPlannerImpl flinkPlanner = context.getFlinkPlanner();

        SqlNode sqlNode = node.getStatement();
        Operation operation;
        if (sqlNode instanceof RichSqlInsert || sqlNode instanceof SqlStatementSet) {
            operation = SqlNodeConverters.convertSqlNode(sqlNode, context).get();
        } else if (sqlNode.getKind().belongsTo(SqlKind.QUERY)) {
            operation = convertSqlQuery(node.getStatement(), flinkPlanner);
        } else if ((sqlNode instanceof SqlCreateTableAs)
                || (sqlNode instanceof SqlReplaceTableAs)) {
            operation =
                    SqlNodeConverters.convertSqlNode(sqlNode, context)
                            .orElseThrow(
                                    () ->
                                            new ValidationException(
                                                    String.format(
                                                            "EXPLAIN statement doesn't support %s",
                                                            sqlNode.getKind())));
        } else {
            throw new ValidationException(
                    String.format("EXPLAIN statement doesn't support %s", sqlNode.getKind()));
        }
        return new ExplainOperation(operation, node.getExplainDetails());
    }

    /** Fallback method for sql query. */
    private Operation convertSqlQuery(SqlNode node, FlinkPlannerImpl flinkPlanner) {
        return toQueryOperation(flinkPlanner, node);
    }

    private PlannerQueryOperation toQueryOperation(FlinkPlannerImpl planner, SqlNode validated) {
        // transform to a relational tree
        RelRoot relational = planner.rel(validated);
        return new PlannerQueryOperation(
                relational.project(),
                () -> SqlNodeConvertUtils.getQuotedSqlString(validated, planner));
    }
}
