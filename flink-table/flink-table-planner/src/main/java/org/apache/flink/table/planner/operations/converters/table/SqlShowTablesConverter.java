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

package org.apache.flink.table.planner.operations.converters.table;

import org.apache.flink.sql.parser.dql.SqlShowTables;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowMaterializedTablesOperation;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.ShowViewsOperation;
import org.apache.flink.table.operations.utils.ShowLikeOperator;
import org.apache.flink.table.planner.operations.converters.AbstractSqlShowConverter;

import javax.annotation.Nullable;

public class SqlShowTablesConverter extends AbstractSqlShowConverter<SqlShowTables> {
    @Override
    public Operation getOperationWithoutPrep(
            SqlShowTables sqlShowCall,
            @Nullable String catalogName,
            @Nullable String databaseName,
            @Nullable ShowLikeOperator likeOp) {
        switch (sqlShowCall.getTableKind()) {
            case MATERIALIZED_TABLE:
                return new ShowMaterializedTablesOperation(catalogName, databaseName, likeOp);
            case TABLE:
                return new ShowTablesOperation(catalogName, databaseName, likeOp);
            case VIEW:
                return new ShowViewsOperation(catalogName, databaseName, likeOp);
            default:
                throw new ValidationException(
                        "Not supported table kind " + sqlShowCall.getTableKind() + " yet");
        }
    }

    @Override
    public Operation getOperation(
            SqlShowTables sqlShowCall,
            @Nullable String catalogName,
            @Nullable String databaseName,
            String prep,
            @Nullable ShowLikeOperator likeOp) {
        switch (sqlShowCall.getTableKind()) {
            case MATERIALIZED_TABLE:
                return new ShowMaterializedTablesOperation(catalogName, databaseName, prep, likeOp);
            case TABLE:
                return new ShowTablesOperation(catalogName, databaseName, prep, likeOp);
            case VIEW:
                return new ShowViewsOperation(catalogName, databaseName, prep, likeOp);
            default:
                throw new ValidationException(
                        "Not supported table kind " + sqlShowCall.getTableKind() + " yet");
        }
    }

    @Override
    public Operation convertSqlNode(SqlShowTables sqlShowTables, ConvertContext context) {
        return convertShowOperation(sqlShowTables, context);
    }
}
