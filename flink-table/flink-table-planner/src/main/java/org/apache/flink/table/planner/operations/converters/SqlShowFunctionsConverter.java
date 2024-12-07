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

import org.apache.flink.sql.parser.dql.SqlShowFunctions;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowFunctionsOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation.FunctionScope;
import org.apache.flink.table.operations.utils.ShowLikeOperator;

import javax.annotation.Nullable;

/** A converter for {@link SqlShowFunctions}. */
public class SqlShowFunctionsConverter extends AbstractSqlShowConverter<SqlShowFunctions> {

    @Override
    public Operation getOperationWithoutPrep(
            SqlShowFunctions sqlShowFunctions,
            @Nullable String catalogName,
            @Nullable String databaseName,
            @Nullable ShowLikeOperator likeOp) {
        final FunctionScope functionScope = getFunctionScope(sqlShowFunctions);
        return new ShowFunctionsOperation(functionScope, catalogName, databaseName, likeOp);
    }

    @Override
    public Operation getOperation(
            SqlShowFunctions sqlShowFunctions,
            @Nullable String catalogName,
            @Nullable String databaseName,
            String prep,
            @Nullable ShowLikeOperator likeOp) {
        final FunctionScope functionScope = getFunctionScope(sqlShowFunctions);
        return new ShowFunctionsOperation(functionScope, prep, catalogName, databaseName, likeOp);
    }

    @Override
    public Operation convertSqlNode(SqlShowFunctions sqlShowFunctions, ConvertContext context) {
        return convertShowOperation(sqlShowFunctions, context);
    }

    private static FunctionScope getFunctionScope(SqlShowFunctions sqlShowFunctions) {
        return sqlShowFunctions.requireUser() ? FunctionScope.USER : FunctionScope.ALL;
    }
}
