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

import org.apache.flink.sql.parser.dql.SqlShowModels;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowModelsOperation;
import org.apache.flink.table.operations.utils.ShowLikeOperator;

import javax.annotation.Nullable;

/** A converter for {@link SqlShowModels}. */
public class SqlShowModelsConverter extends AbstractSqlShowConverter<SqlShowModels> {
    @Override
    public Operation getOperationWithoutPrep(
            SqlShowModels sqlShowCall,
            @Nullable String catalogName,
            @Nullable String databaseName,
            @Nullable ShowLikeOperator likeOp) {
        return new ShowModelsOperation(catalogName, databaseName, likeOp);
    }

    @Override
    public Operation getOperation(
            SqlShowModels sqlShowCall,
            @Nullable String catalogName,
            @Nullable String databaseName,
            String prep,
            @Nullable ShowLikeOperator likeOp) {
        return new ShowModelsOperation(catalogName, databaseName, prep, likeOp);
    }

    @Override
    public Operation convertSqlNode(SqlShowModels sqlShowModels, ConvertContext context) {
        return convertShowOperation(sqlShowModels, context);
    }
}
