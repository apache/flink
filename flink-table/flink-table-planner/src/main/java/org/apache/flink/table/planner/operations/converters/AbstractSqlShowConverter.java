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

import org.apache.flink.sql.parser.dql.SqlShowCall;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.utils.LikeType;
import org.apache.flink.table.operations.utils.ShowLikeOperator;

import java.util.List;
import java.util.function.Function;

public abstract class AbstractSqlShowConverter<T extends SqlShowCall>
        implements SqlNodeConverter<T> {

    protected Operation convertShowOperation(
            T sqlShowCall, Function<List<String>, String> msg, ConvertContext context) {
        final ShowLikeOperator likeOp = getLikeOp(sqlShowCall);
        if (sqlShowCall.getPreposition() == null) {
            return getOperationWithoutPrep(sqlShowCall, likeOp);
        }
        List<String> fullDatabaseName = sqlShowCall.getSqlIdentifierNameList();
        if (fullDatabaseName.size() > 2) {
            throw new ValidationException(msg.apply(fullDatabaseName));
        }
        CatalogManager catalogManager = context.getCatalogManager();
        String catalogName =
                fullDatabaseName.size() == 1
                        ? catalogManager.getCurrentCatalog()
                        : fullDatabaseName.get(0);

        String databaseName =
                fullDatabaseName.size() == 1 ? fullDatabaseName.get(0) : fullDatabaseName.get(1);
        return getOperation(
                sqlShowCall, catalogName, databaseName, sqlShowCall.getPreposition(), likeOp);
    }

    public ShowLikeOperator getLikeOp(SqlShowCall sqlShowCall) {
        return ShowLikeOperator.of(
                LikeType.of(sqlShowCall.getLikeType(), sqlShowCall.isNotLike()),
                sqlShowCall.getLikeSqlPattern());
    }

    public abstract Operation getOperationWithoutPrep(T sqlShowCall, ShowLikeOperator likeOp);

    public abstract Operation getOperation(
            T sqlShowCall,
            String catalogName,
            String databaseName,
            String prep,
            ShowLikeOperator likeOp);

    @Override
    public abstract Operation convertSqlNode(T node, ConvertContext context);
}
