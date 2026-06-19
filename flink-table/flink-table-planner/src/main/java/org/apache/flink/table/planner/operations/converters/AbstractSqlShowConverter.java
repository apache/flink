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

import javax.annotation.Nullable;

import java.util.List;

/** An abstract class for SHOW converters. */
public abstract class AbstractSqlShowConverter<T extends SqlShowCall>
        implements SqlNodeConverter<T> {

    protected Operation convertShowOperation(T sqlShowCall, ConvertContext context) {
        final ShowLikeOperator likeOp = getLikeOp(sqlShowCall);
        if (sqlShowCall.getPreposition() == null) {
            final CatalogManager catalogManager = context.getCatalogManager();
            final String currentCatalogName = catalogManager.getCurrentCatalog();
            final String currentDatabaseName = catalogManager.getCurrentDatabase();
            return getOperationWithoutPrep(
                    sqlShowCall, currentCatalogName, currentDatabaseName, likeOp);
        }
        final List<String> sqlIdentifierNameList = sqlShowCall.getSqlIdentifierNameList();
        if (sqlIdentifierNameList.size() > 2) {
            throw new ValidationException(
                    String.format(
                            "%s from/in identifier [ %s ] format error, it should be [catalog_name.]database_name.",
                            sqlShowCall.getOperator().getName(),
                            String.join(".", sqlIdentifierNameList)));
        }
        final CatalogManager catalogManager = context.getCatalogManager();
        final String catalogName =
                sqlIdentifierNameList.size() == 1
                        ? catalogManager.getCurrentCatalog()
                        : sqlIdentifierNameList.get(0);

        final String databaseName =
                sqlIdentifierNameList.size() == 1
                        ? sqlIdentifierNameList.get(0)
                        : sqlIdentifierNameList.get(1);
        return getOperation(
                sqlShowCall, catalogName, databaseName, sqlShowCall.getPreposition(), likeOp);
    }

    public ShowLikeOperator getLikeOp(SqlShowCall sqlShowCall) {
        return ShowLikeOperator.of(
                LikeType.of(sqlShowCall.getLikeType(), sqlShowCall.isNotLike()),
                sqlShowCall.getLikeSqlPattern());
    }

    public abstract Operation getOperationWithoutPrep(
            T sqlShowCall,
            @Nullable String catalogName,
            @Nullable String databaseName,
            @Nullable ShowLikeOperator likeOp);

    public abstract Operation getOperation(
            T sqlShowCall,
            @Nullable String catalogName,
            @Nullable String databaseName,
            @Nullable String prep,
            @Nullable ShowLikeOperator likeOp);

    @Override
    public abstract Operation convertSqlNode(T node, ConvertContext context);
}
