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

import org.apache.flink.sql.parser.dql.SqlShowDatabases;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowDatabasesOperation;
import org.apache.flink.table.operations.utils.LikeType;
import org.apache.flink.table.operations.utils.ShowLikeOperator;

/** A converter for {@link SqlShowDatabases}. */
public class SqlShowDatabasesConverter implements SqlNodeConverter<SqlShowDatabases> {

    @Override
    public Operation convertSqlNode(SqlShowDatabases sqlShowDatabases, ConvertContext context) {
        final ShowLikeOperator likeOp =
                ShowLikeOperator.of(
                        LikeType.of(sqlShowDatabases.getLikeType(), sqlShowDatabases.isNotLike()),
                        sqlShowDatabases.getLikeSqlPattern());
        if (sqlShowDatabases.getPreposition() == null) {
            final CatalogManager catalogManager = context.getCatalogManager();
            final String currentCatalogName = catalogManager.getCurrentCatalog();
            return new ShowDatabasesOperation(currentCatalogName, likeOp);
        } else {
            return new ShowDatabasesOperation(
                    sqlShowDatabases.getCatalogName(), sqlShowDatabases.getPreposition(), likeOp);
        }
    }
}
