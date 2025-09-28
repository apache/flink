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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Helper class for converting {@link SqlCreateTable} to {@link CreateTableOperation}. */
public class SqlCreateTableConverter extends AbstractCreateTableConverter<SqlCreateTable> {

    @Override
    public Operation convertSqlNode(SqlCreateTable sqlCreateTable, ConvertContext context) {
        // no schema to merge for CREATE TABLE
        ResolvedCatalogTable catalogTable = getResolvedCatalogTable(sqlCreateTable, context, null);

        ObjectIdentifier identifier = getIdentifier(sqlCreateTable, context);
        return getCreateTableOperation(identifier, catalogTable, sqlCreateTable);
    }

    @Override
    protected MergeContext getMergeContext(SqlCreateTable sqlCreateTable, ConvertContext context) {
        return new MergeContext() {
            private final MergeTableLikeUtil mergeTableLikeUtil = new MergeTableLikeUtil(context);

            @Override
            public Schema getMergedSchema(ResolvedSchema schemaToMerge) {
                final Optional<SqlTableConstraint> tableConstraint =
                        sqlCreateTable.getFullConstraints().stream()
                                .filter(SqlTableConstraint::isPrimaryKey)
                                .findAny();
                return mergeTableLikeUtil.mergeTables(
                        Map.of(),
                        Schema.newBuilder().build(),
                        sqlCreateTable.getColumnList().getList(),
                        sqlCreateTable
                                .getWatermark()
                                .map(Collections::singletonList)
                                .orElseGet(Collections::emptyList),
                        tableConstraint.orElse(null));
            }

            @Override
            public Map<String, String> getMergedTableOptions() {
                return SqlCreateTableConverter.this.getDerivedTableOptions(sqlCreateTable);
            }

            @Override
            public List<String> getMergedPartitionKeys() {
                return SqlCreateTableConverter.this.getDerivedPartitionKeys(sqlCreateTable);
            }

            @Override
            public Optional<TableDistribution> getMergedTableDistribution() {
                return SqlCreateTableConverter.this.getDerivedTableDistribution(sqlCreateTable);
            }
        };
    }
}
