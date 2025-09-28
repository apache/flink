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

import org.apache.flink.sql.parser.ddl.SqlCreateTableLike;
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Helper class for converting {@link SqlCreateTableLike} to {@link CreateTableOperation}. */
public class SqlCreateTableLikeConverter extends AbstractCreateTableConverter<SqlCreateTableLike> {

    @Override
    public Operation convertSqlNode(SqlCreateTableLike sqlCreateTableLike, ConvertContext context) {
        ResolvedCatalogTable catalogTable =
                // no schema definition to merge for CREATE TABLE ... LIKE, schema from source table
                // will be merged
                getResolvedCatalogTable(sqlCreateTableLike, context, null);
        final ObjectIdentifier identifier = getIdentifier(sqlCreateTableLike, context);
        return getCreateTableOperation(identifier, catalogTable, sqlCreateTableLike);
    }

    private CatalogTable lookupLikeSourceTable(
            SqlTableLike sqlTableLike, CatalogManager catalogManager) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlTableLike.getSourceTable().names);
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        ContextResolvedTable lookupResult =
                catalogManager
                        .getTable(identifier)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Source table '%s' of the LIKE clause not found in the catalog, at %s",
                                                        identifier,
                                                        sqlTableLike
                                                                .getSourceTable()
                                                                .getParserPosition())));
        if (!(lookupResult.getResolvedTable() instanceof CatalogTable)) {
            throw new ValidationException(
                    String.format(
                            "Source table '%s' of the LIKE clause can not be a VIEW, at %s",
                            identifier, sqlTableLike.getSourceTable().getParserPosition()));
        }
        return lookupResult.getResolvedTable();
    }

    @Override
    protected MergeContext getMergeContext(
            SqlCreateTableLike sqlCreateTableLike, ConvertContext context) {
        return new MergeContext() {
            private final MergeTableLikeUtil mergeTableLikeUtil = new MergeTableLikeUtil(context);
            private final SqlTableLike sqlTableLike = sqlCreateTableLike.getTableLike();
            private final CatalogTable table =
                    lookupLikeSourceTable(sqlTableLike, context.getCatalogManager());
            private final Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy>
                    mergingStrategies =
                            mergeTableLikeUtil.computeMergingStrategies(sqlTableLike.getOptions());

            @Override
            public Schema getMergedSchema(ResolvedSchema schemaToMerge) {
                final Optional<SqlTableConstraint> tableConstraint =
                        sqlCreateTableLike.getFullConstraints().stream()
                                .filter(SqlTableConstraint::isPrimaryKey)
                                .findAny();
                return mergeTableLikeUtil.mergeTables(
                        mergingStrategies,
                        table.getUnresolvedSchema(),
                        sqlCreateTableLike.getColumnList().getList(),
                        sqlCreateTableLike
                                .getWatermark()
                                .map(Collections::singletonList)
                                .orElseGet(Collections::emptyList),
                        tableConstraint.orElse(null));
            }

            @Override
            public Map<String, String> getMergedTableOptions() {
                final Map<String, String> derivedTableOptions =
                        OperationConverterUtils.getProperties(sqlCreateTableLike.getPropertyList());
                return mergeTableLikeUtil.mergeOptions(
                        mergingStrategies.get(SqlTableLike.FeatureOption.OPTIONS),
                        table.getOptions(),
                        derivedTableOptions);
            }

            @Override
            public List<String> getMergedPartitionKeys() {
                return mergeTableLikeUtil.mergePartitions(
                        mergingStrategies.get(SqlTableLike.FeatureOption.PARTITIONS),
                        table.getPartitionKeys(),
                        SqlCreateTableLikeConverter.this.getDerivedPartitionKeys(
                                sqlCreateTableLike));
            }

            @Override
            public Optional<TableDistribution> getMergedTableDistribution() {
                return mergeTableLikeUtil.mergeDistribution(
                        mergingStrategies.get(SqlTableLike.FeatureOption.DISTRIBUTION),
                        table.getDistribution(),
                        SqlCreateTableLikeConverter.this.getDerivedTableDistribution(
                                sqlCreateTableLike));
            }
        };
    }
}
