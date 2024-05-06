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

import org.apache.flink.sql.parser.SqlConstraintValidator;
import org.apache.flink.sql.parser.ddl.SqlCreateMaterializedTable;
import org.apache.flink.sql.parser.ddl.SqlRefreshMode;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.MATERIALIZED_TABLE_FRESHNESS_THRESHOLD;

/** A converter for {@link SqlCreateMaterializedTable}. */
public class SqlCreateMaterializedTableConverter
        implements SqlNodeConverter<SqlCreateMaterializedTable> {

    @Override
    public Operation convertSqlNode(
            SqlCreateMaterializedTable sqlCreateMaterializedTable, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateMaterializedTable.fullTableName());
        ObjectIdentifier identifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);

        // get comment
        String tableComment =
                OperationConverterUtils.getTableComment(sqlCreateMaterializedTable.getComment());

        // get options
        Map<String, String> options = new HashMap<>();
        sqlCreateMaterializedTable
                .getPropertyList()
                .getList()
                .forEach(
                        p ->
                                options.put(
                                        ((SqlTableOption) p).getKeyString(),
                                        ((SqlTableOption) p).getValueString()));

        // get freshness
        Duration freshness =
                MaterializedTableUtils.getMaterializedTableFreshness(
                        sqlCreateMaterializedTable.getFreshness());

        // get refresh mode
        SqlRefreshMode sqlRefreshMode = null;
        if (sqlCreateMaterializedTable.getRefreshMode().isPresent()) {
            sqlRefreshMode =
                    sqlCreateMaterializedTable
                            .getRefreshMode()
                            .get()
                            .getValueAs(SqlRefreshMode.class);
        }
        CatalogMaterializedTable.LogicalRefreshMode logicalRefreshMode =
                MaterializedTableUtils.deriveLogicalRefreshMode(sqlRefreshMode);
        // only MATERIALIZED_TABLE_FRESHNESS_THRESHOLD configured in flink conf yaml work, so we get
        // it from rootConfiguration instead of table config
        CatalogMaterializedTable.RefreshMode refreshMode =
                MaterializedTableUtils.deriveRefreshMode(
                        context.getTableConfig()
                                .getRootConfiguration()
                                .get(MATERIALIZED_TABLE_FRESHNESS_THRESHOLD),
                        freshness,
                        logicalRefreshMode);

        // get query schema and definition query
        SqlNode validateQuery =
                context.getSqlValidator().validate(sqlCreateMaterializedTable.getAsQuery());
        PlannerQueryOperation queryOperation =
                new PlannerQueryOperation(
                        context.toRelRoot(validateQuery).project(),
                        () -> context.toQuotedSqlString(validateQuery));
        String definitionQuery =
                context.expandSqlIdentifiers(queryOperation.asSerializableString());

        // get schema
        ResolvedSchema resolvedSchema = queryOperation.getResolvedSchema();
        Schema.Builder builder = Schema.newBuilder().fromResolvedSchema(resolvedSchema);

        // get and verify partition key
        List<String> partitionKeys =
                sqlCreateMaterializedTable.getPartitionKeyList().getList().stream()
                        .map(p -> ((SqlIdentifier) p).getSimple())
                        .collect(Collectors.toList());
        verifyPartitioningColumnsExist(resolvedSchema, partitionKeys);

        // verify and build primary key
        sqlCreateMaterializedTable
                .getTableConstraint()
                .ifPresent(
                        sqlTableConstraint ->
                                verifyAndBuildPrimaryKey(
                                        builder, resolvedSchema, sqlTableConstraint));

        CatalogMaterializedTable materializedTable =
                CatalogMaterializedTable.newBuilder()
                        .schema(builder.build())
                        .comment(tableComment)
                        .partitionKeys(partitionKeys)
                        .options(options)
                        .definitionQuery(definitionQuery)
                        .freshness(freshness)
                        .logicalRefreshMode(logicalRefreshMode)
                        .refreshMode(refreshMode)
                        .refreshStatus(CatalogMaterializedTable.RefreshStatus.INITIALIZING)
                        .build();

        return new CreateMaterializedTableOperation(
                identifier,
                context.getCatalogManager().resolveCatalogMaterializedTable(materializedTable));
    }

    private static void verifyPartitioningColumnsExist(
            ResolvedSchema resolvedSchema, List<String> partitionKeys) {
        for (String partitionKey : partitionKeys) {
            if (!resolvedSchema.getColumn(partitionKey).isPresent()) {
                throw new ValidationException(
                        String.format(
                                "Partition column '%s' not defined in the query schema. Available columns: [%s].",
                                partitionKey,
                                resolvedSchema.getColumnNames().stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }
        }
    }

    private static void verifyAndBuildPrimaryKey(
            Schema.Builder schemaBuilder,
            ResolvedSchema resolvedSchema,
            SqlTableConstraint sqlTableConstraint) {
        // check constraint type
        try {
            SqlConstraintValidator.validate(sqlTableConstraint);
        } catch (SqlValidateException e) {
            throw new ValidationException(
                    String.format("Primary key validation failed: %s.", e.getMessage()), e);
        }

        List<String> primaryKeyColumns = Arrays.asList(sqlTableConstraint.getColumnNames());
        for (String columnName : primaryKeyColumns) {
            Optional<Column> columnOptional = resolvedSchema.getColumn(columnName);
            if (!columnOptional.isPresent()) {
                throw new ValidationException(
                        String.format(
                                "Primary key column '%s' not defined in the query schema. Available columns: [%s].",
                                columnName,
                                resolvedSchema.getColumnNames().stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }

            if (columnOptional.get().getDataType().getLogicalType().isNullable()) {
                throw new ValidationException(
                        String.format(
                                "Could not create a PRIMARY KEY with nullable column '%s'.\n"
                                        + "A PRIMARY KEY column must be declared on non-nullable physical columns.",
                                columnName));
            }
        }

        // build primary key
        String constraintName =
                sqlTableConstraint
                        .getConstraintName()
                        .orElseGet(
                                () ->
                                        primaryKeyColumns.stream()
                                                .collect(Collectors.joining("_", "PK_", "")));
        schemaBuilder.primaryKeyNamed(constraintName, primaryKeyColumns);
    }
}
