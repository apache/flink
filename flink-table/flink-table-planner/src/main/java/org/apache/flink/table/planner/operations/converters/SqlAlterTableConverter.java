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

import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropColumn;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropConstraint;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropDistribution;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropPrimaryKey;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropWatermark;
import org.apache.flink.sql.parser.ddl.SqlAlterTableOptions;
import org.apache.flink.sql.parser.ddl.SqlAlterTableRename;
import org.apache.flink.sql.parser.ddl.SqlAlterTableRenameColumn;
import org.apache.flink.sql.parser.ddl.SqlAlterTableReset;
import org.apache.flink.sql.parser.ddl.SqlAlterTableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.operations.NopOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterPartitionPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.operations.AlterSchemaConverter;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlAlterTableConverter implements SqlNodeConverter<SqlAlterTable> {

    @Override
    public Operation convertSqlNode(SqlAlterTable node, ConvertContext context) {
        FlinkPlannerImpl flinkPlanner = context.getFlinkPlanner();
        CatalogManager catalogManager = context.getCatalogManager();
        AlterSchemaConverter alterSchemaConverter =
                new AlterSchemaConverter(
                        flinkPlanner.getOrCreateSqlValidator(),
                        context.getEscapeExpression(),
                        catalogManager);

        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(node.fullTableName());
        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        Optional<ContextResolvedTable> optionalCatalogTable =
                catalogManager.getTable(tableIdentifier);
        if (!optionalCatalogTable.isPresent() || optionalCatalogTable.get().isTemporary()) {
            if (node.ifTableExists()) {
                return new NopOperation();
            }
            throw new ValidationException(
                    String.format(
                            "Table %s doesn't exist or is a temporary table.", tableIdentifier));
        }
        CatalogBaseTable baseTable = optionalCatalogTable.get().getResolvedTable();
        if (baseTable instanceof CatalogView) {
            throw new ValidationException("ALTER TABLE for a view is not allowed");
        }
        ResolvedCatalogTable resolvedCatalogTable = (ResolvedCatalogTable) baseTable;
        if (node instanceof SqlAlterTableRename) {
            UnresolvedIdentifier newUnresolvedIdentifier =
                    UnresolvedIdentifier.of(((SqlAlterTableRename) node).fullNewTableName());
            ObjectIdentifier newTableIdentifier =
                    catalogManager.qualifyIdentifier(newUnresolvedIdentifier);
            return new AlterTableRenameOperation(
                    tableIdentifier, newTableIdentifier, node.ifTableExists());
        } else if (node instanceof SqlAlterTableOptions) {
            return convertAlterTableOptions(
                    tableIdentifier,
                    (CatalogTable) baseTable,
                    (SqlAlterTableOptions) node,
                    catalogManager);
        } else if (node instanceof SqlAlterTableReset) {
            return convertAlterTableReset(
                    tableIdentifier, (CatalogTable) baseTable, (SqlAlterTableReset) node);
        } else if (node instanceof SqlAlterTableDropColumn) {
            return alterSchemaConverter.convertAlterSchema(
                    (SqlAlterTableDropColumn) node, resolvedCatalogTable);
        } else if (node instanceof SqlAlterTableDropDistribution) {
            return convertAlterTableDropDistribution(node, resolvedCatalogTable, tableIdentifier);
        } else if (node instanceof SqlAlterTableDropPrimaryKey) {
            return alterSchemaConverter.convertAlterSchema(
                    (SqlAlterTableDropPrimaryKey) node, resolvedCatalogTable);
        } else if (node instanceof SqlAlterTableDropConstraint) {
            return alterSchemaConverter.convertAlterSchema(
                    (SqlAlterTableDropConstraint) node, resolvedCatalogTable);
        } else if (node instanceof SqlAlterTableDropWatermark) {
            return alterSchemaConverter.convertAlterSchema(
                    (SqlAlterTableDropWatermark) node, resolvedCatalogTable);
        } else if (node instanceof SqlAlterTableRenameColumn) {
            return alterSchemaConverter.convertAlterSchema(
                    (SqlAlterTableRenameColumn) node, resolvedCatalogTable);
        } else if (node instanceof SqlAlterTableSchema) {
            return alterSchemaConverter.convertAlterSchema(
                    (SqlAlterTableSchema) node, resolvedCatalogTable);
        } else {
            throw new ValidationException(
                    String.format(
                            "[%s] needs to implement",
                            node.toSqlString(CalciteSqlDialect.DEFAULT)));
        }
    }

    private Operation convertAlterTableOptions(
            ObjectIdentifier tableIdentifier,
            CatalogTable oldTable,
            SqlAlterTableOptions alterTableOptions,
            CatalogManager catalogManager) {
        LinkedHashMap<String, String> partitionKVs = alterTableOptions.getPartitionKVs();
        // it's altering partitions
        if (partitionKVs != null) {
            CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(partitionKVs);
            CatalogPartition catalogPartition =
                    catalogManager
                            .getPartition(tableIdentifier, partitionSpec)
                            .orElseThrow(
                                    () ->
                                            new ValidationException(
                                                    String.format(
                                                            "Partition %s of table %s doesn't exist",
                                                            partitionSpec.getPartitionSpec(),
                                                            tableIdentifier)));
            Map<String, String> newProps = new HashMap<>(catalogPartition.getProperties());
            newProps.putAll(
                    OperationConverterUtils.extractProperties(alterTableOptions.getPropertyList()));
            return new AlterPartitionPropertiesOperation(
                    tableIdentifier,
                    partitionSpec,
                    new CatalogPartitionImpl(newProps, catalogPartition.getComment()));
        } else {
            // it's altering a table
            Map<String, String> changeOptions =
                    OperationConverterUtils.extractProperties(alterTableOptions.getPropertyList());
            Map<String, String> newOptions = new HashMap<>(oldTable.getOptions());
            newOptions.putAll(changeOptions);
            return new AlterTableChangeOperation(
                    tableIdentifier,
                    changeOptions.entrySet().stream()
                            .map(entry -> TableChange.set(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList()),
                    oldTable.copy(newOptions),
                    alterTableOptions.ifTableExists());
        }
    }

    private Operation convertAlterTableReset(
            ObjectIdentifier tableIdentifier,
            CatalogTable oldTable,
            SqlAlterTableReset alterTableReset) {
        Map<String, String> newOptions = new HashMap<>(oldTable.getOptions());
        // reset empty or 'connector' key is not allowed
        Set<String> resetKeys = alterTableReset.getResetKeys();
        if (resetKeys.isEmpty() || resetKeys.contains(FactoryUtil.CONNECTOR.key())) {
            String exMsg =
                    resetKeys.isEmpty()
                            ? "ALTER TABLE RESET does not support empty key"
                            : "ALTER TABLE RESET does not support changing 'connector'";
            throw new ValidationException(exMsg);
        }
        // reset table option keys
        resetKeys.forEach(newOptions::remove);
        return new AlterTableChangeOperation(
                tableIdentifier,
                resetKeys.stream().map(TableChange::reset).collect(Collectors.toList()),
                oldTable.copy(newOptions),
                alterTableReset.ifTableExists());
    }

    /** Convert ALTER TABLE DROP DISTRIBUTION statement. */
    private static AlterTableChangeOperation convertAlterTableDropDistribution(
            SqlAlterTable sqlAlterTable,
            ResolvedCatalogTable resolvedCatalogTable,
            ObjectIdentifier tableIdentifier) {
        if (!resolvedCatalogTable.getDistribution().isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Table %s does not have a distribution to drop.", tableIdentifier));
        }

        List<TableChange> tableChanges = Collections.singletonList(TableChange.dropDistribution());
        CatalogTable.Builder builder =
                CatalogTable.newBuilder()
                        .comment(resolvedCatalogTable.getComment())
                        .options(resolvedCatalogTable.getOptions())
                        .schema(resolvedCatalogTable.getUnresolvedSchema())
                        .partitionKeys(resolvedCatalogTable.getPartitionKeys())
                        .options(resolvedCatalogTable.getOptions());

        resolvedCatalogTable.getSnapshot().ifPresent(builder::snapshot);

        CatalogTable newTable = builder.build();
        return new AlterTableChangeOperation(
                tableIdentifier, tableChanges, newTable, sqlAlterTable.ifTableExists());
    }
}
