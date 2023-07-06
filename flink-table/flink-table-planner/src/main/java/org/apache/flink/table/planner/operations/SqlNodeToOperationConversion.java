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

import org.apache.flink.sql.parser.ddl.SqlAddJar;
import org.apache.flink.sql.parser.ddl.SqlAlterDatabase;
import org.apache.flink.sql.parser.ddl.SqlAlterFunction;
import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlAlterTableCompact;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropColumn;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropConstraint;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropPrimaryKey;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropWatermark;
import org.apache.flink.sql.parser.ddl.SqlAlterTableOptions;
import org.apache.flink.sql.parser.ddl.SqlAlterTableRename;
import org.apache.flink.sql.parser.ddl.SqlAlterTableRenameColumn;
import org.apache.flink.sql.parser.ddl.SqlAlterTableReset;
import org.apache.flink.sql.parser.ddl.SqlAlterTableSchema;
import org.apache.flink.sql.parser.ddl.SqlAnalyzeTable;
import org.apache.flink.sql.parser.ddl.SqlCompilePlan;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.sql.parser.ddl.SqlDropCatalog;
import org.apache.flink.sql.parser.ddl.SqlDropDatabase;
import org.apache.flink.sql.parser.ddl.SqlDropFunction;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlDropView;
import org.apache.flink.sql.parser.ddl.SqlRemoveJar;
import org.apache.flink.sql.parser.ddl.SqlReset;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.ddl.SqlStopJob;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlUseCatalog;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.ddl.SqlUseModules;
import org.apache.flink.sql.parser.ddl.resource.SqlResource;
import org.apache.flink.sql.parser.ddl.resource.SqlResourceType;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dml.SqlBeginStatementSet;
import org.apache.flink.sql.parser.dml.SqlCompileAndExecutePlan;
import org.apache.flink.sql.parser.dml.SqlEndStatementSet;
import org.apache.flink.sql.parser.dml.SqlExecute;
import org.apache.flink.sql.parser.dml.SqlExecutePlan;
import org.apache.flink.sql.parser.dml.SqlStatementSet;
import org.apache.flink.sql.parser.dql.SqlLoadModule;
import org.apache.flink.sql.parser.dql.SqlRichDescribeTable;
import org.apache.flink.sql.parser.dql.SqlRichExplain;
import org.apache.flink.sql.parser.dql.SqlShowCatalogs;
import org.apache.flink.sql.parser.dql.SqlShowColumns;
import org.apache.flink.sql.parser.dql.SqlShowCreateTable;
import org.apache.flink.sql.parser.dql.SqlShowCreateView;
import org.apache.flink.sql.parser.dql.SqlShowCurrentCatalog;
import org.apache.flink.sql.parser.dql.SqlShowCurrentDatabase;
import org.apache.flink.sql.parser.dql.SqlShowDatabases;
import org.apache.flink.sql.parser.dql.SqlShowJars;
import org.apache.flink.sql.parser.dql.SqlShowJobs;
import org.apache.flink.sql.parser.dql.SqlShowModules;
import org.apache.flink.sql.parser.dql.SqlShowTables;
import org.apache.flink.sql.parser.dql.SqlShowViews;
import org.apache.flink.sql.parser.dql.SqlUnloadModule;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ManagedTableListener;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.CompileAndExecutePlanOperation;
import org.apache.flink.table.operations.DeleteFromFilterOperation;
import org.apache.flink.table.operations.DescribeTableOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.LoadModuleOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.NopOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.ShowCatalogsOperation;
import org.apache.flink.table.operations.ShowColumnsOperation;
import org.apache.flink.table.operations.ShowCreateTableOperation;
import org.apache.flink.table.operations.ShowCreateViewOperation;
import org.apache.flink.table.operations.ShowCurrentCatalogOperation;
import org.apache.flink.table.operations.ShowCurrentDatabaseOperation;
import org.apache.flink.table.operations.ShowDatabasesOperation;
import org.apache.flink.table.operations.ShowModulesOperation;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.ShowViewsOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.SourceQueryOperation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.UnloadModuleOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.UseModulesOperation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.ExecutePlanOperation;
import org.apache.flink.table.operations.command.RemoveJarOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.command.ShowJarsOperation;
import org.apache.flink.table.operations.command.ShowJobsOperation;
import org.apache.flink.table.operations.command.StopJobOperation;
import org.apache.flink.table.operations.ddl.AlterCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterPartitionPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.AnalyzeTableOperation;
import org.apache.flink.table.operations.ddl.CompilePlanOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.DropCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.DropCatalogOperation;
import org.apache.flink.table.operations.ddl.DropDatabaseOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.operations.ddl.DropTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.DropViewOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverters;
import org.apache.flink.table.planner.utils.OperationConverterUtils;
import org.apache.flink.table.planner.utils.RowLevelModificationContextUtils;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Mix-in tool class for {@code SqlNode} that allows DDL commands to be converted to {@link
 * Operation}.
 *
 * <p>For every kind of {@link SqlNode}, there needs to have a corresponding #convert(type) method,
 * the 'type' argument should be the subclass of the supported {@link SqlNode}.
 *
 * <p>Every #convert() should return a {@link Operation} which can be used in {@link
 * org.apache.flink.table.delegation.Planner}.
 */
public class SqlNodeToOperationConversion {
    private final FlinkPlannerImpl flinkPlanner;
    private final CatalogManager catalogManager;
    private final SqlCreateTableConverter createTableConverter;
    private final AlterSchemaConverter alterSchemaConverter;

    // ~ Constructors -----------------------------------------------------------

    private SqlNodeToOperationConversion(
            FlinkPlannerImpl flinkPlanner, CatalogManager catalogManager) {
        this.flinkPlanner = flinkPlanner;
        this.catalogManager = catalogManager;
        this.createTableConverter =
                new SqlCreateTableConverter(
                        flinkPlanner.getOrCreateSqlValidator(),
                        catalogManager,
                        this::getQuotedSqlString);
        this.alterSchemaConverter =
                new AlterSchemaConverter(
                        flinkPlanner.getOrCreateSqlValidator(),
                        this::getQuotedSqlString,
                        catalogManager);
    }

    /**
     * This is the main entrance for executing all kinds of DDL/DML {@code SqlNode}s, different
     * SqlNode will have it's implementation in the #convert(type) method whose 'type' argument is
     * subclass of {@code SqlNode}.
     *
     * @param flinkPlanner FlinkPlannerImpl to convertCreateTable sql node to rel node
     * @param catalogManager CatalogManager to resolve full path for operations
     * @param sqlNode SqlNode to execute on
     */
    public static Optional<Operation> convert(
            FlinkPlannerImpl flinkPlanner, CatalogManager catalogManager, SqlNode sqlNode) {
        // validate the query
        final SqlNode validated = flinkPlanner.validate(sqlNode);
        return convertValidatedSqlNode(flinkPlanner, catalogManager, validated);
    }

    /** Convert a validated sql node to Operation. */
    private static Optional<Operation> convertValidatedSqlNode(
            FlinkPlannerImpl flinkPlanner, CatalogManager catalogManager, SqlNode validated) {
        beforeConversion();

        // delegate conversion to the registered converters first
        SqlNodeConvertContext context = new SqlNodeConvertContext(flinkPlanner, catalogManager);
        Optional<Operation> operation = SqlNodeConverters.convertSqlNode(validated, context);
        if (operation.isPresent()) {
            return operation;
        }

        // TODO: all the below conversion logic should be migrated to SqlNodeConverters
        SqlNodeToOperationConversion converter =
                new SqlNodeToOperationConversion(flinkPlanner, catalogManager);
        if (validated instanceof SqlDropCatalog) {
            return Optional.of(converter.convertDropCatalog((SqlDropCatalog) validated));
        } else if (validated instanceof SqlLoadModule) {
            return Optional.of(converter.convertLoadModule((SqlLoadModule) validated));
        } else if (validated instanceof SqlShowCatalogs) {
            return Optional.of(converter.convertShowCatalogs((SqlShowCatalogs) validated));
        } else if (validated instanceof SqlShowCurrentCatalog) {
            return Optional.of(
                    converter.convertShowCurrentCatalog((SqlShowCurrentCatalog) validated));
        } else if (validated instanceof SqlShowModules) {
            return Optional.of(converter.convertShowModules((SqlShowModules) validated));
        } else if (validated instanceof SqlUnloadModule) {
            return Optional.of(converter.convertUnloadModule((SqlUnloadModule) validated));
        } else if (validated instanceof SqlUseCatalog) {
            return Optional.of(converter.convertUseCatalog((SqlUseCatalog) validated));
        } else if (validated instanceof SqlUseModules) {
            return Optional.of(converter.convertUseModules((SqlUseModules) validated));
        } else if (validated instanceof SqlCreateDatabase) {
            return Optional.of(converter.convertCreateDatabase((SqlCreateDatabase) validated));
        } else if (validated instanceof SqlDropDatabase) {
            return Optional.of(converter.convertDropDatabase((SqlDropDatabase) validated));
        } else if (validated instanceof SqlAlterDatabase) {
            return Optional.of(converter.convertAlterDatabase((SqlAlterDatabase) validated));
        } else if (validated instanceof SqlShowDatabases) {
            return Optional.of(converter.convertShowDatabases((SqlShowDatabases) validated));
        } else if (validated instanceof SqlShowCurrentDatabase) {
            return Optional.of(
                    converter.convertShowCurrentDatabase((SqlShowCurrentDatabase) validated));
        } else if (validated instanceof SqlUseDatabase) {
            return Optional.of(converter.convertUseDatabase((SqlUseDatabase) validated));
        } else if (validated instanceof SqlCreateTable) {
            if (validated instanceof SqlCreateTableAs) {
                return Optional.of(
                        converter.createTableConverter.convertCreateTableAS(
                                flinkPlanner, (SqlCreateTableAs) validated));
            }
            return Optional.of(
                    converter.createTableConverter.convertCreateTable((SqlCreateTable) validated));
        } else if (validated instanceof SqlDropTable) {
            return Optional.of(converter.convertDropTable((SqlDropTable) validated));
        } else if (validated instanceof SqlAlterTable) {
            return Optional.of(converter.convertAlterTable((SqlAlterTable) validated));
        } else if (validated instanceof SqlShowTables) {
            return Optional.of(converter.convertShowTables((SqlShowTables) validated));
        } else if (validated instanceof SqlShowColumns) {
            return Optional.of(converter.convertShowColumns((SqlShowColumns) validated));
        } else if (validated instanceof SqlDropView) {
            return Optional.of(converter.convertDropView((SqlDropView) validated));
        } else if (validated instanceof SqlShowViews) {
            return Optional.of(converter.convertShowViews((SqlShowViews) validated));
        } else if (validated instanceof SqlCreateFunction) {
            return Optional.of(converter.convertCreateFunction((SqlCreateFunction) validated));
        } else if (validated instanceof SqlDropFunction) {
            return Optional.of(converter.convertDropFunction((SqlDropFunction) validated));
        } else if (validated instanceof SqlAlterFunction) {
            return Optional.of(converter.convertAlterFunction((SqlAlterFunction) validated));
        } else if (validated instanceof SqlShowCreateTable) {
            return Optional.of(converter.convertShowCreateTable((SqlShowCreateTable) validated));
        } else if (validated instanceof SqlShowCreateView) {
            return Optional.of(converter.convertShowCreateView((SqlShowCreateView) validated));
        } else if (validated instanceof SqlRichExplain) {
            return Optional.of(converter.convertRichExplain((SqlRichExplain) validated));
        } else if (validated instanceof SqlRichDescribeTable) {
            return Optional.of(converter.convertDescribeTable((SqlRichDescribeTable) validated));
        } else if (validated instanceof SqlAddJar) {
            return Optional.of(converter.convertAddJar((SqlAddJar) validated));
        } else if (validated instanceof SqlRemoveJar) {
            return Optional.of(converter.convertRemoveJar((SqlRemoveJar) validated));
        } else if (validated instanceof SqlShowJars) {
            return Optional.of(converter.convertShowJars((SqlShowJars) validated));
        } else if (validated instanceof SqlShowJobs) {
            return Optional.of(converter.convertShowJobs((SqlShowJobs) validated));
        } else if (validated instanceof RichSqlInsert) {
            return Optional.of(converter.convertSqlInsert((RichSqlInsert) validated));
        } else if (validated instanceof SqlBeginStatementSet) {
            return Optional.of(
                    converter.convertBeginStatementSet((SqlBeginStatementSet) validated));
        } else if (validated instanceof SqlEndStatementSet) {
            return Optional.of(converter.convertEndStatementSet((SqlEndStatementSet) validated));
        } else if (validated instanceof SqlSet) {
            return Optional.of(converter.convertSet((SqlSet) validated));
        } else if (validated instanceof SqlReset) {
            return Optional.of(converter.convertReset((SqlReset) validated));
        } else if (validated instanceof SqlStatementSet) {
            return Optional.of(converter.convertSqlStatementSet((SqlStatementSet) validated));
        } else if (validated instanceof SqlExecute) {
            return convertValidatedSqlNode(
                    flinkPlanner, catalogManager, ((SqlExecute) validated).getStatement());
        } else if (validated instanceof SqlExecutePlan) {
            return Optional.of(converter.convertExecutePlan((SqlExecutePlan) validated));
        } else if (validated instanceof SqlCompilePlan) {
            return Optional.of(converter.convertCompilePlan((SqlCompilePlan) validated));
        } else if (validated instanceof SqlCompileAndExecutePlan) {
            return Optional.of(
                    converter.convertCompileAndExecutePlan((SqlCompileAndExecutePlan) validated));
        } else if (validated instanceof SqlAnalyzeTable) {
            return Optional.of(converter.convertAnalyzeTable((SqlAnalyzeTable) validated));
        } else if (validated instanceof SqlStopJob) {
            return Optional.of(converter.convertStopJob((SqlStopJob) validated));
        } else if (validated instanceof SqlDelete) {
            return Optional.of(converter.convertDelete((SqlDelete) validated));
        } else if (validated instanceof SqlUpdate) {
            return Optional.of(converter.convertUpdate((SqlUpdate) validated));
        } else {
            return Optional.empty();
        }
    }

    private static Operation convertValidatedSqlNodeOrFail(
            FlinkPlannerImpl flinkPlanner, CatalogManager catalogManager, SqlNode validated) {
        return convertValidatedSqlNode(flinkPlanner, catalogManager, validated)
                .orElseThrow(
                        () ->
                                new TableException(
                                        "Unsupported node type "
                                                + validated.getClass().getSimpleName()));
    }

    // ~ Tools ------------------------------------------------------------------

    private static void beforeConversion() {
        // clear row-level modification context
        RowLevelModificationContextUtils.clearContext();
    }

    /** Convert DROP TABLE statement. */
    private Operation convertDropTable(SqlDropTable sqlDropTable) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlDropTable.fullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        return new DropTableOperation(
                identifier, sqlDropTable.getIfExists(), sqlDropTable.isTemporary());
    }

    /** convert ALTER TABLE statement. */
    private Operation convertAlterTable(SqlAlterTable sqlAlterTable) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterTable.fullTableName());
        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        Optional<ContextResolvedTable> optionalCatalogTable =
                catalogManager.getTable(tableIdentifier);
        if (!optionalCatalogTable.isPresent() || optionalCatalogTable.get().isTemporary()) {
            if (sqlAlterTable.ifTableExists()) {
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
        if (sqlAlterTable instanceof SqlAlterTableRename) {
            UnresolvedIdentifier newUnresolvedIdentifier =
                    UnresolvedIdentifier.of(
                            ((SqlAlterTableRename) sqlAlterTable).fullNewTableName());
            ObjectIdentifier newTableIdentifier =
                    catalogManager.qualifyIdentifier(newUnresolvedIdentifier);
            return new AlterTableRenameOperation(
                    tableIdentifier, newTableIdentifier, sqlAlterTable.ifTableExists());
        } else if (sqlAlterTable instanceof SqlAlterTableOptions) {
            return convertAlterTableOptions(
                    tableIdentifier,
                    (CatalogTable) baseTable,
                    (SqlAlterTableOptions) sqlAlterTable);
        } else if (sqlAlterTable instanceof SqlAlterTableReset) {
            return convertAlterTableReset(
                    tableIdentifier, (CatalogTable) baseTable, (SqlAlterTableReset) sqlAlterTable);
        } else if (sqlAlterTable instanceof SqlAlterTableDropColumn) {
            return alterSchemaConverter.convertAlterSchema(
                    (SqlAlterTableDropColumn) sqlAlterTable, resolvedCatalogTable);
        } else if (sqlAlterTable instanceof SqlAlterTableDropPrimaryKey) {
            return alterSchemaConverter.convertAlterSchema(
                    (SqlAlterTableDropPrimaryKey) sqlAlterTable, resolvedCatalogTable);
        } else if (sqlAlterTable instanceof SqlAlterTableDropConstraint) {
            return alterSchemaConverter.convertAlterSchema(
                    (SqlAlterTableDropConstraint) sqlAlterTable, resolvedCatalogTable);
        } else if (sqlAlterTable instanceof SqlAlterTableDropWatermark) {
            return alterSchemaConverter.convertAlterSchema(
                    (SqlAlterTableDropWatermark) sqlAlterTable, resolvedCatalogTable);
        } else if (sqlAlterTable instanceof SqlAlterTableRenameColumn) {
            return alterSchemaConverter.convertAlterSchema(
                    (SqlAlterTableRenameColumn) sqlAlterTable, resolvedCatalogTable);
        } else if (sqlAlterTable instanceof SqlAlterTableCompact) {
            return convertAlterTableCompact(
                    tableIdentifier,
                    optionalCatalogTable.get(),
                    (SqlAlterTableCompact) sqlAlterTable);
        } else if (sqlAlterTable instanceof SqlAlterTableSchema) {
            return alterSchemaConverter.convertAlterSchema(
                    (SqlAlterTableSchema) sqlAlterTable, resolvedCatalogTable);
        } else {
            throw new ValidationException(
                    String.format(
                            "[%s] needs to implement",
                            sqlAlterTable.toSqlString(CalciteSqlDialect.DEFAULT)));
        }
    }

    private Operation convertAlterTableOptions(
            ObjectIdentifier tableIdentifier,
            CatalogTable oldTable,
            SqlAlterTableOptions alterTableOptions) {
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

    /**
     * Convert `ALTER TABLE ... COMPACT` operation to {@link ModifyOperation} for Flink's managed
     * table to trigger a compaction batch job.
     */
    private ModifyOperation convertAlterTableCompact(
            ObjectIdentifier tableIdentifier,
            ContextResolvedTable contextResolvedTable,
            SqlAlterTableCompact alterTableCompact) {
        Catalog catalog = catalogManager.getCatalog(tableIdentifier.getCatalogName()).orElse(null);
        ResolvedCatalogTable resolvedCatalogTable = contextResolvedTable.getResolvedTable();

        if (ManagedTableListener.isManagedTable(catalog, resolvedCatalogTable)) {
            Map<String, String> partitionKVs = alterTableCompact.getPartitionKVs();
            CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(Collections.emptyMap());
            if (partitionKVs != null) {
                List<String> partitionKeys = resolvedCatalogTable.getPartitionKeys();
                Set<String> validPartitionKeySet = new HashSet<>(partitionKeys);
                String exMsg =
                        partitionKeys.isEmpty()
                                ? String.format("Table %s is not partitioned.", tableIdentifier)
                                : String.format(
                                        "Available ordered partition columns: [%s]",
                                        partitionKeys.stream()
                                                .collect(Collectors.joining("', '", "'", "'")));
                partitionKVs.forEach(
                        (partitionKey, partitionValue) -> {
                            if (!validPartitionKeySet.contains(partitionKey)) {
                                throw new ValidationException(
                                        String.format(
                                                "Partition column '%s' not defined in the table schema. %s",
                                                partitionKey, exMsg));
                            }
                        });
                partitionSpec = new CatalogPartitionSpec(partitionKVs);
            }
            Map<String, String> compactOptions =
                    catalogManager.resolveCompactManagedTableOptions(
                            resolvedCatalogTable, tableIdentifier, partitionSpec);
            QueryOperation child = new SourceQueryOperation(contextResolvedTable, compactOptions);
            return new SinkModifyOperation(
                    contextResolvedTable,
                    child,
                    partitionSpec.getPartitionSpec(),
                    null, // targetColumns
                    false,
                    compactOptions);
        }
        throw new ValidationException(
                String.format(
                        "ALTER TABLE COMPACT operation is not supported for non-managed table %s",
                        tableIdentifier));
    }

    /** Convert CREATE FUNCTION statement. */
    private Operation convertCreateFunction(SqlCreateFunction sqlCreateFunction) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateFunction.getFunctionIdentifier());
        List<ResourceUri> resourceUris = getFunctionResources(sqlCreateFunction.getResourceInfos());
        if (sqlCreateFunction.isSystemFunction()) {
            return new CreateTempSystemFunctionOperation(
                    unresolvedIdentifier.getObjectName(),
                    sqlCreateFunction.getFunctionClassName().getValueAs(String.class),
                    sqlCreateFunction.isIfNotExists(),
                    parseLanguage(sqlCreateFunction.getFunctionLanguage()),
                    resourceUris);
        } else {
            FunctionLanguage language = parseLanguage(sqlCreateFunction.getFunctionLanguage());
            CatalogFunction catalogFunction =
                    new CatalogFunctionImpl(
                            sqlCreateFunction.getFunctionClassName().getValueAs(String.class),
                            language,
                            resourceUris);
            ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

            return new CreateCatalogFunctionOperation(
                    identifier,
                    catalogFunction,
                    sqlCreateFunction.isIfNotExists(),
                    sqlCreateFunction.isTemporary());
        }
    }

    private List<ResourceUri> getFunctionResources(List<SqlNode> sqlResources) {
        return sqlResources.stream()
                .map(SqlResource.class::cast)
                .map(
                        sqlResource -> {
                            // get resource type
                            SqlResourceType sqlResourceType =
                                    sqlResource.getResourceType().getValueAs(SqlResourceType.class);
                            ResourceType resourceType;
                            switch (sqlResourceType) {
                                case FILE:
                                    resourceType = ResourceType.FILE;
                                    break;
                                case JAR:
                                    resourceType = ResourceType.JAR;
                                    break;
                                case ARCHIVE:
                                    resourceType = ResourceType.ARCHIVE;
                                    break;
                                default:
                                    throw new ValidationException(
                                            String.format(
                                                    "Unsupported resource type: .",
                                                    sqlResourceType));
                            }
                            // get resource path
                            String path = sqlResource.getResourcePath().getValueAs(String.class);
                            return new ResourceUri(resourceType, path);
                        })
                .collect(Collectors.toList());
    }

    /** Convert ALTER FUNCTION statement. */
    private Operation convertAlterFunction(SqlAlterFunction sqlAlterFunction) {
        if (sqlAlterFunction.isSystemFunction()) {
            throw new ValidationException("Alter temporary system function is not supported");
        }

        FunctionLanguage language = parseLanguage(sqlAlterFunction.getFunctionLanguage());
        CatalogFunction catalogFunction =
                new CatalogFunctionImpl(
                        sqlAlterFunction.getFunctionClassName().getValueAs(String.class), language);

        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterFunction.getFunctionIdentifier());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        return new AlterCatalogFunctionOperation(
                identifier,
                catalogFunction,
                sqlAlterFunction.isIfExists(),
                sqlAlterFunction.isTemporary());
    }

    /** Convert DROP FUNCTION statement. */
    private Operation convertDropFunction(SqlDropFunction sqlDropFunction) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlDropFunction.getFunctionIdentifier());
        if (sqlDropFunction.isSystemFunction()) {
            return new DropTempSystemFunctionOperation(
                    unresolvedIdentifier.getObjectName(), sqlDropFunction.getIfExists());
        } else {
            ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

            return new DropCatalogFunctionOperation(
                    identifier, sqlDropFunction.getIfExists(), sqlDropFunction.isTemporary());
        }
    }

    /**
     * Converts language string to the FunctionLanguage.
     *
     * @param languageString the language string from SQL parser
     * @return supported FunctionLanguage otherwise raise UnsupportedOperationException.
     * @throws UnsupportedOperationException if the languageString is not parsable or language is
     *     not supported
     */
    private FunctionLanguage parseLanguage(String languageString) {
        if (StringUtils.isNullOrWhitespaceOnly(languageString)) {
            return FunctionLanguage.JAVA;
        }

        FunctionLanguage language;
        try {
            language = FunctionLanguage.valueOf(languageString);
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException(
                    String.format("Unrecognized function language string %s", languageString), e);
        }

        return language;
    }

    /** Convert statement set into statement. */
    private StatementSetOperation convertSqlStatementSet(SqlStatementSet statementSet) {
        return new StatementSetOperation(
                statementSet.getInserts().stream()
                        .map(this::convertSqlInsert)
                        .map(op -> (ModifyOperation) op)
                        .collect(Collectors.toList()));
    }

    /** Convert insert into statement. */
    private Operation convertSqlInsert(RichSqlInsert insert) {
        // Get sink table name.
        List<String> targetTablePath = ((SqlIdentifier) insert.getTargetTableID()).names;
        // Get sink table hints.
        HintStrategyTable hintStrategyTable =
                flinkPlanner.config().getSqlToRelConverterConfig().getHintStrategyTable();
        List<RelHint> tableHints = SqlUtil.getRelHint(hintStrategyTable, insert.getTableHints());
        Map<String, String> dynamicOptions = FlinkHints.getHintedOptions(tableHints);

        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(targetTablePath);
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        ContextResolvedTable contextResolvedTable = catalogManager.getTableOrError(identifier);

        PlannerQueryOperation query =
                (PlannerQueryOperation)
                        convertValidatedSqlNodeOrFail(
                                flinkPlanner, catalogManager, insert.getSource());
        // TODO calc target column list to index array, currently only simple SqlIdentifiers are
        // available, this should be updated after FLINK-31301 fixed
        int[][] columnIndices =
                getTargetColumnIndices(contextResolvedTable, insert.getTargetColumnList());

        return new SinkModifyOperation(
                contextResolvedTable,
                query,
                insert.getStaticPartitionKVs(),
                columnIndices,
                insert.isOverwrite(),
                dynamicOptions);
    }

    /** Convert BEGIN STATEMENT SET statement. */
    private Operation convertBeginStatementSet(SqlBeginStatementSet sqlBeginStatementSet) {
        return new BeginStatementSetOperation();
    }

    /** Convert END statement. */
    private Operation convertEndStatementSet(SqlEndStatementSet sqlEndStatementSet) {
        return new EndStatementSetOperation();
    }

    /** Convert use catalog statement. */
    private Operation convertUseCatalog(SqlUseCatalog useCatalog) {
        return new UseCatalogOperation(useCatalog.catalogName());
    }

    /** Convert DROP CATALOG statement. */
    private Operation convertDropCatalog(SqlDropCatalog sqlDropCatalog) {
        String catalogName = sqlDropCatalog.catalogName();
        return new DropCatalogOperation(catalogName, sqlDropCatalog.getIfExists());
    }

    /** Convert use database statement. */
    private Operation convertUseDatabase(SqlUseDatabase useDatabase) {
        String[] fullDatabaseName = useDatabase.fullDatabaseName();
        if (fullDatabaseName.length > 2) {
            throw new ValidationException("use database identifier format error");
        }
        String catalogName =
                fullDatabaseName.length == 2
                        ? fullDatabaseName[0]
                        : catalogManager.getCurrentCatalog();
        String databaseName =
                fullDatabaseName.length == 2 ? fullDatabaseName[1] : fullDatabaseName[0];
        return new UseDatabaseOperation(catalogName, databaseName);
    }

    /** Convert CREATE DATABASE statement. */
    private Operation convertCreateDatabase(SqlCreateDatabase sqlCreateDatabase) {
        String[] fullDatabaseName = sqlCreateDatabase.fullDatabaseName();
        if (fullDatabaseName.length > 2) {
            throw new ValidationException("create database identifier format error");
        }
        String catalogName =
                (fullDatabaseName.length == 1)
                        ? catalogManager.getCurrentCatalog()
                        : fullDatabaseName[0];
        String databaseName =
                (fullDatabaseName.length == 1) ? fullDatabaseName[0] : fullDatabaseName[1];
        boolean ignoreIfExists = sqlCreateDatabase.isIfNotExists();
        String databaseComment =
                sqlCreateDatabase
                        .getComment()
                        .map(comment -> comment.getValueAs(NlsString.class).getValue())
                        .orElse(null);
        // set with properties
        Map<String, String> properties = new HashMap<>();
        sqlCreateDatabase
                .getPropertyList()
                .getList()
                .forEach(
                        p ->
                                properties.put(
                                        ((SqlTableOption) p).getKeyString(),
                                        ((SqlTableOption) p).getValueString()));
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(properties, databaseComment);
        return new CreateDatabaseOperation(
                catalogName, databaseName, catalogDatabase, ignoreIfExists);
    }

    /** Convert DROP DATABASE statement. */
    private Operation convertDropDatabase(SqlDropDatabase sqlDropDatabase) {
        String[] fullDatabaseName = sqlDropDatabase.fullDatabaseName();
        if (fullDatabaseName.length > 2) {
            throw new ValidationException("drop database identifier format error");
        }
        String catalogName =
                (fullDatabaseName.length == 1)
                        ? catalogManager.getCurrentCatalog()
                        : fullDatabaseName[0];
        String databaseName =
                (fullDatabaseName.length == 1) ? fullDatabaseName[0] : fullDatabaseName[1];
        return new DropDatabaseOperation(
                catalogName,
                databaseName,
                sqlDropDatabase.getIfExists(),
                sqlDropDatabase.isCascade());
    }

    /** Convert ALTER DATABASE statement. */
    private Operation convertAlterDatabase(SqlAlterDatabase sqlAlterDatabase) {
        String[] fullDatabaseName = sqlAlterDatabase.fullDatabaseName();
        if (fullDatabaseName.length > 2) {
            throw new ValidationException("alter database identifier format error");
        }
        String catalogName =
                (fullDatabaseName.length == 1)
                        ? catalogManager.getCurrentCatalog()
                        : fullDatabaseName[0];
        String databaseName =
                (fullDatabaseName.length == 1) ? fullDatabaseName[0] : fullDatabaseName[1];
        final Map<String, String> properties;
        CatalogDatabase originCatalogDatabase;
        Optional<Catalog> catalog = catalogManager.getCatalog(catalogName);
        if (catalog.isPresent()) {
            try {
                originCatalogDatabase = catalog.get().getDatabase(databaseName);
                properties = new HashMap<>(originCatalogDatabase.getProperties());
            } catch (DatabaseNotExistException e) {
                throw new ValidationException(
                        String.format("Database %s not exists", databaseName), e);
            }
        } else {
            throw new ValidationException(String.format("Catalog %s not exists", catalogName));
        }
        // set with properties
        sqlAlterDatabase
                .getPropertyList()
                .getList()
                .forEach(
                        p ->
                                properties.put(
                                        ((SqlTableOption) p).getKeyString(),
                                        ((SqlTableOption) p).getValueString()));
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(properties, originCatalogDatabase.getComment());
        return new AlterDatabaseOperation(catalogName, databaseName, catalogDatabase);
    }

    /** Convert SHOW CATALOGS statement. */
    private Operation convertShowCatalogs(SqlShowCatalogs sqlShowCatalogs) {
        return new ShowCatalogsOperation();
    }

    /** Convert SHOW CURRENT CATALOG statement. */
    private Operation convertShowCurrentCatalog(SqlShowCurrentCatalog sqlShowCurrentCatalog) {
        return new ShowCurrentCatalogOperation();
    }

    /** Convert SHOW DATABASES statement. */
    private Operation convertShowDatabases(SqlShowDatabases sqlShowDatabases) {
        return new ShowDatabasesOperation();
    }

    /** Convert SHOW CURRENT DATABASE statement. */
    private Operation convertShowCurrentDatabase(SqlShowCurrentDatabase sqlShowCurrentDatabase) {
        return new ShowCurrentDatabaseOperation();
    }

    /** Convert SHOW TABLES statement. */
    private Operation convertShowTables(SqlShowTables sqlShowTables) {
        if (sqlShowTables.getPreposition() == null) {
            return new ShowTablesOperation(
                    sqlShowTables.getLikeSqlPattern(),
                    sqlShowTables.isWithLike(),
                    sqlShowTables.isNotLike());
        }
        String[] fullDatabaseName = sqlShowTables.fullDatabaseName();
        if (fullDatabaseName.length > 2) {
            throw new ValidationException(
                    String.format(
                            "show tables from/in identifier [ %s ] format error",
                            String.join(".", fullDatabaseName)));
        }
        String catalogName =
                (fullDatabaseName.length == 1)
                        ? catalogManager.getCurrentCatalog()
                        : fullDatabaseName[0];
        String databaseName =
                (fullDatabaseName.length == 1) ? fullDatabaseName[0] : fullDatabaseName[1];
        return new ShowTablesOperation(
                catalogName,
                databaseName,
                sqlShowTables.getLikeSqlPattern(),
                sqlShowTables.isWithLike(),
                sqlShowTables.isNotLike(),
                sqlShowTables.getPreposition());
    }

    /** Convert SHOW COLUMNS statement. */
    private Operation convertShowColumns(SqlShowColumns sqlShowColumns) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlShowColumns.fullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        return new ShowColumnsOperation(
                identifier,
                sqlShowColumns.getLikeSqlPattern(),
                sqlShowColumns.isWithLike(),
                sqlShowColumns.isNotLike(),
                sqlShowColumns.getPreposition());
    }

    /** Convert SHOW CREATE TABLE statement. */
    private Operation convertShowCreateTable(SqlShowCreateTable sqlShowCreateTable) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlShowCreateTable.getFullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        return new ShowCreateTableOperation(identifier);
    }

    /** Convert SHOW CREATE VIEW statement. */
    private Operation convertShowCreateView(SqlShowCreateView sqlShowCreateView) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlShowCreateView.getFullViewName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        return new ShowCreateViewOperation(identifier);
    }

    /** Convert DROP VIEW statement. */
    private Operation convertDropView(SqlDropView sqlDropView) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlDropView.fullViewName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        return new DropViewOperation(
                identifier, sqlDropView.getIfExists(), sqlDropView.isTemporary());
    }

    /** Convert SHOW VIEWS statement. */
    private Operation convertShowViews(SqlShowViews sqlShowViews) {
        return new ShowViewsOperation();
    }

    /** Convert RICH EXPLAIN statement. */
    private Operation convertRichExplain(SqlRichExplain sqlExplain) {
        SqlNode sqlNode = sqlExplain.getStatement();
        Operation operation;
        if (sqlNode instanceof RichSqlInsert) {
            operation = convertSqlInsert((RichSqlInsert) sqlNode);
        } else if (sqlNode instanceof SqlStatementSet) {
            operation = convertSqlStatementSet((SqlStatementSet) sqlNode);
        } else if (sqlNode.getKind().belongsTo(SqlKind.QUERY)) {
            operation = convertSqlQuery(sqlExplain.getStatement());
        } else {
            throw new ValidationException(
                    String.format("EXPLAIN statement doesn't support %s", sqlNode.getKind()));
        }
        return new ExplainOperation(operation, sqlExplain.getExplainDetails());
    }

    /** Convert DESCRIBE [EXTENDED] [[catalogName.] dataBasesName].sqlIdentifier. */
    private Operation convertDescribeTable(SqlRichDescribeTable sqlRichDescribeTable) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlRichDescribeTable.fullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        return new DescribeTableOperation(identifier, sqlRichDescribeTable.isExtended());
    }

    /** Convert LOAD MODULE statement. */
    private Operation convertLoadModule(SqlLoadModule sqlLoadModule) {
        String moduleName = sqlLoadModule.moduleName();
        Map<String, String> properties = new HashMap<>();
        for (SqlNode node : sqlLoadModule.getPropertyList().getList()) {
            SqlTableOption option = (SqlTableOption) node;
            properties.put(option.getKeyString(), option.getValueString());
        }
        return new LoadModuleOperation(moduleName, properties);
    }

    private Operation convertAddJar(SqlAddJar sqlAddJar) {
        return new AddJarOperation(sqlAddJar.getPath());
    }

    private Operation convertRemoveJar(SqlRemoveJar sqlRemoveJar) {
        return new RemoveJarOperation(sqlRemoveJar.getPath());
    }

    private Operation convertShowJars(SqlShowJars sqlShowJars) {
        return new ShowJarsOperation();
    }

    /** Convert UNLOAD MODULE statement. */
    private Operation convertUnloadModule(SqlUnloadModule sqlUnloadModule) {
        String moduleName = sqlUnloadModule.moduleName();
        return new UnloadModuleOperation(moduleName);
    }

    /** Convert USE MODULES statement. */
    private Operation convertUseModules(SqlUseModules sqlUseModules) {
        return new UseModulesOperation(sqlUseModules.moduleNames());
    }

    /** Convert SHOW [FULL] MODULES statement. */
    private Operation convertShowModules(SqlShowModules sqlShowModules) {
        return new ShowModulesOperation(sqlShowModules.requireFull());
    }

    /** Convert SET ['key' = 'value']. */
    private Operation convertSet(SqlSet sqlSet) {
        if (sqlSet.getKey() == null && sqlSet.getValue() == null) {
            return new SetOperation();
        } else {
            return new SetOperation(sqlSet.getKeyString(), sqlSet.getValueString());
        }
    }

    /** Convert RESET ['key']. */
    private Operation convertReset(SqlReset sqlReset) {
        return new ResetOperation(sqlReset.getKeyString());
    }

    /** Fallback method for sql query. */
    private Operation convertSqlQuery(SqlNode node) {
        return toQueryOperation(flinkPlanner, node);
    }

    private Operation convertExecutePlan(SqlExecutePlan sqlExecutePlan) {
        return new ExecutePlanOperation(sqlExecutePlan.getPlanFile());
    }

    private Operation convertCompilePlan(SqlCompilePlan compilePlan) {
        return new CompilePlanOperation(
                compilePlan.getPlanFile(),
                compilePlan.isIfNotExists(),
                convertValidatedSqlNodeOrFail(
                        flinkPlanner, catalogManager, compilePlan.getOperandList().get(0)));
    }

    private Operation convertCompileAndExecutePlan(SqlCompileAndExecutePlan compileAndExecutePlan) {
        return new CompileAndExecutePlanOperation(
                compileAndExecutePlan.getPlanFile(),
                convertValidatedSqlNodeOrFail(
                        flinkPlanner,
                        catalogManager,
                        compileAndExecutePlan.getOperandList().get(0)));
    }

    private Operation convertAnalyzeTable(SqlAnalyzeTable analyzeTable) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(analyzeTable.fullTableName());
        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        Optional<ContextResolvedTable> optionalCatalogTable =
                catalogManager.getTable(tableIdentifier);
        if (!optionalCatalogTable.isPresent() || optionalCatalogTable.get().isTemporary()) {
            throw new ValidationException(
                    String.format(
                            "Table %s doesn't exist or is a temporary table.", tableIdentifier));
        }
        CatalogBaseTable baseTable = optionalCatalogTable.get().getResolvedTable();
        if (baseTable instanceof CatalogView) {
            throw new ValidationException("ANALYZE TABLE for a view is not allowed.");
        }
        CatalogTable table = (CatalogTable) baseTable;
        ResolvedSchema schema =
                baseTable.getUnresolvedSchema().resolve(catalogManager.getSchemaResolver());

        LinkedHashMap<String, String> partitions = analyzeTable.getPartitions();
        List<CatalogPartitionSpec> targetPartitionSpecs = null;
        if (table.isPartitioned()) {
            if (!partitions.keySet().equals(new HashSet<>(table.getPartitionKeys()))) {
                throw new ValidationException(
                        String.format(
                                "Invalid ANALYZE TABLE statement. For partition table, all partition keys should be specified explicitly. "
                                        + "The given partition keys: [%s] are not match the target partition keys: [%s].",
                                String.join(",", partitions.keySet()),
                                String.join(",", table.getPartitionKeys())));
            }

            try {
                targetPartitionSpecs = getPartitionSpecs(tableIdentifier, schema, partitions);
            } catch (Exception e) {
                throw new ValidationException(e.getMessage(), e);
            }
        } else if (!partitions.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Invalid ANALYZE TABLE statement. Table: %s is not a partition table, while partition values are given.",
                            tableIdentifier));
        }

        String[] columns = analyzeTable.getColumnNames();
        List<Column> targetColumns;
        if (analyzeTable.isAllColumns()) {
            Preconditions.checkArgument(columns.length == 0);
            // computed column and metadata column will be ignored
            targetColumns =
                    schema.getColumns().stream()
                            .filter(Column::isPhysical)
                            .collect(Collectors.toList());
        } else if (columns.length > 0) {
            targetColumns =
                    Arrays.stream(columns)
                            .map(
                                    c -> {
                                        Optional<Column> colOpt = schema.getColumn(c);
                                        if (!colOpt.isPresent()) {
                                            throw new ValidationException(
                                                    String.format(
                                                            "Column: %s does not exist in the table: %s.",
                                                            c, tableIdentifier));
                                        }
                                        Column col = colOpt.get();
                                        if (col instanceof Column.ComputedColumn) {
                                            throw new ValidationException(
                                                    String.format(
                                                            "Column: %s is a computed column, ANALYZE TABLE does not support computed column.",
                                                            c));
                                        } else if (col instanceof Column.MetadataColumn) {
                                            throw new ValidationException(
                                                    String.format(
                                                            "Column: %s is a metadata column, ANALYZE TABLE does not support metadata column.",
                                                            c));
                                        } else if (col instanceof Column.PhysicalColumn) {
                                            return col;
                                        } else {
                                            throw new ValidationException(
                                                    "Unknown column class: "
                                                            + col.getClass().getSimpleName());
                                        }
                                    })
                            .collect(Collectors.toList());
        } else {
            targetColumns = Collections.emptyList();
        }

        return new AnalyzeTableOperation(tableIdentifier, targetPartitionSpecs, targetColumns);
    }

    private List<CatalogPartitionSpec> getPartitionSpecs(
            ObjectIdentifier tableIdentifier,
            ResolvedSchema schema,
            LinkedHashMap<String, String> partitions)
            throws TableNotPartitionedException, TableNotExistException {
        List<Expression> filters = new ArrayList<>();
        for (Map.Entry<String, String> entry : partitions.entrySet()) {
            if (entry.getValue() != null) {
                CallExpression call =
                        CallExpression.temporary(
                                FunctionIdentifier.of("="),
                                BuiltInFunctionDefinitions.EQUALS,
                                Arrays.asList(
                                        getPartitionKeyExpr(schema, entry.getKey()),
                                        getPartitionValueExpr(
                                                schema, entry.getKey(), entry.getValue())),
                                DataTypes.BOOLEAN());
                filters.add(call);
            }
        }
        if (filters.isEmpty()) {
            return catalogManager
                    .getCatalog(tableIdentifier.getCatalogName())
                    .get()
                    .listPartitions(tableIdentifier.toObjectPath());
        } else {
            return catalogManager
                    .getCatalog(tableIdentifier.getCatalogName())
                    .get()
                    .listPartitionsByFilter(tableIdentifier.toObjectPath(), filters);
        }
    }

    private FieldReferenceExpression getPartitionKeyExpr(
            ResolvedSchema schema, String partitionKey) {
        int fieldIndex = schema.getColumnNames().indexOf(partitionKey);
        if (fieldIndex < 0) {
            throw new ValidationException(
                    String.format(
                            "Partition: %s does not exist in the schema: %s",
                            partitionKey, schema.getColumnNames()));
        }
        return new FieldReferenceExpression(
                partitionKey, schema.getColumnDataTypes().get(fieldIndex), 0, fieldIndex);
    }

    private ValueLiteralExpression getPartitionValueExpr(
            ResolvedSchema schema, String partitionKey, String partitionValue) {
        int fieldIndex = schema.getColumnNames().indexOf(partitionKey);
        if (fieldIndex < 0) {
            throw new ValidationException(
                    String.format(
                            "Partition: %s does not exist in the schema: %s",
                            partitionKey, schema.getColumnNames()));
        }
        DataType dataType = schema.getColumnDataTypes().get(fieldIndex);
        if (partitionValue == null) {
            return new ValueLiteralExpression(null, dataType.nullable());
        }
        Object value;
        switch (dataType.getLogicalType().getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                value = partitionValue;
                break;
            case TINYINT:
                value = Byte.valueOf(partitionValue);
                break;
            case SMALLINT:
                value = Short.valueOf(partitionValue);
                break;
            case INTEGER:
                value = Integer.valueOf(partitionValue);
                break;
            case BIGINT:
                value = Long.valueOf(partitionValue);
                break;
            case FLOAT:
                value = Float.valueOf(partitionValue);
                break;
            case DOUBLE:
                value = Double.valueOf(partitionValue);
                break;
            case DECIMAL:
                value = new BigDecimal(partitionValue);
                break;
            case DATE:
                value = Date.valueOf(partitionValue);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                value = Time.valueOf(partitionValue);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                value = Timestamp.valueOf(partitionValue);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported partition value type: " + dataType.getLogicalType());
        }
        return new ValueLiteralExpression(value, dataType.notNull());
    }

    private Operation convertShowJobs(SqlShowJobs sqlStopJob) {
        return new ShowJobsOperation();
    }

    private Operation convertStopJob(SqlStopJob sqlStopJob) {
        return new StopJobOperation(
                sqlStopJob.getId(), sqlStopJob.isWithSavepoint(), sqlStopJob.isWithDrain());
    }

    private Operation convertDelete(SqlDelete sqlDelete) {
        // set it's delete
        RowLevelModificationContextUtils.setModificationType(
                SupportsRowLevelModificationScan.RowLevelModificationType.DELETE);
        RelRoot deleteRelational = flinkPlanner.rel(sqlDelete);
        LogicalTableModify tableModify = (LogicalTableModify) deleteRelational.rel;
        UnresolvedIdentifier unresolvedTableIdentifier =
                UnresolvedIdentifier.of(tableModify.getTable().getQualifiedName());
        ContextResolvedTable contextResolvedTable =
                catalogManager.getTableOrError(
                        catalogManager.qualifyIdentifier(unresolvedTableIdentifier));
        // try push down delete
        Optional<DynamicTableSink> optionalDynamicTableSink =
                DeletePushDownUtils.getDynamicTableSink(
                        contextResolvedTable, tableModify, catalogManager);
        if (optionalDynamicTableSink.isPresent()) {
            DynamicTableSink dynamicTableSink = optionalDynamicTableSink.get();
            // if the table sink supports delete push down
            if (dynamicTableSink instanceof SupportsDeletePushDown) {
                SupportsDeletePushDown supportsDeletePushDownSink =
                        (SupportsDeletePushDown) dynamicTableSink;
                // get resolved filter expression
                Optional<List<ResolvedExpression>> filters =
                        DeletePushDownUtils.getResolvedFilterExpressions(tableModify);
                if (filters.isPresent()
                        && supportsDeletePushDownSink.applyDeleteFilters(filters.get())) {
                    return new DeleteFromFilterOperation(
                            contextResolvedTable, supportsDeletePushDownSink, filters.get());
                }
            }
        }
        // delete push down is not applicable, use row-level delete
        PlannerQueryOperation queryOperation = new PlannerQueryOperation(tableModify);
        return new SinkModifyOperation(
                contextResolvedTable,
                queryOperation,
                null, // targetColumns
                SinkModifyOperation.ModifyType.DELETE);
    }

    private Operation convertUpdate(SqlUpdate sqlUpdate) {
        // set it's update
        RowLevelModificationContextUtils.setModificationType(
                SupportsRowLevelModificationScan.RowLevelModificationType.UPDATE);
        RelRoot updateRelational = flinkPlanner.rel(sqlUpdate);
        // get target sink table
        LogicalTableModify tableModify = (LogicalTableModify) updateRelational.rel;
        UnresolvedIdentifier unresolvedTableIdentifier =
                UnresolvedIdentifier.of(tableModify.getTable().getQualifiedName());
        ContextResolvedTable contextResolvedTable =
                catalogManager.getTableOrError(
                        catalogManager.qualifyIdentifier(unresolvedTableIdentifier));
        // get query
        PlannerQueryOperation queryOperation = new PlannerQueryOperation(tableModify);

        // TODO calc target column list to index array, currently only simple SqlIdentifiers are
        // available, this should be updated after FLINK-31344 fixed
        int[][] columnIndices =
                getTargetColumnIndices(contextResolvedTable, sqlUpdate.getTargetColumnList());

        return new SinkModifyOperation(
                contextResolvedTable,
                queryOperation,
                columnIndices,
                SinkModifyOperation.ModifyType.UPDATE);
    }

    private int[][] getTargetColumnIndices(
            @Nonnull ContextResolvedTable contextResolvedTable,
            @Nullable SqlNodeList targetColumns) {
        List<String> allColumns = contextResolvedTable.getResolvedSchema().getColumnNames();
        return Optional.ofNullable(targetColumns).orElse(SqlNodeList.EMPTY).stream()
                .mapToInt(c -> allColumns.indexOf(((SqlIdentifier) c).getSimple()))
                .mapToObj(idx -> new int[] {idx})
                .toArray(int[][]::new);
    }

    private String getQuotedSqlString(SqlNode sqlNode) {
        SqlParser.Config parserConfig = flinkPlanner.config().getParserConfig();
        SqlDialect dialect =
                new CalciteSqlDialect(
                        SqlDialect.EMPTY_CONTEXT
                                .withQuotedCasing(parserConfig.unquotedCasing())
                                .withConformance(parserConfig.conformance())
                                .withUnquotedCasing(parserConfig.unquotedCasing())
                                .withIdentifierQuoteString(parserConfig.quoting().string));
        return sqlNode.toSqlString(dialect).getSql();
    }

    private PlannerQueryOperation toQueryOperation(FlinkPlannerImpl planner, SqlNode validated) {
        // transform to a relational tree
        RelRoot relational = planner.rel(validated);
        return new PlannerQueryOperation(relational.project());
    }
}
