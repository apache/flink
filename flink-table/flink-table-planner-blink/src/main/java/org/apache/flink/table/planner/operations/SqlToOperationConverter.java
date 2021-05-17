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

import org.apache.flink.sql.parser.ddl.SqlAddPartitions;
import org.apache.flink.sql.parser.ddl.SqlAddReplaceColumns;
import org.apache.flink.sql.parser.ddl.SqlAlterDatabase;
import org.apache.flink.sql.parser.ddl.SqlAlterFunction;
import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlAlterTableAddConstraint;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropConstraint;
import org.apache.flink.sql.parser.ddl.SqlAlterTableOptions;
import org.apache.flink.sql.parser.ddl.SqlAlterTableRename;
import org.apache.flink.sql.parser.ddl.SqlAlterView;
import org.apache.flink.sql.parser.ddl.SqlAlterViewAs;
import org.apache.flink.sql.parser.ddl.SqlAlterViewProperties;
import org.apache.flink.sql.parser.ddl.SqlAlterViewRename;
import org.apache.flink.sql.parser.ddl.SqlChangeColumn;
import org.apache.flink.sql.parser.ddl.SqlCreateCatalog;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlDropCatalog;
import org.apache.flink.sql.parser.ddl.SqlDropDatabase;
import org.apache.flink.sql.parser.ddl.SqlDropFunction;
import org.apache.flink.sql.parser.ddl.SqlDropPartitions;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlDropView;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlUseCatalog;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.ddl.SqlUseModules;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dml.SqlBeginStatementSet;
import org.apache.flink.sql.parser.dml.SqlEndStatementSet;
import org.apache.flink.sql.parser.dql.SqlLoadModule;
import org.apache.flink.sql.parser.dql.SqlRichDescribeTable;
import org.apache.flink.sql.parser.dql.SqlRichExplain;
import org.apache.flink.sql.parser.dql.SqlShowCatalogs;
import org.apache.flink.sql.parser.dql.SqlShowCreateTable;
import org.apache.flink.sql.parser.dql.SqlShowCurrentCatalog;
import org.apache.flink.sql.parser.dql.SqlShowCurrentDatabase;
import org.apache.flink.sql.parser.dql.SqlShowDatabases;
import org.apache.flink.sql.parser.dql.SqlShowFunctions;
import org.apache.flink.sql.parser.dql.SqlShowModules;
import org.apache.flink.sql.parser.dql.SqlShowPartitions;
import org.apache.flink.sql.parser.dql.SqlShowTables;
import org.apache.flink.sql.parser.dql.SqlShowViews;
import org.apache.flink.sql.parser.dql.SqlUnloadModule;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
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
import org.apache.flink.table.catalog.CatalogViewImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.DescribeTableOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.LoadModuleOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowCatalogsOperation;
import org.apache.flink.table.operations.ShowCreateTableOperation;
import org.apache.flink.table.operations.ShowCurrentCatalogOperation;
import org.apache.flink.table.operations.ShowCurrentDatabaseOperation;
import org.apache.flink.table.operations.ShowDatabasesOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation.FunctionScope;
import org.apache.flink.table.operations.ShowModulesOperation;
import org.apache.flink.table.operations.ShowPartitionsOperation;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.ShowViewsOperation;
import org.apache.flink.table.operations.UnloadModuleOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.UseModulesOperation;
import org.apache.flink.table.operations.ddl.AddPartitionsOperation;
import org.apache.flink.table.operations.ddl.AlterCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterPartitionPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableAddConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTableDropConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTableOptionsOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.AlterViewAsOperation;
import org.apache.flink.table.operations.ddl.AlterViewPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterViewRenameOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.operations.ddl.DropCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.DropCatalogOperation;
import org.apache.flink.table.operations.ddl.DropDatabaseOperation;
import org.apache.flink.table.operations.ddl.DropPartitionsOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.operations.ddl.DropTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.DropViewOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.utils.Expander;
import org.apache.flink.table.planner.utils.OperationConverterUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.StringUtils;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
public class SqlToOperationConverter {
    private final FlinkPlannerImpl flinkPlanner;
    private final CatalogManager catalogManager;
    private final SqlCreateTableConverter createTableConverter;

    // ~ Constructors -----------------------------------------------------------

    private SqlToOperationConverter(FlinkPlannerImpl flinkPlanner, CatalogManager catalogManager) {
        this.flinkPlanner = flinkPlanner;
        this.catalogManager = catalogManager;
        this.createTableConverter =
                new SqlCreateTableConverter(
                        flinkPlanner.getOrCreateSqlValidator(),
                        catalogManager,
                        this::getQuotedSqlString,
                        this::validateTableConstraint);
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
        SqlToOperationConverter converter =
                new SqlToOperationConverter(flinkPlanner, catalogManager);
        if (validated instanceof SqlCreateCatalog) {
            return Optional.of(converter.convertCreateCatalog((SqlCreateCatalog) validated));
        } else if (validated instanceof SqlDropCatalog) {
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
            return Optional.of(
                    converter.createTableConverter.convertCreateTable((SqlCreateTable) validated));
        } else if (validated instanceof SqlDropTable) {
            return Optional.of(converter.convertDropTable((SqlDropTable) validated));
        } else if (validated instanceof SqlAlterTable) {
            return Optional.of(converter.convertAlterTable((SqlAlterTable) validated));
        } else if (validated instanceof SqlShowTables) {
            return Optional.of(converter.convertShowTables((SqlShowTables) validated));
        } else if (validated instanceof SqlCreateView) {
            return Optional.of(converter.convertCreateView((SqlCreateView) validated));
        } else if (validated instanceof SqlDropView) {
            return Optional.of(converter.convertDropView((SqlDropView) validated));
        } else if (validated instanceof SqlAlterView) {
            return Optional.of(converter.convertAlterView((SqlAlterView) validated));
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
        } else if (validated instanceof SqlShowFunctions) {
            return Optional.of(converter.convertShowFunctions((SqlShowFunctions) validated));
        } else if (validated instanceof SqlShowPartitions) {
            return Optional.of(converter.convertShowPartitions((SqlShowPartitions) validated));
        } else if (validated instanceof SqlRichExplain) {
            return Optional.of(converter.convertRichExplain((SqlRichExplain) validated));
        } else if (validated instanceof SqlRichDescribeTable) {
            return Optional.of(converter.convertDescribeTable((SqlRichDescribeTable) validated));
        } else if (validated instanceof RichSqlInsert) {
            return Optional.of(converter.convertSqlInsert((RichSqlInsert) validated));
        } else if (validated instanceof SqlBeginStatementSet) {
            return Optional.of(
                    converter.convertBeginStatementSet((SqlBeginStatementSet) validated));
        } else if (validated instanceof SqlEndStatementSet) {
            return Optional.of(converter.convertEndStatementSet((SqlEndStatementSet) validated));
        } else if (validated.getKind().belongsTo(SqlKind.QUERY)) {
            return Optional.of(converter.convertSqlQuery(validated));
        } else {
            return Optional.empty();
        }
    }

    // ~ Tools ------------------------------------------------------------------

    /** Convert DROP TABLE statement. */
    private Operation convertDropTable(SqlDropTable sqlDropTable) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlDropTable.fullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        return new DropTableOperation(
                identifier, sqlDropTable.getIfExists(), sqlDropTable.isTemporary());
    }

    /** convert ALTER VIEW statement. */
    private Operation convertAlterView(SqlAlterView alterView) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(alterView.fullViewName());
        ObjectIdentifier viewIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        Optional<CatalogManager.TableLookupResult> optionalCatalogTable =
                catalogManager.getTable(viewIdentifier);
        if (!optionalCatalogTable.isPresent() || optionalCatalogTable.get().isTemporary()) {
            throw new ValidationException(
                    String.format(
                            "View %s doesn't exist or is a temporary view.",
                            viewIdentifier.toString()));
        }
        CatalogBaseTable baseTable = optionalCatalogTable.get().getTable();
        if (baseTable instanceof CatalogTable) {
            throw new ValidationException("ALTER VIEW for a table is not allowed");
        }
        if (alterView instanceof SqlAlterViewRename) {
            UnresolvedIdentifier newUnresolvedIdentifier =
                    UnresolvedIdentifier.of(((SqlAlterViewRename) alterView).fullNewViewName());
            ObjectIdentifier newTableIdentifier =
                    catalogManager.qualifyIdentifier(newUnresolvedIdentifier);
            return new AlterViewRenameOperation(viewIdentifier, newTableIdentifier);
        } else if (alterView instanceof SqlAlterViewProperties) {
            SqlAlterViewProperties alterViewProperties = (SqlAlterViewProperties) alterView;
            CatalogView oldView = (CatalogView) baseTable;
            Map<String, String> newProperties = new HashMap<>(oldView.getOptions());
            newProperties.putAll(
                    OperationConverterUtils.extractProperties(
                            alterViewProperties.getPropertyList()));
            CatalogView newView =
                    new CatalogViewImpl(
                            oldView.getOriginalQuery(),
                            oldView.getExpandedQuery(),
                            oldView.getSchema(),
                            newProperties,
                            oldView.getComment());
            return new AlterViewPropertiesOperation(viewIdentifier, newView);
        } else if (alterView instanceof SqlAlterViewAs) {
            SqlAlterViewAs alterViewAs = (SqlAlterViewAs) alterView;
            final SqlNode newQuery = alterViewAs.getNewQuery();

            CatalogView oldView = (CatalogView) baseTable;
            CatalogView newView =
                    convertViewQuery(
                            newQuery,
                            Collections.emptyList(),
                            oldView.getOptions(),
                            oldView.getComment());
            return new AlterViewAsOperation(viewIdentifier, newView);
        } else {
            throw new ValidationException(
                    String.format(
                            "[%s] needs to implement",
                            alterView.toSqlString(CalciteSqlDialect.DEFAULT)));
        }
    }

    /** convert ALTER TABLE statement. */
    private Operation convertAlterTable(SqlAlterTable sqlAlterTable) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterTable.fullTableName());
        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        Optional<CatalogManager.TableLookupResult> optionalCatalogTable =
                catalogManager.getTable(tableIdentifier);
        if (!optionalCatalogTable.isPresent() || optionalCatalogTable.get().isTemporary()) {
            throw new ValidationException(
                    String.format(
                            "Table %s doesn't exist or is a temporary table.",
                            tableIdentifier.toString()));
        }
        CatalogBaseTable baseTable = optionalCatalogTable.get().getTable();
        if (baseTable instanceof CatalogView) {
            throw new ValidationException("ALTER TABLE for a view is not allowed");
        }
        if (sqlAlterTable instanceof SqlAlterTableRename) {
            UnresolvedIdentifier newUnresolvedIdentifier =
                    UnresolvedIdentifier.of(
                            ((SqlAlterTableRename) sqlAlterTable).fullNewTableName());
            ObjectIdentifier newTableIdentifier =
                    catalogManager.qualifyIdentifier(newUnresolvedIdentifier);
            return new AlterTableRenameOperation(tableIdentifier, newTableIdentifier);
        } else if (sqlAlterTable instanceof SqlAlterTableOptions) {
            return convertAlterTableOptions(
                    tableIdentifier,
                    (CatalogTable) baseTable,
                    (SqlAlterTableOptions) sqlAlterTable);
        } else if (sqlAlterTable instanceof SqlAlterTableAddConstraint) {
            SqlTableConstraint constraint =
                    ((SqlAlterTableAddConstraint) sqlAlterTable).getConstraint();
            validateTableConstraint(constraint);
            TableSchema oriSchema =
                    TableSchema.fromResolvedSchema(
                            baseTable
                                    .getUnresolvedSchema()
                                    .resolve(catalogManager.getSchemaResolver()));
            // Sanity check for constraint.
            TableSchema.Builder builder = TableSchemaUtils.builderWithGivenSchema(oriSchema);
            if (constraint.getConstraintName().isPresent()) {
                builder.primaryKey(
                        constraint.getConstraintName().get(), constraint.getColumnNames());
            } else {
                builder.primaryKey(constraint.getColumnNames());
            }
            builder.build();
            return new AlterTableAddConstraintOperation(
                    tableIdentifier,
                    constraint.getConstraintName().orElse(null),
                    constraint.getColumnNames());
        } else if (sqlAlterTable instanceof SqlAlterTableDropConstraint) {
            SqlAlterTableDropConstraint dropConstraint =
                    ((SqlAlterTableDropConstraint) sqlAlterTable);
            String constraintName = dropConstraint.getConstraintName().getSimple();
            TableSchema oriSchema =
                    TableSchema.fromResolvedSchema(
                            baseTable
                                    .getUnresolvedSchema()
                                    .resolve(catalogManager.getSchemaResolver()));
            if (!oriSchema
                    .getPrimaryKey()
                    .filter(pk -> pk.getName().equals(constraintName))
                    .isPresent()) {
                throw new ValidationException(
                        String.format("CONSTRAINT [%s] does not exist", constraintName));
            }
            return new AlterTableDropConstraintOperation(tableIdentifier, constraintName);
        } else if (sqlAlterTable instanceof SqlAddReplaceColumns) {
            return OperationConverterUtils.convertAddReplaceColumns(
                    tableIdentifier,
                    (SqlAddReplaceColumns) sqlAlterTable,
                    (CatalogTable) baseTable,
                    flinkPlanner.getOrCreateSqlValidator());
        } else if (sqlAlterTable instanceof SqlChangeColumn) {
            return OperationConverterUtils.convertChangeColumn(
                    tableIdentifier,
                    (SqlChangeColumn) sqlAlterTable,
                    (CatalogTable) baseTable,
                    flinkPlanner.getOrCreateSqlValidator());
        } else if (sqlAlterTable instanceof SqlAddPartitions) {
            List<CatalogPartitionSpec> specs = new ArrayList<>();
            List<CatalogPartition> partitions = new ArrayList<>();
            SqlAddPartitions addPartitions = (SqlAddPartitions) sqlAlterTable;
            for (int i = 0; i < addPartitions.getPartSpecs().size(); i++) {
                specs.add(new CatalogPartitionSpec(addPartitions.getPartitionKVs(i)));
                Map<String, String> props =
                        OperationConverterUtils.extractProperties(
                                addPartitions.getPartProps().get(i));
                partitions.add(new CatalogPartitionImpl(props, null));
            }
            return new AddPartitionsOperation(
                    tableIdentifier, addPartitions.ifNotExists(), specs, partitions);
        } else if (sqlAlterTable instanceof SqlDropPartitions) {
            SqlDropPartitions dropPartitions = (SqlDropPartitions) sqlAlterTable;
            List<CatalogPartitionSpec> specs = new ArrayList<>();
            for (int i = 0; i < dropPartitions.getPartSpecs().size(); i++) {
                specs.add(new CatalogPartitionSpec(dropPartitions.getPartitionKVs(i)));
            }
            return new DropPartitionsOperation(tableIdentifier, dropPartitions.ifExists(), specs);
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
            Map<String, String> newOptions = new HashMap<>(oldTable.getOptions());
            newOptions.putAll(
                    OperationConverterUtils.extractProperties(alterTableOptions.getPropertyList()));
            return new AlterTableOptionsOperation(tableIdentifier, oldTable.copy(newOptions));
        }
    }

    /** Convert CREATE FUNCTION statement. */
    private Operation convertCreateFunction(SqlCreateFunction sqlCreateFunction) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateFunction.getFunctionIdentifier());

        if (sqlCreateFunction.isSystemFunction()) {
            return new CreateTempSystemFunctionOperation(
                    unresolvedIdentifier.getObjectName(),
                    sqlCreateFunction.getFunctionClassName().getValueAs(String.class),
                    sqlCreateFunction.isIfNotExists(),
                    parseLanguage(sqlCreateFunction.getFunctionLanguage()));
        } else {
            FunctionLanguage language = parseLanguage(sqlCreateFunction.getFunctionLanguage());
            CatalogFunction catalogFunction =
                    new CatalogFunctionImpl(
                            sqlCreateFunction.getFunctionClassName().getValueAs(String.class),
                            language);
            ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

            return new CreateCatalogFunctionOperation(
                    identifier,
                    catalogFunction,
                    sqlCreateFunction.isIfNotExists(),
                    sqlCreateFunction.isTemporary());
        }
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

        PlannerQueryOperation query =
                (PlannerQueryOperation)
                        SqlToOperationConverter.convert(
                                        flinkPlanner, catalogManager, insert.getSource())
                                .orElseThrow(
                                        () ->
                                                new TableException(
                                                        "Unsupported node type "
                                                                + insert.getSource()
                                                                        .getClass()
                                                                        .getSimpleName()));

        return new CatalogSinkModifyOperation(
                identifier,
                query,
                insert.getStaticPartitionKVs(),
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

    /** Convert CREATE CATALOG statement. */
    private Operation convertCreateCatalog(SqlCreateCatalog sqlCreateCatalog) {
        String catalogName = sqlCreateCatalog.catalogName();

        // set with properties
        Map<String, String> properties = new HashMap<>();
        sqlCreateCatalog
                .getPropertyList()
                .getList()
                .forEach(
                        p ->
                                properties.put(
                                        ((SqlTableOption) p).getKeyString(),
                                        ((SqlTableOption) p).getValueString()));

        return new CreateCatalogOperation(catalogName, properties);
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
                        .map(comment -> comment.getNlsString().getValue())
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
        return new ShowTablesOperation();
    }

    /** Convert SHOW CREATE TABLE statement. */
    private Operation convertShowCreateTable(SqlShowCreateTable sqlShowCreateTable) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlShowCreateTable.getFullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        return new ShowCreateTableOperation(identifier);
    }

    /** Convert SHOW FUNCTIONS statement. */
    private Operation convertShowFunctions(SqlShowFunctions sqlShowFunctions) {
        return new ShowFunctionsOperation(
                sqlShowFunctions.requireUser() ? FunctionScope.USER : FunctionScope.ALL);
    }

    /** Convert SHOW PARTITIONS statement. */
    private Operation convertShowPartitions(SqlShowPartitions sqlShowPartitions) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlShowPartitions.fullTableName());
        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        LinkedHashMap<String, String> partitionKVs = sqlShowPartitions.getPartitionKVs();
        if (partitionKVs != null) {
            CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(partitionKVs);
            return new ShowPartitionsOperation(tableIdentifier, partitionSpec);
        }
        return new ShowPartitionsOperation(tableIdentifier, null);
    }

    /** Convert CREATE VIEW statement. */
    private Operation convertCreateView(SqlCreateView sqlCreateView) {
        final SqlNode query = sqlCreateView.getQuery();
        final SqlNodeList fieldList = sqlCreateView.getFieldList();

        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateView.fullViewName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        String comment =
                sqlCreateView.getComment().map(c -> c.getNlsString().getValue()).orElse(null);
        CatalogView catalogView =
                convertViewQuery(
                        query,
                        fieldList.getList(),
                        OperationConverterUtils.extractProperties(
                                sqlCreateView.getProperties().orElse(null)),
                        comment);
        return new CreateViewOperation(
                identifier,
                catalogView,
                sqlCreateView.isIfNotExists(),
                sqlCreateView.isTemporary());
    }

    /** Convert the query part of a VIEW statement. */
    private CatalogView convertViewQuery(
            SqlNode query, List<SqlNode> fieldNames, Map<String, String> props, String comment) {
        // Put the sql string unparse (getQuotedSqlString()) in front of
        // the node conversion (toQueryOperation()),
        // because before Calcite 1.22.0, during sql-to-rel conversion, the SqlWindow
        // bounds state would be mutated as default when they are null (not specified).

        // This bug is fixed in CALCITE-3877 of Calcite 1.23.0.
        String originalQuery = getQuotedSqlString(query);
        SqlNode validateQuery = flinkPlanner.validate(query);
        // The LATERAL operator was eliminated during sql validation, thus the unparsed SQL
        // does not contain LATERAL which is problematic,
        // the issue was resolved in CALCITE-4077
        // (always treat the table function as implicitly LATERAL).
        String expandedQuery =
                Expander.create(flinkPlanner)
                        .expanded(originalQuery)
                        .substitute(this::getQuotedSqlString);

        PlannerQueryOperation operation = toQueryOperation(flinkPlanner, validateQuery);
        ResolvedSchema schema = operation.getResolvedSchema();

        // the view column list in CREATE VIEW is optional, if it's not empty, we should update
        // the column name with the names in view column list.
        if (!fieldNames.isEmpty()) {
            // alias column names:
            List<String> inputFieldNames = schema.getColumnNames();
            List<String> aliasFieldNames =
                    fieldNames.stream().map(SqlNode::toString).collect(Collectors.toList());

            if (inputFieldNames.size() != aliasFieldNames.size()) {
                throw new ValidationException(
                        String.format(
                                "VIEW definition and input fields not match:\n\tDef fields: %s.\n\tInput fields: %s.",
                                aliasFieldNames, inputFieldNames));
            }

            schema = ResolvedSchema.physical(aliasFieldNames, schema.getColumnDataTypes());
        }

        return CatalogView.of(
                Schema.newBuilder().fromResolvedSchema(schema).build(),
                comment,
                originalQuery,
                expandedQuery,
                props);
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
        Operation operation = convertSqlQuery(sqlExplain.getStatement());
        return new ExplainOperation(operation);
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

    /** Fallback method for sql query. */
    private Operation convertSqlQuery(SqlNode node) {
        return toQueryOperation(flinkPlanner, node);
    }

    private void validateTableConstraint(SqlTableConstraint constraint) {
        if (constraint.isUnique()) {
            throw new UnsupportedOperationException("UNIQUE constraint is not supported yet");
        }
        if (constraint.isEnforced()) {
            throw new ValidationException(
                    "Flink doesn't support ENFORCED mode for "
                            + "PRIMARY KEY constraint. ENFORCED/NOT ENFORCED  controls if the constraint "
                            + "checks are performed on the incoming/outgoing data. "
                            + "Flink does not own the data therefore the only supported mode "
                            + "is the NOT ENFORCED mode");
        }
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
