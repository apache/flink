/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sqlexec;

import org.apache.flink.sql.parser.ddl.SqlAlterDatabase;
import org.apache.flink.sql.parser.ddl.SqlAlterFunction;
import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlAlterTableOptions;
import org.apache.flink.sql.parser.ddl.SqlAlterTableRename;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlDropDatabase;
import org.apache.flink.sql.parser.ddl.SqlDropFunction;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlDropView;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlUseCatalog;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dql.SqlRichDescribeTable;
import org.apache.flink.sql.parser.dql.SqlRichExplain;
import org.apache.flink.sql.parser.dql.SqlShowCatalogs;
import org.apache.flink.sql.parser.dql.SqlShowCurrentCatalog;
import org.apache.flink.sql.parser.dql.SqlShowCurrentDatabase;
import org.apache.flink.sql.parser.dql.SqlShowDatabases;
import org.apache.flink.sql.parser.dql.SqlShowFunctions;
import org.apache.flink.sql.parser.dql.SqlShowTables;
import org.apache.flink.sql.parser.dql.SqlShowViews;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.DescribeTableOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.PlannerQueryOperation;
import org.apache.flink.table.operations.ShowCatalogsOperation;
import org.apache.flink.table.operations.ShowCurrentCatalogOperation;
import org.apache.flink.table.operations.ShowCurrentDatabaseOperation;
import org.apache.flink.table.operations.ShowDatabasesOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation.FunctionScope;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.ShowViewsOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterTableOptionsOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.operations.ddl.DropCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.DropDatabaseOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.operations.ddl.DropTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.DropViewOperation;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.StringUtils;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
    private FlinkPlannerImpl flinkPlanner;
    private CatalogManager catalogManager;

    // ~ Constructors -----------------------------------------------------------

    private SqlToOperationConverter(FlinkPlannerImpl flinkPlanner, CatalogManager catalogManager) {
        this.flinkPlanner = flinkPlanner;
        this.catalogManager = catalogManager;
    }

    /**
     * This is the main entrance for executing all kinds of DDL/DML {@code SqlNode}s, different
     * SqlNode will have it's implementation in the #convert(type) method whose 'type' argument is
     * subclass of {@code SqlNode}.
     *
     * @param flinkPlanner FlinkPlannerImpl to convert sql node to rel node
     * @param sqlNode SqlNode to execute on
     */
    public static Optional<Operation> convert(
            FlinkPlannerImpl flinkPlanner, CatalogManager catalogManager, SqlNode sqlNode) {
        // validate the query
        final SqlNode validated = flinkPlanner.validate(sqlNode);
        SqlToOperationConverter converter =
                new SqlToOperationConverter(flinkPlanner, catalogManager);
        if (validated instanceof SqlUseCatalog) {
            return Optional.of(converter.convertUseCatalog((SqlUseCatalog) validated));
        } else if (validated instanceof SqlShowCatalogs) {
            return Optional.of(converter.convertShowCatalogs((SqlShowCatalogs) validated));
        } else if (validated instanceof SqlShowCurrentCatalog) {
            return Optional.of(
                    converter.convertShowCurrentCatalog((SqlShowCurrentCatalog) validated));
        }
        if (validated instanceof SqlCreateDatabase) {
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
            return Optional.of(converter.convertCreateTable((SqlCreateTable) validated));
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
        } else if (validated instanceof SqlShowViews) {
            return Optional.of(converter.convertShowViews((SqlShowViews) validated));
        } else if (validated instanceof SqlCreateFunction) {
            return Optional.of(converter.convertCreateFunction((SqlCreateFunction) validated));
        } else if (validated instanceof SqlAlterFunction) {
            return Optional.of(converter.convertAlterFunction((SqlAlterFunction) validated));
        } else if (validated instanceof SqlDropFunction) {
            return Optional.of(converter.convertDropFunction((SqlDropFunction) validated));
        } else if (validated instanceof SqlShowFunctions) {
            return Optional.of(converter.convertShowFunctions((SqlShowFunctions) validated));
        } else if (validated instanceof SqlRichExplain) {
            return Optional.of(converter.convertRichExplain((SqlRichExplain) validated));
        } else if (validated instanceof SqlRichDescribeTable) {
            return Optional.of(converter.convertDescribeTable((SqlRichDescribeTable) validated));
        } else if (validated instanceof RichSqlInsert) {
            SqlNodeList targetColumnList = ((RichSqlInsert) validated).getTargetColumnList();
            if (targetColumnList != null && targetColumnList.size() != 0) {
                throw new ValidationException("Partial inserts are not supported");
            }
            return Optional.of(converter.convertSqlInsert((RichSqlInsert) validated));
        } else if (validated.getKind().belongsTo(SqlKind.QUERY)) {
            return Optional.of(converter.convertSqlQuery(validated));
        } else {
            return Optional.empty();
        }
    }

    // ~ Tools ------------------------------------------------------------------

    /** Convert the {@link SqlCreateTable} node. */
    private Operation convertCreateTable(SqlCreateTable sqlCreateTable) {
        // primary key and unique keys are not supported
        if (sqlCreateTable.getFullConstraints().size() > 0) {
            throw new TableException("Primary key and unique key are not supported yet.");
        }

        if (sqlCreateTable.getWatermark().isPresent()) {
            throw new TableException(
                    "Watermark statement is not supported in Old Planner, please use Blink Planner instead.");
        }

        if (sqlCreateTable.getTableLike().isPresent()) {
            throw new TableException(
                    "CREATE TABLE ... LIKE statement is not supported in Old Planner, please use Blink Planner instead.");
        }

        // set with properties
        Map<String, String> properties = new HashMap<>();
        sqlCreateTable
                .getPropertyList()
                .getList()
                .forEach(
                        p ->
                                properties.put(
                                        ((SqlTableOption) p).getKeyString(),
                                        ((SqlTableOption) p).getValueString()));

        TableSchema tableSchema = createTableSchema(sqlCreateTable);
        String tableComment =
                sqlCreateTable
                        .getComment()
                        .map(comment -> comment.getNlsString().getValue())
                        .orElse(null);
        // set partition key
        List<String> partitionKeys =
                sqlCreateTable.getPartitionKeyList().getList().stream()
                        .map(p -> ((SqlIdentifier) p).getSimple())
                        .collect(Collectors.toList());

        verifyPartitioningColumnsExist(tableSchema, partitionKeys);

        CatalogTable catalogTable =
                new CatalogTableImpl(tableSchema, partitionKeys, properties, tableComment);

        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateTable.fullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        return new CreateTableOperation(
                identifier,
                catalogTable,
                sqlCreateTable.isIfNotExists(),
                sqlCreateTable.isTemporary());
    }

    private void verifyPartitioningColumnsExist(
            TableSchema mergedSchema, List<String> partitionKeys) {
        for (String partitionKey : partitionKeys) {
            if (!mergedSchema.getTableColumn(partitionKey).isPresent()) {
                throw new ValidationException(
                        String.format(
                                "Partition column '%s' not defined in the table schema. Available columns: [%s]",
                                partitionKey,
                                Arrays.stream(mergedSchema.getFieldNames())
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }
        }
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
        if (sqlAlterTable instanceof SqlAlterTableRename) {
            UnresolvedIdentifier newUnresolvedIdentifier =
                    UnresolvedIdentifier.of(
                            ((SqlAlterTableRename) sqlAlterTable).fullNewTableName());
            ObjectIdentifier newTableIdentifier =
                    catalogManager.qualifyIdentifier(newUnresolvedIdentifier);
            return new AlterTableRenameOperation(tableIdentifier, newTableIdentifier);
        } else if (sqlAlterTable instanceof SqlAlterTableOptions) {
            Optional<CatalogManager.TableLookupResult> optionalCatalogTable =
                    catalogManager.getTable(tableIdentifier);
            if (optionalCatalogTable.isPresent() && !optionalCatalogTable.get().isTemporary()) {
                CatalogTable originalCatalogTable =
                        (CatalogTable) optionalCatalogTable.get().getTable();
                Map<String, String> options = new HashMap<>();
                options.putAll(originalCatalogTable.getOptions());
                ((SqlAlterTableOptions) sqlAlterTable)
                        .getPropertyList()
                        .getList()
                        .forEach(
                                p ->
                                        options.put(
                                                ((SqlTableOption) p).getKeyString(),
                                                ((SqlTableOption) p).getValueString()));
                return new AlterTableOptionsOperation(
                        tableIdentifier, originalCatalogTable.copy(options));
            } else {
                throw new ValidationException(
                        String.format(
                                "Table %s doesn't exist or is a temporary table.",
                                tableIdentifier.toString()));
            }
        } else {
            throw new ValidationException(
                    String.format(
                            "[%s] needs to implement",
                            sqlAlterTable.toSqlString(CalciteSqlDialect.DEFAULT)));
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

    /** Fallback method for sql query. */
    private Operation convertSqlQuery(SqlNode node) {
        return toQueryOperation(flinkPlanner, node);
    }

    /** Convert insert into statement. */
    private Operation convertSqlInsert(RichSqlInsert insert) {
        // get name of sink table
        List<String> targetTablePath = ((SqlIdentifier) insert.getTargetTable()).names;

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
                Collections.emptyMap());
    }

    /** Convert use catalog statement. */
    private Operation convertUseCatalog(SqlUseCatalog useCatalog) {
        return new UseCatalogOperation(useCatalog.catalogName());
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
        Map<String, String> properties = new HashMap<>();
        CatalogDatabase originCatalogDatabase;
        Optional<Catalog> catalog = catalogManager.getCatalog(catalogName);
        if (catalog.isPresent()) {
            try {
                originCatalogDatabase = catalog.get().getDatabase(databaseName);
                properties.putAll(originCatalogDatabase.getProperties());
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

    /** Convert SHOW FUNCTIONS statement. */
    private Operation convertShowFunctions(SqlShowFunctions sqlShowFunctions) {
        return new ShowFunctionsOperation(
                sqlShowFunctions.requireUser() ? FunctionScope.USER : FunctionScope.ALL);
    }

    /** Convert CREATE VIEW statement. */
    private Operation convertCreateView(SqlCreateView sqlCreateView) {
        final SqlNode query = sqlCreateView.getQuery();
        final SqlNodeList fieldList = sqlCreateView.getFieldList();

        SqlNode validateQuery = flinkPlanner.validate(query);
        PlannerQueryOperation operation = toQueryOperation(flinkPlanner, validateQuery);
        ResolvedSchema schema = operation.getResolvedSchema();

        // the view column list in CREATE VIEW is optional, if it's not empty, we should update
        // the column name with the names in view column list.
        if (!fieldList.getList().isEmpty()) {
            // alias column names
            List<String> inputFieldNames = schema.getColumnNames();
            List<String> aliasFieldNames =
                    fieldList.getList().stream()
                            .map(SqlNode::toString)
                            .collect(Collectors.toList());

            if (inputFieldNames.size() != aliasFieldNames.size()) {
                throw new ValidationException(
                        String.format(
                                "VIEW definition and input fields not match:\n\tDef fields: %s.\n\tInput fields: %s.",
                                aliasFieldNames, inputFieldNames));
            }

            schema = ResolvedSchema.physical(aliasFieldNames, schema.getColumnDataTypes());
        }

        String originalQuery = getQuotedSqlString(query);
        String expandedQuery = getQuotedSqlString(validateQuery);
        String comment =
                sqlCreateView.getComment().map(c -> c.getNlsString().getValue()).orElse(null);
        CatalogView catalogView =
                CatalogView.of(
                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                        comment,
                        originalQuery,
                        expandedQuery,
                        Collections.emptyMap());

        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateView.fullViewName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        return new CreateViewOperation(
                identifier,
                catalogView,
                sqlCreateView.isIfNotExists(),
                sqlCreateView.isTemporary());
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

    /** Creates a {@link TableSchema} from {@link SqlCreateTable}. */
    private TableSchema createTableSchema(SqlCreateTable sqlCreateTable) {
        if (!sqlCreateTable.hasRegularColumnsOnly()) {
            throw new TableException(
                    "Only regular columns are supported in the DDL of the old planner.");
        }

        final TableSchema.Builder builder = new TableSchema.Builder();
        final SqlNodeList columnList = sqlCreateTable.getColumnList();
        final List<SqlRegularColumn> physicalColumns =
                columnList.getList().stream()
                        .filter(SqlRegularColumn.class::isInstance)
                        .map(SqlRegularColumn.class::cast)
                        .collect(Collectors.toList());
        for (SqlRegularColumn regularColumn : physicalColumns) {
            SqlDataTypeSpec type = regularColumn.getType();
            boolean nullable = type.getNullable() == null ? true : type.getNullable();
            final RelDataType relType =
                    regularColumn
                            .getType()
                            .deriveType(flinkPlanner.getOrCreateSqlValidator(), nullable);
            builder.field(
                    regularColumn.getName().getSimple(),
                    TypeConversions.fromLegacyInfoToDataType(FlinkTypeFactory.toTypeInfo(relType)));
        }
        return builder.build();
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

    private PlannerQueryOperation toQueryOperation(FlinkPlannerImpl planner, SqlNode validated) {
        // transform to a relational tree
        RelRoot relational = planner.rel(validated);
        return new PlannerQueryOperation(relational.rel);
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
}
