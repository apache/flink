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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.SqlLikeUtils;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleEntry;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.CollectModifyOperation;
import org.apache.flink.table.operations.CompileAndExecutePlanOperation;
import org.apache.flink.table.operations.DescribeTableOperation;
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
import org.apache.flink.table.operations.ShowFunctionsOperation;
import org.apache.flink.table.operations.ShowModulesOperation;
import org.apache.flink.table.operations.ShowPartitionsOperation;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.ShowViewsOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.SourceQueryOperation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.operations.UnloadModuleOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.UseModulesOperation;
import org.apache.flink.table.operations.command.ExecutePlanOperation;
import org.apache.flink.table.operations.ddl.AddPartitionsOperation;
import org.apache.flink.table.operations.ddl.AlterCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterPartitionPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableAddConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTableDropConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTableOperation;
import org.apache.flink.table.operations.ddl.AlterTableOptionsOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.AlterTableSchemaOperation;
import org.apache.flink.table.operations.ddl.AlterViewAsOperation;
import org.apache.flink.table.operations.ddl.AlterViewOperation;
import org.apache.flink.table.operations.ddl.AlterViewPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterViewRenameOperation;
import org.apache.flink.table.operations.ddl.CompilePlanOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateTableASOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.operations.ddl.DropCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.DropCatalogOperation;
import org.apache.flink.table.operations.ddl.DropDatabaseOperation;
import org.apache.flink.table.operations.ddl.DropPartitionsOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.operations.ddl.DropTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.DropViewOperation;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.table.utils.print.PrintStyle;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC;

/**
 * Implementation of {@link TableEnvironment} that works exclusively with Table API interfaces. Only
 * {@link TableSource} is supported as an input and {@link TableSink} as an output. It also does not
 * bind to any particular {@code StreamExecutionEnvironment}.
 */
@Internal
public class TableEnvironmentImpl implements TableEnvironmentInternal {
    // Flag that tells if the TableSource/TableSink used in this environment is stream table
    // source/sink,
    // and this should always be true. This avoids too many hard code.
    private static final boolean IS_STREAM_TABLE = true;
    private final CatalogManager catalogManager;
    private final ModuleManager moduleManager;
    private final OperationTreeBuilder operationTreeBuilder;

    protected final TableConfig tableConfig;
    protected final Executor execEnv;
    protected final FunctionCatalog functionCatalog;
    protected final Planner planner;
    private final boolean isStreamingMode;
    private final ClassLoader userClassLoader;
    private static final String UNSUPPORTED_QUERY_IN_EXECUTE_SQL_MSG =
            "Unsupported SQL query! executeSql() only accepts a single SQL statement of type "
                    + "CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE DATABASE, DROP DATABASE, ALTER DATABASE, "
                    + "CREATE FUNCTION, DROP FUNCTION, ALTER FUNCTION, CREATE CATALOG, DROP CATALOG, "
                    + "USE CATALOG, USE [CATALOG.]DATABASE, SHOW CATALOGS, SHOW DATABASES, SHOW TABLES, SHOW [USER] FUNCTIONS, SHOW PARTITIONS"
                    + "CREATE VIEW, DROP VIEW, SHOW VIEWS, INSERT, DESCRIBE, LOAD MODULE, UNLOAD "
                    + "MODULE, USE MODULES, SHOW [FULL] MODULES.";
    private static final String UNSUPPORTED_QUERY_IN_COMPILE_PLAN_SQL_MSG =
            "Unsupported SQL query! compilePlanSql() only accepts a single SQL statement of type INSERT";

    protected TableEnvironmentImpl(
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            TableConfig tableConfig,
            Executor executor,
            FunctionCatalog functionCatalog,
            Planner planner,
            boolean isStreamingMode,
            ClassLoader userClassLoader) {
        this.catalogManager = catalogManager;
        this.moduleManager = moduleManager;
        this.execEnv = executor;

        this.tableConfig = tableConfig;

        this.functionCatalog = functionCatalog;
        this.planner = planner;
        this.isStreamingMode = isStreamingMode;
        this.userClassLoader = userClassLoader;
        this.operationTreeBuilder =
                OperationTreeBuilder.create(
                        tableConfig,
                        functionCatalog.asLookup(getParser()::parseIdentifier),
                        catalogManager.getDataTypeFactory(),
                        path -> {
                            try {
                                UnresolvedIdentifier unresolvedIdentifier =
                                        getParser().parseIdentifier(path);
                                Optional<SourceQueryOperation> catalogQueryOperation =
                                        scanInternal(unresolvedIdentifier);
                                return catalogQueryOperation.map(
                                        t -> ApiExpressionUtils.tableRef(path, t));
                            } catch (SqlParserException ex) {
                                // The TableLookup is used during resolution of expressions and it
                                // actually might not be an
                                // identifier of a table. It might be a reference to some other
                                // object such as column, local
                                // reference etc. This method should return empty optional in such
                                // cases to fallback for other
                                // identifiers resolution.
                                return Optional.empty();
                            }
                        },
                        (sqlExpression, inputRowType, outputType) -> {
                            try {
                                return getParser()
                                        .parseSqlExpression(
                                                sqlExpression, inputRowType, outputType);
                            } catch (Throwable t) {
                                throw new ValidationException(
                                        String.format("Invalid SQL expression: %s", sqlExpression),
                                        t);
                            }
                        },
                        isStreamingMode);

        catalogManager.initSchemaResolver(
                isStreamingMode, operationTreeBuilder.getResolverBuilder());
    }

    public static TableEnvironmentImpl create(Configuration configuration) {
        return create(EnvironmentSettings.newInstance().withConfiguration(configuration).build());
    }

    public static TableEnvironmentImpl create(EnvironmentSettings settings) {
        // temporary solution until FLINK-15635 is fixed
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        final ExecutorFactory executorFactory =
                FactoryUtil.discoverFactory(
                        classLoader, ExecutorFactory.class, ExecutorFactory.DEFAULT_IDENTIFIER);
        final Executor executor = executorFactory.create(settings.getConfiguration());

        // use configuration to init table config
        final TableConfig tableConfig = new TableConfig();
        tableConfig.setRootConfiguration(executor.getConfiguration());
        tableConfig.addConfiguration(settings.getConfiguration());

        final ModuleManager moduleManager = new ModuleManager();

        final CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(classLoader)
                        .config(tableConfig.getConfiguration())
                        .defaultCatalog(
                                settings.getBuiltInCatalogName(),
                                new GenericInMemoryCatalog(
                                        settings.getBuiltInCatalogName(),
                                        settings.getBuiltInDatabaseName()))
                        .build();

        final FunctionCatalog functionCatalog =
                new FunctionCatalog(tableConfig, catalogManager, moduleManager);

        final Planner planner =
                PlannerFactoryUtil.createPlanner(
                        executor, tableConfig, moduleManager, catalogManager, functionCatalog);

        return new TableEnvironmentImpl(
                catalogManager,
                moduleManager,
                tableConfig,
                executor,
                functionCatalog,
                planner,
                settings.isStreamingMode(),
                classLoader);
    }

    @Override
    public Table fromValues(Object... values) {
        return fromValues(Arrays.asList(values));
    }

    @Override
    public Table fromValues(AbstractDataType<?> rowType, Object... values) {
        return fromValues(rowType, Arrays.asList(values));
    }

    @Override
    public Table fromValues(Expression... values) {
        return createTable(operationTreeBuilder.values(values));
    }

    @Override
    public Table fromValues(AbstractDataType<?> rowType, Expression... values) {
        final DataType resolvedDataType =
                catalogManager.getDataTypeFactory().createDataType(rowType);
        return createTable(operationTreeBuilder.values(resolvedDataType, values));
    }

    @Override
    public Table fromValues(Iterable<?> values) {
        Expression[] exprs =
                StreamSupport.stream(values.spliterator(), false)
                        .map(ApiExpressionUtils::objectToExpression)
                        .toArray(Expression[]::new);
        return fromValues(exprs);
    }

    @Override
    public Table fromValues(AbstractDataType<?> rowType, Iterable<?> values) {
        Expression[] exprs =
                StreamSupport.stream(values.spliterator(), false)
                        .map(ApiExpressionUtils::objectToExpression)
                        .toArray(Expression[]::new);
        return fromValues(rowType, exprs);
    }

    @VisibleForTesting
    public Planner getPlanner() {
        return planner;
    }

    @Override
    public Table fromTableSource(TableSource<?> source) {
        // only accept StreamTableSource and LookupableTableSource here
        // TODO should add a validation, while StreamTableSource is in flink-table-api-java-bridge
        // module now
        return createTable(new TableSourceQueryOperation<>(source, !IS_STREAM_TABLE));
    }

    @Override
    public void registerCatalog(String catalogName, Catalog catalog) {
        catalogManager.registerCatalog(catalogName, catalog);
    }

    @Override
    public Optional<Catalog> getCatalog(String catalogName) {
        return catalogManager.getCatalog(catalogName);
    }

    @Override
    public void loadModule(String moduleName, Module module) {
        moduleManager.loadModule(moduleName, module);
    }

    @Override
    public void useModules(String... moduleNames) {
        moduleManager.useModules(moduleNames);
    }

    @Override
    public void unloadModule(String moduleName) {
        moduleManager.unloadModule(moduleName);
    }

    @Override
    public void registerFunction(String name, ScalarFunction function) {
        functionCatalog.registerTempSystemScalarFunction(name, function);
    }

    @Override
    public void createTemporarySystemFunction(
            String name, Class<? extends UserDefinedFunction> functionClass) {
        final UserDefinedFunction functionInstance =
                UserDefinedFunctionHelper.instantiateFunction(functionClass);
        createTemporarySystemFunction(name, functionInstance);
    }

    @Override
    public void createTemporarySystemFunction(String name, UserDefinedFunction functionInstance) {
        functionCatalog.registerTemporarySystemFunction(name, functionInstance, false);
    }

    @Override
    public boolean dropTemporarySystemFunction(String name) {
        return functionCatalog.dropTemporarySystemFunction(name, true);
    }

    @Override
    public void createFunction(String path, Class<? extends UserDefinedFunction> functionClass) {
        createFunction(path, functionClass, false);
    }

    @Override
    public void createFunction(
            String path,
            Class<? extends UserDefinedFunction> functionClass,
            boolean ignoreIfExists) {
        final UnresolvedIdentifier unresolvedIdentifier = getParser().parseIdentifier(path);
        functionCatalog.registerCatalogFunction(
                unresolvedIdentifier, functionClass, ignoreIfExists);
    }

    @Override
    public boolean dropFunction(String path) {
        final UnresolvedIdentifier unresolvedIdentifier = getParser().parseIdentifier(path);
        return functionCatalog.dropCatalogFunction(unresolvedIdentifier, true);
    }

    @Override
    public void createTemporaryFunction(
            String path, Class<? extends UserDefinedFunction> functionClass) {
        final UserDefinedFunction functionInstance =
                UserDefinedFunctionHelper.instantiateFunction(functionClass);
        createTemporaryFunction(path, functionInstance);
    }

    @Override
    public void createTemporaryFunction(String path, UserDefinedFunction functionInstance) {
        final UnresolvedIdentifier unresolvedIdentifier = getParser().parseIdentifier(path);
        functionCatalog.registerTemporaryCatalogFunction(
                unresolvedIdentifier, functionInstance, false);
    }

    @Override
    public boolean dropTemporaryFunction(String path) {
        final UnresolvedIdentifier unresolvedIdentifier = getParser().parseIdentifier(path);
        return functionCatalog.dropTemporaryCatalogFunction(unresolvedIdentifier, true);
    }

    @Override
    public void createTemporaryTable(String path, TableDescriptor descriptor) {
        Preconditions.checkNotNull(path, "Path must not be null.");
        Preconditions.checkNotNull(descriptor, "Table descriptor must not be null.");

        final ObjectIdentifier tableIdentifier =
                catalogManager.qualifyIdentifier(getParser().parseIdentifier(path));
        catalogManager.createTemporaryTable(descriptor.toCatalogTable(), tableIdentifier, false);
    }

    @Override
    public void createTable(String path, TableDescriptor descriptor) {
        Preconditions.checkNotNull(path, "Path must not be null.");
        Preconditions.checkNotNull(descriptor, "Table descriptor must not be null.");

        final ObjectIdentifier tableIdentifier =
                catalogManager.qualifyIdentifier(getParser().parseIdentifier(path));
        catalogManager.createTable(descriptor.toCatalogTable(), tableIdentifier, false);
    }

    @Override
    public void registerTable(String name, Table table) {
        UnresolvedIdentifier identifier = UnresolvedIdentifier.of(name);
        createTemporaryView(identifier, table);
    }

    @Override
    public void createTemporaryView(String path, Table view) {
        Preconditions.checkNotNull(path, "Path must not be null.");
        Preconditions.checkNotNull(view, "Table view must not be null.");
        UnresolvedIdentifier identifier = getParser().parseIdentifier(path);
        createTemporaryView(identifier, view);
    }

    private void createTemporaryView(UnresolvedIdentifier identifier, Table view) {
        if (((TableImpl) view).getTableEnvironment() != this) {
            throw new TableException(
                    "Only table API objects that belong to this TableEnvironment can be registered.");
        }

        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(identifier);
        QueryOperation queryOperation =
                qualifyQueryOperation(tableIdentifier, view.getQueryOperation());
        CatalogBaseTable tableTable = new QueryOperationCatalogView(queryOperation);

        catalogManager.createTemporaryTable(tableTable, tableIdentifier, false);
    }

    @Override
    public Table scan(String... tablePath) {
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(tablePath);
        return scanInternal(unresolvedIdentifier)
                .map(this::createTable)
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        String.format(
                                                "Table %s was not found.", unresolvedIdentifier)));
    }

    @Override
    public Table from(String path) {
        UnresolvedIdentifier unresolvedIdentifier = getParser().parseIdentifier(path);
        return scanInternal(unresolvedIdentifier)
                .map(this::createTable)
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        String.format(
                                                "Table %s was not found.", unresolvedIdentifier)));
    }

    @Override
    public Table from(TableDescriptor descriptor) {
        Preconditions.checkNotNull(descriptor, "Table descriptor must not be null.");

        final ResolvedCatalogTable resolvedCatalogBaseTable =
                catalogManager.resolveCatalogTable(descriptor.toCatalogTable());
        final QueryOperation queryOperation =
                new SourceQueryOperation(ContextResolvedTable.anonymous(resolvedCatalogBaseTable));
        return createTable(queryOperation);
    }

    private Optional<SourceQueryOperation> scanInternal(UnresolvedIdentifier identifier) {
        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(identifier);

        return catalogManager.getTable(tableIdentifier).map(SourceQueryOperation::new);
    }

    @Override
    public String[] listCatalogs() {
        return catalogManager.listCatalogs().stream().sorted().toArray(String[]::new);
    }

    @Override
    public String[] listModules() {
        return moduleManager.listModules().toArray(new String[0]);
    }

    @Override
    public ModuleEntry[] listFullModules() {
        return moduleManager.listFullModules().toArray(new ModuleEntry[0]);
    }

    @Override
    public String[] listDatabases() {
        return catalogManager
                .getCatalog(catalogManager.getCurrentCatalog())
                .get()
                .listDatabases()
                .toArray(new String[0]);
    }

    @Override
    public String[] listTables() {
        return catalogManager.listTables().stream().sorted().toArray(String[]::new);
    }

    @Override
    public String[] listTables(String catalog, String databaseName) {
        return catalogManager.listTables(catalog, databaseName).stream()
                .sorted()
                .toArray(String[]::new);
    }

    @Override
    public String[] listViews() {
        return catalogManager.listViews().stream().sorted().toArray(String[]::new);
    }

    @Override
    public String[] listTemporaryTables() {
        return catalogManager.listTemporaryTables().stream().sorted().toArray(String[]::new);
    }

    @Override
    public String[] listTemporaryViews() {
        return catalogManager.listTemporaryViews().stream().sorted().toArray(String[]::new);
    }

    @Override
    public boolean dropTemporaryTable(String path) {
        UnresolvedIdentifier unresolvedIdentifier = getParser().parseIdentifier(path);
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        try {
            catalogManager.dropTemporaryTable(identifier, false);
            return true;
        } catch (ValidationException e) {
            return false;
        }
    }

    @Override
    public boolean dropTemporaryView(String path) {
        UnresolvedIdentifier unresolvedIdentifier = getParser().parseIdentifier(path);
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        try {
            catalogManager.dropTemporaryView(identifier, false);
            return true;
        } catch (ValidationException e) {
            return false;
        }
    }

    @Override
    public String[] listUserDefinedFunctions() {
        String[] functions = functionCatalog.getUserDefinedFunctions();
        Arrays.sort(functions);
        return functions;
    }

    @Override
    public String[] listFunctions() {
        String[] functions = functionCatalog.getFunctions();
        Arrays.sort(functions);
        return functions;
    }

    @Override
    public String explainSql(String statement, ExplainDetail... extraDetails) {
        List<Operation> operations = getParser().parse(statement);

        if (operations.size() != 1) {
            throw new TableException(
                    "Unsupported SQL query! explainSql() only accepts a single SQL query.");
        }

        if (operations.get(0) instanceof StatementSetOperation) {
            operations =
                    new ArrayList<>(((StatementSetOperation) operations.get(0)).getOperations());
        }
        return explainInternal(operations, extraDetails);
    }

    @Override
    public String explainInternal(List<Operation> operations, ExplainDetail... extraDetails) {
        operations =
                operations.stream()
                        .filter(o -> !(o instanceof NopOperation))
                        .collect(Collectors.toList());
        // hive parser may generate an NopOperation, in which case we just return an
        // empty string as the plan
        if (operations.isEmpty()) {
            return "";
        } else {
            return planner.explain(operations, extraDetails);
        }
    }

    @Override
    public String[] getCompletionHints(String statement, int position) {
        return planner.getParser().getCompletionHints(statement, position);
    }

    @Override
    public Table sqlQuery(String query) {
        List<Operation> operations = getParser().parse(query);

        if (operations.size() != 1) {
            throw new ValidationException(
                    "Unsupported SQL query! sqlQuery() only accepts a single SQL query.");
        }

        Operation operation = operations.get(0);

        if (operation instanceof QueryOperation && !(operation instanceof ModifyOperation)) {
            return createTable((QueryOperation) operation);
        } else {
            throw new ValidationException(
                    "Unsupported SQL query! sqlQuery() only accepts a single SQL query of type "
                            + "SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.");
        }
    }

    @Override
    public TableResult executeSql(String statement) {
        List<Operation> operations = getParser().parse(statement);

        if (operations.size() != 1) {
            throw new TableException(UNSUPPORTED_QUERY_IN_EXECUTE_SQL_MSG);
        }

        return executeInternal(operations.get(0));
    }

    @Override
    public StatementSet createStatementSet() {
        return new StatementSetImpl(this);
    }

    @Override
    public CompiledPlan loadPlan(PlanReference planReference) {
        try {
            return new CompiledPlanImpl(this, planner.loadPlan(planReference));
        } catch (IOException e) {
            throw new TableException(String.format("Cannot load %s.", planReference), e);
        }
    }

    @Override
    public CompiledPlan compilePlanSql(String stmt) {
        List<Operation> operations = getParser().parse(stmt);

        if (operations.size() != 1 || !(operations.get(0) instanceof ModifyOperation)) {
            throw new TableException(UNSUPPORTED_QUERY_IN_COMPILE_PLAN_SQL_MSG);
        }

        return new CompiledPlanImpl(
                this,
                planner.compilePlan(
                        Collections.singletonList((ModifyOperation) operations.get(0))));
    }

    @Override
    public TableResultInternal executePlan(InternalPlan plan) {
        List<Transformation<?>> transformations = planner.translatePlan(plan);
        List<String> sinkIdentifierNames =
                deduplicateSinkIdentifierNames(plan.getSinkIdentifiers());
        return executeInternal(transformations, sinkIdentifierNames);
    }

    private CompiledPlan compilePlanAndWrite(
            String filePath, boolean ifNotExists, Operation operation) {
        File file = Paths.get(filePath).toFile();
        if (file.exists()) {
            if (ifNotExists) {
                return loadPlan(PlanReference.fromFile(filePath));
            }

            if (!tableConfig.getConfiguration().get(TableConfigOptions.PLAN_FORCE_RECOMPILE)) {
                throw new TableException(
                        String.format(
                                "Cannot overwrite the plan file '%s'. "
                                        + "Either manually remove the file or, "
                                        + "if you're debugging your job, "
                                        + "set the option '%s' to true.",
                                filePath, TableConfigOptions.PLAN_FORCE_RECOMPILE.key()));
            }
        }

        CompiledPlan compiledPlan;
        if (operation instanceof StatementSetOperation) {
            compiledPlan = compilePlan(((StatementSetOperation) operation).getOperations());
        } else if (operation instanceof ModifyOperation) {
            compiledPlan = compilePlan(Collections.singletonList((ModifyOperation) operation));
        } else {
            throw new TableException(
                    "Unsupported operation to compile: "
                            + operation.getClass()
                            + ". This is a bug, please file an issue.");
        }

        compiledPlan.writeToFile(file, false);
        return compiledPlan;
    }

    @Override
    public CompiledPlan compilePlan(List<ModifyOperation> operations) {
        return new CompiledPlanImpl(this, planner.compilePlan(operations));
    }

    @Override
    public TableResultInternal executeInternal(List<ModifyOperation> operations) {
        List<Transformation<?>> transformations = translate(operations);
        List<String> sinkIdentifierNames = extractSinkIdentifierNames(operations);
        TableResultInternal result = executeInternal(transformations, sinkIdentifierNames);
        if (tableConfig.getConfiguration().get(TABLE_DML_SYNC)) {
            try {
                result.await();
            } catch (InterruptedException | ExecutionException e) {
                result.getJobClient().ifPresent(JobClient::cancel);
                throw new TableException("Fail to wait execution finish.", e);
            }
        }
        return result;
    }

    private TableResultInternal executeInternal(
            List<Transformation<?>> transformations, List<String> sinkIdentifierNames) {
        final String defaultJobName = "insert-into_" + String.join(",", sinkIdentifierNames);
        Pipeline pipeline =
                execEnv.createPipeline(
                        transformations, tableConfig.getConfiguration(), defaultJobName);
        try {
            JobClient jobClient = execEnv.executeAsync(pipeline);
            final List<Column> columns = new ArrayList<>();
            Long[] affectedRowCounts = new Long[transformations.size()];
            for (int i = 0; i < transformations.size(); ++i) {
                // use sink identifier name as field name
                columns.add(Column.physical(sinkIdentifierNames.get(i), DataTypes.BIGINT()));
                affectedRowCounts[i] = -1L;
            }

            return TableResultImpl.builder()
                    .jobClient(jobClient)
                    .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                    .schema(ResolvedSchema.of(columns))
                    .resultProvider(
                            new InsertResultProvider(affectedRowCounts).setJobClient(jobClient))
                    .build();
        } catch (Exception e) {
            throw new TableException("Failed to execute sql", e);
        }
    }

    private TableResultInternal executeQueryOperation(QueryOperation operation) {
        CollectModifyOperation sinkOperation = new CollectModifyOperation(operation);
        List<Transformation<?>> transformations =
                translate(Collections.singletonList(sinkOperation));
        final String defaultJobName = "collect";
        Pipeline pipeline =
                execEnv.createPipeline(
                        transformations, tableConfig.getConfiguration(), defaultJobName);
        try {
            JobClient jobClient = execEnv.executeAsync(pipeline);
            ResultProvider resultProvider = sinkOperation.getSelectResultProvider();
            resultProvider.setJobClient(jobClient);
            return TableResultImpl.builder()
                    .jobClient(jobClient)
                    .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                    .schema(operation.getResolvedSchema())
                    .resultProvider(resultProvider)
                    .setPrintStyle(
                            PrintStyle.tableauWithTypeInferredColumnWidths(
                                    // sinkOperation.getConsumedDataType() handles legacy types
                                    DataTypeUtils.expandCompositeTypeToSchema(
                                            sinkOperation.getConsumedDataType()),
                                    resultProvider.getRowDataStringConverter(),
                                    PrintStyle.DEFAULT_MAX_COLUMN_WIDTH,
                                    false,
                                    isStreamingMode))
                    .build();
        } catch (Exception e) {
            throw new TableException("Failed to execute sql", e);
        }
    }

    @Override
    public TableResultInternal executeInternal(Operation operation) {
        if (operation instanceof ModifyOperation) {
            return executeInternal(Collections.singletonList((ModifyOperation) operation));
        } else if (operation instanceof StatementSetOperation) {
            return executeInternal(((StatementSetOperation) operation).getOperations());
        } else if (operation instanceof CreateTableOperation) {
            CreateTableOperation createTableOperation = (CreateTableOperation) operation;
            if (createTableOperation.isTemporary()) {
                catalogManager.createTemporaryTable(
                        createTableOperation.getCatalogTable(),
                        createTableOperation.getTableIdentifier(),
                        createTableOperation.isIgnoreIfExists());
            } else {
                catalogManager.createTable(
                        createTableOperation.getCatalogTable(),
                        createTableOperation.getTableIdentifier(),
                        createTableOperation.isIgnoreIfExists());
            }
            return TableResultImpl.TABLE_RESULT_OK;
        } else if (operation instanceof DropTableOperation) {
            DropTableOperation dropTableOperation = (DropTableOperation) operation;
            if (dropTableOperation.isTemporary()) {
                catalogManager.dropTemporaryTable(
                        dropTableOperation.getTableIdentifier(), dropTableOperation.isIfExists());
            } else {
                catalogManager.dropTable(
                        dropTableOperation.getTableIdentifier(), dropTableOperation.isIfExists());
            }
            return TableResultImpl.TABLE_RESULT_OK;
        } else if (operation instanceof AlterTableOperation) {
            AlterTableOperation alterTableOperation = (AlterTableOperation) operation;
            Catalog catalog =
                    getCatalogOrThrowException(
                            alterTableOperation.getTableIdentifier().getCatalogName());
            String exMsg = getDDLOpExecuteErrorMsg(alterTableOperation.asSummaryString());
            try {
                if (alterTableOperation instanceof AlterTableRenameOperation) {
                    AlterTableRenameOperation alterTableRenameOp =
                            (AlterTableRenameOperation) operation;
                    catalog.renameTable(
                            alterTableRenameOp.getTableIdentifier().toObjectPath(),
                            alterTableRenameOp.getNewTableIdentifier().getObjectName(),
                            false);
                } else if (alterTableOperation instanceof AlterTableOptionsOperation) {
                    AlterTableOptionsOperation alterTablePropertiesOp =
                            (AlterTableOptionsOperation) operation;
                    catalogManager.alterTable(
                            alterTablePropertiesOp.getCatalogTable(),
                            alterTablePropertiesOp.getTableIdentifier(),
                            false);
                } else if (alterTableOperation instanceof AlterTableAddConstraintOperation) {
                    AlterTableAddConstraintOperation addConstraintOP =
                            (AlterTableAddConstraintOperation) operation;
                    CatalogTable oriTable =
                            catalogManager
                                    .getTable(addConstraintOP.getTableIdentifier())
                                    .get()
                                    .getTable();
                    TableSchema.Builder builder =
                            TableSchemaUtils.builderWithGivenSchema(oriTable.getSchema());
                    if (addConstraintOP.getConstraintName().isPresent()) {
                        builder.primaryKey(
                                addConstraintOP.getConstraintName().get(),
                                addConstraintOP.getColumnNames());
                    } else {
                        builder.primaryKey(addConstraintOP.getColumnNames());
                    }
                    CatalogTable newTable =
                            new CatalogTableImpl(
                                    builder.build(),
                                    oriTable.getPartitionKeys(),
                                    oriTable.getOptions(),
                                    oriTable.getComment());
                    catalogManager.alterTable(
                            newTable, addConstraintOP.getTableIdentifier(), false);
                } else if (alterTableOperation instanceof AlterTableDropConstraintOperation) {
                    AlterTableDropConstraintOperation dropConstraintOperation =
                            (AlterTableDropConstraintOperation) operation;
                    CatalogTable oriTable =
                            catalogManager
                                    .getTable(dropConstraintOperation.getTableIdentifier())
                                    .get()
                                    .getTable();
                    CatalogTable newTable =
                            new CatalogTableImpl(
                                    TableSchemaUtils.dropConstraint(
                                            oriTable.getSchema(),
                                            dropConstraintOperation.getConstraintName()),
                                    oriTable.getPartitionKeys(),
                                    oriTable.getOptions(),
                                    oriTable.getComment());
                    catalogManager.alterTable(
                            newTable, dropConstraintOperation.getTableIdentifier(), false);
                } else if (alterTableOperation instanceof AlterPartitionPropertiesOperation) {
                    AlterPartitionPropertiesOperation alterPartPropsOp =
                            (AlterPartitionPropertiesOperation) operation;
                    catalog.alterPartition(
                            alterPartPropsOp.getTableIdentifier().toObjectPath(),
                            alterPartPropsOp.getPartitionSpec(),
                            alterPartPropsOp.getCatalogPartition(),
                            false);
                } else if (alterTableOperation instanceof AlterTableSchemaOperation) {
                    AlterTableSchemaOperation alterTableSchemaOperation =
                            (AlterTableSchemaOperation) alterTableOperation;
                    catalogManager.alterTable(
                            alterTableSchemaOperation.getCatalogTable(),
                            alterTableSchemaOperation.getTableIdentifier(),
                            false);
                } else if (alterTableOperation instanceof AddPartitionsOperation) {
                    AddPartitionsOperation addPartitionsOperation =
                            (AddPartitionsOperation) alterTableOperation;
                    List<CatalogPartitionSpec> specs = addPartitionsOperation.getPartitionSpecs();
                    List<CatalogPartition> partitions =
                            addPartitionsOperation.getCatalogPartitions();
                    boolean ifNotExists = addPartitionsOperation.ifNotExists();
                    ObjectPath tablePath =
                            addPartitionsOperation.getTableIdentifier().toObjectPath();
                    for (int i = 0; i < specs.size(); i++) {
                        catalog.createPartition(
                                tablePath, specs.get(i), partitions.get(i), ifNotExists);
                    }
                } else if (alterTableOperation instanceof DropPartitionsOperation) {
                    DropPartitionsOperation dropPartitionsOperation =
                            (DropPartitionsOperation) alterTableOperation;
                    ObjectPath tablePath =
                            dropPartitionsOperation.getTableIdentifier().toObjectPath();
                    boolean ifExists = dropPartitionsOperation.ifExists();
                    for (CatalogPartitionSpec spec : dropPartitionsOperation.getPartitionSpecs()) {
                        catalog.dropPartition(tablePath, spec, ifExists);
                    }
                }
                return TableResultImpl.TABLE_RESULT_OK;
            } catch (TableAlreadyExistException | TableNotExistException e) {
                throw new ValidationException(exMsg, e);
            } catch (Exception e) {
                throw new TableException(exMsg, e);
            }
        } else if (operation instanceof CreateViewOperation) {
            CreateViewOperation createViewOperation = (CreateViewOperation) operation;
            if (createViewOperation.isTemporary()) {
                catalogManager.createTemporaryTable(
                        createViewOperation.getCatalogView(),
                        createViewOperation.getViewIdentifier(),
                        createViewOperation.isIgnoreIfExists());
            } else {
                catalogManager.createTable(
                        createViewOperation.getCatalogView(),
                        createViewOperation.getViewIdentifier(),
                        createViewOperation.isIgnoreIfExists());
            }
            return TableResultImpl.TABLE_RESULT_OK;
        } else if (operation instanceof DropViewOperation) {
            DropViewOperation dropViewOperation = (DropViewOperation) operation;
            if (dropViewOperation.isTemporary()) {
                catalogManager.dropTemporaryView(
                        dropViewOperation.getViewIdentifier(), dropViewOperation.isIfExists());
            } else {
                catalogManager.dropView(
                        dropViewOperation.getViewIdentifier(), dropViewOperation.isIfExists());
            }
            return TableResultImpl.TABLE_RESULT_OK;
        } else if (operation instanceof AlterViewOperation) {
            AlterViewOperation alterViewOperation = (AlterViewOperation) operation;
            Catalog catalog =
                    getCatalogOrThrowException(
                            alterViewOperation.getViewIdentifier().getCatalogName());
            String exMsg = getDDLOpExecuteErrorMsg(alterViewOperation.asSummaryString());
            try {
                if (alterViewOperation instanceof AlterViewRenameOperation) {
                    AlterViewRenameOperation alterTableRenameOp =
                            (AlterViewRenameOperation) operation;
                    catalog.renameTable(
                            alterTableRenameOp.getViewIdentifier().toObjectPath(),
                            alterTableRenameOp.getNewViewIdentifier().getObjectName(),
                            false);
                } else if (alterViewOperation instanceof AlterViewPropertiesOperation) {
                    AlterViewPropertiesOperation alterTablePropertiesOp =
                            (AlterViewPropertiesOperation) operation;
                    catalogManager.alterTable(
                            alterTablePropertiesOp.getCatalogView(),
                            alterTablePropertiesOp.getViewIdentifier(),
                            false);
                } else if (alterViewOperation instanceof AlterViewAsOperation) {
                    AlterViewAsOperation alterViewAsOperation =
                            (AlterViewAsOperation) alterViewOperation;
                    catalogManager.alterTable(
                            alterViewAsOperation.getNewView(),
                            alterViewAsOperation.getViewIdentifier(),
                            false);
                }
                return TableResultImpl.TABLE_RESULT_OK;
            } catch (TableAlreadyExistException | TableNotExistException e) {
                throw new ValidationException(exMsg, e);
            } catch (Exception e) {
                throw new TableException(exMsg, e);
            }
        } else if (operation instanceof CreateDatabaseOperation) {
            CreateDatabaseOperation createDatabaseOperation = (CreateDatabaseOperation) operation;
            Catalog catalog = getCatalogOrThrowException(createDatabaseOperation.getCatalogName());
            String exMsg = getDDLOpExecuteErrorMsg(createDatabaseOperation.asSummaryString());
            try {
                catalog.createDatabase(
                        createDatabaseOperation.getDatabaseName(),
                        createDatabaseOperation.getCatalogDatabase(),
                        createDatabaseOperation.isIgnoreIfExists());
                return TableResultImpl.TABLE_RESULT_OK;
            } catch (DatabaseAlreadyExistException e) {
                throw new ValidationException(exMsg, e);
            } catch (Exception e) {
                throw new TableException(exMsg, e);
            }
        } else if (operation instanceof DropDatabaseOperation) {
            DropDatabaseOperation dropDatabaseOperation = (DropDatabaseOperation) operation;
            Catalog catalog = getCatalogOrThrowException(dropDatabaseOperation.getCatalogName());
            String exMsg = getDDLOpExecuteErrorMsg(dropDatabaseOperation.asSummaryString());
            try {
                catalog.dropDatabase(
                        dropDatabaseOperation.getDatabaseName(),
                        dropDatabaseOperation.isIfExists(),
                        dropDatabaseOperation.isCascade());
                return TableResultImpl.TABLE_RESULT_OK;
            } catch (DatabaseNotExistException | DatabaseNotEmptyException e) {
                throw new ValidationException(exMsg, e);
            } catch (Exception e) {
                throw new TableException(exMsg, e);
            }
        } else if (operation instanceof AlterDatabaseOperation) {
            AlterDatabaseOperation alterDatabaseOperation = (AlterDatabaseOperation) operation;
            Catalog catalog = getCatalogOrThrowException(alterDatabaseOperation.getCatalogName());
            String exMsg = getDDLOpExecuteErrorMsg(alterDatabaseOperation.asSummaryString());
            try {
                catalog.alterDatabase(
                        alterDatabaseOperation.getDatabaseName(),
                        alterDatabaseOperation.getCatalogDatabase(),
                        false);
                return TableResultImpl.TABLE_RESULT_OK;
            } catch (DatabaseNotExistException e) {
                throw new ValidationException(exMsg, e);
            } catch (Exception e) {
                throw new TableException(exMsg, e);
            }
        } else if (operation instanceof CreateCatalogFunctionOperation) {
            return createCatalogFunction((CreateCatalogFunctionOperation) operation);
        } else if (operation instanceof CreateTempSystemFunctionOperation) {
            return createSystemFunction((CreateTempSystemFunctionOperation) operation);
        } else if (operation instanceof DropCatalogFunctionOperation) {
            return dropCatalogFunction((DropCatalogFunctionOperation) operation);
        } else if (operation instanceof DropTempSystemFunctionOperation) {
            return dropSystemFunction((DropTempSystemFunctionOperation) operation);
        } else if (operation instanceof AlterCatalogFunctionOperation) {
            return alterCatalogFunction((AlterCatalogFunctionOperation) operation);
        } else if (operation instanceof CreateCatalogOperation) {
            return createCatalog((CreateCatalogOperation) operation);
        } else if (operation instanceof DropCatalogOperation) {
            DropCatalogOperation dropCatalogOperation = (DropCatalogOperation) operation;
            String exMsg = getDDLOpExecuteErrorMsg(dropCatalogOperation.asSummaryString());
            try {
                catalogManager.unregisterCatalog(
                        dropCatalogOperation.getCatalogName(), dropCatalogOperation.isIfExists());
                return TableResultImpl.TABLE_RESULT_OK;
            } catch (CatalogException e) {
                throw new ValidationException(exMsg, e);
            }
        } else if (operation instanceof LoadModuleOperation) {
            return loadModule((LoadModuleOperation) operation);
        } else if (operation instanceof UnloadModuleOperation) {
            return unloadModule((UnloadModuleOperation) operation);
        } else if (operation instanceof UseModulesOperation) {
            return useModules((UseModulesOperation) operation);
        } else if (operation instanceof UseCatalogOperation) {
            UseCatalogOperation useCatalogOperation = (UseCatalogOperation) operation;
            catalogManager.setCurrentCatalog(useCatalogOperation.getCatalogName());
            return TableResultImpl.TABLE_RESULT_OK;
        } else if (operation instanceof UseDatabaseOperation) {
            UseDatabaseOperation useDatabaseOperation = (UseDatabaseOperation) operation;
            catalogManager.setCurrentCatalog(useDatabaseOperation.getCatalogName());
            catalogManager.setCurrentDatabase(useDatabaseOperation.getDatabaseName());
            return TableResultImpl.TABLE_RESULT_OK;
        } else if (operation instanceof ShowCatalogsOperation) {
            return buildShowResult("catalog name", listCatalogs());
        } else if (operation instanceof ShowCreateTableOperation) {
            ShowCreateTableOperation showCreateTableOperation =
                    (ShowCreateTableOperation) operation;
            ContextResolvedTable table =
                    catalogManager
                            .getTable(showCreateTableOperation.getTableIdentifier())
                            .orElseThrow(
                                    () ->
                                            new ValidationException(
                                                    String.format(
                                                            "Could not execute SHOW CREATE TABLE. Table with identifier %s does not exist.",
                                                            showCreateTableOperation
                                                                    .getTableIdentifier()
                                                                    .asSerializableString())));

            return TableResultImpl.builder()
                    .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                    .schema(ResolvedSchema.of(Column.physical("result", DataTypes.STRING())))
                    .data(
                            Collections.singletonList(
                                    Row.of(
                                            ShowCreateUtil.buildShowCreateTableRow(
                                                    table.getResolvedTable(),
                                                    showCreateTableOperation.getTableIdentifier(),
                                                    table.isTemporary()))))
                    .build();

        } else if (operation instanceof ShowCreateViewOperation) {
            ShowCreateViewOperation showCreateViewOperation = (ShowCreateViewOperation) operation;
            final ContextResolvedTable table =
                    catalogManager
                            .getTable(showCreateViewOperation.getViewIdentifier())
                            .orElseThrow(
                                    () ->
                                            new ValidationException(
                                                    String.format(
                                                            "Could not execute SHOW CREATE VIEW. View with identifier %s does not exist.",
                                                            showCreateViewOperation
                                                                    .getViewIdentifier()
                                                                    .asSerializableString())));

            return TableResultImpl.builder()
                    .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                    .schema(ResolvedSchema.of(Column.physical("result", DataTypes.STRING())))
                    .data(
                            Collections.singletonList(
                                    Row.of(
                                            ShowCreateUtil.buildShowCreateViewRow(
                                                    table.getResolvedTable(),
                                                    showCreateViewOperation.getViewIdentifier(),
                                                    table.isTemporary()))))
                    .build();
        } else if (operation instanceof ShowCurrentCatalogOperation) {
            return buildShowResult(
                    "current catalog name", new String[] {catalogManager.getCurrentCatalog()});
        } else if (operation instanceof ShowDatabasesOperation) {
            return buildShowResult("database name", listDatabases());
        } else if (operation instanceof ShowCurrentDatabaseOperation) {
            return buildShowResult(
                    "current database name", new String[] {catalogManager.getCurrentDatabase()});
        } else if (operation instanceof ShowModulesOperation) {
            ShowModulesOperation showModulesOperation = (ShowModulesOperation) operation;
            if (showModulesOperation.requireFull()) {
                return buildShowFullModulesResult(listFullModules());
            } else {
                return buildShowResult("module name", listModules());
            }
        } else if (operation instanceof ShowTablesOperation) {
            ShowTablesOperation showTablesOperation = (ShowTablesOperation) operation;
            if (showTablesOperation.getPreposition() == null) {
                return buildShowTablesResult(listTables(), showTablesOperation);
            }
            final String catalogName = showTablesOperation.getCatalogName();
            final String databaseName = showTablesOperation.getDatabaseName();
            Catalog catalog = getCatalogOrThrowException(catalogName);
            if (catalog.databaseExists(databaseName)) {
                return buildShowTablesResult(
                        listTables(catalogName, databaseName), showTablesOperation);
            } else {
                throw new ValidationException(
                        String.format(
                                "Database '%s'.'%s' doesn't exist.", catalogName, databaseName));
            }
        } else if (operation instanceof ShowFunctionsOperation) {
            ShowFunctionsOperation showFunctionsOperation = (ShowFunctionsOperation) operation;
            String[] functionNames = null;
            ShowFunctionsOperation.FunctionScope functionScope =
                    showFunctionsOperation.getFunctionScope();
            switch (functionScope) {
                case USER:
                    functionNames = listUserDefinedFunctions();
                    break;
                case ALL:
                    functionNames = listFunctions();
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "SHOW FUNCTIONS with %s scope is not supported.",
                                    functionScope));
            }
            return buildShowResult("function name", functionNames);
        } else if (operation instanceof ShowViewsOperation) {
            return buildShowResult("view name", listViews());
        } else if (operation instanceof ShowColumnsOperation) {
            ShowColumnsOperation showColumnsOperation = (ShowColumnsOperation) operation;
            Optional<ContextResolvedTable> result =
                    catalogManager.getTable(showColumnsOperation.getTableIdentifier());
            if (result.isPresent()) {
                return buildShowColumnsResult(
                        result.get().getResolvedSchema(), showColumnsOperation);
            } else {
                throw new ValidationException(
                        String.format(
                                "Tables or views with the identifier '%s' doesn't exist.",
                                showColumnsOperation.getTableIdentifier().asSummaryString()));
            }
        } else if (operation instanceof ShowPartitionsOperation) {
            String exMsg = getDDLOpExecuteErrorMsg(operation.asSummaryString());
            try {
                ShowPartitionsOperation showPartitionsOperation =
                        (ShowPartitionsOperation) operation;
                Catalog catalog =
                        getCatalogOrThrowException(
                                showPartitionsOperation.getTableIdentifier().getCatalogName());
                ObjectPath tablePath = showPartitionsOperation.getTableIdentifier().toObjectPath();
                CatalogPartitionSpec partitionSpec = showPartitionsOperation.getPartitionSpec();
                List<CatalogPartitionSpec> partitionSpecs =
                        partitionSpec == null
                                ? catalog.listPartitions(tablePath)
                                : catalog.listPartitions(tablePath, partitionSpec);
                List<String> partitionNames = new ArrayList<>(partitionSpecs.size());
                for (CatalogPartitionSpec spec : partitionSpecs) {
                    List<String> partitionKVs = new ArrayList<>(spec.getPartitionSpec().size());
                    for (Map.Entry<String, String> partitionKV :
                            spec.getPartitionSpec().entrySet()) {
                        partitionKVs.add(partitionKV.getKey() + "=" + partitionKV.getValue());
                    }
                    partitionNames.add(String.join("/", partitionKVs));
                }
                return buildShowResult("partition name", partitionNames.toArray(new String[0]));
            } catch (TableNotExistException e) {
                throw new ValidationException(exMsg, e);
            } catch (Exception e) {
                throw new TableException(exMsg, e);
            }
        } else if (operation instanceof ExplainOperation) {
            ExplainOperation explainOperation = (ExplainOperation) operation;
            ExplainDetail[] explainDetails =
                    explainOperation.getExplainDetails().stream()
                            .map(ExplainDetail::valueOf)
                            .toArray(ExplainDetail[]::new);
            Operation child = ((ExplainOperation) operation).getChild();
            List<Operation> operations;
            if (child instanceof StatementSetOperation) {
                operations = new ArrayList<>(((StatementSetOperation) child).getOperations());
            } else {
                operations = Collections.singletonList(child);
            }
            String explanation = explainInternal(operations, explainDetails);
            return TableResultImpl.builder()
                    .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                    .schema(ResolvedSchema.of(Column.physical("result", DataTypes.STRING())))
                    .data(Collections.singletonList(Row.of(explanation)))
                    .build();
        } else if (operation instanceof DescribeTableOperation) {
            DescribeTableOperation describeTableOperation = (DescribeTableOperation) operation;
            Optional<ContextResolvedTable> result =
                    catalogManager.getTable(describeTableOperation.getSqlIdentifier());
            if (result.isPresent()) {
                return buildDescribeResult(result.get().getResolvedSchema());
            } else {
                throw new ValidationException(
                        String.format(
                                "Tables or views with the identifier '%s' doesn't exist",
                                describeTableOperation.getSqlIdentifier().asSummaryString()));
            }
        } else if (operation instanceof QueryOperation) {
            return executeQueryOperation((QueryOperation) operation);
        } else if (operation instanceof CreateTableASOperation) {
            CreateTableASOperation createTableASOperation = (CreateTableASOperation) operation;
            executeInternal(createTableASOperation.getCreateTableOperation());
            return executeInternal(createTableASOperation.toSinkModifyOperation(catalogManager));
        } else if (operation instanceof ExecutePlanOperation) {
            ExecutePlanOperation executePlanOperation = (ExecutePlanOperation) operation;
            return (TableResultInternal)
                    executePlan(PlanReference.fromFile(executePlanOperation.getFilePath()));
        } else if (operation instanceof CompilePlanOperation) {
            CompilePlanOperation compilePlanOperation = (CompilePlanOperation) operation;
            compilePlanAndWrite(
                    compilePlanOperation.getFilePath(),
                    compilePlanOperation.isIfNotExists(),
                    compilePlanOperation.getOperation());
            return TableResultImpl.TABLE_RESULT_OK;
        } else if (operation instanceof CompileAndExecutePlanOperation) {
            CompileAndExecutePlanOperation compileAndExecutePlanOperation =
                    (CompileAndExecutePlanOperation) operation;
            CompiledPlan compiledPlan =
                    compilePlanAndWrite(
                            compileAndExecutePlanOperation.getFilePath(),
                            true,
                            compileAndExecutePlanOperation.getOperation());
            return (TableResultInternal) compiledPlan.execute();
        } else if (operation instanceof NopOperation) {
            return TableResultImpl.TABLE_RESULT_OK;
        } else {
            throw new TableException(UNSUPPORTED_QUERY_IN_EXECUTE_SQL_MSG);
        }
    }

    private TableResultInternal createCatalog(CreateCatalogOperation operation) {
        String exMsg = getDDLOpExecuteErrorMsg(operation.asSummaryString());
        try {
            String catalogName = operation.getCatalogName();
            Map<String, String> properties = operation.getProperties();

            Catalog catalog =
                    FactoryUtil.createCatalog(
                            catalogName,
                            properties,
                            tableConfig.getConfiguration(),
                            userClassLoader);
            catalogManager.registerCatalog(catalogName, catalog);

            return TableResultImpl.TABLE_RESULT_OK;
        } catch (CatalogException e) {
            throw new ValidationException(exMsg, e);
        }
    }

    private TableResultInternal loadModule(LoadModuleOperation operation) {
        final String exMsg = getDDLOpExecuteErrorMsg(operation.asSummaryString());
        try {
            final Module module =
                    FactoryUtil.createModule(
                            operation.getModuleName(),
                            operation.getOptions(),
                            tableConfig.getConfiguration(),
                            userClassLoader);
            moduleManager.loadModule(operation.getModuleName(), module);
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (ValidationException e) {
            throw new ValidationException(String.format("%s. %s", exMsg, e.getMessage()), e);
        } catch (Exception e) {
            throw new TableException(String.format("%s. %s", exMsg, e.getMessage()), e);
        }
    }

    private TableResultInternal unloadModule(UnloadModuleOperation operation) {
        String exMsg = getDDLOpExecuteErrorMsg(operation.asSummaryString());
        try {
            moduleManager.unloadModule(operation.getModuleName());
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (ValidationException e) {
            throw new ValidationException(String.format("%s. %s", exMsg, e.getMessage()), e);
        }
    }

    private TableResultInternal useModules(UseModulesOperation operation) {
        String exMsg = getDDLOpExecuteErrorMsg(operation.asSummaryString());
        try {
            moduleManager.useModules(operation.getModuleNames().toArray(new String[0]));
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (ValidationException e) {
            throw new ValidationException(String.format("%s. %s", exMsg, e.getMessage()), e);
        }
    }

    private TableResultInternal buildShowResult(String columnName, String[] objects) {
        return buildResult(
                new String[] {columnName},
                new DataType[] {DataTypes.STRING()},
                Arrays.stream(objects).map((c) -> new String[] {c}).toArray(String[][]::new));
    }

    private TableResultInternal buildDescribeResult(ResolvedSchema schema) {
        Object[][] rows = buildTableColumns(schema);
        return buildResult(generateTableColumnsNames(), generateTableColumnsDataTypes(), rows);
    }

    private DataType[] generateTableColumnsDataTypes() {
        return new DataType[] {
            DataTypes.STRING(),
            DataTypes.STRING(),
            DataTypes.BOOLEAN(),
            DataTypes.STRING(),
            DataTypes.STRING(),
            DataTypes.STRING()
        };
    }

    private String[] generateTableColumnsNames() {
        return new String[] {"name", "type", "null", "key", "extras", "watermark"};
    }

    private TableResultInternal buildShowTablesResult(
            String[] tableList, ShowTablesOperation showTablesOp) {
        String[] rows = tableList.clone();
        if (showTablesOp.isUseLike()) {
            rows =
                    Arrays.stream(tableList)
                            .filter(
                                    row ->
                                            showTablesOp.isNotLike()
                                                    != SqlLikeUtils.like(
                                                            row,
                                                            showTablesOp.getLikePattern(),
                                                            "\\"))
                            .toArray(String[]::new);
        }
        return buildShowResult("table name", rows);
    }

    private TableResultInternal buildShowColumnsResult(
            ResolvedSchema schema, ShowColumnsOperation showColumnsOp) {
        Object[][] rows = buildTableColumns(schema);
        if (showColumnsOp.isUseLike()) {
            rows =
                    Arrays.stream(rows)
                            .filter(
                                    row ->
                                            showColumnsOp.isNotLike()
                                                    != SqlLikeUtils.like(
                                                            row[0].toString(),
                                                            showColumnsOp.getLikePattern(),
                                                            "\\"))
                            .toArray(Object[][]::new);
        }
        return buildResult(generateTableColumnsNames(), generateTableColumnsDataTypes(), rows);
    }

    private TableResultInternal buildShowFullModulesResult(ModuleEntry[] moduleEntries) {
        Object[][] rows =
                Arrays.stream(moduleEntries)
                        .map(entry -> new Object[] {entry.name(), entry.used()})
                        .toArray(Object[][]::new);
        return buildResult(
                new String[] {"module name", "used"},
                new DataType[] {DataTypes.STRING(), DataTypes.BOOLEAN()},
                rows);
    }

    private Object[][] buildTableColumns(ResolvedSchema schema) {
        Map<String, String> fieldToWatermark =
                schema.getWatermarkSpecs().stream()
                        .collect(
                                Collectors.toMap(
                                        WatermarkSpec::getRowtimeAttribute,
                                        spec -> spec.getWatermarkExpression().asSummaryString()));

        Map<String, String> fieldToPrimaryKey = new HashMap<>();
        schema.getPrimaryKey()
                .ifPresent(
                        (p) -> {
                            List<String> columns = p.getColumns();
                            columns.forEach(
                                    (c) ->
                                            fieldToPrimaryKey.put(
                                                    c,
                                                    String.format(
                                                            "PRI(%s)",
                                                            String.join(", ", columns))));
                        });

        return schema.getColumns().stream()
                .map(
                        (c) -> {
                            final LogicalType logicalType = c.getDataType().getLogicalType();
                            return new Object[] {
                                c.getName(),
                                logicalType.copy(true).asSummaryString(),
                                logicalType.isNullable(),
                                fieldToPrimaryKey.getOrDefault(c.getName(), null),
                                c.explainExtras().orElse(null),
                                fieldToWatermark.getOrDefault(c.getName(), null)
                            };
                        })
                .toArray(Object[][]::new);
    }

    private TableResultInternal buildResult(String[] headers, DataType[] types, Object[][] rows) {
        ResolvedSchema schema = ResolvedSchema.physical(headers, types);
        ResultProvider provider =
                new StaticResultProvider(
                        Arrays.stream(rows).map(Row::of).collect(Collectors.toList()));
        return TableResultImpl.builder()
                .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                .schema(ResolvedSchema.physical(headers, types))
                .resultProvider(provider)
                .setPrintStyle(
                        PrintStyle.tableauWithDataInferredColumnWidths(
                                schema,
                                provider.getRowDataStringConverter(),
                                Integer.MAX_VALUE,
                                true,
                                false))
                .build();
    }

    /**
     * extract sink identifier names from {@link ModifyOperation}s and deduplicate them with {@link
     * #deduplicateSinkIdentifierNames(List)}.
     */
    private List<String> extractSinkIdentifierNames(List<ModifyOperation> operations) {
        List<String> tableNames = new ArrayList<>(operations.size());
        for (ModifyOperation operation : operations) {
            if (operation instanceof SinkModifyOperation) {
                String fullName =
                        ((SinkModifyOperation) operation)
                                .getContextResolvedTable()
                                .getIdentifier()
                                .asSummaryString();
                tableNames.add(fullName);
            } else {
                throw new UnsupportedOperationException("Unsupported operation: " + operation);
            }
        }
        return deduplicateSinkIdentifierNames(tableNames);
    }

    /**
     * Deduplicate sink identifier names. If there are multiple tables with the same name, an index
     * suffix will be added at the end of the name to ensure each name is unique.
     */
    private List<String> deduplicateSinkIdentifierNames(List<String> tableNames) {
        Map<String, Integer> tableNameToCount = new HashMap<>();
        for (String fullName : tableNames) {
            tableNameToCount.put(fullName, tableNameToCount.getOrDefault(fullName, 0) + 1);
        }

        Map<String, Integer> tableNameToIndex = new HashMap<>();
        return tableNames.stream()
                .map(
                        tableName -> {
                            if (tableNameToCount.get(tableName) == 1) {
                                return tableName;
                            } else {
                                Integer index = tableNameToIndex.getOrDefault(tableName, 0) + 1;
                                tableNameToIndex.put(tableName, index);
                                return tableName + "_" + index;
                            }
                        })
                .collect(Collectors.toList());
    }

    /** Get catalog from catalogName or throw a ValidationException if the catalog not exists. */
    private Catalog getCatalogOrThrowException(String catalogName) {
        return getCatalog(catalogName)
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        String.format("Catalog %s does not exist", catalogName)));
    }

    private String getDDLOpExecuteErrorMsg(String action) {
        return String.format("Could not execute %s", action);
    }

    @Override
    public String getCurrentCatalog() {
        return catalogManager.getCurrentCatalog();
    }

    @Override
    public void useCatalog(String catalogName) {
        catalogManager.setCurrentCatalog(catalogName);
    }

    @Override
    public String getCurrentDatabase() {
        return catalogManager.getCurrentDatabase();
    }

    @Override
    public void useDatabase(String databaseName) {
        catalogManager.setCurrentDatabase(databaseName);
    }

    @Override
    public TableConfig getConfig() {
        return tableConfig;
    }

    @Override
    public Parser getParser() {
        return getPlanner().getParser();
    }

    @Override
    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    @Override
    public OperationTreeBuilder getOperationTreeBuilder() {
        return operationTreeBuilder;
    }

    /**
     * Subclasses can override this method to transform the given QueryOperation to a new one with
     * the qualified object identifier. This is needed for some QueryOperations, e.g.
     * JavaDataStreamQueryOperation, which doesn't know the registered identifier when created
     * ({@code fromDataStream(DataStream)}. But the identifier is required when converting this
     * QueryOperation to RelNode.
     */
    protected QueryOperation qualifyQueryOperation(
            ObjectIdentifier identifier, QueryOperation queryOperation) {
        return queryOperation;
    }

    /**
     * Subclasses can override this method to add additional checks.
     *
     * @param tableSource tableSource to validate
     */
    protected void validateTableSource(TableSource<?> tableSource) {
        TableSourceValidation.validateTableSource(tableSource, tableSource.getTableSchema());
    }

    protected List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
        return planner.translate(modifyOperations);
    }

    @Override
    public void registerTableSourceInternal(String name, TableSource<?> tableSource) {
        validateTableSource(tableSource);
        ObjectIdentifier objectIdentifier =
                catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(name));
        Optional<CatalogBaseTable> table = getTemporaryTable(objectIdentifier);

        if (table.isPresent()) {
            if (table.get() instanceof ConnectorCatalogTable<?, ?>) {
                ConnectorCatalogTable<?, ?> sourceSinkTable =
                        (ConnectorCatalogTable<?, ?>) table.get();
                if (sourceSinkTable.getTableSource().isPresent()) {
                    throw new ValidationException(
                            String.format(
                                    "Table '%s' already exists. Please choose a different name.",
                                    name));
                } else {
                    // wrapper contains only sink (not source)
                    ConnectorCatalogTable sourceAndSink =
                            ConnectorCatalogTable.sourceAndSink(
                                    tableSource,
                                    sourceSinkTable.getTableSink().get(),
                                    !IS_STREAM_TABLE);
                    catalogManager.dropTemporaryTable(objectIdentifier, false);
                    catalogManager.createTemporaryTable(sourceAndSink, objectIdentifier, false);
                }
            } else {
                throw new ValidationException(
                        String.format(
                                "Table '%s' already exists. Please choose a different name.",
                                name));
            }
        } else {
            ConnectorCatalogTable source =
                    ConnectorCatalogTable.source(tableSource, !IS_STREAM_TABLE);
            catalogManager.createTemporaryTable(source, objectIdentifier, false);
        }
    }

    @Override
    public void registerTableSinkInternal(String name, TableSink<?> tableSink) {
        ObjectIdentifier objectIdentifier =
                catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(name));
        Optional<CatalogBaseTable> table = getTemporaryTable(objectIdentifier);

        if (table.isPresent()) {
            if (table.get() instanceof ConnectorCatalogTable<?, ?>) {
                ConnectorCatalogTable<?, ?> sourceSinkTable =
                        (ConnectorCatalogTable<?, ?>) table.get();
                if (sourceSinkTable.getTableSink().isPresent()) {
                    throw new ValidationException(
                            String.format(
                                    "Table '%s' already exists. Please choose a different name.",
                                    name));
                } else {
                    // wrapper contains only sink (not source)
                    ConnectorCatalogTable sourceAndSink =
                            ConnectorCatalogTable.sourceAndSink(
                                    sourceSinkTable.getTableSource().get(),
                                    tableSink,
                                    !IS_STREAM_TABLE);
                    catalogManager.dropTemporaryTable(objectIdentifier, false);
                    catalogManager.createTemporaryTable(sourceAndSink, objectIdentifier, false);
                }
            } else {
                throw new ValidationException(
                        String.format(
                                "Table '%s' already exists. Please choose a different name.",
                                name));
            }
        } else {
            ConnectorCatalogTable sink = ConnectorCatalogTable.sink(tableSink, !IS_STREAM_TABLE);
            catalogManager.createTemporaryTable(sink, objectIdentifier, false);
        }
    }

    private Optional<CatalogBaseTable> getTemporaryTable(ObjectIdentifier identifier) {
        return catalogManager
                .getTable(identifier)
                .filter(ContextResolvedTable::isTemporary)
                .map(ContextResolvedTable::getTable);
    }

    private TableResultInternal createCatalogFunction(
            CreateCatalogFunctionOperation createCatalogFunctionOperation) {
        String exMsg = getDDLOpExecuteErrorMsg(createCatalogFunctionOperation.asSummaryString());
        try {
            if (createCatalogFunctionOperation.isTemporary()) {
                functionCatalog.registerTemporaryCatalogFunction(
                        UnresolvedIdentifier.of(
                                createCatalogFunctionOperation.getFunctionIdentifier().toList()),
                        createCatalogFunctionOperation.getCatalogFunction(),
                        createCatalogFunctionOperation.isIgnoreIfExists());
            } else {
                Catalog catalog =
                        getCatalogOrThrowException(
                                createCatalogFunctionOperation
                                        .getFunctionIdentifier()
                                        .getCatalogName());
                catalog.createFunction(
                        createCatalogFunctionOperation.getFunctionIdentifier().toObjectPath(),
                        createCatalogFunctionOperation.getCatalogFunction(),
                        createCatalogFunctionOperation.isIgnoreIfExists());
            }
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (ValidationException e) {
            throw e;
        } catch (FunctionAlreadyExistException e) {
            throw new ValidationException(e.getMessage(), e);
        } catch (Exception e) {
            throw new TableException(exMsg, e);
        }
    }

    private TableResultInternal alterCatalogFunction(
            AlterCatalogFunctionOperation alterCatalogFunctionOperation) {
        String exMsg = getDDLOpExecuteErrorMsg(alterCatalogFunctionOperation.asSummaryString());
        try {
            CatalogFunction function = alterCatalogFunctionOperation.getCatalogFunction();
            if (alterCatalogFunctionOperation.isTemporary()) {
                throw new ValidationException("Alter temporary catalog function is not supported");
            } else {
                Catalog catalog =
                        getCatalogOrThrowException(
                                alterCatalogFunctionOperation
                                        .getFunctionIdentifier()
                                        .getCatalogName());
                catalog.alterFunction(
                        alterCatalogFunctionOperation.getFunctionIdentifier().toObjectPath(),
                        function,
                        alterCatalogFunctionOperation.isIfExists());
            }
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (ValidationException e) {
            throw e;
        } catch (FunctionNotExistException e) {
            throw new ValidationException(e.getMessage(), e);
        } catch (Exception e) {
            throw new TableException(exMsg, e);
        }
    }

    private TableResultInternal dropCatalogFunction(
            DropCatalogFunctionOperation dropCatalogFunctionOperation) {
        String exMsg = getDDLOpExecuteErrorMsg(dropCatalogFunctionOperation.asSummaryString());
        try {
            if (dropCatalogFunctionOperation.isTemporary()) {
                functionCatalog.dropTempCatalogFunction(
                        dropCatalogFunctionOperation.getFunctionIdentifier(),
                        dropCatalogFunctionOperation.isIfExists());
            } else {
                Catalog catalog =
                        getCatalogOrThrowException(
                                dropCatalogFunctionOperation
                                        .getFunctionIdentifier()
                                        .getCatalogName());

                catalog.dropFunction(
                        dropCatalogFunctionOperation.getFunctionIdentifier().toObjectPath(),
                        dropCatalogFunctionOperation.isIfExists());
            }
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (ValidationException e) {
            throw e;
        } catch (FunctionNotExistException e) {
            throw new ValidationException(e.getMessage(), e);
        } catch (Exception e) {
            throw new TableException(exMsg, e);
        }
    }

    private TableResultInternal createSystemFunction(CreateTempSystemFunctionOperation operation) {
        String exMsg = getDDLOpExecuteErrorMsg(operation.asSummaryString());
        try {
            functionCatalog.registerTemporarySystemFunction(
                    operation.getFunctionName(),
                    operation.getCatalogFunction(),
                    operation.isIgnoreIfExists());
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new TableException(exMsg, e);
        }
    }

    private TableResultInternal dropSystemFunction(DropTempSystemFunctionOperation operation) {
        try {
            functionCatalog.dropTemporarySystemFunction(
                    operation.getFunctionName(), operation.isIfExists());
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new TableException(getDDLOpExecuteErrorMsg(operation.asSummaryString()), e);
        }
    }

    @VisibleForTesting
    public TableImpl createTable(QueryOperation tableOperation) {
        return TableImpl.createTable(
                this,
                tableOperation,
                operationTreeBuilder,
                functionCatalog.asLookup(getParser()::parseIdentifier));
    }

    @Override
    public String explainPlan(InternalPlan compiledPlan, ExplainDetail... extraDetails) {
        return planner.explainPlan(compiledPlan, extraDetails);
    }
}
