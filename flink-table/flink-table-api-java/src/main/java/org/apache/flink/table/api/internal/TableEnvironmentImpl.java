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
import org.apache.flink.core.execution.JobStatusHook;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.ExplainFormat;
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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.CatalogStoreHolder;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StagedTable;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkStagingContext;
import org.apache.flink.table.connector.sink.abilities.SupportsStaging;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.execution.StagingSinkJobStatusHook;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.CatalogStoreFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleEntry;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.CollectModifyOperation;
import org.apache.flink.table.operations.CompileAndExecutePlanOperation;
import org.apache.flink.table.operations.CreateTableASOperation;
import org.apache.flink.table.operations.DeleteFromFilterOperation;
import org.apache.flink.table.operations.ExecutableOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.NopOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.ReplaceTableAsOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.SourceQueryOperation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.operations.command.ExecutePlanOperation;
import org.apache.flink.table.operations.ddl.AnalyzeTableOperation;
import org.apache.flink.table.operations.ddl.CompilePlanOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.utils.ExecutableOperationUtils;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.utils.print.PrintStyle;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
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
    protected final ResourceManager resourceManager;
    private final OperationTreeBuilder operationTreeBuilder;

    protected final TableConfig tableConfig;
    protected final Executor execEnv;
    protected final FunctionCatalog functionCatalog;
    protected final Planner planner;
    private final boolean isStreamingMode;
    private final ExecutableOperation.Context operationCtx;

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
            ResourceManager resourceManager,
            TableConfig tableConfig,
            Executor executor,
            FunctionCatalog functionCatalog,
            Planner planner,
            boolean isStreamingMode) {
        this.catalogManager = catalogManager;
        this.moduleManager = moduleManager;
        this.resourceManager = resourceManager;
        this.execEnv = executor;

        this.tableConfig = tableConfig;

        this.functionCatalog = functionCatalog;
        this.planner = planner;
        this.isStreamingMode = isStreamingMode;
        this.operationTreeBuilder =
                OperationTreeBuilder.create(
                        tableConfig,
                        resourceManager.getUserClassLoader(),
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
                        getParser()::parseSqlExpression,
                        isStreamingMode);
        catalogManager.initSchemaResolver(
                isStreamingMode, operationTreeBuilder.getResolverBuilder());
        this.operationCtx =
                new ExecutableOperationContextImpl(
                        catalogManager,
                        functionCatalog,
                        moduleManager,
                        resourceManager,
                        tableConfig,
                        isStreamingMode);
    }

    public static TableEnvironmentImpl create(Configuration configuration) {
        return create(EnvironmentSettings.newInstance().withConfiguration(configuration).build());
    }

    public static TableEnvironmentImpl create(EnvironmentSettings settings) {
        final MutableURLClassLoader userClassLoader =
                FlinkUserCodeClassLoaders.create(
                        new URL[0], settings.getUserClassLoader(), settings.getConfiguration());

        final ExecutorFactory executorFactory =
                FactoryUtil.discoverFactory(
                        userClassLoader, ExecutorFactory.class, ExecutorFactory.DEFAULT_IDENTIFIER);
        final Executor executor = executorFactory.create(settings.getConfiguration());

        final CatalogStoreFactory catalogStoreFactory =
                TableFactoryUtil.findAndCreateCatalogStoreFactory(
                        settings.getConfiguration(), userClassLoader);
        final CatalogStoreFactory.Context context =
                TableFactoryUtil.buildCatalogStoreFactoryContext(
                        settings.getConfiguration(), userClassLoader);
        catalogStoreFactory.open(context);
        final CatalogStore catalogStore =
                settings.getCatalogStore() != null
                        ? settings.getCatalogStore()
                        : catalogStoreFactory.createCatalogStore();

        // use configuration to init table config
        final TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setRootConfiguration(executor.getConfiguration());
        tableConfig.addConfiguration(settings.getConfiguration());

        final ResourceManager resourceManager =
                new ResourceManager(settings.getConfiguration(), userClassLoader);
        final ModuleManager moduleManager = new ModuleManager();
        final CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(userClassLoader)
                        .config(tableConfig)
                        .defaultCatalog(
                                settings.getBuiltInCatalogName(),
                                new GenericInMemoryCatalog(
                                        settings.getBuiltInCatalogName(),
                                        settings.getBuiltInDatabaseName()))
                        .catalogModificationListeners(
                                TableFactoryUtil.findCatalogModificationListenerList(
                                        settings.getConfiguration(), userClassLoader))
                        .catalogStoreHolder(
                                CatalogStoreHolder.newBuilder()
                                        .catalogStore(catalogStore)
                                        .factory(catalogStoreFactory)
                                        .config(tableConfig)
                                        .classloader(userClassLoader)
                                        .build())
                        .build();

        final FunctionCatalog functionCatalog =
                new FunctionCatalog(tableConfig, resourceManager, catalogManager, moduleManager);

        final Planner planner =
                PlannerFactoryUtil.createPlanner(
                        executor,
                        tableConfig,
                        userClassLoader,
                        moduleManager,
                        catalogManager,
                        functionCatalog);

        return new TableEnvironmentImpl(
                catalogManager,
                moduleManager,
                resourceManager,
                tableConfig,
                executor,
                functionCatalog,
                planner,
                settings.isStreamingMode());
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
    public void createCatalog(String catalogName, CatalogDescriptor catalogDescriptor) {
        catalogManager.createCatalog(catalogName, catalogDescriptor);
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
    public void createTemporarySystemFunction(
            String name, String className, List<ResourceUri> resourceUris) {
        functionCatalog.registerTemporarySystemFunction(name, className, resourceUris);
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
    public void createFunction(String path, String className, List<ResourceUri> resourceUris) {
        createFunction(path, className, resourceUris, false);
    }

    @Override
    public void createFunction(
            String path, String className, List<ResourceUri> resourceUris, boolean ignoreIfExists) {
        final UnresolvedIdentifier unresolvedIdentifier = getParser().parseIdentifier(path);
        functionCatalog.registerCatalogFunction(
                unresolvedIdentifier, className, resourceUris, ignoreIfExists);
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
    public void createTemporaryFunction(
            String path, String className, List<ResourceUri> resourceUris) {
        final UnresolvedIdentifier unresolvedIdentifier = getParser().parseIdentifier(path);
        final CatalogFunction catalogFunction =
                new CatalogFunctionImpl(className, FunctionLanguage.JAVA, resourceUris);
        functionCatalog.registerTemporaryCatalogFunction(
                unresolvedIdentifier, catalogFunction, false);
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
        return catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get().listDatabases()
                .stream()
                .sorted()
                .toArray(String[]::new);
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
    public String explainSql(
            String statement, ExplainFormat format, ExplainDetail... extraDetails) {
        List<Operation> operations = getParser().parse(statement);

        if (operations.size() != 1) {
            throw new TableException(
                    "Unsupported SQL query! explainSql() only accepts a single SQL query.");
        }

        if (operations.get(0) instanceof StatementSetOperation) {
            operations =
                    new ArrayList<>(((StatementSetOperation) operations.get(0)).getOperations());
        }
        return explainInternal(operations, format, extraDetails);
    }

    @Override
    public String explainInternal(
            List<Operation> operations, ExplainFormat format, ExplainDetail... extraDetails) {
        operations =
                operations.stream()
                        .filter(o -> !(o instanceof NopOperation))
                        .collect(Collectors.toList());
        // hive parser may generate an NopOperation, in which case we just return an
        // empty string as the plan
        if (operations.isEmpty()) {
            return "";
        } else {
            if (operations.size() > 1
                    && operations.stream().anyMatch(this::isRowLevelModification)) {
                throw new TableException(
                        "Unsupported SQL query! Only accept a single SQL statement of type DELETE, UPDATE.");
            }
            return planner.explain(operations, format, extraDetails);
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

        Operation operation = operations.get(0);
        return executeInternal(operation);
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

        if (operations.size() != 1
                || !(operations.get(0) instanceof ModifyOperation)
                || isRowLevelModification(operations.get(0))
                || operations.get(0) instanceof CreateTableASOperation) {
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
            String pathString, boolean ignoreIfExists, Operation operation) {
        try {
            ResourceUri planResource = new ResourceUri(ResourceType.FILE, pathString);
            Path planPath = new Path(pathString);
            if (resourceManager.exists(planPath)) {
                if (ignoreIfExists) {
                    return loadPlan(
                            PlanReference.fromFile(
                                    resourceManager.registerFileResource(planResource)));
                }

                if (!tableConfig.get(TableConfigOptions.PLAN_FORCE_RECOMPILE)) {
                    throw new TableException(
                            String.format(
                                    "Cannot overwrite the plan file '%s'. "
                                            + "Either manually remove the file or, "
                                            + "if you're debugging your job, "
                                            + "set the option '%s' to true.",
                                    pathString, TableConfigOptions.PLAN_FORCE_RECOMPILE.key()));
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
            resourceManager.syncFileResource(
                    planResource, path -> compiledPlan.writeToFile(path, false));
            return compiledPlan;
        } catch (IOException e) {
            throw new TableException(
                    String.format("Failed to execute %s statement.", operation.asSummaryString()),
                    e);
        }
    }

    @Override
    public CompiledPlan compilePlan(List<ModifyOperation> operations) {
        return new CompiledPlanImpl(this, planner.compilePlan(operations));
    }

    @Override
    public TableResultInternal executeInternal(List<ModifyOperation> operations) {
        List<ModifyOperation> mapOperations = new ArrayList<>();
        List<JobStatusHook> jobStatusHookList = new LinkedList<>();
        for (ModifyOperation modify : operations) {
            if (modify instanceof CreateTableASOperation) {
                CreateTableASOperation ctasOperation = (CreateTableASOperation) modify;
                mapOperations.add(getModifyOperation(ctasOperation, jobStatusHookList));
            } else if (modify instanceof ReplaceTableAsOperation) {
                ReplaceTableAsOperation rtasOperation = (ReplaceTableAsOperation) modify;
                mapOperations.add(getModifyOperation(rtasOperation, jobStatusHookList));
            } else {
                boolean isRowLevelModification = isRowLevelModification(modify);
                if (isRowLevelModification) {
                    String modifyType =
                            ((SinkModifyOperation) modify).isDelete() ? "DELETE" : "UPDATE";
                    if (operations.size() > 1) {
                        throw new TableException(
                                String.format(
                                        "Unsupported SQL query! Only accept a single SQL statement of type %s.",
                                        modifyType));
                    }
                    if (isStreamingMode) {
                        throw new TableException(
                                String.format(
                                        "%s statement is not supported for streaming mode now.",
                                        modifyType));
                    }
                    if (modify instanceof DeleteFromFilterOperation) {
                        return executeInternal((DeleteFromFilterOperation) modify);
                    }
                }
                mapOperations.add(modify);
            }
        }

        List<Transformation<?>> transformations = translate(mapOperations);
        List<String> sinkIdentifierNames = extractSinkIdentifierNames(mapOperations);
        return executeInternal(transformations, sinkIdentifierNames, jobStatusHookList);
    }

    private ModifyOperation getModifyOperation(
            ReplaceTableAsOperation rtasOperation, List<JobStatusHook> jobStatusHookList) {
        CreateTableOperation createTableOperation = rtasOperation.getCreateTableOperation();
        ObjectIdentifier tableIdentifier = createTableOperation.getTableIdentifier();
        // First check if the replacedTable exists
        Optional<ContextResolvedTable> replacedTable = catalogManager.getTable(tableIdentifier);
        if (!rtasOperation.isCreateOrReplace() && !replacedTable.isPresent()) {
            throw new TableException(
                    String.format(
                            "The table %s to be replaced doesn't exist. "
                                    + "You can try to use CREATE TABLE AS statement or "
                                    + "CREATE OR REPLACE TABLE AS statement.",
                            tableIdentifier));
        }
        Catalog catalog =
                catalogManager.getCatalogOrThrowException(tableIdentifier.getCatalogName());
        ResolvedCatalogTable catalogTable =
                catalogManager.resolveCatalogTable(createTableOperation.getCatalogTable());
        Optional<DynamicTableSink> stagingDynamicTableSink =
                getSupportsStagingDynamicTableSink(createTableOperation, catalog, catalogTable);
        if (stagingDynamicTableSink.isPresent()) {
            // use atomic rtas
            DynamicTableSink dynamicTableSink = stagingDynamicTableSink.get();
            SupportsStaging.StagingPurpose stagingPurpose =
                    rtasOperation.isCreateOrReplace()
                            ? SupportsStaging.StagingPurpose.CREATE_OR_REPLACE_TABLE_AS
                            : SupportsStaging.StagingPurpose.REPLACE_TABLE_AS;

            StagedTable stagedTable =
                    ((SupportsStaging) dynamicTableSink)
                            .applyStaging(new SinkStagingContext(stagingPurpose));
            StagingSinkJobStatusHook stagingSinkJobStatusHook =
                    new StagingSinkJobStatusHook(stagedTable);
            jobStatusHookList.add(stagingSinkJobStatusHook);
            return rtasOperation.toStagedSinkModifyOperation(
                    tableIdentifier, catalogTable, catalog, dynamicTableSink);
        }
        // non-atomic rtas drop table first if exists, then create
        if (replacedTable.isPresent()) {
            catalogManager.dropTable(tableIdentifier, false);
        }
        executeInternal(createTableOperation);
        return rtasOperation.toSinkModifyOperation(catalogManager);
    }

    private ModifyOperation getModifyOperation(
            CreateTableASOperation ctasOperation, List<JobStatusHook> jobStatusHookList) {
        CreateTableOperation createTableOperation = ctasOperation.getCreateTableOperation();
        ObjectIdentifier tableIdentifier = createTableOperation.getTableIdentifier();
        Catalog catalog =
                catalogManager.getCatalogOrThrowException(tableIdentifier.getCatalogName());
        ResolvedCatalogTable catalogTable =
                catalogManager.resolveCatalogTable(createTableOperation.getCatalogTable());
        Optional<DynamicTableSink> stagingDynamicTableSink =
                getSupportsStagingDynamicTableSink(createTableOperation, catalog, catalogTable);
        if (stagingDynamicTableSink.isPresent()) {
            // use atomic ctas
            DynamicTableSink dynamicTableSink = stagingDynamicTableSink.get();
            SupportsStaging.StagingPurpose stagingPurpose =
                    createTableOperation.isIgnoreIfExists()
                            ? SupportsStaging.StagingPurpose.CREATE_TABLE_AS_IF_NOT_EXISTS
                            : SupportsStaging.StagingPurpose.CREATE_TABLE_AS;
            StagedTable stagedTable =
                    ((SupportsStaging) dynamicTableSink)
                            .applyStaging(new SinkStagingContext(stagingPurpose));
            StagingSinkJobStatusHook stagingSinkJobStatusHook =
                    new StagingSinkJobStatusHook(stagedTable);
            jobStatusHookList.add(stagingSinkJobStatusHook);
            return ctasOperation.toStagedSinkModifyOperation(
                    tableIdentifier, catalogTable, catalog, dynamicTableSink);
        }
        // use non-atomic ctas, create table first
        executeInternal(createTableOperation);
        return ctasOperation.toSinkModifyOperation(catalogManager);
    }

    private Optional<DynamicTableSink> getSupportsStagingDynamicTableSink(
            CreateTableOperation createTableOperation,
            Catalog catalog,
            ResolvedCatalogTable catalogTable) {
        if (tableConfig.get(TableConfigOptions.TABLE_RTAS_CTAS_ATOMICITY_ENABLED)) {
            if (!TableFactoryUtil.isLegacyConnectorOptions(
                    catalog,
                    tableConfig,
                    isStreamingMode,
                    createTableOperation.getTableIdentifier(),
                    catalogTable,
                    createTableOperation.isTemporary())) {
                try {
                    DynamicTableSink dynamicTableSink =
                            ExecutableOperationUtils.createDynamicTableSink(
                                    catalog,
                                    () -> moduleManager.getFactory((Module::getTableSinkFactory)),
                                    createTableOperation.getTableIdentifier(),
                                    catalogTable,
                                    Collections.emptyMap(),
                                    tableConfig,
                                    resourceManager.getUserClassLoader(),
                                    createTableOperation.isTemporary());
                    if (dynamicTableSink instanceof SupportsStaging) {
                        return Optional.of(dynamicTableSink);
                    }
                } catch (Exception e) {
                    throw new TableException(
                            String.format(
                                    "Fail to create DynamicTableSink for the table %s, "
                                            + "maybe the table does not support atomicity of CTAS/RTAS, "
                                            + "please set %s to false and try again.",
                                    createTableOperation.getTableIdentifier(),
                                    TableConfigOptions.TABLE_RTAS_CTAS_ATOMICITY_ENABLED.key()),
                            e);
                }
            }
        }
        return Optional.empty();
    }

    private TableResultInternal executeInternal(
            DeleteFromFilterOperation deleteFromFilterOperation) {
        Optional<Long> rows =
                deleteFromFilterOperation.getSupportsDeletePushDownSink().executeDeletion();
        if (rows.isPresent()) {
            return TableResultImpl.builder()
                    .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                    .schema(ResolvedSchema.of(Column.physical("rows affected", DataTypes.BIGINT())))
                    .data(Collections.singletonList(Row.of(rows.get())))
                    .build();
        } else {
            return TableResultImpl.TABLE_RESULT_OK;
        }
    }

    private TableResultInternal executeInternal(
            List<Transformation<?>> transformations, List<String> sinkIdentifierNames) {
        return executeInternal(transformations, sinkIdentifierNames, Collections.emptyList());
    }

    private TableResultInternal executeInternal(
            List<Transformation<?>> transformations,
            List<String> sinkIdentifierNames,
            List<JobStatusHook> jobStatusHookList) {
        final String defaultJobName = "insert-into_" + String.join(",", sinkIdentifierNames);

        resourceManager.addJarConfiguration(tableConfig);

        // We pass only the configuration to avoid reconfiguration with the rootConfiguration
        Pipeline pipeline =
                execEnv.createPipeline(
                        transformations,
                        tableConfig.getConfiguration(),
                        defaultJobName,
                        jobStatusHookList);
        try {
            JobClient jobClient = execEnv.executeAsync(pipeline);
            final List<Column> columns = new ArrayList<>();
            Long[] affectedRowCounts = new Long[transformations.size()];
            for (int i = 0; i < transformations.size(); ++i) {
                // use sink identifier name as field name
                columns.add(Column.physical(sinkIdentifierNames.get(i), DataTypes.BIGINT()));
                affectedRowCounts[i] = -1L;
            }

            TableResultInternal result =
                    TableResultImpl.builder()
                            .jobClient(jobClient)
                            .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                            .schema(ResolvedSchema.of(columns))
                            .resultProvider(
                                    new InsertResultProvider(affectedRowCounts)
                                            .setJobClient(jobClient))
                            .build();
            if (tableConfig.get(TABLE_DML_SYNC)) {
                try {
                    result.await();
                } catch (InterruptedException | ExecutionException e) {
                    result.getJobClient().ifPresent(JobClient::cancel);
                    throw new TableException("Fail to wait execution finish.", e);
                }
            }
            return result;
        } catch (Exception e) {
            throw new TableException("Failed to execute sql", e);
        }
    }

    private TableResultInternal executeQueryOperation(QueryOperation operation) {
        CollectModifyOperation sinkOperation = new CollectModifyOperation(operation);
        List<Transformation<?>> transformations =
                translate(Collections.singletonList(sinkOperation));
        final String defaultJobName = "collect";

        resourceManager.addJarConfiguration(tableConfig);

        // We pass only the configuration to avoid reconfiguration with the rootConfiguration
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
                                    getConfig().get(TableConfigOptions.DISPLAY_MAX_COLUMN_WIDTH),
                                    false,
                                    isStreamingMode))
                    .build();
        } catch (Exception e) {
            throw new TableException("Failed to execute sql", e);
        }
    }

    @Override
    public TableResultInternal executeInternal(Operation operation) {
        // delegate execution to Operation if it implements ExecutableOperation
        if (operation instanceof ExecutableOperation) {
            return ((ExecutableOperation) operation).execute(operationCtx);
        }

        // otherwise, fall back to internal implementation
        if (operation instanceof ModifyOperation) {
            return executeInternal(Collections.singletonList((ModifyOperation) operation));
        } else if (operation instanceof StatementSetOperation) {
            return executeInternal(((StatementSetOperation) operation).getOperations());
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
        } else if (operation instanceof QueryOperation) {
            return executeQueryOperation((QueryOperation) operation);
        } else if (operation instanceof ExecutePlanOperation) {
            ExecutePlanOperation executePlanOperation = (ExecutePlanOperation) operation;
            try {
                return (TableResultInternal)
                        executePlan(
                                PlanReference.fromFile(
                                        resourceManager.registerFileResource(
                                                new ResourceUri(
                                                        ResourceType.FILE,
                                                        executePlanOperation.getFilePath()))));
            } catch (IOException e) {
                throw new TableException(
                        String.format(
                                "Failed to execute %s statement.", operation.asSummaryString()),
                        e);
            }
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
        } else if (operation instanceof AnalyzeTableOperation) {
            if (isStreamingMode) {
                throw new TableException("ANALYZE TABLE is not supported for streaming mode now");
            }
            try {
                return AnalyzeTableUtil.analyzeTable(this, (AnalyzeTableOperation) operation);
            } catch (Exception e) {
                throw new TableException("Failed to execute ANALYZE TABLE command", e);
            }
        } else if (operation instanceof NopOperation) {
            return TableResultImpl.TABLE_RESULT_OK;
        } else {
            throw new TableException(UNSUPPORTED_QUERY_IN_EXECUTE_SQL_MSG);
        }
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
                .map(ContextResolvedTable::getResolvedTable);
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

    private boolean isRowLevelModification(Operation operation) {
        if (operation instanceof SinkModifyOperation) {
            SinkModifyOperation sinkModifyOperation = (SinkModifyOperation) operation;
            return sinkModifyOperation.isDelete() || sinkModifyOperation.isUpdate();
        }
        return false;
    }
}
