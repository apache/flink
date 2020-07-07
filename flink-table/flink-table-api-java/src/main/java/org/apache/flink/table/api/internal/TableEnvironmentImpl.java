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
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
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
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.DescribeTableOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SelectSinkOperation;
import org.apache.flink.table.operations.ShowCatalogsOperation;
import org.apache.flink.table.operations.ShowDatabasesOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.ShowViewsOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.ddl.AddPartitionsOperation;
import org.apache.flink.table.operations.ddl.AlterCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterPartitionPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableAddConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTableDropConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTableOperation;
import org.apache.flink.table.operations.ddl.AlterTablePropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.AlterTableSchemaOperation;
import org.apache.flink.table.operations.ddl.AlterViewAsOperation;
import org.apache.flink.table.operations.ddl.AlterViewOperation;
import org.apache.flink.table.operations.ddl.AlterViewPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterViewRenameOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
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
import org.apache.flink.table.utils.PrintUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Implementation of {@link TableEnvironment} that works exclusively with Table API interfaces.
 * Only {@link TableSource} is supported as an input and {@link TableSink} as an output. It also does
 * not bind to any particular {@code StreamExecutionEnvironment}.
 */
@Internal
public class TableEnvironmentImpl implements TableEnvironmentInternal {
	// Flag that tells if the TableSource/TableSink used in this environment is stream table source/sink,
	// and this should always be true. This avoids too many hard code.
	private static final boolean IS_STREAM_TABLE = true;
	private final CatalogManager catalogManager;
	private final ModuleManager moduleManager;
	private final OperationTreeBuilder operationTreeBuilder;
	private final List<ModifyOperation> bufferedModifyOperations = new ArrayList<>();

	protected final TableConfig tableConfig;
	protected final Executor execEnv;
	protected final FunctionCatalog functionCatalog;
	protected final Planner planner;
	protected final Parser parser;
	private final boolean isStreamingMode;
	private static final String UNSUPPORTED_QUERY_IN_SQL_UPDATE_MSG =
			"Unsupported SQL query! sqlUpdate() only accepts a single SQL statement of type " +
			"INSERT, CREATE TABLE, DROP TABLE, ALTER TABLE, USE CATALOG, USE [CATALOG.]DATABASE, " +
			"CREATE DATABASE, DROP DATABASE, ALTER DATABASE, CREATE FUNCTION, DROP FUNCTION, ALTER FUNCTION, " +
			"CREATE CATALOG, DROP CATALOG, CREATE VIEW, DROP VIEW.";
	private static final String UNSUPPORTED_QUERY_IN_EXECUTE_SQL_MSG =
			"Unsupported SQL query! executeSql() only accepts a single SQL statement of type " +
			"CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE DATABASE, DROP DATABASE, ALTER DATABASE, " +
			"CREATE FUNCTION, DROP FUNCTION, ALTER FUNCTION, CREATE CATALOG, DROP CATALOG, " +
			"USE CATALOG, USE [CATALOG.]DATABASE, SHOW CATALOGS, SHOW DATABASES, SHOW TABLES, SHOW FUNCTIONS, " +
			"CREATE VIEW, DROP VIEW, SHOW VIEWS, INSERT, DESCRIBE.";

	/**
	 * Provides necessary methods for {@link ConnectTableDescriptor}.
	 */
	private final Registration registration = new Registration() {

		@Override
		public void createTemporaryTable(String path, CatalogBaseTable table) {
			UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(path);
			ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(
				unresolvedIdentifier);
			catalogManager.createTemporaryTable(table, objectIdentifier, false);
		}
	};

	protected TableEnvironmentImpl(
			CatalogManager catalogManager,
			ModuleManager moduleManager,
			TableConfig tableConfig,
			Executor executor,
			FunctionCatalog functionCatalog,
			Planner planner,
			boolean isStreamingMode) {
		this.catalogManager = catalogManager;
		this.catalogManager.setCatalogTableSchemaResolver(
				new CatalogTableSchemaResolver(planner.getParser(), isStreamingMode));
		this.moduleManager = moduleManager;
		this.execEnv = executor;

		this.tableConfig = tableConfig;

		this.functionCatalog = functionCatalog;
		this.planner = planner;
		this.parser = planner.getParser();
		this.isStreamingMode = isStreamingMode;
		this.operationTreeBuilder = OperationTreeBuilder.create(
			tableConfig,
			functionCatalog.asLookup(parser::parseIdentifier),
			catalogManager.getDataTypeFactory(),
			path -> {
				try {
					UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(path);
					Optional<CatalogQueryOperation> catalogQueryOperation = scanInternal(unresolvedIdentifier);
					return catalogQueryOperation.map(t -> ApiExpressionUtils.tableRef(path, t));
				} catch (SqlParserException ex) {
					// The TableLookup is used during resolution of expressions and it actually might not be an
					// identifier of a table. It might be a reference to some other object such as column, local
					// reference etc. This method should return empty optional in such cases to fallback for other
					// identifiers resolution.
					return Optional.empty();
				}
			},
			isStreamingMode
		);
	}

	public static TableEnvironmentImpl create(EnvironmentSettings settings) {

		// temporary solution until FLINK-15635 is fixed
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		TableConfig tableConfig = new TableConfig();

		ModuleManager moduleManager = new ModuleManager();

		CatalogManager catalogManager = CatalogManager.newBuilder()
			.classLoader(classLoader)
			.config(tableConfig.getConfiguration())
			.defaultCatalog(
				settings.getBuiltInCatalogName(),
				new GenericInMemoryCatalog(
					settings.getBuiltInCatalogName(),
					settings.getBuiltInDatabaseName()))
			.build();

		FunctionCatalog functionCatalog = new FunctionCatalog(tableConfig, catalogManager, moduleManager);

		Map<String, String> executorProperties = settings.toExecutorProperties();
		Executor executor = ComponentFactoryService.find(ExecutorFactory.class, executorProperties)
			.create(executorProperties);

		Map<String, String> plannerProperties = settings.toPlannerProperties();
		Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
			.create(
				plannerProperties,
				executor,
				tableConfig,
				functionCatalog,
				catalogManager);

		return new TableEnvironmentImpl(
			catalogManager,
			moduleManager,
			tableConfig,
			executor,
			functionCatalog,
			planner,
			settings.isStreamingMode()
		);
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
		final DataType resolvedDataType = catalogManager.getDataTypeFactory().createDataType(rowType);
		return createTable(operationTreeBuilder.values(resolvedDataType, values));
	}

	@Override
	public Table fromValues(Iterable<?> values) {
		Expression[] exprs = StreamSupport.stream(values.spliterator(), false)
			.map(ApiExpressionUtils::objectToExpression)
			.toArray(Expression[]::new);
		return fromValues(exprs);
	}

	@Override
	public Table fromValues(AbstractDataType<?> rowType, Iterable<?> values) {
		Expression[] exprs = StreamSupport.stream(values.spliterator(), false)
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
		// TODO should add a validation, while StreamTableSource is in flink-table-api-java-bridge module now
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
	public void unloadModule(String moduleName) {
		moduleManager.unloadModule(moduleName);
	}

	@Override
	public void registerFunction(String name, ScalarFunction function) {
		functionCatalog.registerTempSystemScalarFunction(
			name,
			function);
	}

	@Override
	public void createTemporarySystemFunction(String name, Class<? extends UserDefinedFunction> functionClass) {
		final UserDefinedFunction functionInstance = UserDefinedFunctionHelper.instantiateFunction(functionClass);
		createTemporarySystemFunction(name, functionInstance);
	}

	@Override
	public void createTemporarySystemFunction(String name, UserDefinedFunction functionInstance) {
		functionCatalog.registerTemporarySystemFunction(
			name,
			functionInstance,
			false);
	}

	@Override
	public boolean dropTemporarySystemFunction(String name) {
		return functionCatalog.dropTemporarySystemFunction(
			name,
			true);
	}

	@Override
	public void createFunction(String path, Class<? extends UserDefinedFunction> functionClass) {
		createFunction(path, functionClass, false);
	}

	@Override
	public void createFunction(String path, Class<? extends UserDefinedFunction> functionClass, boolean ignoreIfExists) {
		final UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(path);
		functionCatalog.registerCatalogFunction(
			unresolvedIdentifier,
			functionClass,
			ignoreIfExists);
	}

	@Override
	public boolean dropFunction(String path) {
		final UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(path);
		return functionCatalog.dropCatalogFunction(
			unresolvedIdentifier,
			true);
	}

	@Override
	public void createTemporaryFunction(String path, Class<? extends UserDefinedFunction> functionClass) {
		final UserDefinedFunction functionInstance = UserDefinedFunctionHelper.instantiateFunction(functionClass);
		createTemporaryFunction(path, functionInstance);
	}

	@Override
	public void createTemporaryFunction(String path, UserDefinedFunction functionInstance) {
		final UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(path);
		functionCatalog.registerTemporaryCatalogFunction(
			unresolvedIdentifier,
			functionInstance,
			false);
	}

	@Override
	public boolean dropTemporaryFunction(String path) {
		final UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(path);
		return functionCatalog.dropTemporaryCatalogFunction(
			unresolvedIdentifier,
			true);
	}

	@Override
	public void registerTable(String name, Table table) {
		UnresolvedIdentifier identifier = UnresolvedIdentifier.of(name);
		createTemporaryView(identifier, table);
	}

	@Override
	public void createTemporaryView(String path, Table view) {
		UnresolvedIdentifier identifier = parser.parseIdentifier(path);
		createTemporaryView(identifier, view);
	}

	private void createTemporaryView(UnresolvedIdentifier identifier, Table view) {
		if (((TableImpl) view).getTableEnvironment() != this) {
			throw new TableException(
					"Only table API objects that belong to this TableEnvironment can be registered.");
		}

		ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(identifier);
		QueryOperation queryOperation = qualifyQueryOperation(tableIdentifier, view.getQueryOperation());
		CatalogBaseTable tableTable = new QueryOperationCatalogView(queryOperation);

		catalogManager.createTemporaryTable(tableTable, tableIdentifier, false);
	}

	@Override
	public Table scan(String... tablePath) {
		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(tablePath);
		return scanInternal(unresolvedIdentifier)
			.map(this::createTable)
			.orElseThrow(() -> new ValidationException(String.format(
				"Table %s was not found.",
				unresolvedIdentifier)));
	}

	@Override
	public Table from(String path) {
		UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(path);
		return scanInternal(unresolvedIdentifier)
			.map(this::createTable)
			.orElseThrow(() -> new ValidationException(String.format(
				"Table %s was not found.",
				unresolvedIdentifier)));
	}

	@Override
	public void insertInto(String targetPath, Table table) {
		UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(targetPath);
		insertIntoInternal(unresolvedIdentifier, table);
	}

	@Override
	public void insertInto(Table table, String sinkPath, String... sinkPathContinued) {
		List<String> fullPath = new ArrayList<>(Arrays.asList(sinkPathContinued));
		fullPath.add(0, sinkPath);
		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(fullPath);

		insertIntoInternal(unresolvedIdentifier, table);
	}

	private void insertIntoInternal(UnresolvedIdentifier unresolvedIdentifier, Table table) {
		ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

		List<ModifyOperation> modifyOperations = Collections.singletonList(
			new CatalogSinkModifyOperation(
				objectIdentifier,
				table.getQueryOperation()));

		buffer(modifyOperations);
	}

	private Optional<CatalogQueryOperation> scanInternal(UnresolvedIdentifier identifier) {
		ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(identifier);

		return catalogManager.getTable(tableIdentifier)
			.map(t -> new CatalogQueryOperation(tableIdentifier, t.getResolvedSchema()));
	}

	@Override
	public ConnectTableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
		return new StreamTableDescriptor(registration, connectorDescriptor);
	}

	@Override
	public String[] listCatalogs() {
		return catalogManager.listCatalogs()
			.stream()
			.sorted()
			.toArray(String[]::new);
	}

	@Override
	public String[] listModules() {
		return moduleManager.listModules().toArray(new String[0]);
	}

	@Override
	public String[] listDatabases() {
		return catalogManager.getCatalog(catalogManager.getCurrentCatalog())
			.get()
			.listDatabases()
			.toArray(new String[0]);
	}

	@Override
	public String[] listTables() {
		return catalogManager.listTables()
			.stream()
			.sorted()
			.toArray(String[]::new);
	}

	@Override
	public String[] listViews() {
		return catalogManager.listViews()
			.stream()
			.sorted()
			.toArray(String[]::new);
	}

	@Override
	public String[] listTemporaryTables() {
		return catalogManager.listTemporaryTables()
			.stream()
			.sorted()
			.toArray(String[]::new);
	}

	@Override
	public String[] listTemporaryViews() {
		return catalogManager.listTemporaryViews()
			.stream()
			.sorted()
			.toArray(String[]::new);
	}

	@Override
	public boolean dropTemporaryTable(String path) {
		UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(path);
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
		UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(path);
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
		return functionCatalog.getUserDefinedFunctions();
	}

	@Override
	public String[] listFunctions() {
		return functionCatalog.getFunctions();
	}

	@Override
	public String explain(Table table) {
		return explain(table, false);
	}

	@Override
	public String explain(Table table, boolean extended) {
		return planner.explain(Collections.singletonList(table.getQueryOperation()), getExplainDetails(extended));
	}

	@Override
	public String explain(boolean extended) {
		List<Operation> operations = bufferedModifyOperations.stream()
				.map(o -> (Operation) o).collect(Collectors.toList());
		return planner.explain(operations, getExplainDetails(extended));
	}

	@Override
	public String explainSql(String statement, ExplainDetail... extraDetails) {
		List<Operation> operations = parser.parse(statement);

		if (operations.size() != 1) {
			throw new TableException("Unsupported SQL query! explainSql() only accepts a single SQL query.");
		}

		return planner.explain(operations, extraDetails);
	}

	@Override
	public String explainInternal(List<Operation> operations, ExplainDetail... extraDetails) {
		return planner.explain(operations, extraDetails);
	}

	@Override
	public String[] getCompletionHints(String statement, int position) {
		return planner.getCompletionHints(statement, position);
	}

	@Override
	public Table sqlQuery(String query) {
		List<Operation> operations = parser.parse(query);

		if (operations.size() != 1) {
			throw new ValidationException(
				"Unsupported SQL query! sqlQuery() only accepts a single SQL query.");
		}

		Operation operation = operations.get(0);

		if (operation instanceof QueryOperation && !(operation instanceof ModifyOperation)) {
			return createTable((QueryOperation) operation);
		} else {
			throw new ValidationException(
				"Unsupported SQL query! sqlQuery() only accepts a single SQL query of type " +
					"SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.");
		}
	}

	@Override
	public TableResult executeSql(String statement) {
		List<Operation> operations = parser.parse(statement);

		if (operations.size() != 1) {
			throw new TableException(UNSUPPORTED_QUERY_IN_EXECUTE_SQL_MSG);
		}

		return executeOperation(operations.get(0));
	}

	@Override
	public StatementSet createStatementSet() {
		return new StatementSetImpl(this);
	}

	@Override
	public TableResult executeInternal(List<ModifyOperation> operations) {
		List<Transformation<?>> transformations = translate(operations);
		List<String> sinkIdentifierNames = extractSinkIdentifierNames(operations);
		String jobName = "insert-into_" + String.join(",", sinkIdentifierNames);
		Pipeline pipeline = execEnv.createPipeline(transformations, tableConfig, jobName);
		try {
			JobClient jobClient = execEnv.executeAsync(pipeline);
			TableSchema.Builder builder = TableSchema.builder();
			Object[] affectedRowCounts = new Long[operations.size()];
			for (int i = 0; i < operations.size(); ++i) {
				// use sink identifier name as field name
				builder.field(sinkIdentifierNames.get(i), DataTypes.BIGINT());
				affectedRowCounts[i] = -1L;
			}

			return TableResultImpl.builder()
					.jobClient(jobClient)
					.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
					.tableSchema(builder.build())
					.data(Collections.singletonList(Row.of(affectedRowCounts)))
					.build();
		} catch (Exception e) {
			throw new TableException("Failed to execute sql", e);
		}
	}

	@Override
	public TableResult executeInternal(QueryOperation operation) {
		SelectSinkOperation sinkOperation = new SelectSinkOperation(operation);
		List<Transformation<?>> transformations = translate(Collections.singletonList(sinkOperation));
		Pipeline pipeline = execEnv.createPipeline(transformations, tableConfig, "collect");
		try {
			JobClient jobClient = execEnv.executeAsync(pipeline);
			SelectResultProvider resultProvider = sinkOperation.getSelectResultProvider();
			resultProvider.setJobClient(jobClient);
			return TableResultImpl.builder()
					.jobClient(jobClient)
					.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
					.tableSchema(operation.getTableSchema())
					.data(resultProvider.getResultIterator())
					.setPrintStyle(TableResultImpl.PrintStyle.tableau(
							PrintUtils.MAX_COLUMN_WIDTH, PrintUtils.NULL_COLUMN, true, isStreamingMode))
					.build();
		} catch (Exception e) {
			throw new TableException("Failed to execute sql", e);
		}
	}

	@Override
	public void sqlUpdate(String stmt) {
		List<Operation> operations = parser.parse(stmt);

		if (operations.size() != 1) {
			throw new TableException(UNSUPPORTED_QUERY_IN_SQL_UPDATE_MSG);
		}

		Operation operation = operations.get(0);
		if (operation instanceof ModifyOperation) {
			buffer(Collections.singletonList((ModifyOperation) operation));
		} else if (operation instanceof CreateTableOperation ||
				operation instanceof DropTableOperation ||
				operation instanceof AlterTableOperation ||
				operation instanceof CreateViewOperation ||
				operation instanceof DropViewOperation ||
				operation instanceof CreateDatabaseOperation ||
				operation instanceof DropDatabaseOperation ||
				operation instanceof AlterDatabaseOperation ||
				operation instanceof CreateCatalogFunctionOperation ||
				operation instanceof CreateTempSystemFunctionOperation ||
				operation instanceof DropCatalogFunctionOperation ||
				operation instanceof DropTempSystemFunctionOperation ||
				operation instanceof AlterCatalogFunctionOperation ||
				operation instanceof CreateCatalogOperation ||
				operation instanceof DropCatalogOperation ||
				operation instanceof UseCatalogOperation ||
				operation instanceof UseDatabaseOperation) {
			executeOperation(operation);
		} else {
			throw new TableException(UNSUPPORTED_QUERY_IN_SQL_UPDATE_MSG);
		}
	}

	private TableResult executeOperation(Operation operation) {
		if (operation instanceof ModifyOperation) {
			return executeInternal(Collections.singletonList((ModifyOperation) operation));
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
						dropTableOperation.getTableIdentifier(),
						dropTableOperation.isIfExists());
			} else {
				catalogManager.dropTable(
						dropTableOperation.getTableIdentifier(),
						dropTableOperation.isIfExists());
			}
			return TableResultImpl.TABLE_RESULT_OK;
		} else if (operation instanceof AlterTableOperation) {
			AlterTableOperation alterTableOperation = (AlterTableOperation) operation;
			Catalog catalog = getCatalogOrThrowException(alterTableOperation.getTableIdentifier().getCatalogName());
			String exMsg = getDDLOpExecuteErrorMsg(alterTableOperation.asSummaryString());
			try {
				if (alterTableOperation instanceof AlterTableRenameOperation) {
					AlterTableRenameOperation alterTableRenameOp = (AlterTableRenameOperation) operation;
					catalog.renameTable(
							alterTableRenameOp.getTableIdentifier().toObjectPath(),
							alterTableRenameOp.getNewTableIdentifier().getObjectName(),
							false);
				} else if (alterTableOperation instanceof AlterTablePropertiesOperation) {
					AlterTablePropertiesOperation alterTablePropertiesOp = (AlterTablePropertiesOperation) operation;
					catalog.alterTable(
							alterTablePropertiesOp.getTableIdentifier().toObjectPath(),
							alterTablePropertiesOp.getCatalogTable(),
							false);
				} else if (alterTableOperation instanceof AlterTableAddConstraintOperation){
					AlterTableAddConstraintOperation addConstraintOP =
							(AlterTableAddConstraintOperation) operation;
					CatalogTable oriTable = (CatalogTable) catalogManager
							.getTable(addConstraintOP.getTableIdentifier())
							.get()
							.getTable();
					TableSchema.Builder builder = TableSchemaUtils
							.builderWithGivenSchema(oriTable.getSchema());
					if (addConstraintOP.getConstraintName().isPresent()) {
						builder.primaryKey(
								addConstraintOP.getConstraintName().get(),
								addConstraintOP.getColumnNames());
					} else {
						builder.primaryKey(addConstraintOP.getColumnNames());
					}
					CatalogTable newTable = new CatalogTableImpl(
							builder.build(),
							oriTable.getPartitionKeys(),
							oriTable.getOptions(),
							oriTable.getComment());
					catalog.alterTable(
							addConstraintOP.getTableIdentifier().toObjectPath(),
							newTable,
							false);
				} else if (alterTableOperation instanceof AlterTableDropConstraintOperation){
					AlterTableDropConstraintOperation dropConstraintOperation =
							(AlterTableDropConstraintOperation) operation;
					CatalogTable oriTable = (CatalogTable) catalogManager
							.getTable(dropConstraintOperation.getTableIdentifier())
							.get()
							.getTable();
					CatalogTable newTable = new CatalogTableImpl(
							TableSchemaUtils.dropConstraint(
									oriTable.getSchema(),
									dropConstraintOperation.getConstraintName()),
							oriTable.getPartitionKeys(),
							oriTable.getOptions(),
							oriTable.getComment());
					catalog.alterTable(
							dropConstraintOperation.getTableIdentifier().toObjectPath(),
							newTable,
							false);
				} else if (alterTableOperation instanceof AlterPartitionPropertiesOperation) {
					AlterPartitionPropertiesOperation alterPartPropsOp = (AlterPartitionPropertiesOperation) operation;
					catalog.alterPartition(alterPartPropsOp.getTableIdentifier().toObjectPath(),
							alterPartPropsOp.getPartitionSpec(),
							alterPartPropsOp.getCatalogPartition(),
							false);
				} else if (alterTableOperation instanceof AlterTableSchemaOperation) {
					AlterTableSchemaOperation alterTableSchemaOperation = (AlterTableSchemaOperation) alterTableOperation;
					catalog.alterTable(alterTableSchemaOperation.getTableIdentifier().toObjectPath(),
							alterTableSchemaOperation.getCatalogTable(),
							false);
				} else if (alterTableOperation instanceof AddPartitionsOperation) {
					AddPartitionsOperation addPartitionsOperation = (AddPartitionsOperation) alterTableOperation;
					List<CatalogPartitionSpec> specs = addPartitionsOperation.getPartitionSpecs();
					List<CatalogPartition> partitions = addPartitionsOperation.getCatalogPartitions();
					boolean ifNotExists = addPartitionsOperation.ifNotExists();
					ObjectPath tablePath = addPartitionsOperation.getTableIdentifier().toObjectPath();
					for (int i = 0; i < specs.size(); i++) {
						catalog.createPartition(tablePath, specs.get(i), partitions.get(i), ifNotExists);
					}
				} else if (alterTableOperation instanceof DropPartitionsOperation) {
					DropPartitionsOperation dropPartitionsOperation = (DropPartitionsOperation) alterTableOperation;
					ObjectPath tablePath = dropPartitionsOperation.getTableIdentifier().toObjectPath();
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
						dropViewOperation.getViewIdentifier(),
						dropViewOperation.isIfExists());
			} else {
				catalogManager.dropView(
						dropViewOperation.getViewIdentifier(),
						dropViewOperation.isIfExists());
			}
			return TableResultImpl.TABLE_RESULT_OK;
		} else if (operation instanceof AlterViewOperation) {
			AlterViewOperation alterViewOperation = (AlterViewOperation) operation;
			Catalog catalog = getCatalogOrThrowException(alterViewOperation.getViewIdentifier().getCatalogName());
			String exMsg = getDDLOpExecuteErrorMsg(alterViewOperation.asSummaryString());
			try {
				if (alterViewOperation instanceof AlterViewRenameOperation) {
					AlterViewRenameOperation alterTableRenameOp = (AlterViewRenameOperation) operation;
					catalog.renameTable(
							alterTableRenameOp.getViewIdentifier().toObjectPath(),
							alterTableRenameOp.getNewViewIdentifier().getObjectName(),
							false);
				} else if (alterViewOperation instanceof AlterViewPropertiesOperation) {
					AlterViewPropertiesOperation alterTablePropertiesOp = (AlterViewPropertiesOperation) operation;
					catalog.alterTable(
							alterTablePropertiesOp.getViewIdentifier().toObjectPath(),
							alterTablePropertiesOp.getCatalogView(),
							false);
				} else if (alterViewOperation instanceof AlterViewAsOperation) {
					AlterViewAsOperation alterViewAsOperation = (AlterViewAsOperation) alterViewOperation;
					catalog.alterTable(alterViewAsOperation.getViewIdentifier().toObjectPath(),
							alterViewAsOperation.getNewView(),
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
			CreateCatalogOperation createCatalogOperation = (CreateCatalogOperation) operation;
			String exMsg = getDDLOpExecuteErrorMsg(createCatalogOperation.asSummaryString());
			try {
				catalogManager.registerCatalog(
						createCatalogOperation.getCatalogName(), createCatalogOperation.getCatalog());
				return TableResultImpl.TABLE_RESULT_OK;
			} catch (CatalogException e) {
				throw new ValidationException(exMsg, e);
			}
		} else if (operation instanceof DropCatalogOperation) {
			DropCatalogOperation dropCatalogOperation = (DropCatalogOperation) operation;
			String exMsg = getDDLOpExecuteErrorMsg(dropCatalogOperation.asSummaryString());
			try {
				catalogManager.unregisterCatalog(dropCatalogOperation.getCatalogName(),
					dropCatalogOperation.isIfExists());
				return TableResultImpl.TABLE_RESULT_OK;
			} catch (CatalogException e) {
				throw new ValidationException(exMsg, e);
			}
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
		} else if (operation instanceof ShowDatabasesOperation) {
			return buildShowResult("database name", listDatabases());
		} else if (operation instanceof ShowTablesOperation) {
			return buildShowResult("table name", listTables());
		} else if (operation instanceof ShowFunctionsOperation) {
			return buildShowResult("function name", listFunctions());
		} else if (operation instanceof ShowViewsOperation) {
			return buildShowResult("view name", listViews());
		} else if (operation instanceof ExplainOperation) {
			String explanation = planner.explain(Collections.singletonList(((ExplainOperation) operation).getChild()));
			return TableResultImpl.builder()
					.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
					.tableSchema(TableSchema.builder().field("result", DataTypes.STRING()).build())
					.data(Collections.singletonList(Row.of(explanation)))
					.setPrintStyle(TableResultImpl.PrintStyle.rawContent())
					.build();
		} else if (operation instanceof DescribeTableOperation) {
			DescribeTableOperation describeTableOperation = (DescribeTableOperation) operation;
			Optional<CatalogManager.TableLookupResult> result =
					catalogManager.getTable(describeTableOperation.getSqlIdentifier());
			if (result.isPresent()) {
				return buildDescribeResult(result.get().getResolvedSchema());
			} else {
				throw new ValidationException(String.format(
						"Tables or views with the identifier '%s' doesn't exist",
						describeTableOperation.getSqlIdentifier().asSummaryString()));
			}
		} else if (operation instanceof QueryOperation) {
			return executeInternal((QueryOperation) operation);
		} else {
			throw new TableException(UNSUPPORTED_QUERY_IN_EXECUTE_SQL_MSG);
		}
	}

	private TableResult buildShowResult(String columnName, String[] objects) {
		return buildResult(
			new String[]{columnName},
			new DataType[]{DataTypes.STRING()},
			Arrays.stream(objects).map((c) -> new String[]{c}).toArray(String[][]::new));
	}

	private TableResult buildDescribeResult(TableSchema schema) {
		Map<String, String> fieldToWatermark =
				schema.getWatermarkSpecs()
						.stream()
						.collect(Collectors.toMap(WatermarkSpec::getRowtimeAttribute, WatermarkSpec::getWatermarkExpr));

		Map<String, String> fieldToPrimaryKey = new HashMap<>();
		schema.getPrimaryKey().ifPresent((p) -> {
			List<String> columns = p.getColumns();
			columns.forEach((c) -> fieldToPrimaryKey.put(c, String.format("PRI(%s)", String.join(", ", columns))));
		});

		Object[][] rows =
			schema.getTableColumns()
				.stream()
				.map((c) -> {
					LogicalType logicalType = c.getType().getLogicalType();
					return new Object[]{
						c.getName(),
						StringUtils.removeEnd(logicalType.toString(), " NOT NULL"),
						logicalType.isNullable(),
						fieldToPrimaryKey.getOrDefault(c.getName(), null),
						c.getExpr().orElse(null),
						fieldToWatermark.getOrDefault(c.getName(), null)};
				}).toArray(Object[][]::new);

		return buildResult(
			new String[]{"name", "type", "null", "key", "computed column", "watermark"},
			new DataType[]{DataTypes.STRING(), DataTypes.STRING(), DataTypes.BOOLEAN(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()},
			rows);
	}

	private TableResult buildResult(String[] headers, DataType[] types, Object[][] rows) {
		return TableResultImpl.builder()
				.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
				.tableSchema(
					TableSchema.builder().fields(
						headers,
						types).build())
				.data(Arrays.stream(rows).map(Row::of).collect(Collectors.toList()))
				.setPrintStyle(TableResultImpl.PrintStyle.tableau(Integer.MAX_VALUE, "", false, false))
				.build();
	}

	/**
	 * extract sink identifier names from {@link ModifyOperation}s.
	 *
	 * <p>If there are multiple ModifyOperations have same name,
	 * an index suffix will be added at the end of the name to ensure each name is unique.
	 */
	private List<String> extractSinkIdentifierNames(List<ModifyOperation> operations) {
		List<String> tableNames = new ArrayList<>(operations.size());
		Map<String, Integer> tableNameToCount = new HashMap<>();
		for (ModifyOperation operation : operations) {
			if (operation instanceof CatalogSinkModifyOperation) {
				ObjectIdentifier identifier = ((CatalogSinkModifyOperation) operation).getTableIdentifier();
				String fullName = identifier.asSummaryString();
				tableNames.add(fullName);
				tableNameToCount.put(fullName, tableNameToCount.getOrDefault(fullName, 0) + 1);
			} else {
				throw new UnsupportedOperationException("Unsupported operation: " + operation);
			}
		}
		Map<String, Integer> tableNameToIndex = new HashMap<>();
		return tableNames.stream().map(tableName -> {
					if (tableNameToCount.get(tableName) == 1) {
						return tableName;
					} else {
						Integer index = tableNameToIndex.getOrDefault(tableName, 0) + 1;
						tableNameToIndex.put(tableName, index);
						return tableName + "_" + index;
					}
				}
		).collect(Collectors.toList());
	}

	/** Get catalog from catalogName or throw a ValidationException if the catalog not exists. */
	private Catalog getCatalogOrThrowException(String catalogName) {
		return getCatalog(catalogName)
				.orElseThrow(() -> new ValidationException(String.format("Catalog %s does not exist", catalogName)));
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
	public JobExecutionResult execute(String jobName) throws Exception {
		Pipeline pipeline = execEnv.createPipeline(translateAndClearBuffer(), tableConfig, jobName);
		return execEnv.execute(pipeline);
	}

	@Override
	public Parser getParser() {
		return parser;
	}

	@Override
	public CatalogManager getCatalogManager() {
		return catalogManager;
	}

	/**
	 * Subclasses can override this method to transform the given QueryOperation to a new one with
	 * the qualified object identifier. This is needed for some QueryOperations, e.g. JavaDataStreamQueryOperation,
	 * which doesn't know the registered identifier when created ({@code fromDataStream(DataStream)}.
	 * But the identifier is required when converting this QueryOperation to RelNode.
	 */
	protected QueryOperation qualifyQueryOperation(ObjectIdentifier identifier, QueryOperation queryOperation) {
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

	/**
	 * Translate the buffered operations to Transformations, and clear the buffer.
	 *
	 * <p>The buffer will be clear even if the `translate` fails. In most cases,
	 * the failure is not retryable (e.g. type mismatch, can't generate physical plan).
	 * If the buffer is not clear after failure, the following `translate` will also fail.
	 */
	protected List<Transformation<?>> translateAndClearBuffer() {
		List<Transformation<?>> transformations;
		try {
			transformations = translate(bufferedModifyOperations);
		} finally {
			bufferedModifyOperations.clear();
		}
		return transformations;
	}

	private List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
		return planner.translate(modifyOperations);
	}

	private void buffer(List<ModifyOperation> modifyOperations) {
		bufferedModifyOperations.addAll(modifyOperations);
	}

	@VisibleForTesting
	protected ExplainDetail[] getExplainDetails(boolean extended) {
		if (extended) {
			if (isStreamingMode) {
				return new ExplainDetail[] { ExplainDetail.ESTIMATED_COST, ExplainDetail.CHANGELOG_MODE };
			} else {
				return new ExplainDetail[] { ExplainDetail.ESTIMATED_COST };
			}
		} else {
			return new ExplainDetail[0];
		}
	}

	@Override
	public void registerTableSourceInternal(String name, TableSource<?> tableSource) {
		validateTableSource(tableSource);
		ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(name));
		Optional<CatalogBaseTable> table = getTemporaryTable(objectIdentifier);

		if (table.isPresent()) {
			if (table.get() instanceof ConnectorCatalogTable<?, ?>) {
				ConnectorCatalogTable<?, ?> sourceSinkTable = (ConnectorCatalogTable<?, ?>) table.get();
				if (sourceSinkTable.getTableSource().isPresent()) {
					throw new ValidationException(String.format(
						"Table '%s' already exists. Please choose a different name.", name));
				} else {
					// wrapper contains only sink (not source)
					ConnectorCatalogTable sourceAndSink = ConnectorCatalogTable.sourceAndSink(
						tableSource,
						sourceSinkTable.getTableSink().get(),
						!IS_STREAM_TABLE);
					catalogManager.dropTemporaryTable(objectIdentifier, false);
					catalogManager.createTemporaryTable(sourceAndSink, objectIdentifier, false);
				}
			} else {
				throw new ValidationException(String.format(
					"Table '%s' already exists. Please choose a different name.", name));
			}
		} else {
			ConnectorCatalogTable source = ConnectorCatalogTable.source(tableSource, !IS_STREAM_TABLE);
			catalogManager.createTemporaryTable(source, objectIdentifier, false);
		}
	}

	@Override
	public void registerTableSinkInternal(String name, TableSink<?> tableSink) {
		ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(name));
		Optional<CatalogBaseTable> table = getTemporaryTable(objectIdentifier);

		if (table.isPresent()) {
			if (table.get() instanceof ConnectorCatalogTable<?, ?>) {
				ConnectorCatalogTable<?, ?> sourceSinkTable = (ConnectorCatalogTable<?, ?>) table.get();
				if (sourceSinkTable.getTableSink().isPresent()) {
					throw new ValidationException(String.format(
						"Table '%s' already exists. Please choose a different name.", name));
				} else {
					// wrapper contains only sink (not source)
					ConnectorCatalogTable sourceAndSink = ConnectorCatalogTable
						.sourceAndSink(sourceSinkTable.getTableSource().get(), tableSink, !IS_STREAM_TABLE);
					catalogManager.dropTemporaryTable(objectIdentifier, false);
					catalogManager.createTemporaryTable(sourceAndSink, objectIdentifier, false);
				}
			} else {
				throw new ValidationException(String.format(
					"Table '%s' already exists. Please choose a different name.", name));
			}
		} else {
			ConnectorCatalogTable sink = ConnectorCatalogTable.sink(tableSink, !IS_STREAM_TABLE);
			catalogManager.createTemporaryTable(sink, objectIdentifier, false);
		}
	}

	private Optional<CatalogBaseTable> getTemporaryTable(ObjectIdentifier identifier) {
		return catalogManager.getTable(identifier)
			.filter(CatalogManager.TableLookupResult::isTemporary)
			.map(CatalogManager.TableLookupResult::getTable);
	}

	private TableResult createCatalogFunction(
			CreateCatalogFunctionOperation createCatalogFunctionOperation) {
		String exMsg = getDDLOpExecuteErrorMsg(createCatalogFunctionOperation.asSummaryString());
		try {
			if (createCatalogFunctionOperation.isTemporary()) {
				functionCatalog.registerTemporaryCatalogFunction(
						UnresolvedIdentifier.of(createCatalogFunctionOperation.getFunctionIdentifier().toList()),
						createCatalogFunctionOperation.getCatalogFunction(),
						createCatalogFunctionOperation.isIgnoreIfExists());
			} else {
				Catalog catalog = getCatalogOrThrowException(
					createCatalogFunctionOperation.getFunctionIdentifier().getCatalogName());
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

	private TableResult alterCatalogFunction(AlterCatalogFunctionOperation alterCatalogFunctionOperation) {
		String exMsg = getDDLOpExecuteErrorMsg(alterCatalogFunctionOperation.asSummaryString());
		try {
			CatalogFunction function = alterCatalogFunctionOperation.getCatalogFunction();
			if (alterCatalogFunctionOperation.isTemporary()) {
				throw new ValidationException(
					"Alter temporary catalog function is not supported");
			} else {
				Catalog catalog = getCatalogOrThrowException(
					alterCatalogFunctionOperation.getFunctionIdentifier().getCatalogName());
				catalog.alterFunction(
					alterCatalogFunctionOperation.getFunctionIdentifier().toObjectPath(),
					function,
					alterCatalogFunctionOperation.isIfExists());
			}
			return TableResultImpl.TABLE_RESULT_OK;
		} catch (ValidationException e) {
			throw e;
		}  catch (FunctionNotExistException e) {
			throw new ValidationException(e.getMessage(), e);
		} catch (Exception e) {
			throw new TableException(exMsg, e);
		}
	}

	private TableResult dropCatalogFunction(DropCatalogFunctionOperation dropCatalogFunctionOperation) {
		String exMsg = getDDLOpExecuteErrorMsg(dropCatalogFunctionOperation.asSummaryString());
		try {
			if (dropCatalogFunctionOperation.isTemporary()) {
				functionCatalog.dropTempCatalogFunction(
					dropCatalogFunctionOperation.getFunctionIdentifier(),
					dropCatalogFunctionOperation.isIfExists());
			} else {
				Catalog catalog = getCatalogOrThrowException
					(dropCatalogFunctionOperation.getFunctionIdentifier().getCatalogName());

				catalog.dropFunction(
					dropCatalogFunctionOperation.getFunctionIdentifier().toObjectPath(),
					dropCatalogFunctionOperation.isIfExists());
			}
			return TableResultImpl.TABLE_RESULT_OK;
		} catch (ValidationException e) {
			throw e;
		}  catch (FunctionNotExistException e) {
			throw new ValidationException(e.getMessage(), e);
		} catch (Exception e) {
			throw new TableException(exMsg, e);
		}
	}

	private TableResult createSystemFunction(CreateTempSystemFunctionOperation operation) {
		String exMsg = getDDLOpExecuteErrorMsg(operation.asSummaryString());
		try {
			functionCatalog.registerTemporarySystemFunction(
					operation.getFunctionName(),
					operation.getFunctionClass(),
					operation.getFunctionLanguage(),
					operation.isIgnoreIfExists());
			return TableResultImpl.TABLE_RESULT_OK;
		} catch (ValidationException e) {
			throw e;
		} catch (Exception e) {
			throw new TableException(exMsg, e);
		}
	}

	private TableResult dropSystemFunction(DropTempSystemFunctionOperation operation) {
		try {
			functionCatalog.dropTemporarySystemFunction(
				operation.getFunctionName(),
				operation.isIfExists());
			return TableResultImpl.TABLE_RESULT_OK;
		} catch (ValidationException e) {
			throw e;
		} catch (Exception e) {
			throw new TableException(getDDLOpExecuteErrorMsg(operation.asSummaryString()), e);
		}
	}

	protected TableImpl createTable(QueryOperation tableOperation) {
		return TableImpl.createTable(
			this,
			tableOperation,
			operationTreeBuilder,
			functionCatalog.asLookup(parser::parseIdentifier));
	}
}
