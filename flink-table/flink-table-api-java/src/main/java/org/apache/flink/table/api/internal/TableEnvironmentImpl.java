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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
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
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinitionUtil;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterTableOperation;
import org.apache.flink.table.operations.ddl.AlterTablePropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.DropCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.DropDatabaseOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.operations.ddl.DropTempSystemFunctionOperation;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Implementation of {@link TableEnvironment} that works exclusively with Table API interfaces.
 * Only {@link TableSource} is supported as an input and {@link TableSink} as an output. It also does
 * not bind to any particular {@code StreamExecutionEnvironment}.
 */
@Internal
public class TableEnvironmentImpl implements TableEnvironment {
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
	private static final String UNSUPPORTED_QUERY_IN_SQL_UPDATE_MSG =
			"Unsupported SQL query! sqlUpdate() only accepts a single SQL statement of type " +
			"INSERT, CREATE TABLE, DROP TABLE, ALTER TABLE, USE CATALOG, USE [CATALOG.]DATABASE, " +
			"CREATE DATABASE, DROP DATABASE, ALTER DATABASE, CREATE FUNCTION, " +
			"DROP FUNCTION, ALTER FUNCTION";

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
		this.moduleManager = moduleManager;
		this.execEnv = executor;

		this.tableConfig = tableConfig;

		this.functionCatalog = functionCatalog;
		this.planner = planner;
		this.parser = planner.getParser();
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
	public void registerTableSource(String name, TableSource<?> tableSource) {
		// only accept StreamTableSource and LookupableTableSource here
		// TODO should add a validation, while StreamTableSource is in flink-table-api-java-bridge module now
		registerTableSourceInternal(name, tableSource);
	}

	@Override
	public void registerTableSink(
			String name,
			String[] fieldNames,
			TypeInformation<?>[] fieldTypes,
			TableSink<?> tableSink) {
		registerTableSink(name, tableSink.configure(fieldNames, fieldTypes));
	}

	@Override
	public void registerTableSink(String name, TableSink<?> configuredSink) {
		// validate
		if (configuredSink.getTableSchema().getFieldCount() == 0) {
			throw new TableException("Table schema cannot be empty.");
		}

		registerTableSinkInternal(name, configuredSink);
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
			.map(t -> new CatalogQueryOperation(tableIdentifier, t.getTable().getSchema()));
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
		return catalogManager.dropTemporaryTable(unresolvedIdentifier);
	}

	@Override
	public boolean dropTemporaryView(String path) {
		UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(path);
		return catalogManager.dropTemporaryView(unresolvedIdentifier);
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
		return planner.explain(Collections.singletonList(table.getQueryOperation()), extended);
	}

	@Override
	public String explain(boolean extended) {
		List<Operation> operations = bufferedModifyOperations.stream()
			.map(o -> (Operation) o).collect(Collectors.toList());
		return planner.explain(operations, extended);
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
	public void sqlUpdate(String stmt) {
		List<Operation> operations = parser.parse(stmt);

		if (operations.size() != 1) {
			throw new TableException(UNSUPPORTED_QUERY_IN_SQL_UPDATE_MSG);
		}

		Operation operation = operations.get(0);

		if (operation instanceof ModifyOperation) {
			buffer(Collections.singletonList((ModifyOperation) operation));
		} else if (operation instanceof CreateTableOperation) {
			CreateTableOperation createTableOperation = (CreateTableOperation) operation;
			catalogManager.createTable(
				createTableOperation.getCatalogTable(),
				createTableOperation.getTableIdentifier(),
				createTableOperation.isIgnoreIfExists());
		} else if (operation instanceof CreateDatabaseOperation) {
			CreateDatabaseOperation createDatabaseOperation = (CreateDatabaseOperation) operation;
			Catalog catalog = getCatalogOrThrowException(createDatabaseOperation.getCatalogName());
			String exMsg = getDDLOpExecuteErrorMsg(createDatabaseOperation.asSummaryString());
			try {
				catalog.createDatabase(
						createDatabaseOperation.getDatabaseName(),
						createDatabaseOperation.getCatalogDatabase(),
						createDatabaseOperation.isIgnoreIfExists());
			} catch (DatabaseAlreadyExistException e) {
				throw new ValidationException(exMsg, e);
			} catch (Exception e) {
				throw new TableException(exMsg, e);
			}
		} else if (operation instanceof DropTableOperation) {
			DropTableOperation dropTableOperation = (DropTableOperation) operation;
			catalogManager.dropTable(
				dropTableOperation.getTableIdentifier(),
				dropTableOperation.isIfExists());
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
				} else if (alterTableOperation instanceof AlterTablePropertiesOperation){
					AlterTablePropertiesOperation alterTablePropertiesOp = (AlterTablePropertiesOperation) operation;
					catalog.alterTable(
							alterTablePropertiesOp.getTableIdentifier().toObjectPath(),
							alterTablePropertiesOp.getCatalogTable(),
							false);
				}
			} catch (TableAlreadyExistException | TableNotExistException e) {
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
			} catch (DatabaseNotExistException e) {
				throw new ValidationException(exMsg, e);
			} catch (Exception e) {
				throw new TableException(exMsg, e);
			}
		} else if (operation instanceof CreateCatalogFunctionOperation) {
			CreateCatalogFunctionOperation createCatalogFunctionOperation = (CreateCatalogFunctionOperation) operation;
			createCatalogFunction(createCatalogFunctionOperation);
		} else if (operation instanceof CreateTempSystemFunctionOperation) {
			CreateTempSystemFunctionOperation createtempSystemFunctionOperation =
				(CreateTempSystemFunctionOperation) operation;
			createSystemFunction(createtempSystemFunctionOperation);
		} else if (operation instanceof AlterCatalogFunctionOperation) {
			AlterCatalogFunctionOperation alterCatalogFunctionOperation = (AlterCatalogFunctionOperation) operation;
			alterCatalogFunction(alterCatalogFunctionOperation);
		} else if (operation instanceof DropCatalogFunctionOperation) {
			DropCatalogFunctionOperation dropCatalogFunctionOperation = (DropCatalogFunctionOperation) operation;
			dropCatalogFunction(dropCatalogFunctionOperation);
		} else if (operation instanceof DropTempSystemFunctionOperation) {
			DropTempSystemFunctionOperation dropTempSystemFunctionOperation =
				(DropTempSystemFunctionOperation) operation;
			dropSystemFunction(dropTempSystemFunctionOperation);
		} else if (operation instanceof CreateCatalogOperation) {
			CreateCatalogOperation createCatalogOperation = (CreateCatalogOperation) operation;
			String exMsg = getDDLOpExecuteErrorMsg(createCatalogOperation.asSummaryString());
			try {
				catalogManager.registerCatalog(
					createCatalogOperation.getCatalogName(), createCatalogOperation.getCatalog());
			} catch (CatalogException e) {
				throw new ValidationException(exMsg, e);
			}
		} else if (operation instanceof UseCatalogOperation) {
			UseCatalogOperation useCatalogOperation = (UseCatalogOperation) operation;
			catalogManager.setCurrentCatalog(useCatalogOperation.getCatalogName());
		} else if (operation instanceof UseDatabaseOperation) {
			UseDatabaseOperation useDatabaseOperation = (UseDatabaseOperation) operation;
			catalogManager.setCurrentCatalog(useDatabaseOperation.getCatalogName());
			catalogManager.setCurrentDatabase(useDatabaseOperation.getDatabaseName());
		} else {
			throw new TableException(UNSUPPORTED_QUERY_IN_SQL_UPDATE_MSG);
		}
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

	private void registerTableSourceInternal(String name, TableSource<?> tableSource) {
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
					catalogManager.createTemporaryTable(sourceAndSink, objectIdentifier, true);
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

	private void registerTableSinkInternal(String name, TableSink<?> tableSink) {
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
					catalogManager.createTemporaryTable(sourceAndSink, objectIdentifier, true);
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

	private void createCatalogFunction(CreateCatalogFunctionOperation createCatalogFunctionOperation) {
		String exMsg = getDDLOpExecuteErrorMsg(createCatalogFunctionOperation.asSummaryString());
		try {
			CatalogFunction function = createCatalogFunctionOperation.getCatalogFunction();
			if (createCatalogFunctionOperation.isTemporary()) {
				boolean exist = functionCatalog.hasTemporaryCatalogFunction(
					createCatalogFunctionOperation.getFunctionIdentifier());
				if (!exist) {
					FunctionDefinition functionDefinition = FunctionDefinitionUtil.createFunctionDefinition(
						createCatalogFunctionOperation.getFunctionName(), function.getClassName());
					registerCatalogFunctionInFunctionCatalog(
						createCatalogFunctionOperation.getFunctionIdentifier(), functionDefinition);
				} else if (!createCatalogFunctionOperation.isIgnoreIfExists()) {
					throw new ValidationException(
						String.format("Temporary catalog function %s is already defined",
						createCatalogFunctionOperation.getFunctionIdentifier().asSerializableString()));
				}
			} else {
				Catalog catalog = getCatalogOrThrowException(
					createCatalogFunctionOperation.getFunctionIdentifier().getCatalogName());
				catalog.createFunction(
					createCatalogFunctionOperation.getFunctionIdentifier().toObjectPath(),
					createCatalogFunctionOperation.getCatalogFunction(),
					createCatalogFunctionOperation.isIgnoreIfExists());
			}
		} catch (ValidationException e) {
			throw e;
		} catch (FunctionAlreadyExistException e) {
			throw new ValidationException(e.getMessage(), e);
		} catch (Exception e) {
			throw new TableException(exMsg, e);
		}
	}

	private void alterCatalogFunction(AlterCatalogFunctionOperation alterCatalogFunctionOperation) {
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
		} catch (ValidationException e) {
			throw e;
		}  catch (FunctionNotExistException e) {
			throw new ValidationException(e.getMessage(), e);
		} catch (Exception e) {
			throw new TableException(exMsg, e);
		}
	}

	private void dropCatalogFunction(DropCatalogFunctionOperation dropCatalogFunctionOperation) {

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
		} catch (ValidationException e) {
			throw e;
		}  catch (FunctionNotExistException e) {
			throw new ValidationException(e.getMessage(), e);
		} catch (Exception e) {
			throw new TableException(exMsg, e);
		}
	}

	private void createSystemFunction(CreateTempSystemFunctionOperation operation) {
		String exMsg = getDDLOpExecuteErrorMsg(operation.asSummaryString());
		try {
				boolean exist = functionCatalog.hasTemporarySystemFunction(operation.getFunctionName());
				if (!exist) {
					FunctionDefinition functionDefinition = FunctionDefinitionUtil.createFunctionDefinition(
						operation.getFunctionName(),
						operation.getFunctionClass());
					registerSystemFunctionInFunctionCatalog(operation.getFunctionName(), functionDefinition);

				} else if (!operation.isIgnoreIfExists()) {
					throw new ValidationException(
						String.format("Temporary system function %s is already defined",
							operation.getFunctionName()));
				}
		} catch (ValidationException e) {
			throw e;
		} catch (Exception e) {
			throw new TableException(exMsg, e);
		}
	}

	private void dropSystemFunction(DropTempSystemFunctionOperation operation) {
		try {
			functionCatalog.dropTemporarySystemFunction(
				operation.getFunctionName(),
				operation.isIfExists());
		} catch (ValidationException e) {
			throw e;
		} catch (Exception e) {
			throw new TableException(getDDLOpExecuteErrorMsg(operation.asSummaryString()), e);
		}
	}

	private <T, ACC> void  registerCatalogFunctionInFunctionCatalog(
		ObjectIdentifier functionIdentifier, FunctionDefinition functionDefinition) {
		if (functionDefinition instanceof ScalarFunctionDefinition) {
			ScalarFunctionDefinition scalarFunction = (ScalarFunctionDefinition) functionDefinition;
			functionCatalog.registerTempCatalogScalarFunction(
				functionIdentifier, scalarFunction.getScalarFunction());
		} else if (functionDefinition instanceof AggregateFunctionDefinition) {
			AggregateFunctionDefinition aggregateFunctionDefinition = (AggregateFunctionDefinition) functionDefinition;
			AggregateFunction<T, ACC > aggregateFunction =
				(AggregateFunction<T, ACC >) aggregateFunctionDefinition.getAggregateFunction();
			TypeInformation<T> typeInfo = UserDefinedFunctionHelper
				.getReturnTypeOfAggregateFunction(aggregateFunction);
			TypeInformation<ACC> accTypeInfo = UserDefinedFunctionHelper
				.getAccumulatorTypeOfAggregateFunction(aggregateFunction);

			functionCatalog.registerTempCatalogAggregateFunction(
				functionIdentifier,
				aggregateFunction,
				typeInfo,
				accTypeInfo);
		} else if (functionDefinition instanceof TableFunctionDefinition) {
			TableFunctionDefinition tableFunctionDefinition = (TableFunctionDefinition) functionDefinition;
			TableFunction<T> tableFunction = (TableFunction<T>) tableFunctionDefinition.getTableFunction();
			TypeInformation<T> typeInfo = UserDefinedFunctionHelper
				.getReturnTypeOfTableFunction(tableFunction);
			functionCatalog.registerTempCatalogTableFunction(
				functionIdentifier,
				tableFunction,
				typeInfo);
		}
	}

	private <T, ACC> void  registerSystemFunctionInFunctionCatalog(
		String functionName, FunctionDefinition functionDefinition) {

		if (functionDefinition instanceof ScalarFunctionDefinition) {
			ScalarFunctionDefinition scalarFunction = (ScalarFunctionDefinition) functionDefinition;
			functionCatalog.registerTempSystemScalarFunction(
				functionName, scalarFunction.getScalarFunction());
		} else if (functionDefinition instanceof AggregateFunctionDefinition) {
			AggregateFunctionDefinition aggregateFunctionDefinition = (AggregateFunctionDefinition) functionDefinition;
			AggregateFunction<T, ACC > aggregateFunction =
				(AggregateFunction<T, ACC >) aggregateFunctionDefinition.getAggregateFunction();
			TypeInformation<T> typeInfo = UserDefinedFunctionHelper
				.getReturnTypeOfAggregateFunction(aggregateFunction);
			TypeInformation<ACC> accTypeInfo = UserDefinedFunctionHelper
				.getAccumulatorTypeOfAggregateFunction(aggregateFunction);
			functionCatalog.registerTempSystemAggregateFunction(
				functionName,
				aggregateFunction,
				typeInfo,
				accTypeInfo);

		} else if (functionDefinition instanceof TableFunctionDefinition) {
			TableFunctionDefinition tableFunctionDefinition = (TableFunctionDefinition) functionDefinition;
			TableFunction<T> tableFunction = (TableFunction<T>) tableFunctionDefinition.getTableFunction();
			TypeInformation<T> typeInfo = UserDefinedFunctionHelper
				.getReturnTypeOfTableFunction(tableFunction);

			functionCatalog.registerTempSystemTableFunction(
				functionName,
				tableFunction,
				typeInfo);
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
