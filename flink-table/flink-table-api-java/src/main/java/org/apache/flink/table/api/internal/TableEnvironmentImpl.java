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
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
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

		@Override
		public void createTableSource(String name, TableSource<?> tableSource) {
			registerTableSource(name, tableSource);
		}

		@Override
		public void createTableSink(String name, TableSink<?> tableSink) {
			registerTableSink(name, tableSink);
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
			functionCatalog,
			path -> {
				try {
					UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(path);
					Optional<CatalogQueryOperation> catalogQueryOperation = scanInternal(unresolvedIdentifier);
					return catalogQueryOperation.map(t -> new TableReferenceExpression(path, t));
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

		CatalogManager catalogManager = new CatalogManager(
			settings.getBuiltInCatalogName(),
			new GenericInMemoryCatalog(settings.getBuiltInCatalogName(), settings.getBuiltInDatabaseName()));

		ModuleManager moduleManager = new ModuleManager();
		FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager, moduleManager);

		Map<String, String> executorProperties = settings.toExecutorProperties();
		Executor executor = ComponentFactoryService.find(ExecutorFactory.class, executorProperties)
			.create(executorProperties);

		TableConfig tableConfig = new TableConfig();
		Map<String, String> plannerProperties = settings.toPlannerProperties();
		Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
			.create(plannerProperties, executor, tableConfig, functionCatalog, catalogManager);

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
		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(fullPath.toArray(new String[0]));

		insertIntoInternal(unresolvedIdentifier, table);
	}

	private void insertIntoInternal(UnresolvedIdentifier unresolvedIdentifier, Table table) {
		ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

		List<ModifyOperation> modifyOperations = Collections.singletonList(
			new CatalogSinkModifyOperation(
				objectIdentifier,
				table.getQueryOperation()));

		if (isEagerOperationTranslation()) {
			translate(modifyOperations);
		} else {
			buffer(modifyOperations);
		}
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
			throw new TableException(
				"Unsupported SQL query! sqlUpdate() only accepts a single SQL statement of type " +
					"INSERT, CREATE TABLE, DROP TABLE, USE CATALOG");
		}

		Operation operation = operations.get(0);

		if (operation instanceof ModifyOperation) {
			List<ModifyOperation> modifyOperations = Collections.singletonList((ModifyOperation) operation);
			if (isEagerOperationTranslation()) {
				translate(modifyOperations);
			} else {
				buffer(modifyOperations);
			}
		} else if (operation instanceof CreateTableOperation) {
			CreateTableOperation createTableOperation = (CreateTableOperation) operation;
			catalogManager.createTable(
				createTableOperation.getCatalogTable(),
				createTableOperation.getTableIdentifier(),
				createTableOperation.isIgnoreIfExists());
		} else if (operation instanceof DropTableOperation) {
			DropTableOperation dropTableOperation = (DropTableOperation) operation;
			catalogManager.dropTable(
				dropTableOperation.getTableIdentifier(),
				dropTableOperation.isIfExists());
		} else if (operation instanceof UseCatalogOperation) {
			UseCatalogOperation useCatalogOperation = (UseCatalogOperation) operation;
			catalogManager.setCurrentCatalog(useCatalogOperation.getCatalogName());
		} else {
			throw new TableException(
				"Unsupported SQL query! sqlUpdate() only accepts a single SQL statements of " +
					"type INSERT, CREATE TABLE, DROP TABLE, USE CATALOG");
		}
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
		translate(bufferedModifyOperations);
		bufferedModifyOperations.clear();
		return execEnv.execute(jobName);
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
	 * Defines the behavior of this {@link TableEnvironment}. If true the queries will
	 * be translated immediately. If false the {@link ModifyOperation}s will be buffered
	 * and translated only when {@link #execute(String)} is called.
	 *
	 * <p>If the {@link TableEnvironment} works in a lazy manner it is undefined what
	 * configurations values will be used. It depends on the characteristic of the particular
	 * parameter. Some might used values current to the time of query construction (e.g. the currentCatalog)
	 * and some use values from the time when {@link #execute(String)} is called (e.g. timeZone).
	 *
	 * @return true if the queries should be translated immediately.
	 */
	protected boolean isEagerOperationTranslation() {
		return false;
	}

	/**
	 * Subclasses can override this method to add additional checks.
	 *
	 * @param tableSource tableSource to validate
	 */
	protected void validateTableSource(TableSource<?> tableSource) {
		TableSourceValidation.validateTableSource(tableSource);
	}

	private void translate(List<ModifyOperation> modifyOperations) {
		List<Transformation<?>> transformations = planner.translate(modifyOperations);

		execEnv.apply(transformations);
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

	protected TableImpl createTable(QueryOperation tableOperation) {
		return TableImpl.createTable(
			this,
			tableOperation,
			operationTreeBuilder,
			functionCatalog);
	}
}
