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
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.util.StringUtils;

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
	private final OperationTreeBuilder operationTreeBuilder;
	private final List<ModifyOperation> bufferedModifyOperations = new ArrayList<>();

	protected final TableConfig tableConfig;
	protected final Executor execEnv;
	protected final FunctionCatalog functionCatalog;
	protected final Planner planner;

	protected TableEnvironmentImpl(
			CatalogManager catalogManager,
			TableConfig tableConfig,
			Executor executor,
			FunctionCatalog functionCatalog,
			Planner planner,
			boolean isStreamingMode) {
		this.catalogManager = catalogManager;
		this.execEnv = executor;

		this.tableConfig = tableConfig;

		this.functionCatalog = functionCatalog;
		this.planner = planner;
		this.operationTreeBuilder = OperationTreeBuilder.create(
			functionCatalog,
			path -> {
				Optional<CatalogQueryOperation> catalogTableOperation = scanInternal(path);
				return catalogTableOperation.map(tableOperation -> new TableReferenceExpression(path, tableOperation));
			},
			isStreamingMode
		);
	}

	public static TableEnvironmentImpl create(EnvironmentSettings settings) {

		CatalogManager catalogManager = new CatalogManager(
			settings.getBuiltInCatalogName(),
			new GenericInMemoryCatalog(settings.getBuiltInCatalogName(), settings.getBuiltInDatabaseName()));

		FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager);

		Map<String, String> executorProperties = settings.toExecutorProperties();
		Executor executor = ComponentFactoryService.find(ExecutorFactory.class, executorProperties)
			.create(executorProperties);

		TableConfig tableConfig = new TableConfig();
		Map<String, String> plannerProperties = settings.toPlannerProperties();
		Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
			.create(plannerProperties, executor, tableConfig, functionCatalog, catalogManager);

		return new TableEnvironmentImpl(
			catalogManager,
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
	public void registerExternalCatalog(String name, ExternalCatalog externalCatalog) {
		catalogManager.registerExternalCatalog(name, externalCatalog);
	}

	@Override
	public ExternalCatalog getRegisteredExternalCatalog(String name) {
		return catalogManager.getExternalCatalog(name).orElseThrow(() -> new CatalogNotExistException(name));
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
	public void registerFunction(String name, ScalarFunction function) {
		functionCatalog.registerScalarFunction(
			name,
			function);
	}

	@Override
	public void registerTable(String name, Table table) {
		if (((TableImpl) table).getTableEnvironment() != this) {
			throw new TableException(
				"Only tables that belong to this TableEnvironment can be registered.");
		}

		CatalogBaseTable tableTable = new QueryOperationCatalogView(table.getQueryOperation());
		registerTableInternal(name, tableTable);
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

		checkValidTableName(name);
		registerTableSinkInternal(name, configuredSink);
	}

	@Override
	public Table scan(String... tablePath) {
		return scanInternal(tablePath).map(this::createTable)
			.orElseThrow(() -> new ValidationException(String.format(
				"Table '%s' was not found.",
				String.join(".", tablePath))));
	}

	private Optional<CatalogQueryOperation> scanInternal(String... tablePath) {
		return catalogManager.resolveTable(tablePath)
			.map(t -> new CatalogQueryOperation(t.getTablePath(), t.getTableSchema()));
	}

	@Override
	public ConnectTableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
		return new StreamTableDescriptor(this, connectorDescriptor);
	}

	@Override
	public String[] listCatalogs() {
		return catalogManager.getCatalogs().toArray(new String[0]);
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
		String currentCatalogName = catalogManager.getCurrentCatalog();
		Optional<Catalog> currentCatalog = catalogManager.getCatalog(currentCatalogName);

		return currentCatalog.map(catalog -> {
			try {
				return catalog.listTables(catalogManager.getCurrentDatabase()).toArray(new String[0]);
			} catch (DatabaseNotExistException e) {
				throw new ValidationException("Current database does not exist", e);
			}
		}).orElseThrow(() ->
			new TableException(String.format("The current catalog %s does not exist.", currentCatalogName)));
	}

	@Override
	public String[] listUserDefinedFunctions() {
		return functionCatalog.getUserDefinedFunctions();
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
		List<Operation> operations = planner.parse(query);

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
	public void insertInto(Table table, String path, String... pathContinued) {
		List<String> fullPath = new ArrayList<>(Arrays.asList(pathContinued));
		fullPath.add(0, path);

		List<ModifyOperation> modifyOperations = Collections.singletonList(
			new CatalogSinkModifyOperation(
				fullPath,
				table.getQueryOperation()));

		if (isEagerOperationTranslation()) {
			translate(modifyOperations);
		} else {
			buffer(modifyOperations);
		}
	}

	@Override
	public void sqlUpdate(String stmt) {
		List<Operation> operations = planner.parse(stmt);

		if (operations.size() != 1) {
			throw new TableException(
				"Unsupported SQL query! sqlUpdate() only accepts a single SQL statement of type " +
					"INSERT, CREATE TABLE, DROP TABLE");
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
			registerCatalogTableInternal(
				createTableOperation.getTablePath(),
				createTableOperation.getCatalogTable(),
				createTableOperation.isIgnoreIfExists());
		} else if (operation instanceof DropTableOperation) {
			String[] name = ((DropTableOperation) operation).getTableName();
			boolean isIfExists = ((DropTableOperation) operation).isIfExists();
			String[] paths = catalogManager.getFullTablePath(Arrays.asList(name));
			Optional<Catalog> catalog = getCatalog(paths[0]);
			if (!catalog.isPresent()) {
				if (!isIfExists) {
					throw new TableException("Catalog " + paths[0] + " does not exist.");
				}
			} else {
				try {
					catalog.get().dropTable(new ObjectPath(paths[1], paths[2]), isIfExists);
				} catch (TableNotExistException e) {
					throw new TableException(e.getMessage());
				}
			}
		} else {
			throw new TableException(
				"Unsupported SQL query! sqlUpdate() only accepts a single SQL statements of " +
					"type INSERT, CREATE TABLE, DROP TABLE");
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

	/**
	 * Registers a {@link CatalogBaseTable} under a given object path. The {@code path} could be
	 * 3 formats:
	 * <ol>
	 *   <li>`catalog.db.table`: A full table path including the catalog name,
	 *   the database name and table name.</li>
	 *   <li>`db.table`: database name following table name, with the current catalog name.</li>
	 *   <li>`table`: Only the table name, with the current catalog name and database  name.</li>
	 * </ol>
	 * The registered tables then can be referenced in Sql queries.
	 *
	 * @param path           The path under which the table will be registered
	 * @param catalogTable   The table to register
	 * @param ignoreIfExists If true, do nothing if there is already same table name under
	 *                       the {@code path}. If false, a TableAlreadyExistException throws.
	 */
	private void registerCatalogTableInternal(String[] path,
			CatalogBaseTable catalogTable,
			boolean ignoreIfExists) {
		String[] fullName = catalogManager.getFullTablePath(Arrays.asList(path));
		Catalog catalog = getCatalog(fullName[0]).orElseThrow(() ->
			new TableException("Catalog " + fullName[0] + " does not exist"));
		ObjectPath objectPath = new ObjectPath(fullName[1], fullName[2]);
		try {
			catalog.createTable(objectPath, catalogTable, ignoreIfExists);
		} catch (Exception e) {
			throw new TableException("Could not register table", e);
		}
	}

	protected void registerTableInternal(String name, CatalogBaseTable table) {
		try {
			checkValidTableName(name);
			ObjectPath path = new ObjectPath(catalogManager.getBuiltInDatabaseName(), name);
			Optional<Catalog> catalog = catalogManager.getCatalog(catalogManager.getBuiltInCatalogName());
			if (catalog.isPresent()) {
				catalog.get().createTable(
					path,
					table,
					false);
			}
		} catch (Exception e) {
			throw new TableException("Could not register table", e);
		}
	}

	private void replaceTableInternal(String name, CatalogBaseTable table) {
		try {
			ObjectPath path = new ObjectPath(catalogManager.getBuiltInDatabaseName(), name);
			Optional<Catalog> catalog = catalogManager.getCatalog(catalogManager.getBuiltInCatalogName());
			if (catalog.isPresent()) {
				catalog.get().alterTable(
					path,
					table,
					false);
			}
		} catch (Exception e) {
			throw new TableException("Could not register table", e);
		}
	}

	private void checkValidTableName(String name) {
		if (StringUtils.isNullOrWhitespaceOnly(name)) {
			throw new ValidationException("A table name cannot be null or consist of only whitespaces.");
		}
	}

	private void registerTableSourceInternal(String name, TableSource<?> tableSource) {
		validateTableSource(tableSource);
		Optional<CatalogBaseTable> table = getCatalogTable(catalogManager.getBuiltInCatalogName(),
			catalogManager.getBuiltInDatabaseName(), name);

		if (table.isPresent()) {
			if (table.get() instanceof ConnectorCatalogTable<?, ?>) {
				ConnectorCatalogTable<?, ?> sourceSinkTable = (ConnectorCatalogTable<?, ?>) table.get();
				if (sourceSinkTable.getTableSource().isPresent()) {
					throw new ValidationException(String.format(
						"Table '%s' already exists. Please choose a different name.", name));
				} else {
					// wrapper contains only sink (not source)
					replaceTableInternal(
						name,
						ConnectorCatalogTable
							.sourceAndSink(tableSource, sourceSinkTable.getTableSink().get(), !IS_STREAM_TABLE));
				}
			} else {
				throw new ValidationException(String.format(
					"Table '%s' already exists. Please choose a different name.", name));
			}
		} else {
			registerTableInternal(name, ConnectorCatalogTable.source(tableSource, !IS_STREAM_TABLE));
		}
	}

	private void registerTableSinkInternal(String name, TableSink<?> tableSink) {
		Optional<CatalogBaseTable> table = getCatalogTable(catalogManager.getBuiltInCatalogName(),
			catalogManager.getBuiltInDatabaseName(), name);

		if (table.isPresent()) {
			if (table.get() instanceof ConnectorCatalogTable<?, ?>) {
				ConnectorCatalogTable<?, ?> sourceSinkTable = (ConnectorCatalogTable<?, ?>) table.get();
				if (sourceSinkTable.getTableSink().isPresent()) {
					throw new ValidationException(String.format(
						"Table '%s' already exists. Please choose a different name.", name));
				} else {
					// wrapper contains only sink (not source)
					replaceTableInternal(
						name,
						ConnectorCatalogTable
							.sourceAndSink(sourceSinkTable.getTableSource().get(), tableSink, !IS_STREAM_TABLE));
				}
			} else {
				throw new ValidationException(String.format(
					"Table '%s' already exists. Please choose a different name.", name));
			}
		} else {
			registerTableInternal(name, ConnectorCatalogTable.sink(tableSink, !IS_STREAM_TABLE));
		}
	}

	private Optional<CatalogBaseTable> getCatalogTable(String... name) {
		return catalogManager.resolveTable(name).flatMap(CatalogManager.ResolvedTable::getCatalogTable);
	}

	protected TableImpl createTable(QueryOperation tableOperation) {
		return TableImpl.createTable(
			this,
			tableOperation,
			operationTreeBuilder,
			functionCatalog);
	}
}
