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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.api.StreamQueryConfig;
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
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.lookups.TableReferenceLookup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationTreeBuilder;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.util.StringUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Implementation of {@link TableEnvironment} that works exclusively with Table API interfaces.
 * Only {@link TableSource} is supported as an input and {@link TableSink} as an output. It also does
 * not bind to any particular {@code StreamExecutionEnvironment}.
 */
@Internal
public class TableEnvironmentImpl implements TableEnvironment {

	private final CatalogManager catalogManager;

	private final String defaultCatalogName;
	private final String defaultDatabaseName;
	private final TableConfig tableConfig;
	private final OperationTreeBuilder operationTreeBuilder;

	protected final Executor execEnv;
	protected final FunctionCatalog functionCatalog;
	protected final QueryConfigProvider queryConfigProvider = new QueryConfigProvider();
	protected final Planner planner;

	protected TableEnvironmentImpl(
			CatalogManager catalogManager,
			TableConfig tableConfig,
			Executor executor,
			FunctionCatalog functionCatalog,
			Planner planner) {
		this.catalogManager = catalogManager;
		this.execEnv = executor;

		this.tableConfig = tableConfig;
		this.tableConfig.addPlannerConfig(queryConfigProvider);
		this.defaultCatalogName = tableConfig.getBuiltInCatalogName();
		this.defaultDatabaseName = tableConfig.getBuiltInDatabaseName();

		this.functionCatalog = functionCatalog;
		this.planner = planner;
		this.operationTreeBuilder = lookupTreeBuilder(
			path -> {
				Optional<CatalogQueryOperation> catalogTableOperation = scanInternal(path);
				return catalogTableOperation.map(tableOperation -> new TableReferenceExpression(path, tableOperation));
			},
			functionCatalog
		);
	}

	private static OperationTreeBuilder lookupTreeBuilder(
		TableReferenceLookup tableReferenceLookup,
		FunctionLookup functionDefinitionCatalog) {
		try {
			Class<?> clazz = Class.forName("org.apache.flink.table.operations.OperationTreeBuilderFactory");
			Method createMethod = clazz.getMethod(
				"create",
				TableReferenceLookup.class,
				FunctionLookup.class);

			return (OperationTreeBuilder) createMethod.invoke(null, tableReferenceLookup, functionDefinitionCatalog);
		} catch (Exception e) {
			throw new TableException(
				"Could not instantiate the operation builder. Make sure the planner module is on the classpath");
		}
	}

	@VisibleForTesting
	public Planner getPlanner() {
		return planner;
	}

	@Override
	public Table fromTableSource(TableSource<?> source) {
		return createTable(new TableSourceQueryOperation<>(source, false));
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
	public TableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
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
		return planner.explain(Collections.singletonList(table.getQueryOperation()), false);
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
	public void sqlUpdate(String stmt) {
		sqlUpdate(stmt, new StreamQueryConfig());
	}

	@Override
	public void insertInto(Table table, QueryConfig queryConfig, String path, String... pathContinued) {
		List<String> fullPath = new ArrayList<>(Arrays.asList(pathContinued));
		fullPath.add(0, path);

		queryConfigProvider.setConfig((StreamQueryConfig) queryConfig);
		List<StreamTransformation<?>> translate = planner.translate(Collections.singletonList(
			new CatalogSinkModifyOperation(
				fullPath,
				table.getQueryOperation())));

		execEnv.apply(translate);
	}

	@Override
	public void insertInto(Table table, String path, String... pathContinued) {
		insertInto(table, new StreamQueryConfig(), path, pathContinued);
	}

	@Override
	public void sqlUpdate(String stmt, QueryConfig config) {
		List<Operation> operations = planner.parse(stmt);

		if (operations.size() != 1) {
			throw new TableException(
				"Unsupported SQL query! sqlUpdate() only accepts a single SQL statement of type INSERT.");
		}

		Operation operation = operations.get(0);

		if (operation instanceof ModifyOperation) {
			queryConfigProvider.setConfig((StreamQueryConfig) config);
			List<StreamTransformation<?>> transformations =
				planner.translate(Collections.singletonList((ModifyOperation) operation));

			execEnv.apply(transformations);
		} else {
			throw new TableException(
				"Unsupported SQL query! sqlUpdate() only accepts a single SQL statements of type INSERT.");
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

	protected void registerTableInternal(String name, CatalogBaseTable table) {
		try {
			checkValidTableName(name);
			ObjectPath path = new ObjectPath(defaultDatabaseName, name);
			Optional<Catalog> catalog = catalogManager.getCatalog(defaultCatalogName);
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
			ObjectPath path = new ObjectPath(defaultDatabaseName, name);
			Optional<Catalog> catalog = catalogManager.getCatalog(defaultCatalogName);
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

	/**
	 * Subclasses can override this method to add additional checks.
	 *
	 * @param tableSource tableSource to validate
	 */
	protected void validateTableSource(TableSource<?> tableSource) {
		TableSourceValidation.validateTableSource(tableSource);
	}

	private void registerTableSourceInternal(String name, TableSource<?> tableSource) {
		validateTableSource(tableSource);
		Optional<CatalogBaseTable> table = getCatalogTable(defaultCatalogName, defaultDatabaseName, name);

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
							.sourceAndSink(tableSource, sourceSinkTable.getTableSink().get(), false));
				}
			} else {
				throw new ValidationException(String.format(
					"Table '%s' already exists. Please choose a different name.", name));
			}
		} else {
			registerTableInternal(name, ConnectorCatalogTable.source(tableSource, false));
		}
	}

	private void registerTableSinkInternal(String name, TableSink<?> tableSink) {
		Optional<CatalogBaseTable> table = getCatalogTable(defaultCatalogName, defaultDatabaseName, name);

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
							.sourceAndSink(sourceSinkTable.getTableSource().get(), tableSink, false));
				}
			} else {
				throw new ValidationException(String.format(
					"Table '%s' already exists. Please choose a different name.", name));
			}
		} else {
			registerTableInternal(name, ConnectorCatalogTable.sink(tableSink, false));
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
