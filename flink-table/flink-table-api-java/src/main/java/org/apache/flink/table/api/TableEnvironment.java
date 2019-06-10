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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.util.Optional;

/**
 * The base class for batch and stream TableEnvironments.
 *
 * <p>The TableEnvironment is a central concept of the Table API and SQL integration. It is
 * responsible for:
 *
 * <ul>
 *     <li>Registering a Table in the internal catalog</li>
 *     <li>Registering an external catalog</li>
 *     <li>Executing SQL queries</li>
 *     <li>Registering a user-defined scalar function. For the user-defined table and aggregate
 *     function, use the StreamTableEnvironment or BatchTableEnvironment</li>
 * </ul>
 */
@PublicEvolving
public interface TableEnvironment {

	/**
	 * Creates a table from a table source.
	 *
	 * @param source table source used as table
	 */
	Table fromTableSource(TableSource<?> source);

	/**
	 * Registers an {@link ExternalCatalog} under a unique name in the TableEnvironment's schema.
	 * All tables registered in the {@link ExternalCatalog} can be accessed.
	 *
	 * @param name The name under which the externalCatalog will be registered.
	 * @param externalCatalog The externalCatalog to register.
	 * @see TableEnvironment#getCatalog(String)
	 * @see TableEnvironment#registerCatalog(String, Catalog)
	 * @deprecated the {@link ExternalCatalog} API is deprecated. Use the corresponding {@link Catalog} API.
	 */
	@Deprecated
	void registerExternalCatalog(String name, ExternalCatalog externalCatalog);

	/**
	 * Gets a registered {@link ExternalCatalog} by name.
	 *
	 * @param name The name to look up the {@link ExternalCatalog}.
	 * @return The {@link ExternalCatalog}.
	 * @see TableEnvironment#getCatalog(String)
	 * @see TableEnvironment#registerCatalog(String, Catalog)
	 * @deprecated the {@link ExternalCatalog} API is deprecated. Use the corresponding {@link Catalog} API.
	 */
	@Deprecated
	ExternalCatalog getRegisteredExternalCatalog(String name);

	/**
	 * Registers a {@link Catalog} under a unique name.
	 * All tables registered in the {@link Catalog} can be accessed.
	 *
	 * @param catalogName The name under which the catalog will be registered.
	 * @param catalog The catalog to register.
	 */
	void registerCatalog(String catalogName, Catalog catalog);

	/**
	 * Gets a registered {@link Catalog} by name.
	 *
	 * @param catalogName The name to look up the {@link Catalog}.
	 * @return The requested catalog, empty if there is no registered catalog with given name.
	 */
	Optional<Catalog> getCatalog(String catalogName);

	/**
	 * Registers a {@link ScalarFunction} under a unique name. Replaces already existing
	 * user-defined functions under this name.
	 */
	void registerFunction(String name, ScalarFunction function);

	/**
	 * Registers a {@link Table} under a unique name in the TableEnvironment's catalog.
	 * Registered tables can be referenced in SQL queries.
	 *
	 * @param name The name under which the table will be registered.
	 * @param table The table to register.
	 */
	void registerTable(String name, Table table);

	/**
	 * Registers an external {@link TableSource} in this {@link TableEnvironment}'s catalog.
	 * Registered tables can be referenced in SQL queries.
	 *
	 * @param name        The name under which the {@link TableSource} is registered.
	 * @param tableSource The {@link TableSource} to register.
	 */
	void registerTableSource(String name, TableSource<?> tableSource);

	/**
	 * Registers an external {@link TableSink} with given field names and types in this
	 * {@link TableEnvironment}'s catalog.
	 * Registered sink tables can be referenced in SQL DML statements.
	 *
	 * @param name The name under which the {@link TableSink} is registered.
	 * @param fieldNames The field names to register with the {@link TableSink}.
	 * @param fieldTypes The field types to register with the {@link TableSink}.
	 * @param tableSink The {@link TableSink} to register.
	 * @deprecated Use {@link #registerTableSink(String, TableSink)} instead.
	 */
	@Deprecated
	void registerTableSink(String name, String[] fieldNames, TypeInformation<?>[] fieldTypes, TableSink<?> tableSink);

	/**
	 * Registers an external {@link TableSink} with already configured field names and field types in
	 * this {@link TableEnvironment}'s catalog.
	 * Registered sink tables can be referenced in SQL DML statements.
	 *
	 * @param name The name under which the {@link TableSink} is registered.
	 * @param configuredSink The configured {@link TableSink} to register.
	 */
	void registerTableSink(String name, TableSink<?> configuredSink);

	/**
	 * Scans a registered table and returns the resulting {@link Table}.
	 *
	 * <p>A table to scan must be registered in the {@link TableEnvironment}. It can be either directly
	 * registered or be an external member of a {@link Catalog}.
	 *
	 * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or
	 * {@link TableEnvironment#useCatalog(String)} for the rules on the path resolution.
	 *
	 * <p>Examples:
	 *
	 * <p>Scanning a directly registered table.
	 * <pre>
	 * {@code
	 *   Table tab = tableEnv.scan("tableName");
	 * }
	 * </pre>
	 *
	 * <p>Scanning a table from a registered catalog.
	 * <pre>
	 * {@code
	 *   Table tab = tableEnv.scan("catalogName", "dbName", "tableName");
	 * }
	 * </pre>
	 *
	 * @param tablePath The path of the table to scan.
	 * @return The resulting {@link Table}.
	 * @see TableEnvironment#useCatalog(String)
	 * @see TableEnvironment#useDatabase(String)
	 */
	Table scan(String... tablePath);

	/**
	 * Writes the {@link Table} to a {@link TableSink} that was registered under the specified name.
	 *
	 * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or
	 * {@link TableEnvironment#useCatalog(String)} for the rules on the path resolution.
	 *
	 * @param table The Table to write to the sink.
	 * @param queryConfig The {@link QueryConfig} to use.
	 * @param sinkPath The first part of the path of the registered {@link TableSink} to which the {@link Table} is
	 *        written. This is to ensure at least the name of the {@link TableSink} is provided.
	 * @param sinkPathContinued The remaining part of the path of the registered {@link TableSink} to which the
	 *        {@link Table} is written.
	 */
	void insertInto(Table table, QueryConfig queryConfig, String sinkPath, String... sinkPathContinued);

	/**
	 * Writes the {@link Table} to a {@link TableSink} that was registered under the specified name.
	 *
	 * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or
	 * {@link TableEnvironment#useCatalog(String)} for the rules on the path resolution.
	 *
	 * @param table The Table to write to the sink.
	 * @param sinkPath The first part of the path of the registered {@link TableSink} to which the {@link Table} is
	 *        written. This is to ensure at least the name of the {@link TableSink} is provided.
	 * @param sinkPathContinued The remaining part of the path of the registered {@link TableSink} to which the
	 *        {@link Table} is written.
	 */
	void insertInto(Table table, String sinkPath, String... sinkPathContinued);

	/**
	 * Creates a table source and/or table sink from a descriptor.
	 *
	 * <p>Descriptors allow for declaring the communication to external systems in an
	 * implementation-agnostic way. The classpath is scanned for suitable table factories that match
	 * the desired configuration.
	 *
	 * <p>The following example shows how to read from a connector using a JSON format and
	 * register a table source as "MyTable":
	 *
	 * <pre>
	 * {@code
	 *
	 * tableEnv
	 *   .connect(
	 *     new ExternalSystemXYZ()
	 *       .version("0.11"))
	 *   .withFormat(
	 *     new Json()
	 *       .jsonSchema("{...}")
	 *       .failOnMissingField(false))
	 *   .withSchema(
	 *     new Schema()
	 *       .field("user-name", "VARCHAR").from("u_name")
	 *       .field("count", "DECIMAL")
	 *   .registerSource("MyTable");
	 * }
	 *</pre>
	 *
	 * @param connectorDescriptor connector descriptor describing the external system
	 */
	TableDescriptor connect(ConnectorDescriptor connectorDescriptor);

	/**
	 * Gets the names of all tables registered directly in this environment.
	 *
	 * @return A list of the names of all registered tables.
	 */
	String[] listTables();

	/**
	 * Gets the names of all user defined functions registered in this environment.
	 */
	String[] listUserDefinedFunctions();

	/**
	 * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
	 * the result of the given {@link Table}.
	 *
	 * @param table The table for which the AST and execution plan will be returned.
	 */
	String explain(Table table);

	/**
	 * Returns completion hints for the given statement at the given cursor position.
	 * The completion happens case insensitively.
	 *
	 * @param statement Partial or slightly incorrect SQL statement
	 * @param position cursor position
	 * @return completion hints that fit at the current cursor position
	 * @deprecated Will be removed in the next release
	 */
	@Deprecated
	String[] getCompletionHints(String statement, int position);

	/**
	 * Evaluates a SQL query on registered tables and retrieves the result as a {@link Table}.
	 *
	 * <p>All tables referenced by the query must be registered in the TableEnvironment.
	 * A {@link Table} is automatically registered when its {@link Table#toString()} method is
	 * called, for example when it is embedded into a String.
	 * Hence, SQL queries can directly reference a {@link Table} as follows:
	 *
	 * <pre>
	 * {@code
	 *   Table table = ...;
	 *   String tableName = table.toString();
	 *   // the table is not registered to the table environment
	 *   tEnv.sqlQuery("SELECT * FROM tableName");
	 * }
	 * </pre>
	 *
	 * @param query The SQL query to evaluate.
	 * @return The result of the query as Table
	 */
	Table sqlQuery(String query);

	/**
	 * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
	 * NOTE: Currently only SQL INSERT statements are supported.
	 *
	 * <p>All tables referenced by the query must be registered in the TableEnvironment.
	 * A {@link Table} is automatically registered when its {@link Table#toString()} method is
	 * called, for example when it is embedded into a String.
	 * Hence, SQL queries can directly reference a {@link Table} as follows:
	 *
	 * <pre>
	 * {@code
	 *   // register the configured table sink into which the result is inserted.
	 *   tEnv.registerTableSink("sinkTable", configuredSink);
	 *   Table sourceTable = ...
	 *   String tableName = sourceTable.toString();
	 *   // sourceTable is not registered to the table environment
	 *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM tableName");
	 * }
	 * </pre>
	 *
	 * @param stmt The SQL statement to evaluate.
	 */
	void sqlUpdate(String stmt);

	/**
	 * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
	 * NOTE: Currently only SQL INSERT statements are supported.
	 *
	 * <p>All tables referenced by the query must be registered in the TableEnvironment.
	 * A {@link Table} is automatically registered when its {@link Table#toString()} method is
	 * called, for example when it is embedded into a String.
	 * Hence, SQL queries can directly reference a {@link Table} as follows:
	 *
	 * <pre>
	 * {@code
	 *   // register the configured table sink into which the result is inserted.
	 *   tEnv.registerTableSink("sinkTable", configuredSink);
	 *   Table sourceTable = ...
	 *   String tableName = sourceTable.toString();
	 *   // sourceTable is not registered to the table environment
	 *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM tableName", config);
	 * }
	 * </pre>
	 *
	 * @param stmt The SQL statement to evaluate.
	 * @param config The {@link QueryConfig} to use.
	 */
	void sqlUpdate(String stmt, QueryConfig config);

	/**
	 * Gets the current default catalog name of the current session.
	 *
	 * @return The current default catalog name that is used for the path resolution.
	 * @see TableEnvironment#useCatalog(String)
	 */
	String getCurrentCatalog();

	/**
	 * Sets the current catalog to the given value. It also sets the default
	 * database to the catalog's default one. See also {@link TableEnvironment#useDatabase(String)}.
	 *
	 * <p>This is used during the resolution of object paths. Both the catalog and database are optional
	 * when referencing catalog objects such as tables, views etc. The algorithm looks for requested objects in
	 * following paths in that order:
	 * <ol>
	 *     <li>{@code [current-catalog].[current-database].[requested-path]}</li>
	 *     <li>{@code [current-catalog].[requested-path]}</li>
	 *     <li>{@code [requested-path]}</li>
	 * </ol>
	 *
	 * <p>Example:
	 *
	 * <p>Given structure with default catalog set to {@code default_catalog} and default database set to
	 * {@code default_database}.
	 * <pre>
	 * root:
	 *   |- default_catalog
	 *       |- default_database
	 *           |- tab1
	 *       |- db1
	 *           |- tab1
	 *   |- cat1
	 *       |- db1
	 *           |- tab1
	 * </pre>
	 *
	 * <p>The following table describes resolved paths:
	 * <table>
	 *     <thead>
	 *         <tr>
	 *             <th>Requested path</th>
	 *             <th>Resolved path</th>
	 *         </tr>
	 *     </thead>
	 *     <tbody>
	 *         <tr>
	 *             <td>tab1</td>
	 *             <td>default_catalog.default_database.tab1</td>
	 *         </tr>
	 *         <tr>
	 *             <td>db1.tab1</td>
	 *             <td>default_catalog.db1.tab1</td>
	 *         </tr>
	 *         <tr>
	 *             <td>cat1.db1.tab1</td>
	 *             <td>cat1.db1.tab1</td>
	 *         </tr>
	 *     </tbody>
	 * </table>
	 *
	 * @param catalogName The name of the catalog to set as the current default catalog.
	 * @see TableEnvironment#useDatabase(String)
	 */
	void useCatalog(String catalogName);

	/**
	 * Gets the current default database name of the running session.
	 *
	 * @return The name of the current database of the current catalog.
	 * @see TableEnvironment#useDatabase(String)
	 */
	String getCurrentDatabase();

	/**
	 * Sets the current default database. It has to exist in the current catalog. That path will be used as
	 * the default one when looking for unqualified object names.
	 *
	 * <p>This is used during the resolution of object paths. Both the catalog and database are optional
	 * when referencing catalog objects such as tables, views etc. The algorithm looks for requested objects in
	 * following paths in that order:
	 * <ol>
	 *     <li>{@code [current-catalog].[current-database].[requested-path]}</li>
	 *     <li>{@code [current-catalog].[requested-path]}</li>
	 *     <li>{@code [requested-path]}</li>
	 * </ol>
	 *
	 * <p>Example:
	 *
	 * <p>Given structure with default catalog set to {@code default_catalog} and default database set to
	 * {@code default_database}.
	 * <pre>
	 * root:
	 *   |- default_catalog
	 *       |- default_database
	 *           |- tab1
	 *       |- db1
	 *           |- tab1
	 *   |- cat1
	 *       |- db1
	 *           |- tab1
	 * </pre>
	 *
	 * <p></p>The following table describes resolved paths:
	 * <table>
	 *     <thead>
	 *         <tr>
	 *             <th>Requested path</th>
	 *             <th>Resolved path</th>
	 *         </tr>
	 *     </thead>
	 *     <tbody>
	 *         <tr>
	 *             <td>tab1</td>
	 *             <td>default_catalog.default_database.tab1</td>
	 *         </tr>
	 *         <tr>
	 *             <td>db1.tab1</td>
	 *             <td>default_catalog.db1.tab1</td>
	 *         </tr>
	 *         <tr>
	 *             <td>cat1.db1.tab1</td>
	 *             <td>cat1.db1.tab1</td>
	 *         </tr>
	 *     </tbody>
	 * </table>
	 *
	 * @param databaseName The name of the database to set as the current database.
	 * @see TableEnvironment#useCatalog(String)
	 */
	void useDatabase(String databaseName);

	/**
	 * Returns the table config that defines the runtime behavior of the Table API.
	 */
	TableConfig getConfig();
}
