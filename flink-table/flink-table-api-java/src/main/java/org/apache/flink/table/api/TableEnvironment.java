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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.util.Optional;

/**
 * A table environment is the base class, entry point, and central context for creating Table and SQL
 * API programs.
 *
 * <p>It is unified both on a language level for all JVM-based languages (i.e. there is no distinction
 * between Scala and Java API) and for bounded and unbounded data processing.
 *
 * <p>A table environment is responsible for:
 * <ul>
 *     <li>Connecting to external systems.</li>
 *     <li>Registering and retrieving {@link Table}s and other meta objects from a catalog.</li>
 *     <li>Executing SQL statements.</li>
 *     <li>Offering further configuration options.</li>
 * </ul>
 *
 * <p>The path in methods such as {@link #createTemporaryView(String, Table)} should be a proper SQL identifier.
 * The syntax is following [[catalog-name.]database-name.]object-name, where the catalog name and database are
 * optional. For path resolution see {@link #useCatalog(String)} and {@link #useDatabase(String)}. All keywords
 * or other special characters need to be escaped.
 *
 * <p>Example: `cat.1`.`db`.`Table` resolves to an object named 'Table' (table is a reserved keyword, thus must
 * be escaped) in a catalog named 'cat.1' and database named 'db'.
 *
 * <p>Note: This environment is meant for pure table programs. If you would like to convert from or to
 * other Flink APIs, it might be necessary to use one of the available language-specific table environments
 * in the corresponding bridging modules.
 */
@PublicEvolving
public interface TableEnvironment {

	/**
	 * Creates a table environment that is the entry point and central context for creating Table and SQL
	 * API programs.
	 *
	 * <p>It is unified both on a language level for all JVM-based languages (i.e. there is no distinction
	 * between Scala and Java API) and for bounded and unbounded data processing.
	 *
	 * <p>A table environment is responsible for:
	 * <ul>
	 *     <li>Connecting to external systems.</li>
	 *     <li>Registering and retrieving {@link Table}s and other meta objects from a catalog.</li>
	 *     <li>Executing SQL statements.</li>
	 *     <li>Offering further configuration options.</li>
	 * </ul>
	 *
	 * <p>Note: This environment is meant for pure table programs. If you would like to convert from or to
	 * other Flink APIs, it might be necessary to use one of the available language-specific table environments
	 * in the corresponding bridging modules.
	 *
	 * @param settings The environment settings used to instantiate the {@link TableEnvironment}.
	 */
	static TableEnvironment create(EnvironmentSettings settings) {
		return TableEnvironmentImpl.create(settings);
	}

	/**
	 * Creates a table from a table source.
	 *
	 * @param source table source used as table
	 */
	Table fromTableSource(TableSource<?> source);

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
	 * Loads a {@link Module} under a unique name. Modules will be kept in the loaded order.
	 * ValidationException is thrown when there is already a module with the same name.
	 *
	 * @param moduleName name of the {@link Module}
	 * @param module the module instance
	 */
	void loadModule(String moduleName, Module module);

	/**
	 * Unloads a {@link Module} with given name.
	 * ValidationException is thrown when there is no module with the given name
	 *
	 * @param moduleName name of the {@link Module}
	 */
	void unloadModule(String moduleName);

	/**
	 * Registers a {@link ScalarFunction} under a unique name. Replaces already existing
	 * user-defined functions under this name.
	 */
	void registerFunction(String name, ScalarFunction function);

	/**
	 * Registers a {@link Table} under a unique name in the TableEnvironment's catalog.
	 * Registered tables can be referenced in SQL queries.
	 *
	 * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists, it will
	 * be inaccessible in the current session. To make the permanent object available again you can drop the
	 * corresponding temporary object.
	 *
	 * @param name The name under which the table will be registered.
	 * @param table The table to register.
	 * @deprecated use {@link #createTemporaryView(String, Table)}
	 */
	@Deprecated
	void registerTable(String name, Table table);

	/**
	 * Registers a {@link Table} API object as a temporary view similar to SQL temporary views.
	 *
	 * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists, it will
	 * be inaccessible in the current session. To make the permanent object available again you can drop the
	 * corresponding temporary object.
	 *
	 * @param path The path under which the view will be registered.
	 *             See also the {@link TableEnvironment} class description for the format of the path.
	 * @param view The view to register.
	 */
	void createTemporaryView(String path, Table view);

	/**
	 * Registers an external {@link TableSource} in this {@link TableEnvironment}'s catalog.
	 * Registered tables can be referenced in SQL queries.
	 *
	 * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists, it will
	 * be inaccessible in the current session. To make the permanent object available again you can drop the
	 * corresponding temporary object.
	 *
	 * @param name        The name under which the {@link TableSource} is registered.
	 * @param tableSource The {@link TableSource} to register.
	 * @deprecated Use {@link #connect(ConnectorDescriptor)} instead.
	 */
	@Deprecated
	void registerTableSource(String name, TableSource<?> tableSource);

	/**
	 * Registers an external {@link TableSink} with given field names and types in this
	 * {@link TableEnvironment}'s catalog.
	 * Registered sink tables can be referenced in SQL DML statements.
	 *
	 * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists, it will
	 * be inaccessible in the current session. To make the permanent object available again you can drop the
	 * corresponding temporary object.
	 *
	 * @param name The name under which the {@link TableSink} is registered.
	 * @param fieldNames The field names to register with the {@link TableSink}.
	 * @param fieldTypes The field types to register with the {@link TableSink}.
	 * @param tableSink The {@link TableSink} to register.
	 * @deprecated Use {@link #connect(ConnectorDescriptor)} instead.
	 */
	@Deprecated
	void registerTableSink(String name, String[] fieldNames, TypeInformation<?>[] fieldTypes, TableSink<?> tableSink);

	/**
	 * Registers an external {@link TableSink} with already configured field names and field types in
	 * this {@link TableEnvironment}'s catalog.
	 * Registered sink tables can be referenced in SQL DML statements.
	 *
	 * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists, it will
	 * be inaccessible in the current session. To make the permanent object available again you can drop the
	 * corresponding temporary object.
	 *
	 * @param name The name under which the {@link TableSink} is registered.
	 * @param configuredSink The configured {@link TableSink} to register.
	 * @deprecated Use {@link #connect(ConnectorDescriptor)} instead.
	 */
	@Deprecated
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
	 * @deprecated use {@link #from(String)}
	 */
	@Deprecated
	Table scan(String... tablePath);

	/**
	 * Reads a registered table and returns the resulting {@link Table}.
	 *
	 * <p>A table to scan must be registered in the {@link TableEnvironment}.
	 *
	 * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or
	 * {@link TableEnvironment#useCatalog(String)} for the rules on the path resolution.
	 *
	 * <p>Examples:
	 *
	 * <p>Reading a table from default catalog and database.
	 * <pre>
	 * {@code
	 *   Table tab = tableEnv.from("tableName");
	 * }
	 * </pre>
	 *
	 * <p>Reading a table from a registered catalog.
	 * <pre>
	 * {@code
	 *   Table tab = tableEnv.from("catalogName.dbName.tableName");
	 * }
	 * </pre>
	 *
	 * <p>Reading a table from a registered catalog with escaping. ({@code Table} is a reserved keyword).
	 * Dots in e.g. a database name also must be escaped.
	 * <pre>
	 * {@code
	 *   Table tab = tableEnv.from("catalogName.`db.Name`.`Table`");
	 * }
	 * </pre>
	 *
	 * @param path The path of a table API object to scan.
	 * @return Either a table or virtual table (=view).
	 * @see TableEnvironment#useCatalog(String)
	 * @see TableEnvironment#useDatabase(String)
	 */
	Table from(String path);

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
	 * @deprecated use {@link #insertInto(String, Table)}
	 */
	@Deprecated
	void insertInto(Table table, String sinkPath, String... sinkPathContinued);

	/**
	 * Instructs to write the content of a {@link Table} API object into a table.
	 *
	 * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or
	 * {@link TableEnvironment#useCatalog(String)} for the rules on the path resolution.
	 *
	 * @param targetPath The path of the registered {@link TableSink} to which the {@link Table} is written.
	 * @param table The Table to write to the sink.
	 */
	void insertInto(String targetPath, Table table);

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
	ConnectTableDescriptor connect(ConnectorDescriptor connectorDescriptor);

	/**
	 * Gets the names of all catalogs registered in this environment.
	 *
	 * @return A list of the names of all registered catalogs.
	 */
	String[] listCatalogs();

	/**
	 * Gets an array of names of all modules in this environment in the loaded order.
	 *
	 * @return A list of the names of all modules in the loaded order.
	 */
	String[] listModules();

	/**
	 * Gets the names of all databases registered in the current catalog.
	 *
	 * @return A list of the names of all registered databases in the current catalog.
	 */
	String[] listDatabases();

	/**
	 * Gets the names of all tables available in the current namespace (the current database of the current catalog).
	 * It returns both temporary and permanent tables and views.
	 *
	 * @return A list of the names of all registered tables in the current database of the current catalog.
	 * @see #listTemporaryTables()
	 * @see #listTemporaryViews()
	 */
	String[] listTables();

	/**
	 * Gets the names of all temporary tables and views available in the current namespace (the current
	 * database of the current catalog).
	 *
	 * @return A list of the names of all registered temporary tables and views in the current database
	 * of the current catalog.
	 * @see #listTables()
	 */
	String[] listTemporaryTables();

	/**
	 * Gets the names of all temporary views available in the current namespace (the current
	 * database of the current catalog).
	 *
	 * @return A list of the names of all registered temporary views in the current database
	 * of the current catalog.
	 * @see #listTables()
	 */
	String[] listTemporaryViews();

	/**
	 * Gets the names of all user defined functions registered in this environment.
	 */
	String[] listUserDefinedFunctions();

	/**
	 * Gets the names of all functions in this environment.
	 */
	String[] listFunctions();

	/**
	 * Drops a temporary table registered in the given path.
	 *
	 * <p>If a permanent table with a given path exists, it will be used
	 * from now on for any queries that reference this path.
	 *
	 * @return true if a table existed in the given path and was removed
	 */
	boolean dropTemporaryTable(String path);

	/**
	 * Drops a temporary view registered in the given path.
	 *
	 * <p>If a permanent table or view with a given path exists, it will be used
	 * from now on for any queries that reference this path.
	 *
	 * @return true if a view existed in the given path and was removed
	 */
	boolean dropTemporaryView(String path);

	/**
	 * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
	 * the result of the given {@link Table}.
	 *
	 * @param table The table for which the AST and execution plan will be returned.
	 */
	String explain(Table table);

	/**
	 * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
	 * the result of the given {@link Table}.
	 *
	 * @param table The table for which the AST and execution plan will be returned.
	 * @param extended if the plan should contain additional properties such as
	 * e.g. estimated cost, traits
	 */
	String explain(Table table, boolean extended);

	/**
	 * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
	 * the result of multiple-sinks plan.
	 *
	 * @param extended if the plan should contain additional properties such as
	 * e.g. estimated cost, traits
	 */
	String explain(boolean extended);

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
	 * NOTE: Currently only SQL INSERT statements and CREATE TABLE statements are supported.
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
	 * <p>A DDL statement can also be executed to create a table:
	 * For example, the below DDL statement would create a CSV table named `tbl1`
	 * into the current catalog:
	 * <blockquote><pre>
	 *    create table tbl1(
	 *      a int,
	 *      b bigint,
	 *      c varchar
	 *    ) with (
	 *      'connector.type' = 'filesystem',
	 *      'format.type' = 'csv',
	 *      'connector.path' = 'xxx'
	 *    )
	 * </pre></blockquote>
	 *
	 * <p>SQL queries can directly execute as follows:
	 *
	 * <blockquote><pre>
	 *    String sinkDDL = "create table sinkTable(
	 *                        a int,
	 *                        b varchar
	 *                      ) with (
	 *                        'connector.type' = 'filesystem',
	 *                        'format.type' = 'csv',
	 *                        'connector.path' = 'xxx'
	 *                      )";
	 *
	 *    String sourceDDL ="create table sourceTable(
	 *                        a int,
	 *                        b varchar
	 *                      ) with (
	 *                        'connector.type' = 'kafka',
	 *                        'update-mode' = 'append',
	 *                        'connector.topic' = 'xxx',
	 *                        'connector.properties.0.key' = 'k0',
	 *                        'connector.properties.0.value' = 'v0',
	 *                        ...
	 *                      )";
	 *
	 *    String query = "INSERT INTO sinkTable SELECT * FROM sourceTable";
	 *
	 *    tEnv.sqlUpdate(sourceDDL);
	 *    tEnv.sqlUpdate(sinkDDL);
	 *    tEnv.sqlUpdate(query);
	 *    tEnv.execute("MyJob");
	 * </pre></blockquote>
	 * This code snippet creates a job to read data from Kafka source into a CSV sink.
	 *
	 * @param stmt The SQL statement to evaluate.
	 */
	void sqlUpdate(String stmt);

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
	@Experimental
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
	@Experimental
	void useDatabase(String databaseName);

	/**
	 * Returns the table config that defines the runtime behavior of the Table API.
	 */
	TableConfig getConfig();

	/**
	 * Triggers the program execution. The environment will execute all parts of
	 * the program.
	 *
	 * <p>The program execution will be logged and displayed with the provided name
	 *
	 * <p><b>NOTE:</b>It is highly advised to set all parameters in the {@link TableConfig}
	 * on the very beginning of the program. It is undefined what configurations values will
	 * be used for the execution if queries are mixed with config changes. It depends on
	 * the characteristic of the particular parameter. For some of them the value from the
	 * point in time of query construction (e.g. the currentCatalog) will be used. On the
	 * other hand some values might be evaluated according to the state from the time when
	 * this method is called (e.g. timeZone).
	 *
	 * @param jobName Desired name of the job
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 * @throws Exception which occurs during job execution.
	 */
	JobExecutionResult execute(String jobName) throws Exception;
}
