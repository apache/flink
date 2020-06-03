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

package org.apache.flink.table.client.gateway;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/**
 * A gateway for communicating with Flink and other external systems.
 */
public interface Executor {

	/**
	 * Starts the executor and ensures that its is ready for commands to be executed.
	 */
	void start() throws SqlExecutionException;

	/**
	 * Open a new session by using the given {@link SessionContext}.
	 *
	 * @param session context to create new session.
	 * @return session identifier to track the session.
	 * @throws SqlExecutionException if any error happen
	 */
	String openSession(SessionContext session) throws SqlExecutionException;

	/**
	 * Close the resources of session for given session id.
	 *
	 * @param sessionId session identifier
	 * @throws SqlExecutionException if any error happen
	 */
	void closeSession(String sessionId) throws SqlExecutionException;

	/**
	 * Lists all session properties that are defined by the executor and the session.
	 */
	Map<String, String> getSessionProperties(String sessionId) throws SqlExecutionException;

	/**
	 * Reset all the properties for the given session identifier.
	 *
	 * @param sessionId to identifier the session
	 * @throws SqlExecutionException if any error happen.
	 */
	void resetSessionProperties(String sessionId) throws SqlExecutionException;

	/**
	 * Set given key's session property to the specific value.
	 *
	 * @param key   of the session property
	 * @param value of the session property
	 * @throws SqlExecutionException if any error happen.
	 */
	void setSessionProperty(String sessionId, String key, String value) throws SqlExecutionException;

	/**
	 * Lists all registered catalogs.
	 */
	List<String> listCatalogs(String sessionid) throws SqlExecutionException;

	/**
	 * Lists all databases in the current catalog.
	 */
	List<String> listDatabases(String sessionId) throws SqlExecutionException;

	/**
	 * Create a table with a DDL statement.
	 */
	void createTable(String sessionId, String ddl) throws SqlExecutionException;

	/**
	 * Drop a table with a DDL statement.
	 */
	void dropTable(String sessionId, String ddl) throws SqlExecutionException;

	/**
	 * Lists all tables in the current database of the current catalog.
	 */
	List<String> listTables(String sessionId) throws SqlExecutionException;

	/**
	 * Lists all user-defined functions known to the executor.
	 */
	List<String> listUserDefinedFunctions(String sessionId) throws SqlExecutionException;

	/**
	 * Executes a SQL statement.
	 */
	TableResult executeSql(String sessionId, String statement) throws SqlExecutionException;

	/**
	 * Lists all functions known to the executor.
	 */
	List<String> listFunctions(String sessionId) throws SqlExecutionException;

	/**
	 * Lists all modules known to the executor in their loaded order.
	 */
	List<String> listModules(String sessionId) throws SqlExecutionException;

	/**
	 * Sets a catalog with given name as the current catalog.
	 */
	void useCatalog(String sessionId, String catalogName) throws SqlExecutionException;

	/**
	 * Sets a database with given name as the current database of the current catalog.
	 */
	void useDatabase(String sessionId, String databaseName) throws SqlExecutionException;

	/**
	 * Returns the schema of a table. Throws an exception if the table could not be found. The
	 * schema might contain time attribute types for helping the user during debugging a query.
	 */
	TableSchema getTableSchema(String sessionId, String name) throws SqlExecutionException;

	/**
	 * Returns a sql parser instance.
	 */
	Parser getSqlParser(String sessionId);

	/**
	 * Returns a list of completion hints for the given statement at the given position.
	 */
	List<String> completeStatement(String sessionId, String statement, int position);

	/**
	 * Submits a Flink SQL query job (detached) and returns the result descriptor.
	 */
	ResultDescriptor executeQuery(String sessionId, String query) throws SqlExecutionException;

	/**
	 * Asks for the next changelog results (non-blocking).
	 */
	TypedResult<List<Tuple2<Boolean, Row>>> retrieveResultChanges(String sessionId, String resultId) throws SqlExecutionException;

	/**
	 * Creates an immutable result snapshot of the running Flink job. Throws an exception if no Flink job can be found.
	 * Returns the number of pages.
	 */
	TypedResult<Integer> snapshotResult(String sessionId, String resultId, int pageSize) throws SqlExecutionException;

	/**
	 * Returns the rows that are part of the current page or throws an exception if the snapshot has been expired.
	 */
	List<Row> retrieveResultPage(String resultId, int page) throws SqlExecutionException;

	/**
	 * Cancels a table program and stops the result retrieval. Blocking until cancellation command has
	 * been sent to cluster.
	 */
	void cancelQuery(String sessionId, String resultId) throws SqlExecutionException;

	/**
	 * Submits a Flink SQL update statement such as INSERT INTO.
	 *
	 * @param sessionId to identify the user session.
	 * @param statement SQL update statement (currently only INSERT INTO is supported)
	 * @return information about the target of the submitted Flink job
	 */
	ProgramTargetDescriptor executeUpdate(String sessionId, String statement) throws SqlExecutionException;
}
