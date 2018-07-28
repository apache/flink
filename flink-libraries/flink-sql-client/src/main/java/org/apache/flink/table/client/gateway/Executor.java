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
import org.apache.flink.table.api.TableSchema;
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
	 * Lists all session properties that are defined by the executor and the session.
	 */
	Map<String, String> getSessionProperties(SessionContext session) throws SqlExecutionException;

	/**
	 * Lists all tables known to the executor.
	 */
	List<String> listTables(SessionContext session) throws SqlExecutionException;

	/**
	 * Lists all user-defined functions known to the executor.
	 */
	List<String> listUserDefinedFunctions(SessionContext session) throws SqlExecutionException;

	/**
	 * Returns the schema of a table. Throws an exception if the table could not be found. The
	 * schema might contain time attribute types for helping the user during debugging a query.
	 */
	TableSchema getTableSchema(SessionContext session, String name) throws SqlExecutionException;

	/**
	 * Returns a string-based explanation about AST and execution plan of the given statement.
	 */
	String explainStatement(SessionContext session, String statement) throws SqlExecutionException;

	/**
	 * Submits a Flink SQL query job (detached) and returns the result descriptor.
	 */
	ResultDescriptor executeQuery(SessionContext session, String query) throws SqlExecutionException;

	/**
	 * Asks for the next changelog results (non-blocking).
	 */
	TypedResult<List<Tuple2<Boolean, Row>>> retrieveResultChanges(SessionContext session, String resultId) throws SqlExecutionException;

	/**
	 * Creates an immutable result snapshot of the running Flink job. Throws an exception if no Flink job can be found.
	 * Returns the number of pages.
	 */
	TypedResult<Integer> snapshotResult(SessionContext session, String resultId, int pageSize) throws SqlExecutionException;

	/**
	 * Returns the rows that are part of the current page or throws an exception if the snapshot has been expired.
	 */
	List<Row> retrieveResultPage(String resultId, int page) throws SqlExecutionException;

	/**
	 * Cancels a table program and stops the result retrieval. Blocking until cancellation command has
	 * been sent to cluster.
	 */
	void cancelQuery(SessionContext session, String resultId) throws SqlExecutionException;

	/**
	 * Submits a Flink SQL update statement such as INSERT INTO.
	 *
	 * @param session context in with the statement is executed
	 * @param statement SQL update statement (currently only INSERT INTO is supported)
	 * @return information about the target of the submitted Flink job
	 */
	ProgramTargetDescriptor executeUpdate(SessionContext session, String statement) throws SqlExecutionException;

	/**
	 * Stops the executor.
	 */
	void stop(SessionContext session);
}
