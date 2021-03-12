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

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** A gateway for communicating with Flink and other external systems. */
public interface Executor {

    /** Starts the executor and ensures that its is ready for commands to be executed. */
    void start() throws SqlExecutionException;

    /**
     * Open a new session by using the given session id.
     *
     * @param sessionId session identifier.
     * @return used session identifier to track the session.
     * @throws SqlExecutionException if any error happen
     */
    String openSession(@Nullable String sessionId) throws SqlExecutionException;

    /**
     * Close the resources of session for given session id.
     *
     * @param sessionId session identifier
     * @throws SqlExecutionException if any error happen
     */
    void closeSession(String sessionId) throws SqlExecutionException;

    /** Lists all session properties that are defined by the executor and the session. */
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
     * @param key of the session property
     * @param value of the session property
     * @throws SqlExecutionException if any error happen.
     */
    void setSessionProperty(String sessionId, String key, String value)
            throws SqlExecutionException;

    /** Executes a SQL statement, and return {@link TableResult} as execution result. */
    TableResult executeSql(String sessionId, String statement) throws SqlExecutionException;

    /** Returns a sql parser instance. */
    Parser getSqlParser(String sessionId);

    /** Returns a list of completion hints for the given statement at the given position. */
    List<String> completeStatement(String sessionId, String statement, int position);

    /** Submits a Flink SQL query job (detached) and returns the result descriptor. */
    ResultDescriptor executeQuery(String sessionId, String query) throws SqlExecutionException;

    /** Asks for the next changelog results (non-blocking). */
    TypedResult<List<Row>> retrieveResultChanges(String sessionId, String resultId)
            throws SqlExecutionException;

    /**
     * Creates an immutable result snapshot of the running Flink job. Throws an exception if no
     * Flink job can be found. Returns the number of pages.
     */
    TypedResult<Integer> snapshotResult(String sessionId, String resultId, int pageSize)
            throws SqlExecutionException;

    /**
     * Returns the rows that are part of the current page or throws an exception if the snapshot has
     * been expired.
     */
    List<Row> retrieveResultPage(String resultId, int page) throws SqlExecutionException;

    /**
     * Cancels a table program and stops the result retrieval. Blocking until cancellation command
     * has been sent to cluster.
     */
    void cancelQuery(String sessionId, String resultId) throws SqlExecutionException;
}
