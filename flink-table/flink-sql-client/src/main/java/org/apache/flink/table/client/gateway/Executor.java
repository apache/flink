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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
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

    /**
     * Returns a copy of {@link Map} of all session configurations that are defined by the executor
     * and the session.
     *
     * <p>Both this method and {@link #getSessionConfig(String)} return the same configuration set,
     * but different return type.
     */
    Map<String, String> getSessionConfigMap(String sessionId) throws SqlExecutionException;

    /**
     * Returns a {@link ReadableConfig} of all session configurations that are defined by the
     * executor and the session.
     *
     * <p>Both this method and {@link #getSessionConfigMap(String)} return the same configuration
     * set, but different return type.
     */
    ReadableConfig getSessionConfig(String sessionId) throws SqlExecutionException;

    /**
     * Reset all the properties for the given session identifier.
     *
     * @param sessionId to identifier the session
     * @throws SqlExecutionException if any error happen.
     */
    void resetSessionProperties(String sessionId) throws SqlExecutionException;

    /**
     * Reset given key's the session property for default value, if key is not defined in config
     * file, then remove it.
     *
     * @param sessionId to identifier the session
     * @param key of need to reset the session property
     * @throws SqlExecutionException if any error happen.
     */
    void resetSessionProperty(String sessionId, String key) throws SqlExecutionException;

    /**
     * Set given key's session property to the specific value.
     *
     * @param key of the session property
     * @param value of the session property
     * @throws SqlExecutionException if any error happen.
     */
    void setSessionProperty(String sessionId, String key, String value)
            throws SqlExecutionException;

    /** Parse a SQL statement to {@link Operation}. */
    Operation parseStatement(String sessionId, String statement) throws SqlExecutionException;

    /** Returns a list of completion hints for the given statement at the given position. */
    List<String> completeStatement(String sessionId, String statement, int position);

    /** Executes an operation, and return {@link TableResult} as execution result. */
    TableResult executeOperation(String sessionId, Operation operation)
            throws SqlExecutionException;

    /** Executes modify operations, and return {@link TableResult} as execution result. */
    TableResult executeModifyOperations(String sessionId, List<ModifyOperation> operations)
            throws SqlExecutionException;

    /** Submits a Flink SQL query job (detached) and returns the result descriptor. */
    ResultDescriptor executeQuery(String sessionId, QueryOperation query)
            throws SqlExecutionException;

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
