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
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A gateway for communicating with Flink and other external systems. */
public interface Executor {

    /** Starts the executor and ensures that its is ready for commands to be executed. */
    void start() throws SqlExecutionException;

    /**
     * Open a new session by using the given session id.
     *
     * @param sessionId session identifier.
     * @throws SqlExecutionException if any error happen
     */
    void openSession(@Nullable String sessionId) throws SqlExecutionException;

    /**
     * Close the resources of session for given session id.
     *
     * @throws SqlExecutionException if any error happen
     */
    void closeSession() throws SqlExecutionException;

    /**
     * Returns a copy of {@link Map} of all session configurations that are defined by the executor
     * and the session.
     *
     * <p>Both this method and {@link #getSessionConfig()} return the same configuration set, but
     * different return type.
     */
    Map<String, String> getSessionConfigMap() throws SqlExecutionException;

    /**
     * Returns a {@link ReadableConfig} of all session configurations that are defined by the
     * executor and the session.
     *
     * <p>Both this method and {@link #getSessionConfigMap()} return the same configuration set, but
     * different return type.
     */
    ReadableConfig getSessionConfig() throws SqlExecutionException;

    /**
     * Reset all the properties for the given session identifier.
     *
     * @throws SqlExecutionException if any error happen.
     */
    void resetSessionProperties() throws SqlExecutionException;

    /**
     * Reset given key's the session property for default value, if key is not defined in config
     * file, then remove it.
     *
     * @param key of need to reset the session property
     * @throws SqlExecutionException if any error happen.
     */
    void resetSessionProperty(String key) throws SqlExecutionException;

    /**
     * Set given key's session property to the specific value.
     *
     * @param key of the session property
     * @param value of the session property
     * @throws SqlExecutionException if any error happen.
     */
    void setSessionProperty(String key, String value) throws SqlExecutionException;

    /** Parse a SQL statement to {@link Operation}. */
    Operation parseStatement(String statement) throws SqlExecutionException;

    /** Returns a list of completion hints for the given statement at the given position. */
    List<String> completeStatement(String statement, int position);

    /** Executes an operation, and return {@link TableResult} as execution result. */
    TableResultInternal executeOperation(Operation operation) throws SqlExecutionException;

    /** Executes modify operations, and return {@link TableResult} as execution result. */
    TableResultInternal executeModifyOperations(List<ModifyOperation> operations)
            throws SqlExecutionException;

    /** Submits a Flink SQL query job (detached) and returns the result descriptor. */
    ResultDescriptor executeQuery(QueryOperation query) throws SqlExecutionException;

    /** Asks for the next changelog results (non-blocking). */
    TypedResult<List<RowData>> retrieveResultChanges(String resultId) throws SqlExecutionException;

    /**
     * Creates an immutable result snapshot of the running Flink job. Throws an exception if no
     * Flink job can be found. Returns the number of pages.
     */
    TypedResult<Integer> snapshotResult(String resultId, int pageSize) throws SqlExecutionException;

    /**
     * Returns the rows that are part of the current page or throws an exception if the snapshot has
     * been expired.
     */
    List<RowData> retrieveResultPage(String resultId, int page) throws SqlExecutionException;

    /**
     * Cancels a table program and stops the result retrieval. Blocking until cancellation command
     * has been sent to cluster.
     */
    void cancelQuery(String resultId) throws SqlExecutionException;

    /** Remove the JAR resource from the classloader with specified session. */
    void removeJar(String jarPath);

    /** Stops a job in the specified session. */
    Optional<String> stopJob(String jobId, boolean isWithSavepoint, boolean isWithDrain)
            throws SqlExecutionException;
}
