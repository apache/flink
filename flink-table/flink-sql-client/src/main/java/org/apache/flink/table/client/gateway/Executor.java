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

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.List;

/** A gateway for communicating with Flink and other external systems. */
public interface Executor extends Closeable {

    /**
     * Open a new session by using the given session id.
     *
     * @param sessionId session identifier
     */
    void openSession(@Nullable String sessionId);

    /** Close the resources of session for given session id. */
    void closeSession();

    /**
     * Configures session with statement.
     *
     * @param statement to initialize the session
     */
    void configureSession(String statement);

    /** Get the configuration of the session. */
    ReadableConfig getSessionConfig();

    /**
     * Execute statement.
     *
     * @param statement to execute
     * @return Iterable results of the statement
     */
    ClientResult executeStatement(String statement);

    /**
     * Returns a list of completion hints for the given statement at the given position.
     *
     * @param statement Partial or slightly incorrect SQL statement
     * @param position cursor position
     * @return completion hints that fit at the current cursor position
     */
    List<String> completeStatement(String statement, int position);
}
