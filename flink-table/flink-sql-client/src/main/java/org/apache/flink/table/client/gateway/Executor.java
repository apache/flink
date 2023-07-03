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
import org.apache.flink.table.gateway.rest.util.RowFormat;
import org.apache.flink.table.gateway.service.context.DefaultContext;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Map;

/** A gateway for communicating with Flink and other external systems. */
public interface Executor extends Closeable {

    /** Create an {@link Executor} to execute commands. */
    static Executor create(
            DefaultContext defaultContext, InetSocketAddress address, String sessionId) {
        return new ExecutorImpl(defaultContext, address, sessionId);
    }

    static Executor create(
            DefaultContext defaultContext,
            InetSocketAddress address,
            String sessionId,
            RowFormat rowFormat) {
        return new ExecutorImpl(defaultContext, address, sessionId, rowFormat);
    }

    static Executor create(DefaultContext defaultContext, URL address, String sessionId) {
        return new ExecutorImpl(defaultContext, address, sessionId);
    }

    /**
     * Configures session with statement.
     *
     * @param statement to initialize the session
     */
    void configureSession(String statement);

    /**
     * Get the configuration of the session.
     *
     * @return the session configuration.
     */
    ReadableConfig getSessionConfig();

    /**
     * Get the map configuration of the session.
     *
     * @return the map session configuration.
     */
    Map<String, String> getSessionConfigMap();

    /**
     * Execute statement.
     *
     * @param statement to execute
     * @return Iterable results of the statement
     */
    StatementResult executeStatement(String statement);

    /**
     * Returns a list of completion hints for the given statement at the given position.
     *
     * @param statement Partial or slightly incorrect SQL statement
     * @param position cursor position
     * @return completion hints that fit at the current cursor position
     */
    List<String> completeStatement(String statement, int position);

    /** Close the {@link Executor} and process all exceptions. */
    void close();
}
