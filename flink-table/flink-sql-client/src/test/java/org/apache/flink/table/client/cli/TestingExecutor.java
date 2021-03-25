/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.client.cli;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.operations.Operation;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A customizable {@link Executor} for testing purposes. */
class TestingExecutor implements Executor {

    TestingExecutor() {}

    @Override
    public void start() throws SqlExecutionException {}

    @Override
    public String openSession(@Nullable String sessionId) throws SqlExecutionException {
        return sessionId;
    }

    @Override
    public void closeSession(String sessionId) throws SqlExecutionException {}

    @Override
    public Map<String, String> getSessionProperties(String sessionId) throws SqlExecutionException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public ReadableConfig getSessionConfig(String sessionId) throws SqlExecutionException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public void resetSessionProperties(String sessionId) throws SqlExecutionException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public void resetSessionProperty(String sessionId, String key) throws SqlExecutionException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public void setSessionProperty(String sessionId, String key, String value)
            throws SqlExecutionException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public TableResult executeOperation(String sessionId, Operation operation)
            throws SqlExecutionException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public Optional<Operation> parseStatement(String sessionId, String statement)
            throws SqlExecutionException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public List<String> completeStatement(String sessionId, String statement, int position) {
        throw new UnsupportedOperationException("Not implemented.");
    }
}
