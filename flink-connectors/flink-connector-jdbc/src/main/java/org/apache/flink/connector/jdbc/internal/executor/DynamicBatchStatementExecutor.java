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

package org.apache.flink.connector.jdbc.internal.executor;

import org.apache.flink.connector.jdbc.JdbcKeyCreator;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.JdbcStatementFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;

/**
 * A {@link JdbcBatchStatementExecutor} that creates and executes supplied statement for given the
 * records
 */
class DynamicBatchStatementExecutor<T> implements JdbcBatchStatementExecutor<T>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicBatchStatementExecutor.class);

    private final JdbcStatementBuilder<T> parameterSetter;
    private final transient JdbcStatementFactory<T> sqlFactory;
    private final transient Function<T, String> keyExtractor;

    private final List<T> batch;
    private transient Map<String, PreparedStatement> statementPool; // LRU cache?
    private transient Connection connection;

    DynamicBatchStatementExecutor(
            JdbcStatementFactory<T> sqlFactory,
            JdbcStatementBuilder<T> parameterSetter,
            JdbcKeyCreator<T> keyExtractor) {
        this.sqlFactory = sqlFactory;
        this.parameterSetter = parameterSetter;
        this.keyExtractor = keyExtractor;
        this.batch = new ArrayList<>();
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.connection = connection;
        this.statementPool = new HashMap<>();
    }

    @Override
    public void addToBatch(T record) {
        batch.add(record);
    }

    @Override
    public void executeBatch() throws SQLException {
        Set<PreparedStatement> usedStatements = new HashSet<>();
        if (!batch.isEmpty()) {
            LOG.debug("Executing statements for {} rows", batch.size());
            for (T r : batch) {
                String key = keyExtractor.apply(r);
                if (!statementPool.containsKey(key)) {
                    prepareStatement(r, key);
                }

                PreparedStatement stmt = statementPool.get(key);
                parameterSetter.accept(stmt, r);
                stmt.addBatch();
                usedStatements.add(stmt);
            }

            for (PreparedStatement stmt : usedStatements) {
                stmt.executeBatch();
            }
            batch.clear();
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (statementPool != null) {
            for (PreparedStatement stmt : statementPool.values()) {
                stmt.close();
            }
            statementPool = null;
        }
    }

    private void prepareStatement(T value, String key) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sqlFactory.apply(value));
        statementPool.put(key, stmt);
    }
}
