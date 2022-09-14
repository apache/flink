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

package org.apache.flink.connector.jdbc.internal.executor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;

/** Executes the given JDBC statement in batch for the accumulated records. */
@Internal
public interface JdbcBatchStatementExecutor<T> {

    /** Create statements from connection. */
    void prepareStatements(Connection connection) throws SQLException;

    void addToBatch(T record) throws SQLException;

    /** Submits a batch of commands to the database for execution. */
    void executeBatch() throws SQLException;

    /** Close JDBC related statements. */
    void closeStatements() throws SQLException;

    static <T, K> JdbcBatchStatementExecutor<T> keyed(
            String sql, Function<T, K> keyExtractor, JdbcStatementBuilder<K> statementBuilder) {
        return new KeyedBatchStatementExecutor<>(sql, keyExtractor, statementBuilder);
    }

    static <T, V> JdbcBatchStatementExecutor<T> simple(
            String sql, JdbcStatementBuilder<V> paramSetter, Function<T, V> valueTransformer) {
        return new SimpleBatchStatementExecutor<>(sql, paramSetter, valueTransformer);
    }
}
