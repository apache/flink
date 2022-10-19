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

package org.apache.flink.connector.jdbc.sink2;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.sink2.statement.JdbcQueryStatement;
import org.apache.flink.connector.jdbc.sink2.statement.SimpleJdbcQueryStatement;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Builder to construct {@link JdbcSink}. */
public class JdbcSinkBuilder<IN> {

    private JdbcConnectionProvider connectionProvider;
    private JdbcExecutionOptions executionOptions;
    private JdbcQueryStatement<IN> queryStatement;

    protected JdbcSinkBuilder() {
        executionOptions = JdbcExecutionOptions.defaults();
    }

    public JdbcSinkBuilder<IN> withExecutionOptions(JdbcExecutionOptions executionOptions) {
        this.executionOptions = checkNotNull(executionOptions, "executionOptions cannot be empty");
        return this;
    }

    public JdbcSinkBuilder<IN> withConnectionProvider(JdbcConnectionProvider connectionProvider) {
        this.connectionProvider =
                checkNotNull(connectionProvider, "connectionProvider cannot be empty");
        return this;
    }

    public JdbcSinkBuilder<IN> withConnectionProvider(JdbcConnectionOptions connectionOptions) {
        return withConnectionProvider(new SimpleJdbcConnectionProvider(connectionOptions));
    }

    public JdbcSinkBuilder<IN> withQueryStatement(JdbcQueryStatement<IN> queryStatement) {
        this.queryStatement = queryStatement;
        return this;
    }

    public JdbcSinkBuilder<IN> withQueryStatement(
            String query, JdbcStatementBuilder<IN> statement) {
        this.queryStatement = new SimpleJdbcQueryStatement<>(query, statement);
        return this;
    }

    public JdbcSink<IN> build() {
        checkNotNull(connectionProvider, "connectionProvider cannot be empty");
        checkNotNull(executionOptions, "executionOptions cannot be empty");
        checkNotNull(queryStatement, "queryStatement cannot be empty");

        return new JdbcSink<>(connectionProvider, executionOptions, queryStatement);
    }
}
