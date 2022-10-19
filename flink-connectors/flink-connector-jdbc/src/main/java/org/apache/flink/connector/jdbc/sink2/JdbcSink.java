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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.sink2.statement.JdbcQueryStatement;

import java.io.IOException;

/**
 * Flink Sink to produce data into a jdbc database.
 *
 * @see JdbcSinkBuilder on how to construct a JdbcSink
 */
public class JdbcSink<IN> implements Sink<IN> {

    private final JdbcConnectionProvider connectionProvider;
    private final JdbcExecutionOptions executionOptions;
    private final JdbcQueryStatement<IN> queryStatement;

    public JdbcSink(
            JdbcConnectionProvider connectionProvider,
            JdbcExecutionOptions executionOptions,
            JdbcQueryStatement<IN> queryStatement) {
        this.connectionProvider = connectionProvider;
        this.executionOptions = executionOptions;
        this.queryStatement = queryStatement;
    }

    @Override
    public JdbcWriter<IN> createWriter(InitContext context) throws IOException {
        return new JdbcWriter<>(
                this.connectionProvider, this.executionOptions, this.queryStatement);
    }

    public static <IN> JdbcSinkBuilder<IN> builder() {
        return new JdbcSinkBuilder<>();
    }
}
