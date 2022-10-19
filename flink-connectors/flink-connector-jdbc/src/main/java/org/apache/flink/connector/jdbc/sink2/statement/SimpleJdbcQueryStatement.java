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

package org.apache.flink.connector.jdbc.sink2.statement;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/** A simple implementation for {@link JdbcQueryStatement}. */
public class SimpleJdbcQueryStatement<IN> implements JdbcQueryStatement<IN> {
    private final String query;
    private final JdbcStatementBuilder<IN> statement;

    public SimpleJdbcQueryStatement(String query, JdbcStatementBuilder<IN> statement) {
        this.query = query;
        this.statement = statement;
    }

    @Override
    public String query() {
        return query;
    }

    @Override
    public void map(PreparedStatement ps, IN data) throws SQLException {
        statement.accept(ps, data);
    }
}
