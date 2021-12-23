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

package org.apache.flink.connector.jdbc.internal.connection;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;

/** create and save jdbcConnection int the queue. */
@NotThreadSafe
public class JdbcConnectionPoolManager implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionPoolManager.class);

    private static final long serialVersionUID = 1L;

    private LinkedBlockingQueue<JdbcConnectionEntry> connectionEntries;

    private final JdbcConnectionOptions jdbcOptions;

    private String[] keyNames;
    private String query;

    public JdbcConnectionPoolManager(
            JdbcConnectionOptions jdbcOptions,
            int connectionPoolSize,
            String[] keyNames,
            String query) {
        LOG.info("The max connectionPoolSize is {} ", connectionPoolSize);
        this.connectionEntries = new LinkedBlockingQueue<>(connectionPoolSize);
        this.jdbcOptions = jdbcOptions;
        this.keyNames = keyNames;
        this.query = query;
    }

    public JdbcConnectionEntry createConnectionEntry() throws SQLException, ClassNotFoundException {
        JdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(jdbcOptions);
        Connection dbConn = connectionProvider.getOrEstablishConnection();
        FieldNamedPreparedStatement statement =
                FieldNamedPreparedStatement.prepareStatement(dbConn, query, keyNames);
        return new JdbcConnectionEntry(connectionProvider, statement);
    }

    public void createAndAddConnectionEntry()
            throws SQLException, ClassNotFoundException, InterruptedException {
        connectionEntries.put(createConnectionEntry());
    }

    public FieldNamedPreparedStatement getStatement() throws InterruptedException {
        return getConnectionEntry().getStatement();
    }

    public JdbcConnectionEntry getConnectionEntry() throws InterruptedException {
        return connectionEntries.take();
    }

    public void returnConnectionEntry(JdbcConnectionEntry entry) throws InterruptedException {
        connectionEntries.put(entry);
    }

    public boolean isConnectionValid(JdbcConnectionEntry entry) throws SQLException {
        return entry.getConnectionProvider().isConnectionValid();
    }

    public JdbcConnectionEntry checkAndCreateConnection(JdbcConnectionEntry entry)
            throws SQLException, ClassNotFoundException, InterruptedException {
        if (!isConnectionValid(entry)) {
            entry.getStatement().close();
            entry.getConnectionProvider().closeConnection();
            entry = createConnectionEntry();
        }
        return entry;
    }

    public void closeAll() {
        connectionEntries.stream()
                .forEach(
                        entry -> {
                            entry.getConnectionProvider().closeConnection();
                        });
        connectionEntries.clear();
    }
}
