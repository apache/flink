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

package org.apache.flink.connector.jdbc.xa.h2;

import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import java.sql.Connection;
import java.sql.SQLException;

class H2XaConnectionWrapper implements XAConnection {

    private final XAConnection wrapped;

    H2XaConnectionWrapper(XAConnection wrapped) {
        this.wrapped = wrapped;
    }

    private XAResource wrapResource(XAResource wr) {
        return new H2XaResourceWrapper(wr);
    }

    @Override
    public XAResource getXAResource() throws SQLException {
        return wrapResource(wrapped.getXAResource());
    }

    @Override
    public Connection getConnection() throws SQLException {
        return wrapped.getConnection();
    }

    @Override
    public void close() throws SQLException {
        wrapped.close();
    }

    @Override
    public void addConnectionEventListener(ConnectionEventListener listener) {
        wrapped.addConnectionEventListener(listener);
    }

    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        wrapped.removeConnectionEventListener(listener);
    }

    @Override
    public void addStatementEventListener(StatementEventListener listener) {
        wrapped.addStatementEventListener(listener);
    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {
        wrapped.removeStatementEventListener(listener);
    }
}
