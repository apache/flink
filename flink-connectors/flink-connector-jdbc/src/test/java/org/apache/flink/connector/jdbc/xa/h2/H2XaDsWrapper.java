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

import javax.sql.XAConnection;
import javax.sql.XADataSource;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * Wraps H2 {@link XADataSource} to eventually wrap it's {@link javax.transaction.xa.XAResource
 * XAResources} into {@link H2XaResourceWrapper}.
 */
public class H2XaDsWrapper implements XADataSource {

    private final XADataSource wrapped;

    H2XaDsWrapper(XADataSource wrapped) {
        this.wrapped = wrapped;
    }

    private XAConnection wrapXAConnection(XAConnection wc) {
        return new H2XaConnectionWrapper(wc);
    }

    @Override
    public XAConnection getXAConnection() throws SQLException {
        return wrapXAConnection(wrapped.getXAConnection());
    }

    @Override
    public XAConnection getXAConnection(String user, String password) throws SQLException {
        return wrapXAConnection(wrapped.getXAConnection(user, password));
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return wrapped.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        wrapped.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        wrapped.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return wrapped.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return wrapped.getParentLogger();
    }
}
