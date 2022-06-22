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

package org.apache.flink.table.gateway.service;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.session.Session;
import org.apache.flink.table.gateway.service.session.SessionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** The implementation of the {@link SqlGatewayService} interface. */
public class SqlGatewayServiceImpl implements SqlGatewayService {

    private static final Logger LOG = LoggerFactory.getLogger(SqlGatewayServiceImpl.class);

    private final SessionManager sessionManager;

    public SqlGatewayServiceImpl(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @Override
    public SessionHandle openSession(SessionEnvironment environment) throws SqlGatewayException {
        try {
            return sessionManager.openSession(environment).getSessionHandle();
        } catch (Throwable e) {
            LOG.error("Failed to openSession.", e);
            throw new SqlGatewayException("Failed to openSession.", e);
        }
    }

    @Override
    public void closeSession(SessionHandle sessionHandle) throws SqlGatewayException {
        try {
            sessionManager.closeSession(sessionHandle);
        } catch (Throwable e) {
            LOG.error("Failed to closeSession.", e);
            throw new SqlGatewayException("Failed to closeSession.", e);
        }
    }

    @Override
    public Map<String, String> getSessionConfig(SessionHandle sessionHandle)
            throws SqlGatewayException {
        try {
            return getSession(sessionHandle).getSessionConfig();
        } catch (Throwable e) {
            LOG.error("Failed to getSessionConfig.", e);
            throw new SqlGatewayException("Failed to getSessionConfig.", e);
        }
    }

    @VisibleForTesting
    Session getSession(SessionHandle sessionHandle) {
        return sessionManager.getSession(sessionHandle);
    }
}
