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

package org.apache.flink.table.gateway.service.session;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.gateway.service.operation.OperationManager;

import java.io.Closeable;
import java.util.Map;

/**
 * Similar to HTTP Session, which could maintain user identity and store user-specific data during
 * multiple request/response interactions between a client and the gateway server.
 */
public class Session implements Closeable {

    private final SessionContext sessionContext;
    private volatile long lastAccessTime;

    public Session(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.lastAccessTime = System.currentTimeMillis();
    }

    public void touch() {
        this.lastAccessTime = System.currentTimeMillis();
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public SessionHandle getSessionHandle() {
        return sessionContext.getSessionId();
    }

    public Map<String, String> getSessionConfig() {
        return sessionContext.getSessionConf().toMap();
    }

    public EndpointVersion getEndpointVersion() {
        return sessionContext.getEndpointVersion();
    }

    public OperationManager getOperationManager() {
        return sessionContext.getOperationManager();
    }

    public OperationExecutor createExecutor() {
        return sessionContext.createOperationExecutor(new Configuration());
    }

    public OperationExecutor createExecutor(Configuration executionConfig) {
        return sessionContext.createOperationExecutor(executionConfig);
    }

    @Override
    public void close() {
        sessionContext.close();
    }
}
