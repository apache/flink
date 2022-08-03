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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.api.utils.ThreadUtils;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.context.SessionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_CHECK_INTERVAL;
import static org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_IDLE_TIMEOUT;
import static org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_MAX_NUM;
import static org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions.SQL_GATEWAY_WORKER_KEEPALIVE_TIME;
import static org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions.SQL_GATEWAY_WORKER_THREADS_MAX;
import static org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions.SQL_GATEWAY_WORKER_THREADS_MIN;

/** Manage the lifecycle of the {@code Session}. */
public class SessionManager {

    private static final Logger LOG = LoggerFactory.getLogger(SessionManager.class);
    private static final String OPERATION_POOL_NAME = "sql-gateway-operation-pool";

    private final DefaultContext defaultContext;

    private final long idleTimeout;
    private final long checkInterval;
    private final int maxSessionCount;

    private final Map<SessionHandle, Session> sessions;

    private ExecutorService operationExecutorService;
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture<?> timeoutCheckerFuture;

    public SessionManager(DefaultContext defaultContext) {
        this.defaultContext = defaultContext;
        ReadableConfig conf = defaultContext.getFlinkConfig();
        this.idleTimeout = conf.get(SQL_GATEWAY_SESSION_IDLE_TIMEOUT).toMillis();
        this.checkInterval = conf.get(SQL_GATEWAY_SESSION_CHECK_INTERVAL).toMillis();
        this.maxSessionCount = conf.get(SQL_GATEWAY_SESSION_MAX_NUM);
        this.sessions = new ConcurrentHashMap<>();
    }

    public void start() {
        if (checkInterval > 0 && idleTimeout > 0) {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            timeoutCheckerFuture =
                    scheduledExecutorService.scheduleAtFixedRate(
                            () -> {
                                LOG.debug(
                                        "Start to cleanup expired sessions, current session count: {}",
                                        sessions.size());
                                for (Map.Entry<SessionHandle, Session> entry :
                                        sessions.entrySet()) {
                                    SessionHandle sessionId = entry.getKey();
                                    Session session = entry.getValue();
                                    if (isSessionExpired(session)) {
                                        LOG.info("Session {} is expired, closing it...", sessionId);
                                        closeSession(session);
                                    }
                                }
                                LOG.debug(
                                        "Removing expired session finished, current session count: {}",
                                        sessions.size());
                            },
                            checkInterval,
                            checkInterval,
                            TimeUnit.MILLISECONDS);
        }

        ReadableConfig conf = defaultContext.getFlinkConfig();
        operationExecutorService =
                ThreadUtils.newThreadPool(
                        conf.get(SQL_GATEWAY_WORKER_THREADS_MIN),
                        conf.get(SQL_GATEWAY_WORKER_THREADS_MAX),
                        conf.get(SQL_GATEWAY_WORKER_KEEPALIVE_TIME).toMillis(),
                        OPERATION_POOL_NAME);
    }

    public void stop() {
        if (scheduledExecutorService != null) {
            timeoutCheckerFuture.cancel(true);
            scheduledExecutorService.shutdown();
        }
        if (operationExecutorService != null) {
            operationExecutorService.shutdown();
        }
        LOG.info("SessionManager is stopped.");
    }

    public Session getSession(SessionHandle sessionHandle) throws SqlGatewayException {
        Session session = sessions.get(sessionHandle);
        if (session == null) {
            String msg = String.format("Session '%s' does not exist.", sessionHandle);
            LOG.warn(msg);
            throw new SqlGatewayException(msg);
        }
        session.touch();
        return session;
    }

    /**
     * Register the session into the {@link SessionManager}.
     *
     * <p>Use synchronized to keep the checkSessionCount and build the Session are atomic.
     */
    public synchronized Session openSession(SessionEnvironment environment)
            throws SqlGatewayException {
        // check session limit
        checkSessionCount();

        Session session = null;
        SessionHandle sessionId = null;
        do {
            sessionId = SessionHandle.create();
        } while (sessions.containsKey(sessionId));

        SessionContext sessionContext =
                SessionContext.create(
                        defaultContext,
                        sessionId,
                        environment.getSessionEndpointVersion(),
                        Configuration.fromMap(environment.getSessionConfig()),
                        operationExecutorService);

        environment.getRegisteredCatalogs().forEach(sessionContext::registerCatalog);
        environment.getRegisteredModules().forEach(sessionContext::registerModuleAtHead);
        environment.getDefaultCatalog().ifPresent(sessionContext::setCurrentCatalog);
        environment.getDefaultDatabase().ifPresent(sessionContext::setCurrentDatabase);

        session = new Session(sessionContext);
        sessions.put(sessionId, session);

        LOG.info(
                "Session {} is opened, and the number of current sessions is {}.",
                session.getSessionHandle(),
                sessions.size());

        return session;
    }

    public void closeSession(SessionHandle sessionId) throws SqlGatewayException {
        Session session = getSession(sessionId);
        closeSession(session);
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private void checkSessionCount() throws SqlGatewayException {
        if (maxSessionCount <= 0) {
            return;
        }
        if (sessions.size() >= maxSessionCount) {
            String msg =
                    String.format(
                            "Failed to create session, the count of active sessions exceeds the max count: %s",
                            maxSessionCount);
            LOG.warn(msg);
            throw new SqlGatewayException(msg);
        }
    }

    private boolean isSessionExpired(Session session) {
        if (idleTimeout > 0) {
            return (System.currentTimeMillis() - session.getLastAccessTime()) > idleTimeout;
        } else {
            return false;
        }
    }

    private void closeSession(Session session) {
        SessionHandle sessionId = session.getSessionHandle();
        sessions.remove(sessionId);
        session.close();
        LOG.info("Session: {} is closed.", sessionId);
    }

    @VisibleForTesting
    boolean isSessionAlive(SessionHandle sessionId) {
        return sessions.containsKey(sessionId);
    }

    @VisibleForTesting
    public int currentSessionCount() {
        return sessions.size();
    }

    @VisibleForTesting
    public int getOperationCount(SessionHandle sessionHandle) {
        return getSession(sessionHandle).getOperationManager().getOperationCount();
    }
}
