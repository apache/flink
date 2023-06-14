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

package org.apache.flink.table.gateway;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpoint;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.cli.SqlGatewayOptions;
import org.apache.flink.table.gateway.cli.SqlGatewayOptionsParser;
import org.apache.flink.table.gateway.service.SqlGatewayServiceImpl;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.session.SessionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/** Main entry point for the SQL Gateway. */
public class SqlGateway {

    private static final Logger LOG = LoggerFactory.getLogger(SqlGateway.class);

    private final Configuration defaultConfig;
    private final SessionManager sessionManager;
    private final List<SqlGatewayEndpoint> endpoints;
    private final CountDownLatch latch;

    public SqlGateway(Configuration defaultConfig, SessionManager sessionManager) {
        this.defaultConfig = defaultConfig;
        this.sessionManager = sessionManager;
        this.endpoints = new ArrayList<>();
        this.latch = new CountDownLatch(1);
    }

    public void start() throws Exception {
        sessionManager.start();

        SqlGatewayService sqlGatewayService = new SqlGatewayServiceImpl(sessionManager);
        try {
            endpoints.addAll(
                    SqlGatewayEndpointFactoryUtils.createSqlGatewayEndpoint(
                            sqlGatewayService, defaultConfig));
            for (SqlGatewayEndpoint endpoint : endpoints) {
                endpoint.start();
            }
        } catch (Throwable t) {
            LOG.error("Failed to start the endpoints.", t);
            throw new SqlGatewayException("Failed to start the endpoints.", t);
        }
    }

    public void stop() {
        for (SqlGatewayEndpoint endpoint : endpoints) {
            stopEndpointSilently(endpoint);
        }
        if (sessionManager != null) {
            sessionManager.stop();
        }
        latch.countDown();
    }

    public void waitUntilStop() throws Exception {
        latch.await();
    }

    public static void main(String[] args) {
        startSqlGateway(System.out, args);
    }

    @VisibleForTesting
    static void startSqlGateway(PrintStream stream, String[] args) {
        SqlGatewayOptions cliOptions = SqlGatewayOptionsParser.parseSqlGatewayOptions(args);

        if (cliOptions.isPrintHelp()) {
            SqlGatewayOptionsParser.printHelpSqlGateway(stream);
            return;
        }

        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(LOG, "SqlGateway", args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        DefaultContext defaultContext =
                DefaultContext.load(
                        ConfigurationUtils.createConfiguration(cliOptions.getDynamicConfigs()),
                        Collections.emptyList(),
                        true,
                        true);
        SqlGateway gateway =
                new SqlGateway(
                        defaultContext.getFlinkConfig(), SessionManager.create(defaultContext));
        try {
            Runtime.getRuntime().addShutdownHook(new ShutdownThread(gateway));
            gateway.start();
            gateway.waitUntilStop();
        } catch (Throwable t) {
            // User uses ctrl + c to cancel the Gateway manually
            if (t instanceof InterruptedException) {
                LOG.info("Caught " + t.getClass().getSimpleName() + ". Shutting down.");
                return;
            }
            // make space in terminal
            stream.println();
            stream.println();

            if (t instanceof SqlGatewayException) {
                // Exception that the gateway can not handle.
                throw (SqlGatewayException) t;
            } else {
                LOG.error(
                        "SqlGateway must stop. Unexpected exception. This is a bug. Please consider filing an issue.",
                        t);
                throw new SqlGatewayException(
                        "Unexpected exception. This is a bug. Please consider filing an issue.", t);
            }
        } finally {
            gateway.stop();
        }
    }

    private void stopEndpointSilently(SqlGatewayEndpoint endpoint) {
        try {
            endpoint.stop();
        } catch (Exception e) {
            LOG.error("Failed to stop the endpoint. Ignore.", e);
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class ShutdownThread extends Thread {

        private final SqlGateway gateway;

        public ShutdownThread(SqlGateway gateway) {
            this.gateway = gateway;
        }

        @Override
        public void run() {
            // Shutdown the gateway
            LOG.info("Shutting down the Flink SqlGateway...");

            try {
                gateway.stop();
            } catch (Exception e) {
                LOG.error("Failed to shut down the Flink SqlGateway: " + e.getMessage(), e);
                System.out.println("Failed to shut down the Flink SqlGateway: " + e.getMessage());
            }

            LOG.info("Flink SqlGateway has been shutdown.");
        }
    }
}
