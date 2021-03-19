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

package org.apache.flink.table.client;

import org.apache.flink.table.client.cli.CliClient;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.local.LocalContextUtils;
import org.apache.flink.table.client.gateway.local.LocalExecutor;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * SQL Client for submitting SQL statements. The client can be executed in two modes: a gateway and
 * embedded mode.
 *
 * <p>- In embedded mode, the SQL CLI is tightly coupled with the executor in a common process. This
 * allows for submitting jobs without having to start an additional component.
 *
 * <p>- In future versions: In gateway mode, the SQL CLI client connects to the REST API of the
 * gateway and allows for managing queries via console.
 *
 * <p>For debugging in an IDE you can execute the main method of this class using: "--defaults
 * /path/to/sql-client-defaults.yaml --jar /path/to/target/flink-sql-client-*.jar"
 *
 * <p>Make sure that the FLINK_CONF_DIR environment variable is set.
 */
public class SqlClient {

    private static final Logger LOG = LoggerFactory.getLogger(SqlClient.class);

    private final boolean isEmbedded;
    private final CliOptions options;

    public static final String MODE_EMBEDDED = "embedded";
    public static final String MODE_GATEWAY = "gateway";

    public SqlClient(boolean isEmbedded, CliOptions options) {
        this.isEmbedded = isEmbedded;
        this.options = options;
    }

    private void start() {
        if (isEmbedded) {
            // create local executor with default environment

            DefaultContext defaultContext = LocalContextUtils.buildDefaultContext(options);
            final Executor executor = new LocalExecutor(defaultContext);
            executor.start();

            // Open an new session
            String sessionId = executor.openSession(options.getSessionId());
            try {
                // add shutdown hook
                Runtime.getRuntime()
                        .addShutdownHook(new EmbeddedShutdownThread(sessionId, executor));

                // do the actual work
                openCli(sessionId, executor);
            } finally {
                executor.closeSession(sessionId);
            }
        } else {
            throw new SqlClientException("Gateway mode is not supported yet.");
        }
    }

    /**
     * Opens the CLI client for executing SQL statements.
     *
     * @param sessionId session identifier for the current client.
     * @param executor executor
     */
    private void openCli(String sessionId, Executor executor) {
        Path historyFilePath;
        if (options.getHistoryFilePath() != null) {
            historyFilePath = Paths.get(options.getHistoryFilePath());
        } else {
            historyFilePath =
                    Paths.get(
                            System.getProperty("user.home"),
                            SystemUtils.IS_OS_WINDOWS ? "flink-sql-history" : ".flink-sql-history");
        }

        try (CliClient cli = new CliClient(sessionId, executor, historyFilePath)) {
            // interactive CLI mode
            if (options.getUpdateStatement() == null) {
                cli.open();
            }
            // execute single update statement
            else {
                final boolean success = cli.submitUpdate(options.getUpdateStatement());
                if (!success) {
                    throw new SqlClientException(
                            "Could not submit given SQL update statement to cluster.");
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    public static void main(String[] args) {
        final String mode;
        final String[] modeArgs;
        if (args.length < 1 || args[0].startsWith("-")) {
            // mode is not specified, use the default `embedded` mode
            mode = MODE_EMBEDDED;
            modeArgs = args;
        } else {
            // mode is specified, extract the mode value and reaming args
            mode = args[0];
            // remove mode
            modeArgs = Arrays.copyOfRange(args, 1, args.length);
        }

        switch (mode) {
            case MODE_EMBEDDED:
                final CliOptions options = CliOptionsParser.parseEmbeddedModeClient(modeArgs);
                if (options.isPrintHelp()) {
                    CliOptionsParser.printHelpEmbeddedModeClient();
                } else {
                    try {
                        final SqlClient client = new SqlClient(true, options);
                        client.start();
                    } catch (SqlClientException e) {
                        // make space in terminal
                        System.out.println();
                        System.out.println();
                        LOG.error("SQL Client must stop.", e);
                        throw e;
                    } catch (Throwable t) {
                        // make space in terminal
                        System.out.println();
                        System.out.println();
                        LOG.error(
                                "SQL Client must stop. Unexpected exception. This is a bug. Please consider filing an issue.",
                                t);
                        throw new SqlClientException(
                                "Unexpected exception. This is a bug. Please consider filing an issue.",
                                t);
                    }
                }
                break;

            case MODE_GATEWAY:
                throw new SqlClientException("Gateway mode is not supported yet.");

            default:
                CliOptionsParser.printHelpClient();
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class EmbeddedShutdownThread extends Thread {

        private final String sessionId;
        private final Executor executor;

        public EmbeddedShutdownThread(String sessionId, Executor executor) {
            this.sessionId = sessionId;
            this.executor = executor;
        }

        @Override
        public void run() {
            // Shutdown the executor
            System.out.println("\nShutting down the session...");
            executor.closeSession(sessionId);
            System.out.println("done.");
        }
    }
}
