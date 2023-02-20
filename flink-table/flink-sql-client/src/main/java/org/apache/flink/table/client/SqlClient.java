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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.client.cli.CliClient;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.apache.flink.table.client.gateway.DefaultContextUtils;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SingleSessionManager;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.gateway.SqlGateway;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpointFactory;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.util.NetUtils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Supplier;

import static org.apache.flink.table.client.cli.CliClient.DEFAULT_TERMINAL_FACTORY;
import static org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils.getSqlGatewayOptionPrefix;

/**
 * SQL Client for submitting SQL statements. The client can be executed in two modes: a gateway and
 * embedded mode.
 *
 * <p>- In embedded mode, the SQL CLI is tightly coupled with the executor in a common process. This
 * allows for submitting jobs without having to start an additional component.
 *
 * <p>- In gateway mode, the SQL CLI client connects to the REST API of the gateway and allows for
 * managing queries via console.
 */
public class SqlClient {

    private static final Logger LOG = LoggerFactory.getLogger(SqlClient.class);

    private final boolean isGatewayMode;
    private final CliOptions options;
    private final Supplier<Terminal> terminalFactory;

    public static final String MODE_EMBEDDED = "embedded";
    public static final String MODE_GATEWAY = "gateway";
    public static final String MODE_NONE = "";

    public SqlClient(
            boolean isGatewayMode, CliOptions options, Supplier<Terminal> terminalFactory) {
        this.isGatewayMode = isGatewayMode;
        this.options = options;
        this.terminalFactory = terminalFactory;
    }

    private void start() {
        if (isGatewayMode) {
            CliOptions.GatewayCliOptions gatewayCliOptions = (CliOptions.GatewayCliOptions) options;
            try (Executor executor =
                    Executor.create(
                            DefaultContextUtils.buildDefaultContext(gatewayCliOptions),
                            gatewayCliOptions
                                    .getGatewayAddress()
                                    .orElseThrow(
                                            () ->
                                                    new SqlClientException(
                                                            "Please specify the address of the SQL Gateway with command line option"
                                                                    + " '-e,--endpoint <SQL Gateway address>' in the gateway mode.")),
                            options.getSessionId())) {
                // add shutdown hook
                Runtime.getRuntime().addShutdownHook(new ShutdownThread(executor));
                openCli(executor);
            }
        } else {
            DefaultContext defaultContext =
                    DefaultContextUtils.buildDefaultContext(
                            (CliOptions.EmbeddedCliOptions) options);
            try (EmbeddedGateway embeddedGateway = EmbeddedGateway.create(defaultContext);
                    Executor executor =
                            Executor.create(
                                    defaultContext,
                                    InetSocketAddress.createUnresolved(
                                            embeddedGateway.getAddress(),
                                            embeddedGateway.getPort()),
                                    options.getSessionId())) {
                // add shutdown hook
                Runtime.getRuntime().addShutdownHook(new ShutdownThread(executor, embeddedGateway));
                openCli(executor);
            }
        }
    }

    /**
     * Opens the CLI client for executing SQL statements.
     *
     * @param executor executor
     */
    private void openCli(Executor executor) {
        Path historyFilePath;
        if (options.getHistoryFilePath() != null) {
            historyFilePath = Paths.get(options.getHistoryFilePath());
        } else {
            historyFilePath =
                    Paths.get(
                            System.getProperty("user.home"),
                            SystemUtils.IS_OS_WINDOWS ? "flink-sql-history" : ".flink-sql-history");
        }

        boolean hasSqlFile = options.getSqlFile() != null;
        boolean hasUpdateStatement = options.getUpdateStatement() != null;
        if (hasSqlFile && hasUpdateStatement) {
            throw new IllegalArgumentException(
                    String.format(
                            "Please use either option %s or %s. The option %s is deprecated and it's suggested to use %s instead.",
                            CliOptionsParser.OPTION_FILE,
                            CliOptionsParser.OPTION_UPDATE,
                            CliOptionsParser.OPTION_UPDATE.getOpt(),
                            CliOptionsParser.OPTION_FILE.getOpt()));
        }

        try (CliClient cli = new CliClient(terminalFactory, executor, historyFilePath)) {
            if (options.getInitFile() != null) {
                boolean success = cli.executeInitialization(readFromURL(options.getInitFile()));
                if (!success) {
                    System.out.println(
                            String.format(
                                    "Failed to initialize from sql script: %s. Please refer to the LOG for detailed error messages.",
                                    options.getInitFile()));
                    return;
                } else {
                    System.out.println(
                            String.format(
                                    "Successfully initialized from sql script: %s",
                                    options.getInitFile()));
                }
            }

            if (!hasSqlFile && !hasUpdateStatement) {
                cli.executeInInteractiveMode();
            } else {
                cli.executeInNonInteractiveMode(readExecutionContent());
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    public static void main(String[] args) {
        startClient(args, DEFAULT_TERMINAL_FACTORY);
    }

    @VisibleForTesting
    protected static void startClient(String[] args, Supplier<Terminal> terminalFactory) {
        final String mode;
        final String[] modeArgs;
        if (args.length < 1 || args[0].startsWith("-")) {
            // mode is not specified, use the default `embedded` mode
            mode = "";
            modeArgs = args;
        } else {
            // mode is specified, extract the mode value and reaming args
            mode = args[0];
            // remove mode
            modeArgs = Arrays.copyOfRange(args, 1, args.length);
        }

        final CliOptions options;
        switch (mode) {
            case MODE_EMBEDDED:
                options = CliOptionsParser.parseEmbeddedModeClient(modeArgs);
                if (options.isPrintHelp()) {
                    CliOptionsParser.printHelpEmbeddedModeClient(terminalFactory.get().writer());
                    return;
                }
                break;
            case MODE_GATEWAY:
                options = CliOptionsParser.parseGatewayModeClient(modeArgs);
                if (options.isPrintHelp()) {
                    CliOptionsParser.printHelpGatewayModeClient(terminalFactory.get().writer());
                    return;
                }
                break;
            case MODE_NONE:
                options = CliOptionsParser.parseEmbeddedModeClient(modeArgs);
                if (options.isPrintHelp()) {
                    CliOptionsParser.printHelpClient(terminalFactory.get().writer());
                    return;
                }
                break;
            default:
                CliOptionsParser.printHelpClient(terminalFactory.get().writer());
                return;
        }

        try {
            final SqlClient client =
                    new SqlClient(mode.equals(MODE_GATEWAY), options, terminalFactory);
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
                    "Unexpected exception. This is a bug. Please consider filing an issue.", t);
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class EmbeddedGateway implements Closeable {

        private static final String ADDRESS = "localhost";

        private final NetUtils.Port port;
        private final SqlGateway sqlGateway;

        public static EmbeddedGateway create(DefaultContext defaultContext) {
            NetUtils.Port port = NetUtils.getAvailablePort();

            Configuration defaultConfig = defaultContext.getFlinkConfig();
            Configuration restConfig = new Configuration();
            // always use localhost
            restConfig.set(SqlGatewayRestOptions.ADDRESS, ADDRESS);
            restConfig.set(SqlGatewayRestOptions.PORT, port.getPort());
            defaultConfig.addAll(
                    restConfig,
                    getSqlGatewayOptionPrefix(SqlGatewayRestEndpointFactory.IDENTIFIER));
            SqlGateway sqlGateway =
                    new SqlGateway(defaultConfig, new SingleSessionManager(defaultContext));
            try {
                sqlGateway.start();
                LOG.info("Start embedded gateway on port {}", port.getPort());
            } catch (Throwable t) {
                closePort(port);
                throw new SqlClientException("Failed to start the embedded sql-gateway.", t);
            }

            return new EmbeddedGateway(sqlGateway, port);
        }

        private EmbeddedGateway(SqlGateway sqlGateway, NetUtils.Port port) {
            this.sqlGateway = sqlGateway;
            this.port = port;
        }

        String getAddress() {
            return ADDRESS;
        }

        int getPort() {
            return port.getPort();
        }

        @Override
        public void close() {
            sqlGateway.stop();
            closePort(port);
        }

        private static void closePort(NetUtils.Port port) {
            try {
                port.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private static class ShutdownThread extends Thread {

        private final Executor executor;
        private @Nullable final EmbeddedGateway gateway;

        private ShutdownThread(Executor executor) {
            this(executor, null);
        }

        public ShutdownThread(Executor executor, @Nullable EmbeddedGateway gateway) {
            this.executor = executor;
            this.gateway = gateway;
        }

        @Override
        public void run() {
            // Shutdown the executor
            System.out.println("\nShutting down the session...");
            executor.close();
            if (gateway != null) {
                gateway.close();
            }
            System.out.println("done.");
        }
    }

    private String readExecutionContent() {
        if (options.getSqlFile() != null) {
            return readFromURL(options.getSqlFile());
        } else {
            return options.getUpdateStatement().trim();
        }
    }

    private String readFromURL(URL file) {
        try {
            return IOUtils.toString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SqlExecutionException(
                    String.format("Fail to read content from the %s.", file.getPath()), e);
        }
    }
}
