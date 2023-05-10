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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.client.cli.TerminalUtils;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.apache.flink.configuration.DeploymentOptions.TARGET;
import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SqlClient}. */
class SqlClientTest {

    @TempDir private Path tempFolder;

    @RegisterExtension
    @Order(1)
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(
                    () -> {
                        Configuration configuration = new Configuration();
                        configuration.set(TARGET, "yarn-session");
                        return configuration;
                    });

    @RegisterExtension
    @Order(2)
    private static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    private Map<String, String> originalEnv;

    private String historyPath;

    @BeforeEach
    void before() throws IOException {
        originalEnv = System.getenv();

        // prepare conf dir
        File confFolder = Files.createTempDirectory(tempFolder, "conf").toFile();
        File confYaml = new File(confFolder, "flink-conf.yaml");
        if (!confYaml.createNewFile()) {
            throw new IOException("Can't create testing flink-conf.yaml file.");
        }

        // adjust the test environment for the purposes of this test
        Map<String, String> map = new HashMap<>(System.getenv());
        map.put(ENV_FLINK_CONF_DIR, confFolder.getAbsolutePath());
        CommonTestUtils.setEnv(map);

        historyPath = Files.createTempFile(tempFolder, "history", "").toFile().getPath();
    }

    @AfterEach
    void after() {
        CommonTestUtils.setEnv(originalEnv);
    }

    @Test
    void testEmbeddedWithOptions() throws Exception {
        String[] args = new String[] {"embedded", "-hist", historyPath};
        String actual = runSqlClient(args);
        assertThat(actual).contains("Command history file path: " + historyPath);
    }

    @Test
    void testEmbeddedWithLongOptions() throws Exception {
        String[] args = new String[] {"embedded", "--history", historyPath};
        String actual = runSqlClient(args);
        assertThat(actual).contains("Command history file path: " + historyPath);
    }

    @Test
    void testEmbeddedWithoutOptions() throws Exception {
        String[] args = new String[] {"embedded"};
        String actual = runSqlClient(args);
        assertThat(actual).contains("Command history file path: ");
    }

    @Test
    void testEmbeddedWithConfigOptions() throws Exception {
        String[] args = new String[] {"embedded", "-D", "key1=val1", "-D", "key2=val2"};
        String output = runSqlClient(args, "SET;\nQUIT;\n", false);
        assertThat(output).contains("key1", "val1", "key2", "val2");
    }

    @Test
    void testEmptyOptions() throws Exception {
        String[] args = new String[] {};
        String actual = runSqlClient(args);
        assertThat(actual).contains("Command history file path");
    }

    @Test
    void testGatewayModeHostnamePort() throws Exception {
        String[] args =
                new String[] {
                    "gateway",
                    "-e",
                    String.format(
                            "%s:%d",
                            SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                            SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort())
                };
        String actual = runSqlClient(args, String.join("\n", "SET;", "QUIT;"), false);
        assertThat(actual).contains("execution.target", "yarn-session");
    }

    @Test
    void testGatewayModeUrl() throws Exception {
        String[] args =
                new String[] {
                    "gateway",
                    "-e",
                    String.format(
                            "http://%s:%d",
                            SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                            SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort())
                };
        String actual = runSqlClient(args, String.join("\n", "SET;", "QUIT;"), false);
        assertThat(actual).contains("execution.target", "yarn-session");
    }

    @Test
    void testGatewayModeWithoutAddress() throws Exception {
        String[] args = new String[] {"gateway"};
        assertThrows(
                "Please specify the address of the SQL Gateway with command line option"
                        + " '-e,--endpoint <SQL Gateway address>' in the gateway mode.",
                SqlClientException.class,
                () -> runSqlClient(args));
    }

    @Test
    void testErrorMessage() throws Exception {
        // prepare statements which will throw exception
        String stmts =
                "CREATE TABLE T (a int) WITH ('connector' = 'invalid');\n"
                        + "SELECT * FROM T;\n"
                        + "QUIT;\n";
        String[] args = new String[] {};
        String output = runSqlClient(args, stmts, false);
        assertThat(output)
                .contains(
                        "org.apache.flink.table.api.ValidationException: Could not find any factory for identifier 'invalid'");

        // shouldn't contain error stack
        String[] errorStack =
                new String[] {
                    "at org.apache.flink.table.factories.FactoryUtil.discoverFactory",
                    "at org.apache.flink.table.factories.FactoryUtil.createDynamicTableSource"
                };
        for (String stack : errorStack) {
            assertThat(output).doesNotContain(stack);
        }
    }

    @Test
    void testVerboseErrorMessage() throws Exception {
        // prepare statements which will throw exception
        String stmts =
                "CREATE TABLE T (a int) WITH ('connector' = 'invalid');\n"
                        + "SET 'sql-client.verbose' = 'true';\n"
                        + "SELECT * FROM T;\n"
                        + "QUIT;\n";
        String[] args = new String[] {};
        String output = runSqlClient(args, stmts, false);
        String[] errors =
                new String[] {
                    "org.apache.flink.table.api.ValidationException: Could not find any factory for identifier 'invalid'",
                    "at org.apache.flink.table.factories.FactoryUtil.discoverFactory",
                    "at org.apache.flink.table.factories.FactoryUtil.createDynamicTableSource"
                };
        for (String error : errors) {
            assertThat(output).contains(error);
        }
    }

    @Test
    void testInitFile() throws Exception {
        List<String> statements =
                Arrays.asList(
                        "-- define table \n"
                                + "CREATE TABLE source ("
                                + "id INT,"
                                + "val STRING"
                                + ") WITH ("
                                + "  'connector' = 'values'"
                                + "); \n",
                        " -- define config \nSET 'key' = 'value';\n");
        String initFile = createSqlFile(statements, "init-sql.sql");

        String[] args = new String[] {"-i", initFile};
        String output = runSqlClient(args, "SET;\nQUIT;\n", false);
        assertThat(output).contains("key", "value");
    }

    @Test
    void testExecuteSqlFile() throws Exception {
        List<String> statements = Collections.singletonList("HELP;\n");
        String sqlFilePath = createSqlFile(statements, "test-sql.sql");
        String[] args = new String[] {"-f", sqlFilePath};
        String output = runSqlClient(args);
        final URL url = getClass().getClassLoader().getResource("sql-client-help-command.out");
        final String help = FileUtils.readFileUtf8(new File(url.getFile()));

        for (String command : help.split("\n")) {
            assertThat(output).contains(command);
        }
    }

    @Test
    void testDisplayMultiLineSqlInInteractiveMode() throws Exception {
        List<String> statements =
                Arrays.asList(
                        "-- define table \n"
                                + "CREATE TABLE source ("
                                + "id INT"
                                + ") WITH ("
                                + "  'connector' = 'datagen'"
                                + "); \n",
                        "CREATE TABLE sink ( id INT) WITH ( 'connector' = 'blackhole');");
        String initFile = createSqlFile(statements, "init-sql.sql");
        String[] args = new String[] {"-i", initFile};
        String output =
                runSqlClient(
                        args,
                        String.join(
                                "\n",
                                Arrays.asList(
                                        "EXPLAIN STATEMENT SET",
                                        "BEGIN",
                                        "INSERT INTO sink SELECT * FROM source;",
                                        "",
                                        "INSERT INTO sink SELECT * FROM source;",
                                        "",
                                        "END;\n")),
                        true);
        assertThat(output)
                .contains(
                        "Flink SQL> EXPLAIN STATEMENT SET\n"
                                + "> BEGIN\n"
                                + "> INSERT INTO sink SELECT * FROM source;\n"
                                + "> \n"
                                + "> INSERT INTO sink SELECT * FROM source;\n"
                                + "> \n"
                                + "> END;");
    }

    @Test
    void testExecuteSqlWithHDFSFile() {
        String[] args = new String[] {"-f", "hdfs://path/to/file/test.sql"};
        assertThatThrownBy(() -> runSqlClient(args))
                .isInstanceOf(SqlClientException.class)
                .hasMessage("SQL Client only supports to load files in local.");
    }

    @Test
    public void testPrintEmbeddedModeHelp() throws Exception {
        runTestCliHelp(new String[] {"embedded", "--help"}, "cli/embedded-mode-help.out");
    }

    @Test
    public void testPrintGatewayModeHelp() throws Exception {
        runTestCliHelp(new String[] {"gateway", "--help"}, "cli/gateway-mode-help.out");
    }

    @Test
    public void testPrintAllModeHelp() throws Exception {
        runTestCliHelp(new String[] {"--help"}, "cli/all-mode-help.out");
    }

    private void runTestCliHelp(String[] args, String expected) throws Exception {
        String actual =
                new String(
                        Files.readAllBytes(
                                Paths.get(
                                        Preconditions.checkNotNull(
                                                        SqlClientTest.class
                                                                .getClassLoader()
                                                                .getResource(expected))
                                                .toURI())));
        assertThat(runSqlClient(args)).isEqualTo(actual);
    }

    private String runSqlClient(String[] args) throws Exception {
        return runSqlClient(args, "QUIT;\n", false);
    }

    private String runSqlClient(String[] args, String statements, boolean printInput)
            throws Exception {
        try (OutputStream out = new ByteArrayOutputStream();
                Terminal terminal =
                        TerminalUtils.createDumbTerminal(
                                new ByteArrayInputStream(
                                        statements.getBytes(StandardCharsets.UTF_8)),
                                out)) {
            if (printInput) {
                // The default terminal has an empty size. Here increase the terminal to allow
                // the line reader print the input string.
                terminal.setSize(new Size(160, 80));
            }
            SqlClient.startClient(args, () -> terminal);
            return out.toString().replace("\r\n", System.lineSeparator());
        }
    }

    private String createSqlFile(List<String> statements, String name) throws IOException {
        // create sql file
        File sqlFileFolder = Files.createTempDirectory(tempFolder, "sql-file").toFile();
        File sqlFile = new File(sqlFileFolder, name);
        if (!sqlFile.createNewFile()) {
            throw new IOException(String.format("Can't create testing %s.", name));
        }
        String sqlFilePath = sqlFile.getPath();
        Files.write(
                Paths.get(sqlFilePath),
                statements,
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND);
        return sqlFilePath;
    }
}
