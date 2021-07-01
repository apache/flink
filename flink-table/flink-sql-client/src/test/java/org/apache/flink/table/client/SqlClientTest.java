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

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.client.cli.TerminalUtils;
import org.apache.flink.util.FileUtils;

import org.jline.terminal.Terminal;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for {@link SqlClient}. */
public class SqlClientTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private Map<String, String> originalEnv;

    private String historyPath;

    @Rule public Timeout timeout = new Timeout(1000, TimeUnit.SECONDS);

    @Before
    public void before() throws IOException {
        originalEnv = System.getenv();

        // prepare conf dir
        File confFolder = tempFolder.newFolder("conf");
        File confYaml = new File(confFolder, "flink-conf.yaml");
        if (!confYaml.createNewFile()) {
            throw new IOException("Can't create testing flink-conf.yaml file.");
        }

        // adjust the test environment for the purposes of this test
        Map<String, String> map = new HashMap<>(System.getenv());
        map.put(ENV_FLINK_CONF_DIR, confFolder.getAbsolutePath());
        CommonTestUtils.setEnv(map);

        historyPath = tempFolder.newFile("history").toString();
    }

    @After
    public void after() {
        CommonTestUtils.setEnv(originalEnv);
    }

    @Test
    public void testEmbeddedWithOptions() throws Exception {
        String[] args = new String[] {"embedded", "-hist", historyPath};
        String actual = runSqlClient(args);
        assertThat(actual, containsString("Command history file path: " + historyPath));
    }

    @Test
    public void testEmbeddedWithLongOptions() throws Exception {
        String[] args = new String[] {"embedded", "--history", historyPath};
        String actual = runSqlClient(args);
        assertThat(actual, containsString("Command history file path: " + historyPath));
    }

    @Test
    public void testEmbeddedWithoutOptions() throws Exception {
        String[] args = new String[] {"embedded"};
        String actual = runSqlClient(args);
        assertThat(actual, containsString("Command history file path: "));
    }

    @Test
    public void testEmptyOptions() throws Exception {
        String[] args = new String[] {};
        String actual = runSqlClient(args);
        assertThat(actual, containsString("Command history file path"));
    }

    @Test
    public void testUnsupportedGatewayMode() {
        String[] args = new String[] {"gateway"};
        thrown.expect(SqlClientException.class);
        thrown.expectMessage("Gateway mode is not supported yet.");
        SqlClient.main(args);
    }

    @Test
    public void testErrorMessage() throws Exception {
        // prepare statements which will throw exception
        String stmts =
                "CREATE TABLE T (a int) WITH ('connector' = 'invalid');\n"
                        + "SELECT * FROM T;\n"
                        + "QUIT;\n";
        String[] args = new String[] {};
        String output = runSqlClient(args, stmts);
        assertThat(
                output,
                containsString(
                        "org.apache.flink.table.api.ValidationException: Could not find any factory for identifier 'invalid'"));

        // shouldn't contain error stack
        String[] errorStack =
                new String[] {
                    "at org.apache.flink.table.factories.FactoryUtil.discoverFactory",
                    "at org.apache.flink.table.factories.FactoryUtil.createTableSource"
                };
        for (String stack : errorStack) {
            assertThat(output, not(containsString(stack)));
        }
    }

    @Test
    public void testVerboseErrorMessage() throws Exception {
        // prepare statements which will throw exception
        String stmts =
                "CREATE TABLE T (a int) WITH ('connector' = 'invalid');\n"
                        + "SET 'sql-client.verbose' = 'true';\n"
                        + "SELECT * FROM T;\n"
                        + "QUIT;\n";
        String[] args = new String[] {};
        String output = runSqlClient(args, stmts);
        String[] errors =
                new String[] {
                    "org.apache.flink.table.api.ValidationException: Could not find any factory for identifier 'invalid'",
                    "at org.apache.flink.table.factories.FactoryUtil.discoverFactory",
                    "at org.apache.flink.table.factories.FactoryUtil.createTableSource"
                };
        for (String error : errors) {
            assertThat(output, containsString(error));
        }
    }

    @Test
    public void testInitFile() throws Exception {
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
        String output = runSqlClient(args, "SET;\nQUIT;\n");
        assertThat(output, containsString("'key' = 'value'"));
    }

    @Test
    public void testExecuteSqlFile() throws Exception {
        List<String> statements = Collections.singletonList("HELP;\n");
        String sqlFilePath = createSqlFile(statements, "test-sql.sql");
        String[] args = new String[] {"-f", sqlFilePath};
        String output = runSqlClient(args);
        final URL url = getClass().getClassLoader().getResource("sql-client-help-command.out");
        final String help = FileUtils.readFileUtf8(new File(url.getFile()));

        for (String command : help.split("\n")) {
            assertThat(output, containsString(command));
        }
    }

    @Test
    public void testExecuteSqlWithHDFSFile() throws Exception {
        String[] args = new String[] {"-f", "hdfs://path/to/file/test.sql"};
        thrown.expect(SqlClientException.class);
        thrown.expectMessage("SQL Client only supports to load files in local.");
        runSqlClient(args);
    }

    private String runSqlClient(String[] args) throws Exception {
        return runSqlClient(args, "QUIT;\n");
    }

    private String runSqlClient(String[] args, String statements) throws Exception {
        try (OutputStream out = new ByteArrayOutputStream();
                Terminal terminal =
                        TerminalUtils.createDumbTerminal(
                                new ByteArrayInputStream(
                                        statements.getBytes(StandardCharsets.UTF_8)),
                                out)) {
            SqlClient.startClient(args, () -> terminal);
            return out.toString();
        }
    }

    private String createSqlFile(List<String> statements, String name) throws IOException {
        // create sql file
        File sqlFileFolder = tempFolder.newFolder("sql-file");
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
