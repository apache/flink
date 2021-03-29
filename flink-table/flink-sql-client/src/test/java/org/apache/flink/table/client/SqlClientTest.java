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
import org.apache.flink.table.client.cli.TerminalStreamsResource;
import org.apache.flink.util.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
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
import java.util.Objects;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for {@link SqlClient}. */
public class SqlClientTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule public final TerminalStreamsResource useSystemStream = TerminalStreamsResource.INSTANCE;

    private PrintStream originalPrintStream;

    private InputStream originalInputStream;

    private ByteArrayOutputStream testOutputStream;

    private Map<String, String> originalEnv;

    private String historyPath;

    @Before
    public void before() throws IOException {
        originalEnv = System.getenv();
        originalPrintStream = System.out;
        originalInputStream = System.in;
        testOutputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(testOutputStream, true));
        // send "QUIT;" command to gracefully shutdown the terminal
        System.setIn(new ByteArrayInputStream("QUIT;\n".getBytes(StandardCharsets.UTF_8)));

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
        System.setOut(originalPrintStream);
        System.setIn(originalInputStream);
        CommonTestUtils.setEnv(originalEnv);
    }

    private String getStdoutString() {
        return testOutputStream.toString();
    }

    @Test
    public void testEmbeddedWithOptions() {
        String[] args = new String[] {"embedded", "-hist", historyPath};
        SqlClient.main(args);
        assertThat(getStdoutString(), containsString("Command history file path: " + historyPath));
    }

    @Test
    public void testEmbeddedWithLongOptions() {
        String[] args = new String[] {"embedded", "--history", historyPath};
        SqlClient.main(args);
        assertThat(getStdoutString(), containsString("Command history file path: " + historyPath));
    }

    @Test
    public void testEmbeddedWithoutOptions() {
        String[] args = new String[] {"embedded"};
        SqlClient.main(args);
        assertThat(getStdoutString(), containsString("Command history file path"));
    }

    @Test
    public void testEmptyOptions() {
        String[] args = new String[] {};
        SqlClient.main(args);
        assertThat(getStdoutString(), containsString("Command history file path"));
    }

    @Test
    public void testUnsupportedGatewayMode() {
        String[] args = new String[] {"gateway"};
        thrown.expect(SqlClientException.class);
        thrown.expectMessage("Gateway mode is not supported yet.");
        SqlClient.main(args);
    }

    @Test
    public void testPrintHelpForUnknownMode() throws IOException {
        String[] args = new String[] {"unknown"};
        SqlClient.main(args);
        final URL url = getClass().getClassLoader().getResource("sql-client-help.out");
        Objects.requireNonNull(url);
        final String help = FileUtils.readFileUtf8(new File(url.getFile()));
        assertEquals(help, getStdoutString());
    }

    @Test
    public void testErrorMessage() {
        // prepare statements which will throw exception
        String stmts =
                "CREATE TABLE T (a int) WITH ('connector' = 'invalid');\n"
                        + "SELECT * FROM T;\n"
                        + "QUIT;\n";
        System.setIn(new ByteArrayInputStream(stmts.getBytes(StandardCharsets.UTF_8)));
        String[] args = new String[] {};
        SqlClient.main(args);
        String output = getStdoutString();
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
    public void testVerboseErrorMessage() {
        // prepare statements which will throw exception
        String stmts =
                "CREATE TABLE T (a int) WITH ('connector' = 'invalid');\n"
                        + "SET sql-client.verbose=true;\n"
                        + "SELECT * FROM T;\n"
                        + "QUIT;\n";
        System.setIn(new ByteArrayInputStream(stmts.getBytes(StandardCharsets.UTF_8)));
        String[] args = new String[] {};
        SqlClient.main(args);
        String output = getStdoutString();
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
    public void testInitFile() throws IOException {
        List<String> statements =
                Arrays.asList(
                        "CREATE TABLE source ("
                                + "id INT,"
                                + "val STRING"
                                + ") WITH ("
                                + "  'connector' = 'values'"
                                + ");\n",
                        "SET key=value;\n");
        String initFile = createSqlFile(statements, "init-sql.sql");

        String[] args = new String[] {"-i", initFile};
        SqlClient.main(args);

        assertThat(getStdoutString(), containsString("Successfully initialized from sql script: "));
    }

    @Test
    public void testExecuteSqlFile() throws IOException {
        List<String> statements = Collections.singletonList("HELP;");
        String sqlFilePath = createSqlFile(statements, "test-sql.sql");
        String[] args = new String[] {"-f", sqlFilePath};
        SqlClient.main(args);
        final URL url = getClass().getClassLoader().getResource("sql-client-help-command.out");
        final String help = FileUtils.readFileUtf8(new File(url.getFile()));
        final String output = getStdoutString();

        for (String command : help.split("\n")) {
            assertThat(output, containsString(command));
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
