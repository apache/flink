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

import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;

/** Base class for test {@link SqlClient}. */
class SqlClientTestBase {
    @TempDir private Path tempFolder;

    protected String historyPath;

    protected Map<String, String> originalEnv;

    @BeforeEach
    void before() throws IOException {
        originalEnv = System.getenv();

        // prepare conf dir
        File confFolder = Files.createTempDirectory(tempFolder, "conf").toFile();
        File confYaml = new File(confFolder, "config.yaml");
        if (!confYaml.createNewFile()) {
            throw new IOException("Can't create testing config.yaml file.");
        }
        writeConfigOptionsToConfYaml(confYaml.toPath());
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

    protected String createSqlFile(List<String> statements, String name) throws IOException {
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

    public static String runSqlClient(String[] args) throws Exception {
        return runSqlClient(args, "QUIT;\n", false);
    }

    public static String runSqlClient(String[] args, String statements, boolean printInput)
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

    protected void writeConfigOptionsToConfYaml(Path confYamlPath) throws IOException {
        // no-op for default.
    }
}
