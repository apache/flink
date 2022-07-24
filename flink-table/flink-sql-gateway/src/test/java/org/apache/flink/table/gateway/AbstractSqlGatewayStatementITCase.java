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

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.gateway.utils.SqlScriptReader;
import org.apache.flink.table.gateway.utils.TestSqlStatement;
import org.apache.flink.table.utils.print.PrintStyle;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.flink.table.gateway.utils.SqlScriptReader.HINT_START_OF_OUTPUT;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceNodeIdInOperator;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceStreamNodeId;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Base ITCase tests for statements. */
public abstract class AbstractSqlGatewayStatementITCase extends AbstractTestBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractSqlGatewayStatementITCase.class);

    @RegisterExtension
    @Order(1)
    public static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    @RegisterExtension
    @Order(2)
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    private static final String RESOURCE_DIR = "sql/";
    private static final Pattern PATTERN = Pattern.compile(".*\\.q$");

    protected static SqlGatewayService service;

    private final Map<String, String> replaceVars = new HashMap<>();

    @BeforeAll
    public static void setUp() {
        service = SQL_GATEWAY_SERVICE_EXTENSION.getService();
    }

    @BeforeEach
    public void before(@TempDir Path temporaryFolder) throws Exception {
        // initialize new folders for every test, so the vars can be reused by every SQL scripts
        replaceVars.put(
                "$VAR_STREAMING_PATH",
                Files.createDirectory(temporaryFolder.resolve("streaming")).toFile().getPath());
        replaceVars.put(
                "$VAR_BATCH_PATH",
                Files.createDirectory(temporaryFolder.resolve("batch")).toFile().getPath());
    }

    @ParameterizedTest
    @MethodSource("listFlinkSqlTests")
    public void testFlinkSqlStatements(String sqlPath) throws Exception {
        resetSessionForFlinkSqlStatements();
        runTest(sqlPath);
    }

    /**
     * Returns printed results for each ran SQL statements.
     *
     * @param statements the SQL statements to run
     * @return the stringified results
     */
    protected List<String> runStatements(List<TestSqlStatement> statements) {
        List<String> output = new ArrayList<>();
        for (TestSqlStatement statement : statements) {
            StringBuilder builder = new StringBuilder();
            builder.append(statement.getComment());
            builder.append(statement.getSql());

            String trimmedSql = statement.getSql().trim();
            if (trimmedSql.endsWith(";")) {
                trimmedSql = trimmedSql.substring(0, trimmedSql.length() - 1);
            }
            try {
                builder.append(runSingleStatement(trimmedSql));
            } catch (Throwable t) {
                LOG.error("Failed to execute statements.", t);
                builder.append(
                        AbstractSqlGatewayStatementITCase.Tag.ERROR.addTag(
                                stringifyException(t).trim() + "\n"));
            }
            output.add(builder.toString());
        }

        return output;
    }

    // -------------------------------------------------------------------------------------------
    // Utility
    // -------------------------------------------------------------------------------------------

    /** Mark the output type. */
    public enum Tag {
        INFO("!info"),

        OK("!ok"),

        ERROR("!error");

        private final String tag;

        Tag(String tag) {
            this.tag = tag;
        }

        public String addTag(String content) {
            return HINT_START_OF_OUTPUT + "\n" + content + tag + "\n";
        }
    }

    /** Mark the statement type. */
    public enum StatementType {
        SHOW_CREATE("SHOW CREATE"),

        EXPLAIN("EXPLAIN"),

        QUERY("SELECT"),

        OTHERS();

        private final String beginWith;

        StatementType(String beginWith) {
            this.beginWith = beginWith;
        }

        StatementType() {
            this("");
        }

        public static StatementType match(String sql) {
            String processed = sql.trim().toUpperCase();

            if (processed.startsWith(SHOW_CREATE.beginWith)) {
                return SHOW_CREATE;
            } else if (processed.startsWith(EXPLAIN.beginWith)) {
                return EXPLAIN;
            } else if (processed.startsWith(QUERY.beginWith)) {
                return QUERY;
            } else {
                return OTHERS;
            }
        }
    }

    protected String getInputFromPath(String sqlPath) throws IOException {
        // replace the placeholder with specified value if exists
        String[] keys = replaceVars.keySet().toArray(new String[0]);
        String[] values = Arrays.stream(keys).map(replaceVars::get).toArray(String[]::new);

        return StringUtils.replaceEach(
                IOUtils.toString(
                        checkNotNull(
                                AbstractSqlGatewayStatementITCase.class.getResourceAsStream(
                                        "/" + sqlPath)),
                        StandardCharsets.UTF_8),
                keys,
                values);
    }

    private static Stream<String> listFlinkSqlTests() throws Exception {
        final File jarFile =
                new File(
                        AbstractSqlGatewayStatementITCase.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .getPath());

        if (jarFile.isFile()) {
            List<String> files = new ArrayList<>();
            try (JarFile jar = new JarFile(jarFile)) {
                // gives ALL entries in jar
                final Enumeration<JarEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    final String name = entries.nextElement().getName();
                    // filter according to the path
                    if (name.startsWith(RESOURCE_DIR) && PATTERN.matcher(name).matches()) {
                        files.add(name);
                    }
                }
            }
            return files.stream();
        } else {
            return listTestSpecInTheSameModule(RESOURCE_DIR);
        }
    }

    protected static Stream<String> listTestSpecInTheSameModule(String resourceDir)
            throws Exception {
        return IOUtils.readLines(
                        checkNotNull(
                                AbstractSqlGatewayStatementITCase.class
                                        .getClassLoader()
                                        .getResourceAsStream(resourceDir)),
                        StandardCharsets.UTF_8)
                .stream()
                .map(name -> Paths.get(resourceDir, name).toString());
    }

    protected void runTest(String sqlPath) throws Exception {
        String in = getInputFromPath(sqlPath);
        List<TestSqlStatement> testSqlStatements = SqlScriptReader.parseSqlScript(in);

        assertThat(String.join("", runStatements(testSqlStatements))).isEqualTo(in);
    }

    protected void resetSessionForFlinkSqlStatements() throws Exception {}

    /**
     * Returns printed results for each ran SQL statements.
     *
     * @param statement the SQL statement to run
     * @return the printed results in tableau style
     */
    protected abstract String runSingleStatement(String statement) throws Exception;

    protected abstract String stringifyException(Throwable t);

    protected abstract boolean isStreaming() throws Exception;

    protected String toString(
            StatementType type,
            ResolvedSchema schema,
            RowDataToStringConverter converter,
            Iterator<RowData> iterator)
            throws Exception {
        if (type.equals(StatementType.EXPLAIN) || type.equals(StatementType.SHOW_CREATE)) {
            return Tag.OK.addTag(
                    replaceStreamNodeId(
                                    replaceNodeIdInOperator(
                                            iterator.next().getString(0).toString()))
                            + "\n");
        } else {
            ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            PrintStyle style =
                    PrintStyle.tableauWithDataInferredColumnWidths(
                            schema,
                            converter,
                            Integer.MAX_VALUE,
                            true,
                            type.equals(StatementType.QUERY) && isStreaming());

            PrintWriter writer = new PrintWriter(outContent);
            style.print(iterator, writer);
            return Tag.OK.addTag(outContent.toString());
        }
    }
}
