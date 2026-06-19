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
import org.apache.flink.table.gateway.service.utils.Constants;
import org.apache.flink.table.gateway.utils.SqlScriptReader;
import org.apache.flink.table.gateway.utils.TestSqlStatement;
import org.apache.flink.table.utils.print.PrintStyle;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.test.util.AbstractTestBaseJUnit4;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

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
import java.util.stream.Collectors;

import static org.apache.flink.table.gateway.utils.SqlScriptReader.HINT_START_OF_OUTPUT;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceNodeIdInOperator;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceStreamNodeId;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Framework to run sql script in tests. */
public abstract class AbstractSqlGatewayStatementITCaseBase extends AbstractTestBaseJUnit4 {

    private static final String RESOURCE_DIR = "sql/";
    private static final Pattern PATTERN = Pattern.compile(".*\\.q$");

    protected final Map<String, String> replaceVars = new HashMap<>();

    @Parameter public TestParameters parameters;

    @Parameters(name = "parameters={0}")
    public static List<TestParameters> parameters() throws Exception {
        return listFlinkSqlTests().stream().map(TestParameters::new).collect(Collectors.toList());
    }

    @BeforeEach
    public void before(@TempDir Path temporaryFolder) throws Exception {
        // initialize new folders for every test, so the vars can be reused by every SQL scripts
        replaceVars.put(
                "$VAR_STREAMING_PATH",
                Files.createDirectory(temporaryFolder.resolve("streaming")).toFile().getPath());
        replaceVars.put(
                "$VAR_STREAMING_PATH2",
                Files.createDirectory(temporaryFolder.resolve("streaming2")).toFile().getPath());
        replaceVars.put(
                "$VAR_STREAMING_PATH3",
                Files.createDirectory(temporaryFolder.resolve("streaming3")).toFile().getPath());
        replaceVars.put(
                "$VAR_BATCH_PATH",
                Files.createDirectory(temporaryFolder.resolve("batch")).toFile().getPath());
        replaceVars.put(
                "$VAR_BATCH_CTAS_PATH",
                Files.createDirectory(temporaryFolder.resolve("batch_ctas")).toFile().getPath());
        replaceVars.put(
                "$VAR_STREAMING_PLAN_PATH",
                Files.createDirectory(temporaryFolder.resolve("streaming_compiled_plan"))
                        .toFile()
                        .getPath());
        replaceVars.put(
                "$VAR_STREAMING_PLAN_RELATIVE_PATH",
                new File(".")
                        .getCanonicalFile()
                        .toPath()
                        .relativize(Paths.get(replaceVars.get("$VAR_STREAMING_PLAN_PATH")))
                        .toString());
    }

    @TestTemplate
    public void testFlinkSqlStatements() throws Exception {
        prepareEnvironment();
        if (skip()) {
            return;
        }
        runTest(parameters.getSqlPath());
    }

    protected void runTest(String sqlPath) throws Exception {
        String in = getInputFromPath(sqlPath);
        List<TestSqlStatement> testSqlStatements = SqlScriptReader.parseSqlScript(in);

        assertThat(String.join("", runStatements(testSqlStatements))).isEqualTo(in);
    }

    // -------------------------------------------------------------------------------------------
    // Utility
    // -------------------------------------------------------------------------------------------

    protected abstract boolean isStreaming() throws Exception;

    protected abstract String runStatements(List<TestSqlStatement> statements) throws Exception;

    protected boolean skip() {
        return false;
    }

    protected void prepareEnvironment() throws Exception {}

    /** Parameters of the test spec. */
    protected static class TestParameters {

        protected final String sqlPath;

        public TestParameters(String sqlPath) {
            this.sqlPath = sqlPath;
        }

        public String getSqlPath() {
            return sqlPath;
        }

        @Override
        public String toString() {
            return "TestParameters{" + "sqlPath='" + sqlPath + '\'' + '}';
        }
    }

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

    protected static List<String> listFlinkSqlTests() throws Exception {
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
            return files;
        } else {
            return listTestSpecInTheSameModule(RESOURCE_DIR);
        }
    }

    protected static List<String> listTestSpecInTheSameModule(String resourceDir) throws Exception {
        return IOUtils.readLines(
                        checkNotNull(
                                AbstractSqlGatewayStatementITCase.class
                                        .getClassLoader()
                                        .getResourceAsStream(resourceDir)),
                        StandardCharsets.UTF_8)
                .stream()
                .map(name -> Paths.get(resourceDir, name).toString())
                .collect(Collectors.toList());
    }

    protected String toString(
            StatementType type,
            ResolvedSchema schema,
            RowDataToStringConverter converter,
            Iterator<RowData> iterator)
            throws Exception {
        if (type.equals(StatementType.EXPLAIN) || type.equals(StatementType.SHOW_CREATE)) {
            StringBuilder sb = new StringBuilder();
            while (iterator.hasNext()) {
                sb.append(iterator.next().getString(0).toString());
            }
            return Tag.OK.addTag(
                    replaceStreamNodeId(replaceNodeIdInOperator(sb.toString())) + "\n");
        } else if (schema.getColumn(0)
                .map(col -> col.getName().equals(Constants.JOB_ID))
                .orElse(false)) {
            // ignore output of the job id
            return Tag.INFO.addTag("Job ID:\n");
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

    protected String removeRowNumber(String s) {
        return s.replaceAll("at line \\d+", "at line 0");
    }
}
